"""
MySQL CDC source — reads changes via binlog using python-mysql-replication.
Requires MySQL with binlog_format = ROW and binlog_row_image = FULL.

Setup (run once):
    SET GLOBAL binlog_format = 'ROW';
    SET GLOBAL binlog_row_image = 'FULL';
    -- Grant replication privileges:
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_MYSQL_TYPES = {
    "tinyint":   "smallint",
    "smallint":  "smallint",
    "mediumint": "integer",
    "int":       "integer",
    "bigint":    "bigint",
    "float":     "float",
    "double":    "double",
    "decimal":   "numeric",
    "varchar":   "varchar",
    "char":      "varchar",
    "text":      "text",
    "tinytext":  "text",
    "mediumtext":"text",
    "longtext":  "text",
    "blob":      "bytea",
    "datetime":  "timestamp",
    "timestamp": "timestamp",
    "date":      "date",
    "time":      "time",
    "boolean":   "boolean",
    "bool":      "boolean",
    "json":      "json",
    "enum":      "varchar",
    "set":       "varchar",
}


class MySQLSource(CDCSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._stream = None
        self._snap_conn = None

    def connect(self):
        try:
            import pymysql
        except ImportError:
            raise SystemExit("pymysql required: pip install pymysql")

        conn_cfg = self.cfg["connection"]
        self._snap_conn = __import__("pymysql").connect(
            host=conn_cfg.get("host", "localhost"),
            port=conn_cfg.get("port", 3306),
            user=conn_cfg["user"],
            password=conn_cfg.get("password", ""),
            database=conn_cfg["database"],
            cursorclass=__import__("pymysql.cursors", fromlist=["DictCursor"]).DictCursor,
        )
        logger.info("Connected to MySQL %s", conn_cfg.get("host"))

    def get_schema(self, table: str) -> List[ColumnSchema]:
        db, tbl = (table.split(".", 1)) if "." in table else (self.cfg["connection"]["database"], table)
        with self._snap_conn.cursor() as cur:
            cur.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
            """, (db, tbl))
            return [
                ColumnSchema(
                    name=row["COLUMN_NAME"],
                    data_type=_MYSQL_TYPES.get(row["DATA_TYPE"].lower(), "varchar"),
                    nullable=(row["IS_NULLABLE"] == "YES"),
                    primary_key=(row["COLUMN_KEY"] == "PRI"),
                )
                for row in cur.fetchall()
            ]

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        _, tbl = (table.split(".", 1)) if "." in table else (None, table)
        with self._snap_conn.cursor() as cur:
            cur.execute(f"SELECT {','.join(col_names)} FROM `{tbl}`")
            for row in cur:
                yield ChangeEvent(
                    op=Operation.SNAPSHOT,
                    source_name=self.name,
                    source_table=table,
                    before=None,
                    after=dict(zip(col_names, row.values())),
                    schema=schema,
                    timestamp=datetime.now(timezone.utc),
                    offset=None,
                )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        _, tbl = (table.split(".", 1)) if "." in table else (None, table)
        with self._snap_conn.cursor() as cur:
            if start_after is None:
                cur.execute(
                    f"SELECT {','.join(col_names)} FROM `{tbl}`"
                    f" ORDER BY `{cursor_col}` LIMIT %s",
                    (chunk_size,),
                )
            else:
                cur.execute(
                    f"SELECT {','.join(col_names)} FROM `{tbl}`"
                    f" WHERE `{cursor_col}` > %s ORDER BY `{cursor_col}` LIMIT %s",
                    (start_after, chunk_size),
                )
            for row in cur:
                yield ChangeEvent(
                    op=Operation.SNAPSHOT,
                    source_name=self.name,
                    source_table=table,
                    before=None,
                    after=dict(zip(col_names, row.values())),
                    schema=schema,
                    timestamp=datetime.now(timezone.utc),
                    offset=None,
                )

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        try:
            from pymysqlreplication import BinLogStreamReader
            from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
        except ImportError:
            raise SystemExit("mysql-replication required: pip install mysql-replication")

        conn_cfg = self.cfg["connection"]
        # "snap:done" / "snap:..." means snapshot finished — stream from current binlog position
        clean_offset = offset if (offset and not str(offset).startswith("snap:")) else None
        log_file = (clean_offset or {}).get("log_file") if isinstance(clean_offset, dict) else None
        log_pos  = (clean_offset or {}).get("log_pos") if isinstance(clean_offset, dict) else None

        # If no saved offset, get current binlog position — avoids calling SHOW MASTER STATUS
        # inside the library, which was removed in MySQL 8.4 (replaced by SHOW BINARY LOG STATUS).
        if log_file is None or log_pos is None:
            with self._snap_conn.cursor() as _cur:
                try:
                    _cur.execute("SHOW BINARY LOG STATUS")
                except Exception:
                    _cur.execute("SHOW MASTER STATUS")
                row = _cur.fetchone()
                if row:
                    log_file = row.get("File") or row.get("Filename") or list(row.values())[0]
                    log_pos  = row.get("Position") or list(row.values())[1]
                else:
                    log_file, log_pos = None, 4

        self._stream = BinLogStreamReader(
            connection_settings={
                "host":   conn_cfg.get("host", "localhost"),
                "port":   conn_cfg.get("port", 3306),
                "user":   conn_cfg["user"],
                "passwd": conn_cfg.get("password", ""),
            },
            server_id=conn_cfg.get("server_id", 1001),
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            log_file=log_file,
            log_pos=log_pos,
            resume_stream=True,
            blocking=True,
        )

        db_filter = conn_cfg["database"]
        _, tbl_filter = (table.split(".", 1)) if "." in table else (None, table)
        _schema_cache: Dict[str, Any] = {}

        def _remap(values: dict, col_names: List[str]) -> dict:
            # pymysqlreplication v1.0 may return UNKNOWN_COL* keys; remap by position
            vals = list(values.values())
            if len(vals) == len(col_names):
                return dict(zip(col_names, vals))
            return values

        for event in self._stream:
            if event.schema != db_filter:
                continue
            if tbl_filter and event.table != tbl_filter:
                continue

            full_table = f"{event.schema}.{event.table}"
            if full_table not in _schema_cache:
                _schema_cache[full_table] = self.get_schema(full_table)
            schema = _schema_cache[full_table]
            col_names = [c.name for c in schema]

            new_offset = {"log_file": self._stream.log_file, "log_pos": self._stream.log_pos}

            for row in event.rows:
                if isinstance(event, WriteRowsEvent):
                    yield ChangeEvent(
                        op=Operation.INSERT, source_name=self.name,
                        source_table=full_table, before=None,
                        after=_remap(row["values"], col_names), schema=schema,
                        timestamp=datetime.now(timezone.utc), offset=new_offset,
                    )
                elif isinstance(event, UpdateRowsEvent):
                    yield ChangeEvent(
                        op=Operation.UPDATE, source_name=self.name,
                        source_table=full_table,
                        before=_remap(row["before_values"], col_names),
                        after=_remap(row["after_values"], col_names), schema=schema,
                        timestamp=datetime.now(timezone.utc), offset=new_offset,
                    )
                elif isinstance(event, DeleteRowsEvent):
                    yield ChangeEvent(
                        op=Operation.DELETE, source_name=self.name,
                        source_table=full_table, before=_remap(row["values"], col_names),
                        after=None, schema=schema,
                        timestamp=datetime.now(timezone.utc), offset=new_offset,
                    )

    def close(self):
        if self._stream:    self._stream.close()
        if self._snap_conn: self._snap_conn.close()
