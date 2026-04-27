"""
SQL Server CDC source — uses SQL Server's built-in CDC feature.

Setup (run once as sysadmin):
    EXEC sys.sp_cdc_enable_db;
    EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name   = 'customers',
        @role_name     = NULL;

Drivers (in order of preference):
    pip install pymssql          # pure-Python, no system driver needed (recommended)
    pip install pyodbc           # requires Microsoft ODBC Driver 17/18 + unixodbc
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_OP_MAP = {
    1: Operation.DELETE,
    2: Operation.INSERT,
    4: Operation.UPDATE,
    # 3 = before-image of UPDATE — skipped, we use the after-image (4)
}

# SQL Server → normalised type
_TYPE_MAP = {
    "int":              "integer",
    "bigint":           "bigint",
    "smallint":         "smallint",
    "tinyint":          "smallint",
    "bit":              "boolean",
    "decimal":          "numeric",
    "numeric":          "numeric",
    "float":            "double",
    "real":             "float",
    "money":            "numeric",
    "smallmoney":       "numeric",
    "char":             "varchar",
    "nchar":            "varchar",
    "varchar":          "varchar",
    "nvarchar":         "varchar",
    "text":             "text",
    "ntext":            "text",
    "datetime":         "timestamp",
    "datetime2":        "timestamp",
    "smalldatetime":    "timestamp",
    "date":             "date",
    "time":             "time",
    "datetimeoffset":   "timestamp",
    "uniqueidentifier": "varchar",
    "binary":           "bytea",
    "varbinary":        "bytea",
    "xml":              "varchar",
}


def _connect_pymssql(cfg: Dict) -> Any:
    import pymssql
    return pymssql.connect(
        server=cfg.get("host", "localhost"),
        port=int(cfg.get("port", 1433)),
        user=cfg["user"],
        password=cfg.get("password", ""),
        database=cfg["database"],
        as_dict=True,
    )


def _connect_pyodbc(cfg: Dict) -> Any:
    import pyodbc
    driver = cfg.get("driver", "ODBC Driver 17 for SQL Server")
    server = cfg.get("host", "localhost")
    port   = int(cfg.get("port", 1433))
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{port};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['user']};"
        f"PWD={cfg.get('password', '')};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


class SQLServerSource(CDCSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._conn_cfg: Dict = {}
        self._use_dict = True   # pymssql returns dicts; pyodbc returns tuples

    def connect(self):
        conn_cfg = self.cfg.get("connection", self.cfg)
        missing = [k for k in ("host", "database", "user") if not conn_cfg.get(k)]
        if missing:
            raise ValueError(f"Missing SQL Server connection fields: {', '.join(missing)}")
        self._conn_cfg = conn_cfg

        # Probe once to pick driver and validate credentials
        try:
            conn = _connect_pymssql(conn_cfg)
            conn.close()
            self._use_dict = True
            logger.info("Connected to SQL Server %s (pymssql)", conn_cfg.get("host"))
        except ImportError:
            conn = _connect_pyodbc(conn_cfg)
            conn.close()
            self._use_dict = False
            logger.info("Connected to SQL Server %s (pyodbc)", conn_cfg.get("host"))

    def _new_conn(self):
        """Create a fresh, dedicated connection for one operation."""
        if self._use_dict:
            return _connect_pymssql(self._conn_cfg)
        return _connect_pyodbc(self._conn_cfg)

    def _row_to_dict(self, row, col_names: List[str]) -> Dict:
        if self._use_dict:
            return dict(row)
        return dict(zip(col_names, row))

    def get_schema(self, table: str) -> List[ColumnSchema]:
        schema_name, table_name = _split(table)
        conn = self._new_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT c.COLUMN_NAME, c.DATA_TYPE, "
                "  CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk "
                "FROM INFORMATION_SCHEMA.COLUMNS c "
                "LEFT JOIN ("
                "  SELECT cu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
                "  JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu "
                "    ON tc.CONSTRAINT_NAME = cu.CONSTRAINT_NAME "
                "  WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY' AND tc.TABLE_SCHEMA=%s AND tc.TABLE_NAME=%s"
                ") pk ON c.COLUMN_NAME = pk.COLUMN_NAME "
                "WHERE c.TABLE_SCHEMA=%s AND c.TABLE_NAME=%s "
                "ORDER BY c.ORDINAL_POSITION",
                (schema_name, table_name, schema_name, table_name),
            )
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        result = []
        for row in rows:
            if self._use_dict:
                name, dtype, is_pk = row["COLUMN_NAME"], row["DATA_TYPE"], row["is_pk"]
            else:
                name, dtype, is_pk = row[0], row[1], row[2]
            result.append(ColumnSchema(
                name=name,
                data_type=_TYPE_MAP.get(dtype.lower(), "varchar"),
                primary_key=bool(is_pk),
            ))
        return result

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        schema_name, table_name = _split(table)
        conn = self._new_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT {_cols(col_names)} FROM [{schema_name}].[{table_name}]")
            while True:
                rows = cur.fetchmany(2000)
                if not rows:
                    break
                for row in rows:
                    yield ChangeEvent(
                        op=Operation.SNAPSHOT,
                        source_name=self.name,
                        source_table=table,
                        schema=schema,
                        before=None,
                        after=self._row_to_dict(row, col_names),
                        timestamp=datetime.now(timezone.utc),
                        offset=None,
                    )
            cur.close()
        finally:
            conn.close()

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        schema_name, table_name = _split(table)
        conn = self._new_conn()
        try:
            cur = conn.cursor()
            if start_after is None:
                cur.execute(
                    f"SELECT TOP {chunk_size} {_cols(col_names)} "
                    f"FROM [{schema_name}].[{table_name}] ORDER BY [{cursor_col}]"
                )
            else:
                cur.execute(
                    f"SELECT TOP {chunk_size} {_cols(col_names)} "
                    f"FROM [{schema_name}].[{table_name}] "
                    f"WHERE [{cursor_col}] > %s ORDER BY [{cursor_col}]",
                    (start_after,),
                )
            for row in cur.fetchall():
                yield ChangeEvent(
                    op=Operation.SNAPSHOT,
                    source_name=self.name,
                    source_table=table,
                    schema=schema,
                    before=None,
                    after=self._row_to_dict(row, col_names),
                    timestamp=datetime.now(timezone.utc),
                    offset=None,
                )
            cur.close()
        finally:
            conn.close()

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        capture = _capture_instance(table)
        poll_interval = int(self._conn_cfg.get("poll_interval", 5))

        raw_offset = offset if (offset and not str(offset).startswith("snap:")) else None
        # LSNs are stored as hex strings; convert back to bytes for queries
        if isinstance(raw_offset, str):
            try:
                current_lsn: Optional[bytes] = bytes.fromhex(raw_offset)
            except ValueError:
                current_lsn = None
        else:
            current_lsn = raw_offset

        if current_lsn is None:
            conn = self._new_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT sys.fn_cdc_get_min_lsn(%s) AS v", (capture,))
                row = cur.fetchone()
                current_lsn = (row["v"] if self._use_dict else row[0]) if row else None
                cur.close()
            finally:
                conn.close()


        while True:
            # Each poll cycle gets its own connection to avoid session conflicts
            conn = self._new_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT sys.fn_cdc_get_max_lsn() AS v")
                row = cur.fetchone()
                max_lsn = (row["v"] if self._use_dict else row[0]) if row else None
                cur.close()

                if max_lsn and current_lsn and current_lsn < max_lsn:
                    cur = conn.cursor()
                    try:
                        cur.execute(
                            f"SELECT __$operation, __$start_lsn, {_cols(col_names)} "
                            f"FROM cdc.fn_cdc_get_all_changes_{capture}(%s, %s, N'all') "
                            f"WHERE __$operation != 3 ORDER BY __$start_lsn",
                            (current_lsn, max_lsn),
                        )
                        for row in cur.fetchall():
                            if self._use_dict:
                                op_code = row["__$operation"]
                                lsn     = row["__$start_lsn"]
                                values  = {k: v for k, v in row.items()
                                           if not k.startswith("__$")}
                            else:
                                op_code = row[0]
                                lsn     = row[1]
                                values  = dict(zip(col_names, row[2:]))

                            op = _OP_MAP.get(op_code)
                            if op is None:
                                continue
                            # Hex-encode bytes LSN so it's JSON-serializable
                            lsn_str = lsn.hex() if isinstance(lsn, (bytes, bytearray)) else lsn
                            yield ChangeEvent(
                                op=op,
                                source_name=self.name,
                                source_table=table,
                                schema=schema,
                                before=values if op == Operation.DELETE else None,
                                after=values if op != Operation.DELETE else None,
                                timestamp=datetime.now(timezone.utc),
                                offset=lsn_str,
                            )
                        current_lsn = max_lsn  # bytes — used only for comparisons within this loop
                    except Exception as exc:
                        logger.error("CDC query error for %s: %s", table, exc)
                    finally:
                        cur.close()
            except Exception as exc:
                logger.warning("[%s] Poll error: %s", table, exc)
            finally:
                conn.close()

            time.sleep(poll_interval)

    def close(self):
        pass  # No persistent connection to close


def _split(table: str):
    parts = table.split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("dbo", parts[0])


def _capture_instance(table: str) -> str:
    schema_name, table_name = _split(table)
    return f"{schema_name}_{table_name}"


def _cols(names: List[str]) -> str:
    return ", ".join(f"[{n}]" for n in names)
