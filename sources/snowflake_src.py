"""
Snowflake source — uses Snowflake STREAM objects to capture DML changes.

The connector creates a STREAM on each watched table automatically.
Streams are consumed inside a transaction so Snowflake advances the offset atomically.

Setup (grant required privileges):
    GRANT USAGE  ON WAREHOUSE <wh>    TO ROLE <role>;
    GRANT USAGE  ON DATABASE  <db>    TO ROLE <role>;
    GRANT USAGE  ON SCHEMA    <schema> TO ROLE <role>;
    GRANT SELECT, REFERENCES ON TABLE <table> TO ROLE <role>;
    GRANT CREATE STREAM ON SCHEMA <schema> TO ROLE <role>;

Requires: pip install snowflake-connector-python
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)


class SnowflakeSource(CDCSource):

    def connect(self):
        try:
            import snowflake.connector
        except ImportError:
            raise SystemExit("snowflake-connector-python required: pip install snowflake-connector-python")

        conn_cfg = self.cfg.get("connection", self.cfg)
        missing = [k for k in ("account", "user", "database") if not conn_cfg.get(k)]
        if missing:
            raise ValueError(f"Missing required connection fields: {', '.join(missing)}")

        self._conn = snowflake.connector.connect(
            account=conn_cfg["account"],
            user=conn_cfg["user"],
            password=conn_cfg.get("password", ""),
            database=conn_cfg["database"],
            schema=conn_cfg.get("schema", "PUBLIC"),
            warehouse=conn_cfg.get("warehouse"),
            role=conn_cfg.get("role"),
        )
        self._conn_cfg = conn_cfg
        logger.info("Connected to Snowflake account=%s db=%s", conn_cfg["account"], conn_cfg["database"])

    def get_schema(self, table: str) -> List[ColumnSchema]:
        schema_name, table_name = self._split(table)
        cur = self._conn.cursor()
        cur.execute(f'SHOW COLUMNS IN TABLE "{schema_name}"."{table_name}"')
        cols = []
        for row in cur.fetchall():
            col_name = row[2]
            try:
                dt = json.loads(row[3]).get("type", "TEXT")
            except Exception:
                dt = "TEXT"
            cols.append(ColumnSchema(col_name, dt))
        cur.close()
        return cols

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        schema_name, table_name = self._split(table)
        pks = self._get_pks(table)

        cur = self._conn.cursor()
        cur.execute(f'SELECT {_cols(col_names)} FROM "{schema_name}"."{table_name}"')
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
                    primary_keys=pks,
                    after=dict(zip(col_names, row)),
                    before=None,
                )
        cur.close()

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        pks = self._get_pks(table)
        schema_name, _ = self._split(table)
        stream_fqn = self._ensure_stream(table)
        poll_interval = int(self._conn_cfg.get("poll_interval", 30))

        while True:
            # Check whether the stream has data before starting a transaction
            cur = self._conn.cursor()
            cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{stream_fqn}')")
            has_data = cur.fetchone()[0]
            cur.close()

            if has_data:
                txn = self._conn.cursor()
                try:
                    txn.execute("BEGIN")
                    data_cur = self._conn.cursor()
                    data_cur.execute(
                        f'SELECT {_cols(col_names)}, '
                        f'METADATA$ACTION, METADATA$ISUPDATE '
                        f'FROM {stream_fqn} '
                        f'ORDER BY METADATA$ROW_ID'
                    )
                    rows = data_cur.fetchall()
                    data_cur.close()
                    txn.execute("COMMIT")

                    for row in rows:
                        values = dict(zip(col_names, row[:len(col_names)]))
                        action    = row[-2]   # "INSERT" or "DELETE"
                        is_update = row[-1]   # True/False

                        if is_update:
                            op = Operation.UPDATE
                        elif action == "INSERT":
                            op = Operation.INSERT
                        else:
                            op = Operation.DELETE

                        yield ChangeEvent(
                            op=op,
                            source_name=self.name,
                            source_table=table,
                            schema=schema,
                            primary_keys=pks,
                            after=values if op != Operation.DELETE else None,
                            before=values if op == Operation.DELETE else None,
                        )
                except Exception as exc:
                    try:
                        txn.execute("ROLLBACK")
                    except Exception:
                        pass
                    logger.error("Snowflake stream consume error for %s: %s", table, exc)
                finally:
                    txn.close()

            time.sleep(poll_interval)

    def close(self):
        if hasattr(self, "_conn") and self._conn:
            self._conn.close()

    def _split(self, table: str):
        parts = table.split(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return self._conn_cfg.get("schema", "PUBLIC"), parts[0]

    def _ensure_stream(self, table: str) -> str:
        """Create stream if not exists; return fully-qualified stream name."""
        schema_name, table_name = self._split(table)
        stream_name = f"dremio_cdc_{table_name}"
        fqn = f'"{schema_name}"."{stream_name}"'
        cur = self._conn.cursor()
        cur.execute(
            f'CREATE STREAM IF NOT EXISTS {fqn} '
            f'ON TABLE "{schema_name}"."{table_name}" '
            f'SHOW_INITIAL_ROWS = FALSE'
        )
        cur.close()
        logger.info("Ensured Snowflake stream %s", fqn)
        return fqn

    def _get_pks(self, table: str) -> List[str]:
        schema_name, table_name = self._split(table)
        cur = self._conn.cursor()
        cur.execute(f'SHOW PRIMARY KEYS IN TABLE "{schema_name}"."{table_name}"')
        # column_name is at index 4 in the SHOW PRIMARY KEYS result
        pks = [row[4] for row in cur.fetchall()]
        cur.close()
        return pks


def _cols(names: List[str]) -> str:
    return ", ".join(f'"{n}"' for n in names)
