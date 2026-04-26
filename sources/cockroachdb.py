"""
CockroachDB source — uses EXPERIMENTAL CHANGEFEED to stream DML changes.

CockroachDB is wire-compatible with PostgreSQL (psycopg2 works), but uses
its own changefeed mechanism rather than Postgres logical replication.

The EXPERIMENTAL CHANGEFEED statement returns an infinite result set;
each row is a (table, key_json, value_json) tuple where value_json contains
{"after": {...}, "updated": "<timestamp>"} or null for deletes.

Setup (grant required privileges):
    GRANT SELECT ON TABLE <table> TO <user>;
    GRANT CHANGEFEED ON TABLE <table> TO <user>;

Requires: pip install psycopg2-binary
"""
from __future__ import annotations

import json
import logging
import threading
import queue
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

import psycopg2
import psycopg2.extras

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)


class CockroachDBSource(CDCSource):

    def connect(self):
        conn_cfg = self.cfg.get("connection", self.cfg)
        missing = [k for k in ("host", "database", "user") if not conn_cfg.get(k)]
        if missing:
            raise ValueError(f"Missing required connection fields: {', '.join(missing)}")

        self._conn_cfg = conn_cfg
        # Regular connection for schema introspection and snapshots
        self._snap_conn = psycopg2.connect(
            host=conn_cfg.get("host", "localhost"),
            port=int(conn_cfg.get("port", 26257)),
            dbname=conn_cfg["database"],
            user=conn_cfg["user"],
            password=conn_cfg.get("password", ""),
            sslmode=conn_cfg.get("sslmode", "disable"),
        )
        self._snap_conn.autocommit = True
        self._table_queues: Dict[str, queue.Queue] = {}
        self._stream_thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()
        logger.info("Connected to CockroachDB %s db=%s", conn_cfg.get("host"), conn_cfg["database"])

    def get_schema(self, table: str) -> List[ColumnSchema]:
        schema_name, table_name = _split(table)
        with self._snap_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                (schema_name, table_name),
            )
            return [ColumnSchema(r[0], r[1]) for r in cur.fetchall()]

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        schema_name, table_name = _split(table)
        pks = self._get_pks(table)

        with self._snap_conn.cursor() as cur:
            col_list = ", ".join(f'"{c}"' for c in col_names)
            cur.execute(
                f'SELECT {col_list} '
                f'FROM "{schema_name}"."{table_name}" AS OF SYSTEM TIME follower_read_timestamp()'
            )
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

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        # Register a per-table queue
        q: queue.Queue = queue.Queue(maxsize=10_000)
        self._table_queues[table] = q

        # Start the shared changefeed thread (once for all tables on this source)
        with threading.Lock():
            if self._stream_thread is None or not self._stream_thread.is_alive():
                tables = list(self._table_queues.keys())
                self._stream_thread = threading.Thread(
                    target=self._run_changefeed,
                    args=(tables, offset),
                    daemon=True,
                )
                self._stream_thread.start()

        while not self._stop_flag.is_set():
            try:
                event = q.get(timeout=1.0)
                yield event
            except queue.Empty:
                continue

    def _run_changefeed(self, tables: List[str], cursor: Optional[str]):
        """Background thread: runs EXPERIMENTAL CHANGEFEED and fans events to per-table queues."""
        conn = psycopg2.connect(
            host=self._conn_cfg.get("host", "localhost"),
            port=int(self._conn_cfg.get("port", 26257)),
            dbname=self._conn_cfg["database"],
            user=self._conn_cfg["user"],
            password=self._conn_cfg.get("password", ""),
            sslmode=self._conn_cfg.get("sslmode", "disable"),
        )
        conn.autocommit = True

        table_list = ", ".join(
            f'"{_split(t)[0]}"."{_split(t)[1]}"' for t in tables
        )
        cursor_clause = f", cursor='{cursor}'" if cursor else ""
        sql = (
            f"EXPERIMENTAL CHANGEFEED FOR {table_list} "
            f"WITH updated, resolved='10s'{cursor_clause}"
        )
        logger.info("Starting CockroachDB changefeed: %s", sql)

        try:
            cur = conn.cursor()
            cur.execute(sql)
            for row in cur:
                if self._stop_flag.is_set():
                    break
                tbl_name, key_json, value_json = row[0], row[1], row[2]
                if value_json is None:
                    # Resolved timestamp heartbeat — ignore
                    continue
                self._dispatch(tbl_name, key_json, value_json)
        except Exception as exc:
            logger.error("CockroachDB changefeed error: %s", exc)
        finally:
            conn.close()

    def _dispatch(self, tbl_name: str, key_json: str, value_json: str):
        """Parse a changefeed row and put it on the right table queue."""
        try:
            value = json.loads(value_json) if isinstance(value_json, str) else value_json
            after = value.get("after")

            # Match tbl_name back to a fully-qualified table key
            target_key = next(
                (k for k in self._table_queues if _split(k)[1].lower() == tbl_name.lower()),
                None,
            )
            if target_key is None:
                return

            q = self._table_queues[target_key]
            schema = self.get_schema(target_key)
            pks = self._get_pks(target_key)

            if after is not None:
                op = Operation.INSERT  # CRDB doesn't distinguish INSERT vs UPDATE in basic mode
            else:
                # Delete: parse key JSON for PK values
                keys = json.loads(key_json) if isinstance(key_json, str) else key_json
                after = None
                before = dict(zip(pks, keys)) if isinstance(keys, list) else {}
                op = Operation.DELETE

            ts_str = value.get("updated", "")
            try:
                ts = datetime.fromisoformat(ts_str.rstrip("Z")).replace(tzinfo=timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)

            event = ChangeEvent(
                op=op,
                source_name=self.name,
                source_table=target_key,
                schema=schema,
                primary_keys=pks,
                after=after,
                before=before if op == Operation.DELETE else None,
                offset=ts_str,
            )
            q.put_nowait(event)
        except Exception as exc:
            logger.warning("Failed to parse changefeed row: %s", exc)

    def close(self):
        self._stop_flag.set()
        if hasattr(self, "_snap_conn") and self._snap_conn:
            self._snap_conn.close()

    def _get_pks(self, table: str) -> List[str]:
        schema_name, table_name = _split(table)
        with self._snap_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.key_column_usage "
                "WHERE table_schema=%s AND table_name=%s "
                "ORDER BY ordinal_position",
                (schema_name, table_name),
            )
            return [r[0] for r in cur.fetchall()]


def _split(table: str):
    parts = table.split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])
