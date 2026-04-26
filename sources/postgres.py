"""
PostgreSQL CDC source — reads changes via logical replication (pgoutput plugin).
Requires Postgres 10+ with wal_level = logical.

Setup (run once as superuser):
    CREATE PUBLICATION dremio_cdc FOR ALL TABLES;
    -- The replication slot is created automatically on first connect.
"""
from __future__ import annotations

import logging
import queue
import struct
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

# Postgres epoch starts 2000-01-01
_PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

# OID → normalised type (subset; falls back to VARCHAR)
_OID_TYPES = {
    16:   "boolean",
    20:   "bigint",
    21:   "smallint",
    23:   "integer",
    25:   "text",
    700:  "float",
    701:  "double",
    1043: "varchar",
    1082: "date",
    1083: "time",
    1114: "timestamp",
    1184: "timestamp",
    1700: "numeric",
    2950: "uuid",
    3802: "jsonb",
    114:  "json",
}


def _oid_to_type(oid: int) -> str:
    return _OID_TYPES.get(oid, "varchar")


class PostgresSource(CDCSource):
    """
    Connects to Postgres using psycopg2 logical replication with the pgoutput plugin.
    Parses binary pgoutput messages to emit ChangeEvent objects.
    """

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._snap_conn = None
        self._conn_cfg: Dict = {}
        self._relations: Dict[int, Dict] = {}   # relation_id → {name, schema, columns}
        conn = cfg.get("connection", cfg)
        self._slot = conn.get("replication_slot", cfg.get("replication_slot", "dremio_cdc"))
        self._publication = conn.get("publication", cfg.get("publication", "dremio_cdc"))
        # Shared replication stream — one connection fans out to per-table queues
        self._table_queues: Dict[str, queue.Queue] = {}
        self._stream_started = False
        self._stream_lock = threading.Lock()

    def connect(self):
        try:
            import psycopg2
            import psycopg2.extras
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        except ImportError:
            raise SystemExit("psycopg2 required: pip install psycopg2-binary")

        conn_cfg = self.cfg.get("connection", self.cfg)  # accept flat or nested config
        self._conn_cfg = conn_cfg
        missing = [k for k in ("host", "database", "user") if not conn_cfg.get(k)]
        if missing:
            raise ValueError(f"Missing required connection fields: {', '.join(missing)}")
        self._snap_conn = psycopg2.connect(
            host=conn_cfg.get("host", "localhost"),
            port=int(conn_cfg.get("port", 5432)),
            dbname=conn_cfg["database"],
            user=conn_cfg["user"],
            password=conn_cfg.get("password", ""),
        )
        self._snap_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Ensure publication exists
        with self._snap_conn.cursor() as cur:
            cur.execute("SELECT pubname FROM pg_publication WHERE pubname = %s", (self._publication,))
            if not cur.fetchone():
                cur.execute(f"CREATE PUBLICATION {self._publication} FOR ALL TABLES")
                logger.info("Created publication %s", self._publication)

        # Ensure replication slot exists
        with self._snap_conn.cursor() as cur:
            cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s", (self._slot,))
            if not cur.fetchone():
                cur.execute(f"SELECT pg_create_logical_replication_slot('{self._slot}', 'pgoutput')")
                logger.info("Created replication slot %s", self._slot)

        logger.info("Connected to Postgres %s", conn_cfg.get("host"))

    def get_schema(self, table: str) -> List[ColumnSchema]:
        schema, tbl = (table.split(".", 1) + ["public"])[:2] if "." in table else ("public", table)
        with self._snap_conn.cursor() as cur:
            cur.execute("""
                SELECT a.attname, a.atttypid, a.attnotnull,
                       (SELECT count(*) FROM pg_index i
                        JOIN pg_attribute ia ON ia.attrelid=i.indrelid AND ia.attnum=ANY(i.indkey)
                        WHERE i.indrelid=a.attrelid AND i.indisprimary AND ia.attname=a.attname) AS is_pk
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname=%s AND c.relname=%s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (schema, tbl))
            return [
                ColumnSchema(
                    name=row[0],
                    data_type=_oid_to_type(row[1]),
                    nullable=not row[2],
                    primary_key=bool(row[3]),
                )
                for row in cur.fetchall()
            ]

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        # Use a regular cursor with fetchmany — named (server-side) cursors require
        # a transaction, but _snap_conn is in autocommit mode.
        with self._snap_conn.cursor() as cur:
            cur.execute(f'SELECT {",".join(col_names)} FROM {table}')
            while True:
                rows = cur.fetchmany(2000)
                if not rows:
                    break
                for row in rows:
                    yield ChangeEvent(
                        op=Operation.SNAPSHOT,
                        source_name=self.name,
                        source_table=table,
                        before=None,
                        after=dict(zip(col_names, row)),
                        schema=schema,
                        timestamp=datetime.now(timezone.utc),
                        offset=None,
                    )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        with self._snap_conn.cursor() as cur:
            if start_after is None:
                cur.execute(
                    f'SELECT {",".join(col_names)} FROM {table}'
                    f' ORDER BY {cursor_col} LIMIT %s',
                    (chunk_size,),
                )
            else:
                cur.execute(
                    f'SELECT {",".join(col_names)} FROM {table}'
                    f' WHERE {cursor_col} > %s ORDER BY {cursor_col} LIMIT %s',
                    (start_after, chunk_size),
                )
            rows = cur.fetchall()
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name,
                source_table=table,
                before=None,
                after=dict(zip(col_names, row)),
                schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=None,
            )

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        # A Postgres replication slot allows only ONE active connection at a time.
        # We maintain a single shared replication thread for all tables in this source
        # and fan events out to per-table queues.
        q: queue.Queue = queue.Queue(maxsize=500)
        self._table_queues[table] = q

        with self._stream_lock:
            if not self._stream_started:
                self._stream_started = True
                # "snap:done" / "snap:..." means snapshot finished — start from slot beginning
                clean_offset = offset if (offset and not str(offset).startswith("snap:")) else None
                start_lsn = clean_offset or "0/0"
                threading.Thread(
                    target=self._run_replication_stream,
                    args=(start_lsn,),
                    daemon=True,
                    name=f"pg-repl/{self.name}",
                ).start()

        while True:
            try:
                item = q.get(timeout=1)
            except queue.Empty:
                yield None   # heartbeat — lets the worker check batch timeout
                continue
            if item is None:   # sentinel — stream ended
                break
            yield item

    def _run_replication_stream(self, start_lsn: str):
        """Single background thread that owns the replication connection and fans
        out events to each registered table queue."""
        import psycopg2
        import psycopg2.extras

        conn_cfg = self._conn_cfg
        repl_conn = psycopg2.connect(
            host=conn_cfg.get("host", "localhost"),
            port=conn_cfg.get("port", 5432),
            dbname=conn_cfg["database"],
            user=conn_cfg["user"],
            password=conn_cfg.get("password", ""),
            connection_factory=psycopg2.extras.LogicalReplicationConnection,
        )
        cur = repl_conn.cursor()
        cur.start_replication(
            slot_name=self._slot,
            decode=False,
            start_lsn=start_lsn,
            options={"proto_version": "1", "publication_names": self._publication},
        )

        def _consume(msg):
            events = self._parse_message(msg.payload)
            for ev in events:
                if not ev:
                    continue
                q = self._table_queues.get(ev.source_table)
                if q is None:
                    # Try short name match (e.g. "public.customers" → "customers")
                    for tbl_key, tbl_q in self._table_queues.items():
                        if ev.source_table.endswith(f".{tbl_key}") or tbl_key.endswith(f".{ev.source_table}"):
                            tbl_q.put(ev)
                else:
                    q.put(ev)
            msg.cursor.send_feedback(flush_lsn=msg.data_start)

        try:
            cur.consume_stream(_consume)
        except Exception as exc:
            logger.error("Replication stream error: %s", exc)
        finally:
            repl_conn.close()
            for q in self._table_queues.values():
                q.put(None)

    # ── pgoutput binary parser ────────────────────────────────────────────────

    def _parse_message(self, data: bytes) -> List[Optional[ChangeEvent]]:
        if not data:
            return []
        msg_type = chr(data[0])

        if msg_type == 'R':
            self._parse_relation(data[1:])
            return []
        if msg_type == 'I':
            return [self._parse_insert(data[1:])]
        if msg_type == 'U':
            return [self._parse_update(data[1:])]
        if msg_type == 'D':
            return [self._parse_delete(data[1:])]
        return []

    def _parse_relation(self, data: bytes):
        pos = 0
        rel_id = struct.unpack_from(">I", data, pos)[0]; pos += 4
        ns_end = data.index(b'\x00', pos)
        namespace = data[pos:ns_end].decode(); pos = ns_end + 1
        name_end = data.index(b'\x00', pos)
        name = data[pos:name_end].decode(); pos = name_end + 1
        pos += 1  # replica identity
        num_cols = struct.unpack_from(">H", data, pos)[0]; pos += 2
        columns = []
        for _ in range(num_cols):
            flags = data[pos]; pos += 1
            col_end = data.index(b'\x00', pos)
            col_name = data[pos:col_end].decode(); pos = col_end + 1
            type_oid = struct.unpack_from(">I", data, pos)[0]; pos += 4
            pos += 4  # atttypmod
            columns.append(ColumnSchema(
                name=col_name,
                data_type=_oid_to_type(type_oid),
                primary_key=bool(flags & 1),
            ))
        self._relations[rel_id] = {"namespace": namespace, "name": name, "columns": columns}

    def _decode_tuple(self, data: bytes, pos: int, columns: List[ColumnSchema]):
        num_cols = struct.unpack_from(">H", data, pos)[0]; pos += 2
        row = {}
        for i in range(min(num_cols, len(columns))):
            kind = chr(data[pos]); pos += 1
            if kind == 'n':
                row[columns[i].name] = None
            elif kind == 'u':
                row[columns[i].name] = None  # unchanged toast
            elif kind == 't':
                length = struct.unpack_from(">I", data, pos)[0]; pos += 4
                val = data[pos:pos + length].decode("utf-8", errors="replace"); pos += length
                row[columns[i].name] = val
        return row, pos

    def _rel_event(self, data: bytes):
        rel_id = struct.unpack_from(">I", data, 0)[0]
        rel = self._relations.get(rel_id, {})
        table = f"{rel.get('namespace', 'public')}.{rel.get('name', 'unknown')}"
        cols = rel.get("columns", [])
        return rel_id, table, cols, 4

    def _parse_insert(self, data: bytes) -> Optional[ChangeEvent]:
        rel_id, table, cols, pos = self._rel_event(data)
        pos += 1  # 'N'
        after, _ = self._decode_tuple(data, pos, cols)
        return ChangeEvent(
            op=Operation.INSERT, source_name=self.name, source_table=table,
            before=None, after=after, schema=cols,
            timestamp=datetime.now(timezone.utc), offset=None,
        )

    def _parse_update(self, data: bytes) -> Optional[ChangeEvent]:
        rel_id, table, cols, pos = self._rel_event(data)
        kind = chr(data[pos]); pos += 1
        before = None
        if kind in ('K', 'O'):
            before, pos = self._decode_tuple(data, pos, cols)
            pos += 1  # 'N'
        after, _ = self._decode_tuple(data, pos, cols)
        return ChangeEvent(
            op=Operation.UPDATE, source_name=self.name, source_table=table,
            before=before, after=after, schema=cols,
            timestamp=datetime.now(timezone.utc), offset=None,
        )

    def _parse_delete(self, data: bytes) -> Optional[ChangeEvent]:
        rel_id, table, cols, pos = self._rel_event(data)
        pos += 1  # 'K' or 'O'
        before, _ = self._decode_tuple(data, pos, cols)
        return ChangeEvent(
            op=Operation.DELETE, source_name=self.name, source_table=table,
            before=before, after=None, schema=cols,
            timestamp=datetime.now(timezone.utc), offset=None,
        )

    def close(self):
        if self._snap_conn: self._snap_conn.close()
