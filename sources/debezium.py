"""
Debezium HTTP adapter source.

Debezium Server (https://debezium.io/documentation/reference/stable/operations/debezium-server.html)
is configured with the HTTP sink so it POSTs change events to this adapter's /events endpoint,
which feeds them into the CDC framework.

Supported Debezium source connectors (any database Debezium supports):
    Oracle       — requires LogMiner; see debezium/oracle.properties
    SQL Server   — requires SQL Server Agent + CDC enabled; see debezium/sqlserver.properties
    DB2          — requires ASN Capture; see debezium/db2.properties
    MySQL        — alternative to the native MySQL source
    PostgreSQL   — alternative to the native Postgres source

Quick start (Oracle example):
    # 1. Copy and edit the pre-built config:
    cp debezium/oracle.properties debezium/application.properties
    # Edit: database.hostname, database.user, database.password, database.dbname, database.pdb.name

    # 2. Start Debezium Server alongside the CDC daemon:
    docker run --rm --name debezium-server \\
        -p 8765:8765 \\
        -v $(pwd)/debezium/application.properties:/debezium/conf/application.properties \\
        debezium/server:2.7

    # 3. Configure the CDC daemon:
    sources:
      - name: oracle_prod
        type: debezium
        listen_port: 8765
        tables:
          - HR.EMPLOYEES
          - HR.DEPARTMENTS

Oracle prerequisites:
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
    -- The LogMiner user needs: CREATE SESSION, LOGMINING, SELECT on V$* views, etc.
    -- See debezium/oracle.properties for the full privilege list.
"""
from __future__ import annotations

import json
import logging
import queue
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

# Debezium op codes → internal Operation
_OP_MAP = {
    "c": Operation.INSERT,
    "u": Operation.UPDATE,
    "d": Operation.DELETE,
    "r": Operation.SNAPSHOT,
}

# Debezium field type → normalised type
_TYPE_MAP = {
    "string":         "varchar",
    "int8":           "smallint",
    "int16":          "smallint",
    "int32":          "integer",
    "int64":          "bigint",
    "float32":        "float",
    "float64":        "double",
    "boolean":        "boolean",
    "bytes":          "bytea",
    # Debezium logical types (passed as name field)
    "io.debezium.time.Date":                 "date",
    "io.debezium.time.Time":                 "time",
    "io.debezium.time.MicroTime":            "time",
    "io.debezium.time.Timestamp":            "timestamp",
    "io.debezium.time.MicroTimestamp":       "timestamp",
    "io.debezium.time.ZonedTimestamp":       "timestamp",
    "io.debezium.data.VariableScaleDecimal": "numeric",
    "org.apache.kafka.connect.data.Decimal": "numeric",
}

# DDL / heartbeat / schema-change op codes — not yielded as data events
_SKIP_OPS = {"$", "t"}   # "$" = truncate, "t" = schema-change tombstone

# Oracle-specific Debezium source fields to strip from payloads
_ORACLE_META_FIELDS = frozenset({
    "scn", "commit_scn", "lcr_position", "transaction_id",
    "ts_us", "snapshot", "sequence",
})


def _parse_schema(debezium_schema: Dict) -> List[ColumnSchema]:
    """Parse Debezium envelope schema into ColumnSchema list.

    Handles two formats:
      - Simplified (used in tests): schema.fields = column fields directly
      - Full envelope (real Debezium): schema.fields = envelope fields (before/after/source/...);
        column fields are nested inside the "after" field's sub-schema.
    """
    cols = []
    fields = debezium_schema.get("fields", [])
    pk_fields = set(debezium_schema.get("primaryKey") or [])

    # Detect full envelope format: fields contain "after" as a named field
    field_names = {f.get("field") for f in fields}
    if "after" in field_names:
        after_entry = next((f for f in fields if f.get("field") == "after"), {})
        fields = after_entry.get("fields", fields)

    for f in fields:
        name = f.get("field", "")
        if not name:
            continue
        raw_type = f.get("type", "string")
        # Prefer logical type name if present (e.g. io.debezium.time.Timestamp)
        logical = (f.get("name") or "").strip()
        dtype = _TYPE_MAP.get(logical) or _TYPE_MAP.get(raw_type, "varchar")
        cols.append(ColumnSchema(
            name=name,
            data_type=dtype,
            nullable=f.get("optional", True),
            primary_key=(name in pk_fields),
        ))
    return cols


def _is_ddl_event(payload: Dict) -> bool:
    """Return True for DDL / heartbeat / schema-change events that carry no row data."""
    value = payload.get("payload", {})
    # Debezium schema-change events have a "databaseName" but no "op"
    if "databaseName" in value and "op" not in value:
        return True
    # Heartbeat events
    source = value.get("source", {})
    if source.get("connector") == "heartbeat":
        return True
    op = value.get("op", "")
    if op in _SKIP_OPS:
        return True
    return False


def _coerce_oracle_row(row: Optional[Dict]) -> Optional[Dict]:
    """
    Oracle's Debezium connector sometimes wraps NUMBER/DATE values in nested
    dicts like {"scale": 2, "value": "AABB..."} (VariableScaleDecimal bytes).
    Flatten these to a plain string so downstream sinks don't choke.
    """
    if not row:
        return row
    out = {}
    for k, v in row.items():
        if isinstance(v, dict) and "value" in v:
            out[k] = str(v.get("value", ""))
        else:
            out[k] = v
    return out


class DebeziumSource(CDCSource):
    """
    Listens for Debezium Server HTTP sink events and converts them to ChangeEvents.
    Acts as a passive receiver — the streaming generator blocks until events arrive.

    One instance handles one listen_port. If you have multiple Debezium sources
    (e.g. Oracle + SQL Server), give each a different listen_port in config.yml
    and run a separate Debezium Server instance per source.
    """

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._q: queue.Queue = queue.Queue(maxsize=10_000)
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def connect(self):
        port = int(self.cfg.get("listen_port", 8765))
        q = self._q

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                try:
                    q.put_nowait(json.loads(body))
                    self.send_response(200)
                except queue.Full:
                    logger.warning("Debezium event queue full — dropping event")
                    self.send_response(503)
                except Exception as exc:
                    logger.warning("Bad Debezium payload: %s", exc)
                    self.send_response(400)
                self.end_headers()

            def log_message(self, *args):
                pass   # silence access log

        self._server = HTTPServer(("0.0.0.0", port), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info("Debezium adapter listening on port %d", port)

    def get_schema(self, table: str) -> List[ColumnSchema]:
        return []   # schema derived from incoming events

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        return iter([])   # Debezium handles snapshots internally

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        table_filter = table.split(".")[-1].upper() if table else None
        while True:
            try:
                payload = self._q.get(timeout=1.0)
            except queue.Empty:
                yield None  # let engine do timeout-based flushing
                continue

            if _is_ddl_event(payload):
                logger.debug("Skipping DDL/heartbeat event")
                continue

            try:
                yield from self._convert(payload, table_filter)
            except Exception as exc:
                logger.warning("Failed to parse Debezium event: %s", exc)

    def _convert(self, payload: Dict, table_filter: Optional[str]) -> List[ChangeEvent]:
        schema_meta = payload.get("schema", {})
        value       = payload.get("payload", {})

        source     = value.get("source", {})
        table      = source.get("table") or source.get("collection") or "unknown"
        connector  = source.get("connector", "")
        # Oracle uses "schema" for the namespace; DB2 also uses "schema" (not "db" which is the database name)
        if connector in ("oracle", "db2"):
            db = source.get("schema") or ""
        else:
            db = source.get("db") or source.get("schema") or ""
        full_table = f"{db}.{table}" if db else table

        # Table filter is case-insensitive (Oracle uppercases everything)
        if table_filter and table.upper() != table_filter:
            return []

        op_code = value.get("op", "c")
        op      = _OP_MAP.get(op_code, Operation.INSERT)
        before  = _coerce_oracle_row(value.get("before"))
        after   = _coerce_oracle_row(value.get("after"))
        ts_ms   = value.get("ts_ms", 0)

        schema = _parse_schema(schema_meta)
        if not schema and (after or before):
            row = after or before
            schema = [ColumnSchema(k, "varchar") for k in row.keys()]

        return [ChangeEvent(
            op=op,
            source_name=self.name,
            source_table=full_table,
            before=before,
            after=after,
            schema=schema,
            timestamp=datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
            offset={"ts_ms": ts_ms, "table": full_table,
                    "scn":  source.get("scn"),                                   # Oracle SCN
                    "lsn":  source.get("lsn") or source.get("commit_lsn"),      # SQL Server/Postgres LSN; DB2 commit_lsn
                    "gtid": source.get("gtid")},                                 # MySQL GTID
        )]

    def close(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
