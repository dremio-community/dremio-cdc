"""
Dremio sink — writes CDC events to Dremio tables via the REST SQL API.

For each source table it:
  1. Creates the target table if it doesn't exist (with _cdc_* metadata columns)
  2. Applies schema evolution (ALTER TABLE ADD COLUMN) when new columns appear
  3. Executes MERGE INTO for INSERT / UPDATE / SNAPSHOT events
  4. Executes DELETE FROM for DELETE events
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from .event import ChangeEvent, ColumnSchema, Operation

logger = logging.getLogger(__name__)

# CDC metadata columns appended to every target table
_CDC_META_COLS = [
    ColumnSchema("_cdc_op",     "VARCHAR",   nullable=True),
    ColumnSchema("_cdc_source", "VARCHAR",   nullable=True),
    ColumnSchema("_cdc_ts",     "TIMESTAMP", nullable=True),
]

# Maps normalised column type → Dremio SQL type
_TYPE_MAP = {
    "varchar":   "VARCHAR",
    "text":      "VARCHAR",
    "char":      "VARCHAR",
    "int":       "INT",
    "integer":   "INT",
    "bigint":    "BIGINT",
    "smallint":  "INT",
    "serial":    "INT",
    "bigserial": "BIGINT",
    "double":    "DOUBLE",
    "float":     "FLOAT",
    "numeric":   "DOUBLE",
    "decimal":   "DOUBLE",
    "boolean":   "BOOLEAN",
    "bool":      "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date":      "DATE",
    "time":      "TIME",
    "json":      "VARCHAR",
    "jsonb":     "VARCHAR",
    "uuid":      "VARCHAR",
    "bytea":     "VARBINARY",
}


def _dremio_type(col_type: str) -> str:
    return _TYPE_MAP.get(col_type.lower().split("(")[0], "VARCHAR")


def _quote(name: str) -> str:
    return f'"{name}"'


def _quote_table(path: str) -> str:
    """Quote each dot-separated segment of a Dremio table path."""
    return ".".join(_quote(p) for p in path.split("."))


class DremioSink:
    def __init__(self, cfg: Dict[str, Any]):
        self._host       = cfg.get("host", "localhost")
        self._port       = cfg.get("port", 9047)
        self._ssl        = cfg.get("ssl", False)
        self._user       = cfg.get("user", "admin")
        self._password   = cfg.get("password", "")
        self._pat        = cfg.get("pat", "")
        self._project_id = cfg.get("project_id", "")
        self._namespace  = cfg.get("target_namespace", "cdc")
        self._token: Optional[str] = None
        self._known_schemas: Dict[str, List[str]] = {}

        scheme = "https" if self._ssl else "http"
        self._base = f"{scheme}://{self._host}:{self._port}"
        # Dremio Cloud uses a project-scoped API path
        self._is_cloud = bool(self._project_id or "dremio.cloud" in self._host)

    # ── Auth ─────────────────────────────────────────────────────────────────

    def connect(self):
        """Authenticate against Dremio. Logs a warning (does not raise) if unreachable."""
        try:
            if self._pat:
                self._token = self._pat
                self._bearer = True
            else:
                r = requests.post(
                    f"{self._base}/apiv2/login",
                    json={"userName": self._user, "password": self._password},
                    timeout=15,
                )
                r.raise_for_status()
                self._token = r.json()["token"]
                self._bearer = False
            logger.info("Connected to Dremio at %s", self._base)
        except Exception as exc:
            logger.warning("Dremio unreachable at startup (%s) — will retry on first write", exc)
            self._token = None

    def _headers(self) -> Dict[str, str]:
        if self._bearer:
            return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}
        return {"Authorization": f"_dremio{self._token}", "Content-Type": "application/json"}

    # ── SQL execution ─────────────────────────────────────────────────────────

    def _sql_url(self) -> str:
        if self._is_cloud:
            return f"{self._base}/v0/projects/{self._project_id}/sql"
        return f"{self._base}/api/v3/sql"

    def _job_url(self, job_id: str) -> str:
        if self._is_cloud:
            return f"{self._base}/v0/projects/{self._project_id}/job/{job_id}"
        return f"{self._base}/api/v3/job/{job_id}"

    def _sql(self, sql: str, retries: int = 3) -> Optional[Dict]:
        """Submit a SQL job and poll until complete."""
        if self._token is None:
            self.connect()  # deferred connect if startup failed
        for attempt in range(retries):
            try:
                r = requests.post(
                    self._sql_url(),
                    headers=self._headers(),
                    json={"sql": sql},
                    timeout=30,
                )
                if r.status_code == 401:
                    self.connect()
                    continue
                r.raise_for_status()
                job_id = r.json()["id"]
                return self._poll(job_id)
            except Exception as exc:
                if attempt == retries - 1:
                    raise
                logger.warning("SQL attempt %d failed: %s", attempt + 1, exc)
                time.sleep(2 ** attempt)

    def _poll(self, job_id: str, timeout: int = 60) -> Dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = requests.get(
                self._job_url(job_id),
                headers=self._headers(),
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            state = data.get("jobState", "")
            if state == "COMPLETED":
                return data
            if state in ("FAILED", "CANCELED", "CANCELLED"):
                raise RuntimeError(f"Dremio job {job_id} {state}: {data.get('errorMessage', '')}")
            time.sleep(0.5)
        raise TimeoutError(f"Dremio job {job_id} timed out after {timeout}s")

    # ── Schema management ─────────────────────────────────────────────────────

    def _target_path(self, source_table: str) -> str:
        """e.g. 'public.customers' → 'cdc.public_customers' in Dremio namespace."""
        safe = source_table.replace(".", "_")
        return f"{self._namespace}.{safe}"

    def ensure_table(self, source_table: str, schema: List[ColumnSchema]):
        path = self._target_path(source_table)
        cols = list(schema) + _CDC_META_COLS
        col_defs = ",\n  ".join(
            f"{_quote(c.name)} {_dremio_type(c.data_type)}" for c in cols
        )
        ddl = f"CREATE TABLE IF NOT EXISTS {_quote_table(path)} (\n  {col_defs}\n)"
        self._sql(ddl)
        # Only seed known_schemas on first encounter; evolve_schema handles additions
        if source_table not in self._known_schemas:
            self._known_schemas[source_table] = [c.name for c in cols]
        logger.info("Ensured table %s", path)

    def evolve_schema(self, source_table: str, schema: List[ColumnSchema]):
        """Add any columns that appeared in the source but aren't in the target yet."""
        known = set(self._known_schemas.get(source_table, []))
        path = self._target_path(source_table)
        for col in schema:
            if col.name not in known:
                ddl = f"ALTER TABLE {_quote_table(path)} ADD COLUMNS ({_quote(col.name)} {_dremio_type(col.data_type)})"
                try:
                    self._sql(ddl)
                    known.add(col.name)
                    logger.info("Added column %s.%s", path, col.name)
                except Exception as exc:
                    logger.warning("Could not add column %s: %s", col.name, exc)
        self._known_schemas[source_table] = list(known)

    # ── Write events ──────────────────────────────────────────────────────────

    def write_batch(self, events: List[ChangeEvent]):
        if not events:
            return

        # Group by table
        by_table: Dict[str, List[ChangeEvent]] = {}
        for ev in events:
            by_table.setdefault(ev.source_table, []).append(ev)

        for table, tevents in by_table.items():
            schema = tevents[0].schema
            pks = tevents[0].primary_keys

            self.ensure_table(table, schema)
            self.evolve_schema(table, schema)

            upserts = [e for e in tevents if e.op != Operation.DELETE]
            deletes = [e for e in tevents if e.op == Operation.DELETE]

            if upserts and pks:
                # Deduplicate by PK — keep last event per key to avoid
                # "target row matched more than once" in Dremio MERGE
                seen: Dict[tuple, ChangeEvent] = {}
                for ev in upserts:
                    key = tuple((ev.after or {}).get(pk) for pk in pks)
                    seen[key] = ev
                upserts = list(seen.values())
                self._merge(table, schema, pks, upserts)
            elif upserts:
                self._insert(table, schema, upserts)

            for ev in deletes:
                if pks and ev.before:
                    self._delete(table, pks, ev)

    def _escape(self, val: Any) -> str:
        from datetime import date, datetime
        from decimal import Decimal
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        if isinstance(val, Decimal):
            return str(float(val))
        if isinstance(val, (int, float)):
            return str(val)
        if isinstance(val, datetime):
            ts = val.strftime('%Y-%m-%d %H:%M:%S') + f".{val.microsecond // 1000:03d}"
            return f"CAST('{ts}' AS TIMESTAMP)"
        if isinstance(val, date):
            return f"CAST('{val.isoformat()}' AS DATE)"
        if isinstance(val, bytes):
            return "'" + val.hex() + "'"
        return "'" + str(val).replace("'", "''") + "'"

    def _merge(self, source_table: str, schema: List[ColumnSchema], pks: List[str], events: List[ChangeEvent]):
        path = _quote_table(self._target_path(source_table))
        all_cols = [c.name for c in schema] + ["_cdc_op", "_cdc_source", "_cdc_ts"]
        col_list = ", ".join(_quote(c) for c in all_cols)

        rows = []
        for ev in events:
            row = dict(ev.after or {})
            row["_cdc_op"]     = ev.op.value
            row["_cdc_source"] = ev.source_name
            row["_cdc_ts"]     = ev.timestamp
            vals = ", ".join(self._escape(row.get(c)) for c in all_cols)
            rows.append(f"({vals})")

        values_sql = ",\n  ".join(rows)
        # Dremio MERGE: ON clause uses unquoted alias.col; UPDATE SET omits table alias.
        on_clause = " AND ".join(f't.{pk} = s.{pk}' for pk in pks)
        update_set = ", ".join(
            f'{_quote(c)} = s.{_quote(c)}'
            for c in all_cols if c not in pks
        )
        insert_cols = col_list
        insert_vals = ", ".join(f"s.{_quote(c)}" for c in all_cols)

        sql = f"""
MERGE INTO {path} AS t
USING (VALUES
  {values_sql}
) AS s({col_list})
ON {on_clause}
WHEN MATCHED THEN UPDATE SET {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
""".strip()
        self._sql(sql)

    def _insert(self, source_table: str, schema: List[ColumnSchema], events: List[ChangeEvent]):
        """Fallback INSERT for tables without primary keys."""
        path = _quote_table(self._target_path(source_table))
        all_cols = [c.name for c in schema] + ["_cdc_op", "_cdc_source", "_cdc_ts"]
        col_list = ", ".join(_quote(c) for c in all_cols)

        for ev in events:
            row = dict(ev.after or {})
            row["_cdc_op"]     = ev.op.value
            row["_cdc_source"] = ev.source_name
            row["_cdc_ts"]     = ev.timestamp
            vals = ", ".join(self._escape(row.get(c)) for c in all_cols)
            self._sql(f"INSERT INTO {path} ({col_list}) VALUES ({vals})")

    def _delete(self, source_table: str, pks: List[str], ev: ChangeEvent):
        path = _quote_table(self._target_path(source_table))
        where = " AND ".join(
            f"{_quote(pk)} = {self._escape((ev.before or {}).get(pk))}"
            for pk in pks
        )
        self._sql(f"DELETE FROM {path} WHERE {where}")
