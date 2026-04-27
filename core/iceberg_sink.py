"""
Direct Iceberg sink (Mode B) — writes CDC events straight to Iceberg table files
via PyIceberg.

## Recommended target: Dremio Open Catalog

Dremio Open Catalog (built on Apache Polaris) is the native Iceberg REST catalog
in both Dremio Cloud and Dremio Enterprise. Writing directly to it means tables
appear in Dremio instantly — no separate metadata refresh step needed.

  Dremio Cloud endpoint:    https://catalog.dremio.cloud/api/iceberg
  On-prem Open Catalog:     http://<host>:8181/api/catalog
  Auth:                     Bearer PAT (same token used for Dremio SQL)
  Credential vending:       Dremio handles S3/GCS/ADLS credentials automatically

## Alternative catalogs

Any Iceberg REST-compatible catalog works: standalone Apache Polaris, Nessie,
AWS Glue, Hive Metastore. For catalogs outside Dremio, a metadata refresh call
(`ALTER TABLE REFRESH METADATA`) is issued after each batch so Dremio picks up
new files. When the catalog URI points to Dremio itself, this call is skipped.

## Write modes (configurable via write_mode)

  "append"  — fastest; appends all events including _cdc_op column; ideal for
              high-throughput event logs; query with MAX(ts)/QUALIFY for latest state
  "merge"   — true upsert; inserts new rows + equality-deletes superseded rows;
              gives a clean current-state table; slightly more write overhead
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from .event import ChangeEvent, ColumnSchema, Operation

logger = logging.getLogger(__name__)

# ── PyArrow / PyIceberg type mappings ─────────────────────────────────────────

def _pa_type(col_type: str):
    import pyarrow as pa
    t = col_type.lower().split("(")[0]
    return {
        "varchar": pa.string(),
        "text":    pa.string(),
        "char":    pa.string(),
        "int":     pa.int32(),
        "integer": pa.int32(),
        "bigint":  pa.int64(),
        "smallint":pa.int16(),
        "float":   pa.float32(),
        "double":  pa.float64(),
        "numeric": pa.float64(),
        "decimal": pa.float64(),
        "boolean": pa.bool_(),
        "bool":    pa.bool_(),
        "timestamp": pa.timestamp("us"),
        "date":    pa.date32(),
        "time":    pa.time64("us"),
        "bytea":   pa.binary(),
        "json":    pa.string(),
        "jsonb":   pa.string(),
        "uuid":    pa.string(),
        # Spanner / BigQuery native types
        "int64":   pa.int64(),
        "float64": pa.float64(),
        "string":  pa.string(),
        "bytes":   pa.binary(),
        "numeric": pa.float64(),
    }.get(t, pa.string())


def _iceberg_type(col_type: str):
    from pyiceberg.types import (
        StringType, IntegerType, LongType, FloatType, DoubleType,
        BooleanType, TimestampType, DateType, TimeType, BinaryType,
    )
    t = col_type.lower().split("(")[0]
    return {
        "varchar":   StringType(),
        "text":      StringType(),
        "char":      StringType(),
        "int":       IntegerType(),
        "integer":   IntegerType(),
        "bigint":    LongType(),
        "smallint":  IntegerType(),
        "float":     FloatType(),
        "double":    DoubleType(),
        "numeric":   DoubleType(),
        "decimal":   DoubleType(),
        "boolean":   BooleanType(),
        "bool":      BooleanType(),
        "timestamp": TimestampType(),
        "date":      DateType(),
        "time":      TimeType(),
        "bytea":     BinaryType(),
        "json":      StringType(),
        "jsonb":     StringType(),
        "uuid":      StringType(),
        # Spanner / BigQuery native types
        "int64":     LongType(),
        "float64":   DoubleType(),
        "string":    StringType(),
        "bytes":     BinaryType(),
    }.get(t, StringType())


# ── CDC metadata columns ───────────────────────────────────────────────────────
# _cdc_op         — CDC operation: insert / update / delete / snapshot
# _cdc_source     — connector instance name from config
# _cdc_ts         — event timestamp (e.g. Pub/Sub publish time, DB commit time)
# _cdc_ingest_ts  — wall-clock time the CDC engine processed this row

_CDC_META = [
    ColumnSchema("_cdc_op",         "varchar"),
    ColumnSchema("_cdc_source",     "varchar"),
    ColumnSchema("_cdc_ts",         "timestamp"),
    ColumnSchema("_cdc_ingest_ts",  "timestamp"),
]


# Dremio Open Catalog URI patterns — writes land directly in Dremio's catalog,
# so no separate metadata refresh call is needed.
_DREMIO_OPEN_CATALOG_HOSTS = (
    "catalog.dremio.cloud",   # Dremio Cloud
    "/api/catalog",           # on-prem Open Catalog path
    "/api/iceberg",           # alternate on-prem path
)


def _is_dremio_open_catalog(uri: str) -> bool:
    return any(marker in uri for marker in _DREMIO_OPEN_CATALOG_HOSTS)


class IcebergSink:
    def __init__(self, iceberg_cfg: Dict[str, Any], dremio_cfg: Dict[str, Any]):
        """
        iceberg_cfg — catalog connection (type, uri, warehouse, token, etc.)
        dremio_cfg  — Dremio connection used only for metadata refresh when the
                      catalog is NOT Dremio Open Catalog itself
        """
        self._iceberg_cfg  = iceberg_cfg
        self._dremio_cfg   = dremio_cfg
        self._namespace    = iceberg_cfg.get("target_namespace", "cdc")
        self._write_mode   = iceberg_cfg.get("write_mode", "merge")   # "append" | "merge"
        self._sort_by: List[str] = [
            c.strip() for c in iceberg_cfg.get("sort_by", "").split(",") if c.strip()
        ]
        self._catalog      = None
        self._dremio_token: Optional[str] = None
        self._dremio_bearer = False
        self._known_tables: set = set()
        # If the catalog IS Dremio Open Catalog, skip the metadata refresh —
        # writes appear in Dremio immediately via the native Polaris integration.
        self._skip_refresh = _is_dremio_open_catalog(iceberg_cfg.get("uri", ""))

    # ── Catalog connection ────────────────────────────────────────────────────

    def connect(self):
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError:
            raise SystemExit("pyiceberg + pyarrow required: pip install 'pyiceberg[pyarrow]'")

        cfg = dict(self._iceberg_cfg)
        catalog_type = cfg.pop("type", "rest")
        cfg.pop("target_namespace", None)
        cfg.pop("write_mode", None)
        cfg.pop("sort_by", None)

        self._catalog = load_catalog(catalog_type, **cfg)

        # Ensure namespace exists
        try:
            self._catalog.create_namespace(self._namespace)
        except Exception:
            pass   # already exists

        if self._skip_refresh:
            logger.info("Connected to Dremio Open Catalog (Polaris) namespace=%s — "
                        "metadata refresh not needed", self._namespace)
        else:
            logger.info("Connected to Iceberg catalog (%s) namespace=%s",
                        catalog_type, self._namespace)
            # Connect to Dremio for post-batch metadata refreshes
            self._dremio_connect()

    # ── Dremio auth (for metadata refresh only) ────────────────────────────────

    def _dremio_connect(self):
        cfg = self._dremio_cfg
        if not cfg:
            return
        pat = cfg.get("pat", "")
        if pat:
            self._dremio_token  = pat
            self._dremio_bearer = True
        else:
            scheme = "https" if cfg.get("ssl") else "http"
            base   = f"{scheme}://{cfg.get('host','localhost')}:{cfg.get('port',9047)}"
            r = requests.post(
                f"{base}/apiv2/login",
                json={"userName": cfg.get("user","admin"), "password": cfg.get("password","")},
                timeout=15,
            )
            r.raise_for_status()
            self._dremio_token  = r.json()["token"]
            self._dremio_bearer = False
            self._dremio_base   = base

    def _dremio_headers(self) -> Dict[str, str]:
        if self._dremio_bearer:
            return {"Authorization": f"Bearer {self._dremio_token}", "Content-Type": "application/json"}
        return {"Authorization": f"_dremio{self._dremio_token}", "Content-Type": "application/json"}

    def _dremio_refresh(self, dremio_table_path: str):
        """Tell Dremio to re-read the Iceberg metadata for this table.
        Skipped when the catalog is Dremio Open Catalog — writes are immediately visible."""
        if self._skip_refresh or not self._dremio_token or not self._dremio_cfg:
            return
        cfg    = self._dremio_cfg
        scheme = "https" if cfg.get("ssl") else "http"
        base   = getattr(self, "_dremio_base",
                         f"{scheme}://{cfg.get('host','localhost')}:{cfg.get('port',9047)}")
        sql = f'ALTER TABLE {dremio_table_path} REFRESH METADATA'
        try:
            r = requests.post(
                f"{base}/api/v3/sql",
                headers=self._dremio_headers(),
                json={"sql": sql},
                timeout=15,
            )
            r.raise_for_status()
            job_id = r.json()["id"]
            # Poll briefly — refresh jobs are fast
            for _ in range(30):
                jr = requests.get(f"{base}/api/v3/job/{job_id}",
                                  headers=self._dremio_headers(), timeout=10)
                state = jr.json().get("jobState", "")
                if state == "COMPLETED":
                    logger.debug("Dremio metadata refreshed: %s", dremio_table_path)
                    return
                if state in ("FAILED", "CANCELED", "CANCELLED"):
                    logger.warning("Metadata refresh failed for %s: %s",
                                   dremio_table_path, jr.json().get("errorMessage",""))
                    return
                time.sleep(0.5)
        except Exception as exc:
            logger.warning("Metadata refresh error for %s: %s", dremio_table_path, exc)

    # ── Schema helpers ─────────────────────────────────────────────────────────

    def _table_identifier(self, source_table: str) -> str:
        safe = source_table.replace(".", "_").replace("-", "_").lower()
        return f"{self._namespace}.{safe}"

    def _dremio_table_path(self, source_table: str) -> str:
        safe = source_table.replace(".", "_").replace("-", "_").lower()
        return f'"{self._namespace}"."{safe}"'

    def _ensure_table(self, source_table: str, schema: List[ColumnSchema]):
        if source_table in self._known_tables:
            return

        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField

        identifier = self._table_identifier(source_table)
        all_cols = list(schema) + _CDC_META

        fields = [
            NestedField(
                field_id=i + 1,
                name=col.name,
                field_type=_iceberg_type(col.data_type),
                required=False,   # always nullable — PyArrow arrays are nullable by default
            )
            for i, col in enumerate(all_cols)
        ]
        iceberg_schema = Schema(*fields)

        try:
            sort_order = self._build_sort_order(iceberg_schema)
            kwargs = {"identifier": identifier, "schema": iceberg_schema}
            if sort_order is not None:
                kwargs["sort_order"] = sort_order
            self._catalog.create_table(**kwargs)
            logger.info("Created Iceberg table %s%s", identifier,
                        f" (sort_by={self._sort_by})" if sort_order else "")
        except Exception:
            pass   # table already exists

        self._known_tables.add(source_table)

    def _build_sort_order(self, iceberg_schema):
        """Build a PyIceberg SortOrder from self._sort_by column names, or None."""
        if not self._sort_by:
            return None
        try:
            from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
            from pyiceberg.transforms import IdentityTransform
            fields = []
            for col_name in self._sort_by:
                try:
                    f = iceberg_schema.find_field(col_name)
                    fields.append(SortField(
                        source_id=f.field_id,
                        transform=IdentityTransform(),
                        direction=SortDirection.ASC,
                        null_order=NullOrder.NULLS_LAST,
                    ))
                except Exception:
                    logger.warning("sort_by column '%s' not found in schema — skipping", col_name)
            return SortOrder(*fields) if fields else None
        except ImportError:
            logger.warning("PyIceberg sort order not available — sort_by ignored")
            return None

    def _evolve_schema(self, source_table: str, schema: List[ColumnSchema]):
        """Add any new columns to an existing Iceberg table's schema."""
        identifier = self._table_identifier(source_table)
        table = self._catalog.load_table(identifier)
        existing = {f.name for f in table.schema().fields}
        all_cols = list(schema) + _CDC_META
        new_cols = [c for c in all_cols if c.name not in existing]
        if not new_cols:
            return
        with table.update_schema() as upd:
            for col in new_cols:
                upd.add_column(path=col.name, field_type=_iceberg_type(col.data_type))
        logger.info("Evolved Iceberg schema for %s: added %s", source_table, [c.name for c in new_cols])

    def _to_arrow(self, rows: List[Dict], schema: List[ColumnSchema]) -> "pyarrow.Table":
        import pyarrow as pa

        all_cols = list(schema) + _CDC_META
        col_names = [c.name for c in all_cols]
        pa_schema = pa.schema([(c.name, _pa_type(c.data_type)) for c in all_cols])

        columns: Dict[str, List] = {c: [] for c in col_names}
        for row in rows:
            for col in col_names:
                val = row.get(col)
                columns[col].append(val)

        arrays = []
        for col in all_cols:
            try:
                arrays.append(pa.array(columns[col.name], type=_pa_type(col.data_type)))
            except Exception:
                arrays.append(pa.array([str(v) if v is not None else None for v in columns[col.name]],
                                       type=pa.string()))

        return pa.table(dict(zip(col_names, arrays)), schema=pa_schema)

    # ── Write events ───────────────────────────────────────────────────────────

    def write_batch(self, events: List[ChangeEvent]):
        if not events:
            return

        by_table: Dict[str, List[ChangeEvent]] = {}
        for ev in events:
            by_table.setdefault(ev.source_table, []).append(ev)

        for source_table, tevents in by_table.items():
            schema = tevents[0].schema
            pks    = tevents[0].primary_keys

            self._ensure_table(source_table, schema)
            self._evolve_schema(source_table, schema)
            identifier = self._table_identifier(source_table)
            table = self._catalog.load_table(identifier)

            if self._write_mode == "append":
                self._write_append(table, tevents, schema)
            else:
                self._write_merge(table, tevents, schema, pks)

            self._dremio_refresh(self._dremio_table_path(source_table))
            logger.info("Wrote %d events → %s (%s mode)", len(tevents), identifier, self._write_mode)

    def _enrich(self, ev: ChangeEvent) -> Dict:
        """Add _cdc_* metadata to a row dict."""
        row = dict(ev.row or {})
        row["_cdc_op"]        = ev.op.value
        row["_cdc_source"]    = ev.source_name
        row["_cdc_ts"]        = ev.timestamp
        row["_cdc_ingest_ts"] = datetime.now(timezone.utc)
        return row

    def _write_append(self, table, events: List[ChangeEvent], schema: List[ColumnSchema]):
        """Append all events as-is — fastest, preserves full history."""
        rows = [self._enrich(ev) for ev in events if ev.row is not None]
        if not rows:
            return
        arrow_table = self._to_arrow(rows, schema)
        table.append(arrow_table)

    def _write_merge(self, table, events: List[ChangeEvent], schema: List[ColumnSchema], pks: List[str]):
        """
        True upsert using Iceberg row-level operations:
          - Inserts/updates: append new row + equality-delete previous version
          - Deletes: equality-delete the row
        Falls back to append if no primary keys are defined.
        """
        if not pks:
            self._write_append(table, events, schema)
            return

        from pyiceberg.expressions import And, EqualTo

        upserts = [e for e in events if e.op != Operation.DELETE]
        deletes = [e for e in events if e.op == Operation.DELETE]

        # ── Deletes + superseded rows via equality delete ──────────────────
        rows_to_delete = []
        for ev in deletes:
            row = ev.before or ev.after
            if row:
                rows_to_delete.append(row)
        for ev in upserts:
            if ev.before:        # UPDATE — remove old version by before-image
                rows_to_delete.append(ev.before)
            elif ev.after:       # INSERT / SNAPSHOT — remove any existing row with same PK
                rows_to_delete.append(ev.after)

        if rows_to_delete:
            for row in rows_to_delete:
                try:
                    exprs = [EqualTo(pk, row[pk]) for pk in pks if pk in row]
                    if exprs:
                        expr = exprs[0]
                        for e in exprs[1:]:
                            expr = And(expr, e)
                        table.delete(expr)
                except Exception as exc:
                    logger.warning("Row delete failed: %s", exc)

        # ── Append new/updated rows (deduplicated by PK — last write wins) ─
        # If a batch contains both INSERT(id=1) and UPDATE(id=1→...), keep only
        # the latest event per PK so we don't write stale rows to Iceberg.
        seen_pks: set = set()
        deduped: List[Dict] = []
        for ev in reversed(upserts):   # latest event first
            if ev.after is None:
                continue
            pk_val = tuple(ev.after.get(pk) for pk in pks) if pks else None
            if pk_val is not None and pk_val in seen_pks:
                continue
            if pk_val is not None:
                seen_pks.add(pk_val)
            deduped.append(self._enrich(ev))
        deduped.reverse()   # restore chronological order

        if deduped:
            arrow_table = self._to_arrow(deduped, schema)
            table.append(arrow_table)
