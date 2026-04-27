"""
Google Cloud Datastream → GCS source connector.

Polls a GCS bucket for Avro (or NDJSON) change-event files written by
Google Cloud Datastream and yields ChangeEvents to the CDC engine.

GCS path layout (Datastream default):
  gs://{bucket}/{path_prefix}/{SCHEMA}/{TABLE}/YYYY/MM/DD/HH/{seq}.avro

Files are processed in lexicographic order, which matches Datastream's
chronological write order. Offsets are encoded as "{file_path}#{record_index}"
so the connector can resume from within a partially-processed file — necessary
when a file spans more than one engine batch.

Datastream metadata fields in each record (standard format):
  _metadata_timestamp              event time
  _metadata_change_sequence_number monotonic sequence (ordering within table)
  _metadata_operation              INSERT | UPDATE | DELETE (if present)
  _metadata_deleted                true for deletes (fallback when no op field)
  _metadata_primary_keys           list of PK column names (auto-wired to schema)
  _metadata_schema / _metadata_table  source identifiers

Set snapshot_on_first_run: false — Datastream writes its own initial full-load
backfill as INSERT events in GCS, and stream() with offset=None processes them.

Config keys under connection:
  project_id              GCP project ID (required)
  bucket                  GCS bucket name (required)
  path_prefix             Path prefix inside the bucket matching your Datastream
                          destination root path (e.g. "datastream/prod")
  credentials_file        Path to service-account JSON (optional — uses ADC)
  poll_interval_seconds   Seconds between GCS list calls when idle (default: 10)
  file_format             "avro" (default) or "json" (NDJSON)
"""
from __future__ import annotations

import io
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_META = "_metadata_"

# ── Avro → CDC type mapping ───────────────────────────────────────────────────

_AVRO_PRIMITIVES = {
    "boolean": "boolean",
    "int":     "int",
    "long":    "bigint",
    "float":   "float",
    "double":  "double",
    "string":  "varchar",
    "bytes":   "varchar",
    "enum":    "varchar",
}

_AVRO_LOGICAL = {
    "timestamp-millis":        "timestamp",
    "timestamp-micros":        "timestamp",
    "local-timestamp-millis":  "timestamp",
    "local-timestamp-micros":  "timestamp",
    "date":                    "date",
    "decimal":                 "double",
    "uuid":                    "varchar",
}


def _avro_to_cdc_type(field_schema) -> str:
    """Recursively map an Avro field schema to a CDC type string."""
    if isinstance(field_schema, list):
        # Union — take first non-null branch
        non_null = [t for t in field_schema if t != "null"]
        return _avro_to_cdc_type(non_null[0]) if non_null else "varchar"
    if isinstance(field_schema, dict):
        logical = field_schema.get("logicalType") or field_schema.get("logical_type")
        if logical and logical in _AVRO_LOGICAL:
            return _AVRO_LOGICAL[logical]
        inner = field_schema.get("type", "string")
        if isinstance(inner, (dict, list)):
            return _avro_to_cdc_type(inner)
        return _AVRO_PRIMITIVES.get(str(inner), "varchar")
    return _AVRO_PRIMITIVES.get(str(field_schema), "varchar")


# ── Offset encoding ───────────────────────────────────────────────────────────

def _encode_offset(file_path: str, record_idx: int) -> str:
    """Encode a file + record position as a single offset string."""
    return f"{file_path}#{record_idx}"


def _decode_offset(offset: Any) -> Tuple[Optional[str], Optional[int]]:
    """
    Decode an offset string into (file_path, record_idx).
    record_idx=None means the file was fully processed (legacy file-only offset).
    """
    if not offset or not isinstance(offset, str):
        return None, None
    if "#" in offset:
        idx = offset.rfind("#")
        try:
            return offset[:idx], int(offset[idx + 1:])
        except ValueError:
            pass
    # Legacy: plain file path — treat as fully processed
    return offset, None


# ── Record helpers ────────────────────────────────────────────────────────────

def _parse_op(record: Dict) -> Operation:
    """Determine CDC operation from Datastream metadata."""
    op_raw = str(record.get("_metadata_operation", "")).upper()
    if op_raw == "DELETE":
        return Operation.DELETE
    if op_raw == "UPDATE":
        return Operation.UPDATE
    if op_raw in ("INSERT", "READ"):
        return Operation.INSERT
    # Fallback: is_deleted flag
    if record.get("_metadata_deleted") or record.get("_metadata_is_deleted"):
        return Operation.DELETE
    return Operation.INSERT


def _strip_meta(record: Dict) -> Dict:
    return {k: v for k, v in record.items() if not k.startswith(_META)}


def _normalise(row: Dict) -> Dict:
    out = {}
    for k, v in row.items():
        out[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
    return out


def _parse_ts(record: Dict) -> datetime:
    raw = record.get("_metadata_timestamp") or record.get("_metadata_read_timestamp")
    if raw is None:
        return datetime.now(timezone.utc)
    if isinstance(raw, (int, float)):
        # Datastream timestamps can be seconds, millis, or micros.
        # Millis for 2001-2100 are in [1e12, 4e12]; micros are in [1e15, 4e15].
        # Use 1e14 as the millis/micros boundary to avoid misclassifying millis.
        if raw > 1e14:
            raw /= 1_000_000.0
        elif raw > 1e10:
            raw /= 1_000.0
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _pk_fields(record: Dict) -> List[str]:
    pk = record.get("_metadata_primary_keys") or record.get("_metadata_primary_key")
    if isinstance(pk, list):
        return [str(k) for k in pk]
    if isinstance(pk, str) and pk:
        return [pk]
    return []


def _infer_schema_from_record(record: Dict, pk_fields: List[str]) -> List[ColumnSchema]:
    data = _strip_meta(record)
    schema = []
    for k, v in data.items():
        if isinstance(v, bool):
            typ = "boolean"
        elif isinstance(v, int):
            typ = "bigint"
        elif isinstance(v, float):
            typ = "double"
        elif isinstance(v, (dict, list)):
            typ = "varchar"
        else:
            typ = "varchar"
        schema.append(ColumnSchema(name=k, data_type=typ, primary_key=(k in pk_fields)))
    return schema


# ── Source class ──────────────────────────────────────────────────────────────

class DatastreamSource(CDCSource):
    """
    Polls GCS for Avro/JSON files written by Google Cloud Datastream.

    Each entry in `tables` is a "SCHEMA.TABLE" name matching the Datastream
    stream config (e.g. "HR.EMPLOYEES", "public.orders"). The connector maps
    this to the GCS path: {path_prefix}/HR/EMPLOYEES/YYYY/MM/DD/HH/{seq}.avro
    """

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._project_id: str           = conn["project_id"]
        self._bucket_name: str          = conn["bucket"]
        self._path_prefix: str          = conn.get("path_prefix", "").strip("/")
        self._creds_file: Optional[str] = conn.get("credentials_file")
        self._poll_interval: int        = int(conn.get("poll_interval_seconds", 10))
        self._file_format: str          = conn.get("file_format", "avro").lower()
        self._client                    = None
        self._bucket                    = None

    # ── Connection ─────────────────────────────────────────────────────────

    def connect(self):
        try:
            from google.cloud import storage
        except ImportError:
            raise SystemExit(
                "google-cloud-storage required: pip install google-cloud-storage"
            )

        if self._creds_file:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(
                self._creds_file,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self._client = storage.Client(project=self._project_id, credentials=creds)
        else:
            self._client = storage.Client(project=self._project_id)

        self._bucket = self._client.bucket(self._bucket_name)
        logger.info(
            "Connected to GCS bucket gs://%s (project=%s)",
            self._bucket_name, self._project_id,
        )

    # ── GCS helpers ────────────────────────────────────────────────────────

    def _gcs_prefix(self, table: str) -> str:
        """
        Map a table name to its GCS prefix.
        "HR.EMPLOYEES" → "my-prefix/HR/EMPLOYEES/"
        "public.orders" → "my-prefix/public/orders/"
        """
        path = table.replace(".", "/")
        if self._path_prefix:
            return f"{self._path_prefix}/{path}/"
        return f"{path}/"

    def _list_files(self, table: str, after: Optional[str] = None) -> List[str]:
        """
        Return sorted GCS object names under the table prefix.
        If `after` is given, only return files lexicographically after it
        (i.e. files not yet processed).
        """
        prefix = self._gcs_prefix(table)
        ext = f".{self._file_format}"
        blobs = self._client.list_blobs(self._bucket_name, prefix=prefix)
        paths = sorted(b.name for b in blobs if b.name.endswith(ext))
        if after:
            paths = [p for p in paths if p > after]
        return paths

    # ── File reading ───────────────────────────────────────────────────────

    def _read_avro(self, object_name: str) -> List[Dict]:
        try:
            import fastavro
        except ImportError:
            raise SystemExit("fastavro required: pip install fastavro")
        data = self._bucket.blob(object_name).download_as_bytes()
        return list(fastavro.reader(io.BytesIO(data)))

    def _read_json(self, object_name: str) -> List[Dict]:
        text = self._bucket.blob(object_name).download_as_text()
        records = []
        for line in text.strip().splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))
        return records

    def _read_file(self, object_name: str) -> List[Dict]:
        return self._read_avro(object_name) if self._file_format == "avro" else self._read_json(object_name)

    # ── Schema inference ───────────────────────────────────────────────────

    def _schema_from_avro_writer(self, object_name: str) -> List[ColumnSchema]:
        """Use the embedded Avro writer schema for accurate type inference."""
        try:
            import fastavro
        except ImportError:
            return []
        data = self._bucket.blob(object_name).download_as_bytes()
        reader = fastavro.reader(io.BytesIO(data))
        ws = reader.writer_schema
        if not ws or ws.get("type") != "record":
            return []
        # Pull PK list from any first record if possible
        records = list(reader)
        pk_fields_: List[str] = _pk_fields(records[0]) if records else []
        cols = []
        for field in ws.get("fields", []):
            fname = field["name"]
            if fname.startswith(_META):
                continue
            cols.append(ColumnSchema(
                name=fname,
                data_type=_avro_to_cdc_type(field.get("type", "string")),
                primary_key=(fname in pk_fields_),
            ))
        return cols

    def get_schema(self, table: str) -> List[ColumnSchema]:
        files = self._list_files(table)
        if not files:
            logger.info("[%s/%s] No files in GCS yet — schema unavailable", self.name, table)
            return []
        try:
            if self._file_format == "avro":
                schema = self._schema_from_avro_writer(files[0])
                if schema:
                    return schema
            records = self._read_file(files[0])
            if records:
                pks = _pk_fields(records[0])
                return _infer_schema_from_record(records[0], pks)
        except Exception as exc:
            logger.warning("[%s/%s] Schema inference failed: %s", self.name, table, exc)
        return []

    # ── Snapshot ───────────────────────────────────────────────────────────

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        """
        Datastream writes its initial full-load backfill as INSERT events in GCS.
        stream() with offset=None processes those files automatically.
        Set snapshot_on_first_run: false — this method is intentionally a no-op.
        """
        logger.info(
            "[%s/%s] Datastream handles backfill via GCS files. "
            "Set snapshot_on_first_run: false — stream() will process all files from the start.",
            self.name, table,
        )
        return
        yield

    # ── Streaming ──────────────────────────────────────────────────────────

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        """
        Poll GCS for Datastream files and yield ChangeEvents indefinitely.

        Offset format: "{gcs_object_path}#{record_index}"
        - record_idx = index of the LAST yielded record in that file
        - On restart, reads that file again and skips to record_idx+1
        - Correct even when a large file spans multiple engine batches
        """
        last_file, last_record_idx = _decode_offset(offset)

        logger.info(
            "[%s/%s] Streaming from gs://%s/%s (after=%s#%s)",
            self.name, table, self._bucket_name,
            self._gcs_prefix(table),
            last_file or "beginning",
            last_record_idx if last_record_idx is not None else "*",
        )

        # Resume mid-file if we have a record-level offset
        if last_file and last_record_idx is not None:
            try:
                records = self._read_file(last_file)
                start = last_record_idx + 1
                for idx, record in enumerate(records[start:], start=start):
                    event = self._make_event(table, record, _encode_offset(last_file, idx))
                    if event:
                        yield event
            except Exception as exc:
                logger.warning(
                    "[%s/%s] Failed to resume %s from record %d: %s — skipping rest of file",
                    self.name, table, last_file, last_record_idx + 1, exc,
                )

        while True:
            try:
                new_files = self._list_files(table, after=last_file)
            except Exception as exc:
                logger.warning(
                    "[%s/%s] GCS list error: %s — retrying in %ds",
                    self.name, table, exc, self._poll_interval,
                )
                time.sleep(self._poll_interval)
                yield None
                continue

            if not new_files:
                time.sleep(self._poll_interval)
                yield None
                continue

            for file_path in new_files:
                try:
                    records = self._read_file(file_path)
                except Exception as exc:
                    logger.warning(
                        "[%s/%s] Failed to read %s: %s — skipping",
                        self.name, table, file_path, exc,
                    )
                    last_file = file_path
                    continue

                for idx, record in enumerate(records):
                    event = self._make_event(table, record, _encode_offset(file_path, idx))
                    if event:
                        yield event

                last_file = file_path

    def _make_event(self, table: str, record: Dict, offset: str) -> Optional[ChangeEvent]:
        """Convert a Datastream record dict to a ChangeEvent."""
        try:
            op     = _parse_op(record)
            pks    = _pk_fields(record)
            data   = _normalise(_strip_meta(record))
            schema = _infer_schema_from_record(record, pks)
            ts     = _parse_ts(record)
            seq    = (
                record.get("_metadata_change_sequence_number")
                or record.get("_metadata_log_position")
                or offset
            )
            return ChangeEvent(
                op=op,
                source_name=self.name,
                source_table=table,
                before=data if op == Operation.DELETE else None,
                after=data if op != Operation.DELETE else None,
                schema=schema,
                timestamp=ts,
                offset=seq,
            )
        except Exception as exc:
            logger.warning("[%s/%s] Skipping malformed record: %s", self.name, table, exc)
            return None

    def close(self):
        if self._client:
            self._client.close()
