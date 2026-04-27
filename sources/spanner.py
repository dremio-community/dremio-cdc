"""
Google Cloud Spanner source — uses Spanner Change Streams to capture DML changes.

Prerequisites (run once per database):
    1. Grant the service account roles/spanner.databaseReader and
       roles/spanner.databaseUser (needed to create the change stream via DDL).
    2. Enable rangefeed on the instance (enabled by default on Spanner).

The connector auto-creates a change stream named DremiocdcStream (configurable)
covering ALL tables on first connect using OLD_AND_NEW_VALUES capture — giving
full before/after images for UPDATEs. Existing streams are reused as-is.

Authentication:
    - Application Default Credentials (gcloud auth application-default login), or
    - Set credentials_file to a service account key JSON path in the connection config.

Requires: pip install google-cloud-spanner
"""
from __future__ import annotations

import json
import logging
import queue
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_OP_MAP = {"INSERT": Operation.INSERT, "UPDATE": Operation.UPDATE, "DELETE": Operation.DELETE}


class SpannerSource(CDCSource):

    def connect(self):
        from google.cloud import spanner as gcs
        self._gcs = gcs

        conn_cfg = self.cfg.get("connection", self.cfg)
        missing = [k for k in ("project", "instance", "database") if not conn_cfg.get(k)]
        if missing:
            raise ValueError(f"Missing required connection fields: {', '.join(missing)}")

        self._project     = conn_cfg["project"]
        self._instance_id = conn_cfg["instance"]
        self._database_id = conn_cfg["database"]
        self._stream_name = conn_cfg.get("change_stream", "DremiocdcStream")
        creds_file        = conn_cfg.get("credentials_file")

        import os
        client_kwargs: dict = {"project": self._project}
        if os.environ.get("SPANNER_EMULATOR_HOST"):
            # Emulator ignores auth — use anonymous credentials to skip ADC lookup
            from google.auth.credentials import AnonymousCredentials
            client_kwargs["credentials"] = AnonymousCredentials()
        elif creds_file:
            from google.oauth2 import service_account
            client_kwargs["credentials"] = service_account.Credentials.from_service_account_file(
                creds_file,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

        client          = gcs.Client(**client_kwargs)
        self._database  = client.instance(self._instance_id).database(self._database_id)

        self._ensure_change_stream()

        self._table_queues: Dict[str, queue.Queue] = {}
        self._stream_thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

        logger.info(
            "Connected to Cloud Spanner project=%s instance=%s database=%s stream=%s",
            self._project, self._instance_id, self._database_id, self._stream_name,
        )

    # ── Schema ───────────────────────────────────────────────────────────────────

    def get_schema(self, table: str) -> List[ColumnSchema]:
        table_name = _bare(table)
        pks = set(self._get_pks(table_name))
        with self._database.snapshot() as snap:
            rows = snap.execute_sql(
                "SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = @t ORDER BY ORDINAL_POSITION",
                params={"t": table_name},
                param_types={"t": self._gcs.param_types.STRING},
            )
            return [ColumnSchema(r[0], r[1], primary_key=(r[0] in pks)) for r in rows]

    # ── Snapshot ─────────────────────────────────────────────────────────────────

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        table_name = _bare(table)
        schema     = self.get_schema(table)
        col_names  = [c.name for c in schema]
        col_list   = ", ".join(f"`{c}`" for c in col_names)

        with self._database.snapshot() as snap:
            for row in snap.execute_sql(f"SELECT {col_list} FROM `{table_name}`"):
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

    # ── Streaming ─────────────────────────────────────────────────────────────────

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        q: queue.Queue = queue.Queue(maxsize=10_000)
        self._table_queues[table] = q

        with threading.Lock():
            if self._stream_thread is None or not self._stream_thread.is_alive():
                self._stream_thread = threading.Thread(
                    target=self._run_change_stream,
                    args=(offset,),
                    daemon=True,
                )
                self._stream_thread.start()

        while not self._stop_flag.is_set():
            try:
                yield q.get(timeout=1.0)
            except queue.Empty:
                yield None

    def _run_change_stream(self, start_offset: Optional[str]):
        """Read the change stream, follow child partitions as they appear."""
        try:
            start_ts = _parse_ts(start_offset) if start_offset else \
                       datetime.now(timezone.utc)

            # Partition queue: (token, start_timestamp).
            # Initial query uses None partition_token (root partition).
            part_q: queue.Queue = queue.Queue()
            part_q.put((None, start_ts))

            while not self._stop_flag.is_set():
                try:
                    token, ts = part_q.get(timeout=1.0)
                except queue.Empty:
                    continue
                self._read_partition(token, ts, part_q)
        except Exception as exc:
            logger.error("Spanner change stream thread error: %s", exc, exc_info=True)

    def _read_partition(self, token: str, start_ts: datetime, part_q: queue.Queue):
        if token is None:
            sql = (
                f"SELECT ChangeRecord FROM READ_{self._stream_name}("
                f"  start_timestamp => @start_ts,"
                f"  end_timestamp   => NULL,"
                f"  partition_token => NULL,"
                f"  heartbeat_milliseconds => 5000"
                f")"
            )
            params     = {"start_ts": start_ts}
            param_types = {"start_ts": self._gcs.param_types.TIMESTAMP}
        else:
            sql = (
                f"SELECT ChangeRecord FROM READ_{self._stream_name}("
                f"  start_timestamp => @start_ts,"
                f"  end_timestamp   => NULL,"
                f"  partition_token => @token,"
                f"  heartbeat_milliseconds => 5000"
                f")"
            )
            params     = {"start_ts": start_ts, "token": token}
            param_types = {
                "start_ts": self._gcs.param_types.TIMESTAMP,
                "token":    self._gcs.param_types.STRING,
            }
        try:
            with self._database.snapshot() as snap:
                for row in snap.execute_sql(sql, params=params, param_types=param_types):
                    if self._stop_flag.is_set():
                        return
                    for record in (row[0] or []):
                        self._process_record(record, part_q)
        except Exception as exc:
            logger.error("Spanner partition error (token=%r): %s", token or "(root)", exc)

    def _process_record(self, record, part_q: queue.Queue):
        # The Spanner Python client returns STRUCT values as positional lists.
        # ChangeRecord layout: [data_change_records, heartbeat_records, child_partitions_records]
        try:
            data_changes    = record[0] or []
            child_part_recs = record[2] or []
        except (IndexError, TypeError):
            return

        for dcr in data_changes:
            self._dispatch_data_change(dcr)

        for cpr in child_part_recs:
            # ChildPartitionsRecord: [start_timestamp, record_sequence, child_partitions]
            try:
                child_ts = _parse_ts(cpr[0]) or datetime.now(timezone.utc)
                for cp in (cpr[2] or []):
                    # ChildPartition: [token, parent_partition_tokens]
                    part_q.put((cp[0], child_ts))
            except (IndexError, TypeError):
                continue

    def _dispatch_data_change(self, dcr):
        """Parse a DataChangeRecord (positional list) and emit ChangeEvents.

        Field order: commit_timestamp(0), record_sequence(1), transaction_id(2),
        is_last_record(3), table_name(4), column_types(5), mods(6), mod_type(7), ...
        Mod field order: keys(0), new_values(1), old_values(2)
        """
        try:
            commit_ts  = dcr[0]
            table_name = dcr[4]
            mods       = dcr[6] or []
            mod_type   = dcr[7]
        except (IndexError, TypeError):
            return

        op = _OP_MAP.get(mod_type, Operation.INSERT)
        ts = _parse_ts(commit_ts) or datetime.now(timezone.utc)

        target = next(
            (k for k in self._table_queues if _bare(k).lower() == table_name.lower()),
            None,
        )
        if target is None:
            return

        schema = self.get_schema(target)
        q      = self._table_queues[target]

        for mod in mods:
            try:
                keys     = _coerce(mod[0])
                old_vals = _coerce(mod[1])  # emulator returns [keys, old_values, new_values]
                new_vals = _coerce(mod[2])
            except (IndexError, TypeError):
                continue

            after  = {**keys, **new_vals} if op != Operation.DELETE else None
            before = {**keys, **old_vals} if op in (Operation.UPDATE, Operation.DELETE) else None

            try:
                q.put_nowait(ChangeEvent(
                    op=op,
                    source_name=self.name,
                    source_table=target,
                    before=before,
                    after=after,
                    schema=schema,
                    timestamp=ts,
                    offset=str(commit_ts),
                ))
            except queue.Full:
                logger.warning("Spanner queue full for %s, dropping event", target)

    # ── Lifecycle ─────────────────────────────────────────────────────────────────

    def close(self):
        self._stop_flag.set()

    # ── Helpers ───────────────────────────────────────────────────────────────────

    def _ensure_change_stream(self):
        with self._database.snapshot() as snap:
            existing = list(snap.execute_sql(
                "SELECT CHANGE_STREAM_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
                "WHERE CHANGE_STREAM_NAME = @n",
                params={"n": self._stream_name},
                param_types={"n": self._gcs.param_types.STRING},
            ))
        if existing:
            logger.info("Using existing Spanner change stream %s", self._stream_name)
            return

        logger.info("Creating Spanner change stream %s", self._stream_name)
        op = self._database.update_ddl([
            f"CREATE CHANGE STREAM `{self._stream_name}` "
            f"FOR ALL "
            f"OPTIONS (value_capture_type = 'OLD_AND_NEW_VALUES')"
        ])
        op.result(timeout=120)
        logger.info("Change stream %s created", self._stream_name)

    def _get_pks(self, table_name: str) -> List[str]:
        with self._database.snapshot() as snap:
            rows = snap.execute_sql(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS "
                "WHERE TABLE_NAME = @t AND INDEX_NAME = 'PRIMARY_KEY' "
                "ORDER BY ORDINAL_POSITION",
                params={"t": table_name},
                param_types={"t": self._gcs.param_types.STRING},
            )
            return [r[0] for r in rows]


# ── Module helpers ────────────────────────────────────────────────────────────────

def _bare(table: str) -> str:
    """Strip schema prefix — Spanner doesn't use schemas."""
    return table.split(".")[-1]


def _parse_ts(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    try:
        return datetime.fromisoformat(str(value).rstrip("Z")).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _coerce(value) -> dict:
    """Ensure a mod field is a plain dict (may arrive as JSON string or struct)."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return value if isinstance(value, dict) else {}
