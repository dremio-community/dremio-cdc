"""
Google Cloud Pub/Sub CDC source.

Each entry in `tables` is a Pub/Sub subscription name. Messages are
deserialized as JSON and written to an Iceberg table of the same name.

Ack-on-commit: messages are acked only AFTER the Iceberg batch write
succeeds (via on_batch_committed), preserving at-least-once delivery.
A background thread extends ack deadlines for in-flight messages to
cover the full flush window.

Config keys under connection:
  project_id           GCP project ID (required)
  credentials_file     Path to service-account JSON (optional — uses ADC if omitted)
  ack_deadline_seconds Ack deadline for pulled messages (default: 120)
  max_messages_per_pull Max messages per pull call (default: 100)
  pull_timeout_seconds  How long to wait for messages before yielding a heartbeat (default: 5)

Per-source optional keys:
  op_field             JSON field name that carries the CDC operation ("insert"/"update"/"delete")
  op_map               Mapping of op_field values to CDC ops (see example config)
  primary_key_field    JSON field (or comma-separated list) to mark as primary key
  message_format       "json" only for now (avro/protobuf planned)
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_DEFAULT_OP_MAP = {
    "insert":  Operation.INSERT,
    "create":  Operation.INSERT,
    "update":  Operation.UPDATE,
    "modify":  Operation.UPDATE,
    "delete":  Operation.DELETE,
    "remove":  Operation.DELETE,
}


def _infer_type(val: Any) -> str:
    if isinstance(val, bool):
        return "boolean"
    if isinstance(val, int):
        return "bigint"
    if isinstance(val, float):
        return "double"
    if isinstance(val, (dict, list)):
        return "varchar"  # JSON-encoded
    return "varchar"


def _infer_schema(row: Dict, pk_fields: List[str]) -> List[ColumnSchema]:
    return [
        ColumnSchema(
            name=k,
            data_type=_infer_type(v),
            primary_key=(k in pk_fields),
        )
        for k, v in row.items()
    ]


def _normalise_row(row: Dict) -> Dict:
    """Flatten nested dicts/lists to JSON strings so they fit a flat Iceberg schema."""
    out = {}
    for k, v in row.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v)
        else:
            out[k] = v
    return out


def _parse_op(row: Dict, op_field: Optional[str], op_map: Dict[str, Operation]) -> Operation:
    if not op_field or op_field not in row:
        return Operation.INSERT
    raw = str(row[op_field]).lower()
    return op_map.get(raw, Operation.INSERT)


class _DeadlineExtender(threading.Thread):
    """
    Periodically calls modify_ack_deadline on all pending ack_ids to prevent
    Pub/Sub redelivering messages that are still being processed.
    """

    def __init__(self, subscriber, subscription_path: str, pending: List[str],
                 lock: threading.Lock, deadline_seconds: int):
        super().__init__(daemon=True, name=f"ack-extender/{subscription_path}")
        self._sub      = subscriber
        self._path     = subscription_path
        self._pending  = pending
        self._lock     = lock
        self._deadline = deadline_seconds
        self._stop     = threading.Event()
        # Extend at 1/3 of the deadline so we're never close to expiry
        self._interval = max(10, deadline_seconds // 3)

    def run(self):
        while not self._stop.is_set():
            time.sleep(self._interval)
            with self._lock:
                ids = list(self._pending)
            if ids:
                try:
                    self._sub.modify_ack_deadline(
                        request={
                            "subscription": self._path,
                            "ack_ids": ids,
                            "ack_deadline_seconds": self._deadline,
                        }
                    )
                    logger.debug("Extended ack deadline for %d message(s) on %s", len(ids), self._path)
                except Exception as exc:
                    logger.warning("Failed to extend ack deadline on %s: %s", self._path, exc)

    def stop(self):
        self._stop.set()


class PubSubSource(CDCSource):
    """
    Pulls messages from one Google Cloud Pub/Sub subscription per configured table.
    Acks are deferred until the engine confirms the batch was written to Iceberg.
    """

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._subscriber       = None
        self._project_id: str  = cfg["connection"]["project_id"]
        self._creds_file: Optional[str] = cfg["connection"].get("credentials_file")
        self._ack_deadline: int = int(cfg["connection"].get("ack_deadline_seconds", 120))
        self._max_messages: int = int(cfg["connection"].get("max_messages_per_pull", 100))
        self._pull_timeout: int = int(cfg["connection"].get("pull_timeout_seconds", 5))

        self._op_field: Optional[str] = cfg.get("op_field")
        raw_op_map: Dict[str, str]    = cfg.get("op_map", {})
        self._op_map: Dict[str, Operation] = {
            **_DEFAULT_OP_MAP,
            **{k.lower(): Operation(v.lower()) for k, v in raw_op_map.items()},
        }
        pk_raw = cfg.get("primary_key_field", "")
        self._pk_fields: List[str] = [f.strip() for f in pk_raw.split(",") if f.strip()]

        # Per-subscription state: ack_ids pending commit
        # key = subscription name (== table name), value = list of ack_ids
        self._pending_acks: Dict[str, List[str]] = {}
        self._pending_locks: Dict[str, threading.Lock] = {}
        self._extenders: Dict[str, _DeadlineExtender] = {}

    def connect(self):
        try:
            from google.cloud import pubsub_v1
        except ImportError:
            raise SystemExit(
                "google-cloud-pubsub required: pip install google-cloud-pubsub"
            )

        import os
        emulator_host = os.environ.get("PUBSUB_EMULATOR_HOST") or self.cfg.get("connection", {}).get("emulator_host")
        if emulator_host:
            import google.auth.credentials
            from google.api_core import client_options as co
            self._subscriber = pubsub_v1.SubscriberClient(
                credentials=google.auth.credentials.AnonymousCredentials(),
                client_options=co.ClientOptions(api_endpoint=emulator_host),
            )
        elif self._creds_file:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(
                self._creds_file,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self._subscriber = pubsub_v1.SubscriberClient(credentials=creds)
        else:
            # Application Default Credentials (gcloud auth, Workload Identity, etc.)
            self._subscriber = pubsub_v1.SubscriberClient()

        logger.info("Connected to Pub/Sub (project=%s)", self._project_id)

        # Initialise per-subscription state
        for table in self.tables:
            self._pending_acks[table]  = []
            self._pending_locks[table] = threading.Lock()

    def _subscription_path(self, table: str) -> str:
        return self._subscriber.subscription_path(self._project_id, table)

    def get_schema(self, table: str) -> List[ColumnSchema]:
        """Pull one message to infer schema; return empty list if no messages yet."""
        path = self._subscription_path(table)
        try:
            response = self._subscriber.pull(
                request={"subscription": path, "max_messages": 1},
                timeout=self._pull_timeout,
            )
        except Exception:
            return []

        if not response.received_messages:
            return []

        msg = response.received_messages[0].message
        # Immediately nack so Pub/Sub redelivers — we're just peeking at the schema
        self._subscriber.modify_ack_deadline(
            request={
                "subscription": path,
                "ack_ids": [response.received_messages[0].ack_id],
                "ack_deadline_seconds": 0,
            }
        )
        try:
            row = json.loads(msg.data.decode("utf-8"))
            row = _normalise_row(row)
            return _infer_schema(row, self._pk_fields)
        except Exception:
            return []

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        """Pub/Sub has no historical store — snapshot is a no-op."""
        logger.info(
            "[%s/%s] Pub/Sub has no snapshot — set snapshot_on_first_run: false "
            "in config to suppress this message.",
            self.name, table,
        )
        return
        yield  # make this a generator

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        """
        Pull messages from the subscription in a loop, yielding one ChangeEvent
        per message. Yields None (heartbeat) when no messages are available so
        the engine can flush partial batches on timeout.

        ack_ids are accumulated in _pending_acks[table] and only acked after
        on_batch_committed() is called by the engine.
        """
        path = self._subscription_path(table)
        lock = self._pending_locks[table]

        # Start background deadline extender for this subscription
        extender = _DeadlineExtender(
            self._subscriber, path,
            self._pending_acks[table], lock,
            self._ack_deadline,
        )
        extender.start()
        self._extenders[table] = extender

        logger.info("[%s/%s] Streaming from subscription %s", self.name, table, path)

        try:
            while True:
                try:
                    response = self._subscriber.pull(
                        request={
                            "subscription": path,
                            "max_messages": self._max_messages,
                        },
                        timeout=self._pull_timeout,
                    )
                except Exception as exc:
                    logger.warning("[%s/%s] Pull error: %s — retrying in 5s", self.name, table, exc)
                    time.sleep(5)
                    yield None  # heartbeat so engine can flush pending batch
                    continue

                if not response.received_messages:
                    yield None  # heartbeat — no messages right now
                    continue

                for received in response.received_messages:
                    msg     = received.message
                    ack_id  = received.ack_id

                    try:
                        raw  = msg.data.decode("utf-8")
                        row  = json.loads(raw)
                        row  = _normalise_row(row)
                    except Exception as exc:
                        logger.warning(
                            "[%s/%s] Failed to decode message %s: %s — skipping",
                            self.name, table, msg.message_id, exc,
                        )
                        # Ack bad messages immediately so they don't block the queue
                        self._subscriber.acknowledge(
                            request={"subscription": path, "ack_ids": [ack_id]}
                        )
                        continue

                    # Add to pending before yielding — extender may pick it up immediately
                    with lock:
                        self._pending_acks[table].append(ack_id)

                    op      = _parse_op(row, self._op_field, self._op_map)
                    schema  = _infer_schema(row, self._pk_fields)

                    # publish_time may be a Protobuf Timestamp or a Python datetime
                    pub_ts = msg.publish_time
                    if pub_ts:
                        if hasattr(pub_ts, "ToDatetime"):
                            ts = pub_ts.ToDatetime(tzinfo=timezone.utc)
                        elif isinstance(pub_ts, datetime):
                            ts = pub_ts if pub_ts.tzinfo else pub_ts.replace(tzinfo=timezone.utc)
                        else:
                            ts = datetime.now(timezone.utc)
                    else:
                        ts = datetime.now(timezone.utc)

                    before = row if op == Operation.DELETE else None
                    after  = row if op != Operation.DELETE else None

                    yield ChangeEvent(
                        op=op,
                        source_name=self.name,
                        source_table=table,
                        before=before,
                        after=after,
                        schema=schema,
                        timestamp=ts,
                        offset=msg.message_id,   # used by engine for lag tracking
                    )

        finally:
            extender.stop()

    def on_batch_committed(self, table: str, offset: Any):
        """
        Called by the engine after a batch has been successfully written to Iceberg.
        Acks all pending messages for this subscription.
        """
        path = self._subscription_path(table)
        lock = self._pending_locks[table]

        with lock:
            ack_ids = list(self._pending_acks[table])
            self._pending_acks[table].clear()

        if not ack_ids:
            return

        # Pub/Sub ack accepts up to 1000 ack_ids per request
        chunk_size = 1000
        for i in range(0, len(ack_ids), chunk_size):
            chunk = ack_ids[i : i + chunk_size]
            try:
                self._subscriber.acknowledge(
                    request={"subscription": path, "ack_ids": chunk}
                )
            except Exception as exc:
                logger.error(
                    "[%s/%s] Failed to ack %d message(s): %s — Pub/Sub will redeliver",
                    self.name, table, len(chunk), exc,
                )

        logger.debug("[%s/%s] Acked %d message(s)", self.name, table, len(ack_ids))

    def close(self):
        for extender in self._extenders.values():
            extender.stop()
        if self._subscriber:
            self._subscriber.close()
