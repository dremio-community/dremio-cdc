"""
Dead Letter Queue — persists failed batches and supports automatic/manual replay.

When a flush to Dremio fails, events land here instead of being dropped.
A DLQWorker thread retries pending entries at a configurable interval.

Entry statuses:
  pending   — waiting to be retried
  replayed  — successfully re-flushed
  exhausted — exceeded max_retries; needs human action
  discarded — manually discarded via API/UI
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation

logger = logging.getLogger(__name__)


# ── Event serialization ───────────────────────────────────────────────────────

def _serialize_event(ev: ChangeEvent) -> dict:
    return {
        "op":           ev.op.value,
        "source_name":  ev.source_name,
        "source_table": ev.source_table,
        "before":       ev.before,
        "after":        ev.after,
        "schema": [
            {"name": c.name, "data_type": c.data_type,
             "nullable": c.nullable, "primary_key": c.primary_key}
            for c in ev.schema
        ],
        "timestamp": ev.timestamp.isoformat() if ev.timestamp else None,
        "offset":    str(ev.offset) if ev.offset is not None else None,
        "tx_id":     ev.tx_id,
    }


def _deserialize_event(d: dict) -> ChangeEvent:
    ts_str = d.get("timestamp")
    ts = datetime.fromisoformat(ts_str) if ts_str else datetime.now(timezone.utc)
    return ChangeEvent(
        op=Operation(d["op"]),
        source_name=d["source_name"],
        source_table=d["source_table"],
        before=d.get("before"),
        after=d.get("after"),
        schema=[
            ColumnSchema(
                c["name"], c["data_type"],
                c.get("nullable", True), c.get("primary_key", False),
            )
            for c in d.get("schema", [])
        ],
        timestamp=ts,
        offset=d.get("offset"),
        tx_id=d.get("tx_id"),
    )


# ── Store ─────────────────────────────────────────────────────────────────────

class DeadLetterQueue:
    def __init__(self, db_path: str = "./cdc_dlq.db", max_retries: int = 3):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._max_retries = max_retries
        self._init()

    def _init(self):
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS dlq_entries (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    source       TEXT NOT NULL,
                    tbl          TEXT NOT NULL,
                    events_json  TEXT NOT NULL,
                    event_count  INTEGER NOT NULL DEFAULT 0,
                    error        TEXT,
                    retry_count  INTEGER NOT NULL DEFAULT 0,
                    max_retries  INTEGER NOT NULL DEFAULT 3,
                    status       TEXT NOT NULL DEFAULT 'pending',
                    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """)
            self._conn.commit()

    def push(self, source: str, table: str, batch: List[ChangeEvent], error: str) -> int:
        events_json = json.dumps([_serialize_event(ev) for ev in batch])
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO dlq_entries
                   (source, tbl, events_json, event_count, error, max_retries)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (source, table, events_json, len(batch), error, self._max_retries),
            )
            self._conn.commit()
            entry_id = cur.lastrowid
        logger.warning("DLQ: parked %d events from %s/%s (entry %d): %s",
                       len(batch), source, table, entry_id, error)
        return entry_id

    def get_pending(self, limit: int = 50) -> List[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, source, tbl, event_count, error, retry_count, max_retries, status, created_at "
                "FROM dlq_entries WHERE status='pending' ORDER BY id ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]

    def get_all(self, limit: int = 200) -> List[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, source, tbl, event_count, error, retry_count, max_retries, status, created_at "
                "FROM dlq_entries ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]

    def get_events(self, entry_id: int) -> List[ChangeEvent]:
        with self._lock:
            row = self._conn.execute(
                "SELECT events_json FROM dlq_entries WHERE id=?", (entry_id,)
            ).fetchone()
        if not row:
            return []
        return [_deserialize_event(d) for d in json.loads(row[0])]

    def mark_replayed(self, entry_id: int):
        self._set_status(entry_id, "replayed")

    def mark_failed(self, entry_id: int, error: str):
        with self._lock:
            self._conn.execute(
                """UPDATE dlq_entries
                   SET retry_count = retry_count + 1,
                       error       = ?,
                       status      = CASE
                           WHEN retry_count + 1 >= max_retries THEN 'exhausted'
                           ELSE 'pending'
                       END,
                       updated_at  = datetime('now')
                   WHERE id = ?""",
                (error, entry_id),
            )
            self._conn.commit()

    def reset_to_pending(self, entry_id: int):
        with self._lock:
            self._conn.execute(
                "UPDATE dlq_entries SET status='pending', retry_count=0, "
                "updated_at=datetime('now') WHERE id=?",
                (entry_id,),
            )
            self._conn.commit()

    def discard(self, entry_id: int):
        self._set_status(entry_id, "discarded")

    def discard_all(self):
        with self._lock:
            self._conn.execute(
                "UPDATE dlq_entries SET status='discarded', updated_at=datetime('now') "
                "WHERE status IN ('pending', 'exhausted')"
            )
            self._conn.commit()

    def reset_all_exhausted(self):
        with self._lock:
            self._conn.execute(
                "UPDATE dlq_entries SET status='pending', retry_count=0, "
                "updated_at=datetime('now') WHERE status='exhausted'"
            )
            self._conn.commit()

    def stats(self) -> dict:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*), COALESCE(SUM(event_count), 0) "
                "FROM dlq_entries GROUP BY status"
            ).fetchall()
        result = {s: {"entries": 0, "events": 0}
                  for s in ("pending", "replayed", "exhausted", "discarded")}
        for status, cnt, ev_cnt in rows:
            if status in result:
                result[status] = {"entries": cnt, "events": ev_cnt}
        return result

    def pending_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM dlq_entries WHERE status='pending'"
            ).fetchone()
        return row[0] if row else 0

    def _set_status(self, entry_id: int, status: str):
        with self._lock:
            self._conn.execute(
                "UPDATE dlq_entries SET status=?, updated_at=datetime('now') WHERE id=?",
                (status, entry_id),
            )
            self._conn.commit()


def _row_to_dict(row) -> dict:
    id_, source, tbl, event_count, error, retry_count, max_retries, status, created_at = row
    return {
        "id":          id_,
        "source":      source,
        "table":       tbl,
        "event_count": event_count,
        "error":       error,
        "retry_count": retry_count,
        "max_retries": max_retries,
        "status":      status,
        "created_at":  created_at,
    }


# ── Retry worker ──────────────────────────────────────────────────────────────

class DLQWorker(threading.Thread):
    """Background thread that replays pending DLQ entries against the sink."""

    def __init__(self, dlq: DeadLetterQueue, sink: Any, interval_s: float = 60.0):
        super().__init__(daemon=True, name="DLQWorker")
        self._dlq      = dlq
        self._sink     = sink
        self._interval = interval_s
        self._stop     = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        while not self._stop.wait(self._interval):
            try:
                self._retry_pending()
            except Exception as exc:
                logger.error("DLQWorker error: %s", exc)

    def _retry_pending(self):
        pending = self._dlq.get_pending(limit=10)
        if not pending:
            return
        logger.info("DLQWorker: retrying %d pending entries", len(pending))
        for entry in pending:
            eid = entry["id"]
            try:
                events = self._dlq.get_events(eid)
                if not events:
                    self._dlq.discard(eid)
                    continue
                self._sink.write_batch(events)
                self._dlq.mark_replayed(eid)
                logger.info("DLQWorker: replayed entry %d (%d events from %s/%s)",
                            eid, len(events), entry["source"], entry["table"])
            except Exception as exc:
                self._dlq.mark_failed(eid, str(exc))
                logger.warning("DLQWorker: replay failed for entry %d: %s", eid, exc)
