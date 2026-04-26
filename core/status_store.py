"""
Thread-safe store for per-worker status metrics.
Workers write here; the UI backend and /metrics endpoint read here.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple


@dataclass
class WorkerStatus:
    source_name: str
    table: str
    state: str = "idle"                   # idle | snapshotting | streaming | paused | error
    events_written: int = 0
    error_count: int = 0
    last_source_ts: Optional[float] = None  # unix ts of most recent source event
    last_flush_ts: Optional[float] = None   # unix ts of last successful write
    last_flush_duration_ms: float = 0.0
    last_offset: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    # Rolling 60-second event rate
    events_last_minute: int = 0
    _minute_bucket: int = 0
    _bucket_start: float = field(default_factory=time.time)
    # Sparkline: (unix_ts, events_per_minute) samples, last 10 minutes
    _rate_history: Deque[Tuple[float, int]] = field(default_factory=lambda: deque(maxlen=20))
    # Adaptive batch tuner — current dynamic batch size
    current_batch_size: int = 500
    # Schema drift — set when a column change is detected
    schema_drift: Optional[str] = None


class StatusStore:
    """Singleton-ish store; one per CDCEngine instance."""

    def __init__(self):
        self._lock = threading.Lock()
        self._workers: Dict[str, WorkerStatus] = {}
        self._engine_state: str = "stopped"
        self._engine_started_at: Optional[float] = None

    # ── Engine-level ──────────────────────────────────────────────────────────

    def set_engine_state(self, state: str):
        with self._lock:
            self._engine_state = state
            if state == "running" and self._engine_started_at is None:
                self._engine_started_at = time.time()
            elif state == "stopped":
                self._engine_started_at = None

    def get_engine_state(self) -> str:
        with self._lock:
            return self._engine_state

    # ── Worker-level ──────────────────────────────────────────────────────────

    def _key(self, source: str, table: str) -> str:
        return f"{source}/{table}"

    def register(self, source: str, table: str):
        key = self._key(source, table)
        with self._lock:
            self._workers[key] = WorkerStatus(
                source_name=source, table=table,
                started_at=time.time(), state="idle",
            )

    def set_state(self, source: str, table: str, state: str, error: str = None):
        key = self._key(source, table)
        with self._lock:
            w = self._workers.get(key)
            if w:
                w.state = state
                if error:
                    w.error = error

    def record_flush(
        self,
        source: str,
        table: str,
        count: int,
        offset=None,
        source_ts: Optional[float] = None,
        flush_duration_ms: float = 0.0,
    ):
        key = self._key(source, table)
        now = time.time()
        with self._lock:
            w = self._workers.get(key)
            if not w:
                return
            w.events_written += count
            w.last_flush_ts = now
            w.last_flush_duration_ms = flush_duration_ms
            if source_ts is not None:
                w.last_source_ts = source_ts
            if offset is not None:
                w.last_offset = str(offset)
            # Rolling 60-second event rate
            if now - w._bucket_start > 60:
                w.events_last_minute = w._minute_bucket
                w._rate_history.append((w._bucket_start, w._minute_bucket))
                w._minute_bucket = 0
                w._bucket_start = now
            w._minute_bucket += count

    def record_error(self, source: str, table: str):
        key = self._key(source, table)
        with self._lock:
            w = self._workers.get(key)
            if w:
                w.error_count += 1

    def set_batch_size(self, source: str, table: str, size: int):
        key = self._key(source, table)
        with self._lock:
            w = self._workers.get(key)
            if w:
                w.current_batch_size = size

    def set_drift(self, source: str, table: str, summary: Optional[str]):
        key = self._key(source, table)
        with self._lock:
            w = self._workers.get(key)
            if w:
                w.schema_drift = summary

    def snapshot(self) -> dict:
        with self._lock:
            now = time.time()
            workers = []
            total_events = 0
            total_errors = 0

            for w in self._workers.values():
                # Current-bucket rate
                epm = w.events_last_minute
                if now - w._bucket_start <= 60:
                    epm = w._minute_bucket

                # Lag: how long since source produced an event we processed
                lag_s = None
                if w.last_source_ts and w.state in ("streaming", "snapshotting"):
                    lag_s = round(now - w.last_source_ts, 2)

                # Pipeline lag: age of data at the moment it landed in the sink
                pipeline_lag_s = None
                if w.last_source_ts and w.last_flush_ts:
                    pipeline_lag_s = round(w.last_flush_ts - w.last_source_ts, 2)

                # Sparkline history: recent (ts, epm) samples
                history = list(w._rate_history)

                total_events += w.events_written
                total_errors += w.error_count

                workers.append({
                    "source":               w.source_name,
                    "table":                w.table,
                    "state":                w.state,
                    "events_written":       w.events_written,
                    "events_per_minute":    epm,
                    "lag_seconds":          lag_s,
                    "pipeline_lag_seconds": pipeline_lag_s,
                    "last_flush_duration_ms": w.last_flush_duration_ms,
                    "error_count":          w.error_count,
                    "current_batch_size":   w.current_batch_size,
                    "schema_drift":         w.schema_drift,
                    "last_source_ts":       w.last_source_ts,
                    "last_flush_ts":        w.last_flush_ts,
                    "last_offset":          w.last_offset,
                    "error":                w.error,
                    "started_at":           w.started_at,
                    "rate_history":         history,
                })

            return {
                "engine_state":   self._engine_state,
                "engine_started_at": self._engine_started_at,
                "workers":        workers,
                "summary": {
                    "total_events":  total_events,
                    "total_errors":  total_errors,
                    "active_workers": sum(1 for w in self._workers.values() if w.state == "streaming"),
                    "total_workers":  len(self._workers),
                },
            }
