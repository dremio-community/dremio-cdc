"""
CDC Engine — orchestrates sources, batching, and the sink.

For each configured source + table pair it:
  1. Runs an initial snapshot if no saved offset exists
  2. Streams change events indefinitely
  3. Collects events into batches (size or timeout based)
  4. Writes each batch to the configured sink (Dremio SQL or Iceberg direct)
  5. Commits the offset only after a successful write
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Type, Union

from core.dremio_sink import DremioSink
from core.dlq import DeadLetterQueue, DLQWorker
from core.event import ChangeEvent
from core.masking import MaskingEngine
from core.offset_store import get_offset_store
from core.schema_store import SchemaStore
from core.status_store import StatusStore
from core.ts_trigger import TransformStudioTrigger
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_SOURCE_REGISTRY: Dict[str, Type[CDCSource]] = {}


class AdaptiveBatchTuner:
    """
    Adjusts batch_size after every flush using two signals:
      - Throughput (events/s) → high throughput → grow batch (fewer, larger flushes)
      - Lag → low lag → shrink batch (stay responsive when idle)

    Changes are capped at ±50 % per step to avoid oscillation.
    """

    def __init__(self, initial: int, min_size: int, max_size: int, batch_timeout: float):
        self.batch_size = max(min_size, min(max_size, initial))
        self._min = min_size
        self._max = max_size
        self._timeout = batch_timeout

    def tune(self, flush_count: int, flush_duration_s: float, lag_s: Optional[float]) -> int:
        if flush_duration_s <= 0 or flush_count == 0:
            return self.batch_size

        # Target: match the number of events that arrive in one timeout window
        eps = flush_count / flush_duration_s
        ideal = int(eps * self._timeout)

        # Lag guard: when we're current (lag < 2 s) don't over-buffer
        if lag_s is not None and lag_s < 2.0:
            ideal = min(ideal, self._min * 2)

        ideal = max(self._min, min(self._max, ideal))

        # Smooth: max 50 % change per step
        lo = max(self._min, int(self.batch_size * 0.67))
        hi = min(self._max, int(self.batch_size * 1.50))
        self.batch_size = max(lo, min(hi, ideal))
        return self.batch_size


def register_source(type_name: str, cls: Type[CDCSource]):
    _SOURCE_REGISTRY[type_name] = cls


def _load_sources():
    from sources.postgres    import PostgresSource
    from sources.mysql       import MySQLSource
    from sources.mariadb     import MariaDBSource
    from sources.mongodb     import MongoDBSource
    from sources.dynamodb    import DynamoDBSource
    from sources.debezium    import DebeziumSource
    from sources.oracle      import OracleSource
    from sources.sqlserver   import SQLServerSource
    from sources.snowflake_src import SnowflakeSource
    from sources.cockroachdb import CockroachDBSource
    from sources.spanner     import SpannerSource
    from sources.pubsub      import PubSubSource
    from sources.datastream  import DatastreamSource

    register_source("postgres",    PostgresSource)
    register_source("mysql",       MySQLSource)
    register_source("mariadb",     MariaDBSource)
    register_source("mongodb",     MongoDBSource)
    register_source("dynamodb",    DynamoDBSource)
    register_source("debezium",    DebeziumSource)
    register_source("oracle",      OracleSource)
    register_source("db2",         DebeziumSource)
    register_source("sqlserver",   SQLServerSource)
    register_source("snowflake",   SnowflakeSource)
    register_source("cockroachdb", CockroachDBSource)
    register_source("spanner",     SpannerSource)
    register_source("pubsub",      PubSubSource)
    register_source("datastream",  DatastreamSource)


class TableWorker(threading.Thread):
    """One worker thread per (source, table) pair."""

    def __init__(
        self,
        source: CDCSource,
        table: str,
        sink: Union[DremioSink, "IcebergSink"],
        offset_store,
        status_store: StatusStore,
        options: Dict[str, Any],
        schema_store: Optional[SchemaStore] = None,
        dlq: Optional[DeadLetterQueue] = None,
    ):
        super().__init__(daemon=True, name=f"{source.name}/{table}")
        self.source        = source
        self.table         = table
        self.sink          = sink
        self.offset_store  = offset_store
        self.status        = status_store
        self.batch_size    = options.get("batch_size", 500)
        self.batch_timeout = options.get("batch_timeout_seconds", 10)
        self.do_snapshot   = options.get("snapshot_on_first_run", True)
        self._stop_flag    = threading.Event()

        adaptive = options.get("adaptive_batching", True)
        self._tuner: Optional[AdaptiveBatchTuner] = (
            AdaptiveBatchTuner(
                initial=self.batch_size,
                min_size=options.get("min_batch_size", 100),
                max_size=options.get("max_batch_size", 5000),
                batch_timeout=self.batch_timeout,
            ) if adaptive else None
        )

        # Incremental snapshot
        self._incr_snapshot   = options.get("incremental_snapshot", False)
        self._snap_chunk_size = options.get("snapshot_chunk_size", 10_000)
        self._snap_cursor_col = options.get("snapshot_cursor_column") or None

        # Schema drift
        self._schema_store  = schema_store
        self._drift_action  = options.get("schema_drift_action", "alert")
        self._drift_every   = options.get("schema_drift_check_every_n_batches", 10)
        self._flush_count   = 0

        # Dead letter queue
        self._dlq = dlq

        # Column masking and Transform Studio trigger (set by CDCEngine after construction)
        self._masker: Optional[MaskingEngine] = None
        self._ts_trigger: Optional[TransformStudioTrigger] = None

        self.status.register(source.name, table)
        self.status.set_batch_size(source.name, table, self.batch_size)

    def stop(self):
        self._stop_flag.set()

    def run(self):
        source_name = self.source.name
        table       = self.table

        try:
            # ── Initial snapshot ──────────────────────────────────────────────
            offset = self.offset_store.get(source_name, table)
            snap_in_progress = isinstance(offset, str) and offset.startswith("snap:") and offset != "snap:done"
            need_snapshot    = (offset is None or snap_in_progress) and self.do_snapshot

            if need_snapshot:
                self.status.set_state(source_name, table, "snapshotting")
                from sources.base import CDCSource as _Base
                use_incr = (
                    self._incr_snapshot
                    and type(self.source).incremental_snapshot is not _Base.incremental_snapshot
                )

                if use_incr:
                    self._run_incremental_snapshot(table, source_name, offset if snap_in_progress else None)
                else:
                    logger.info("[%s/%s] Starting full snapshot", source_name, table)
                    batch: List[ChangeEvent] = []
                    for ev in self.source.snapshot(table):
                        if self._stop_flag.is_set():
                            return
                        batch.append(ev)
                        if len(batch) >= self.batch_size:
                            self._flush(batch, offset=None)
                            batch = []
                    if batch:
                        self._flush(batch, offset=None)
                    self.offset_store.set(source_name, table, "snap:done")
                    logger.info("[%s/%s] Full snapshot complete", source_name, table)

                # Re-read offset; "snap:done" → treat as None for streaming
                offset = self.offset_store.get(source_name, table)

            if isinstance(offset, str) and offset.startswith("snap:"):
                offset = None   # stream from slot/binlog beginning

            # ── Streaming loop ────────────────────────────────────────────────
            self.status.set_state(source_name, table, "streaming")
            logger.info("[%s/%s] Starting stream (offset=%s)", source_name, table, offset)
            batch = []
            last_flush = time.time()
            last_offset = None

            for ev in self.source.stream(table, offset):
                if self._stop_flag.is_set():
                    break
                now = time.time()
                if ev is None:  # heartbeat from source (no events in queue window)
                    if batch and (now - last_flush) >= self.batch_timeout:
                        self._flush(batch, last_offset)
                        batch = []
                        last_flush = now
                    continue
                batch.append(ev)
                if ev.offset is not None:
                    last_offset = ev.offset

                if len(batch) >= self.batch_size or (now - last_flush) >= self.batch_timeout:
                    self._flush(batch, last_offset)
                    batch = []
                    last_flush = now

            if batch:
                self._flush(batch, last_offset)

            self.status.set_state(source_name, table, "paused")

        except Exception as exc:
            self.status.set_state(source_name, table, "error", error=str(exc))
            logger.error("[%s/%s] Worker crashed: %s", source_name, table, exc)

    def _run_incremental_snapshot(self, table: str, source_name: str, resume_offset: Optional[str]):
        """Cursor-based chunked snapshot. Saves progress after every chunk so restarts resume mid-table."""
        cursor_col = self._snap_cursor_col or self.source.get_pk_column(table)
        if not cursor_col:
            logger.warning("[%s/%s] No cursor column found — falling back to full snapshot", source_name, table)
            for ev in self.source.snapshot(table):
                if self._stop_flag.is_set():
                    return
            return

        # Parse resume point from saved offset "snap:{col}:{val}"
        last_val: Optional[str] = None
        if resume_offset:
            parts = resume_offset.split(":", 2)
            if len(parts) == 3:
                cursor_col = parts[1]
                last_val   = parts[2]

        logger.info("[%s/%s] Incremental snapshot (cursor=%s, start_after=%s, chunk=%d)",
                    source_name, table, cursor_col, last_val, self._snap_chunk_size)
        total = 0
        while not self._stop_flag.is_set():
            chunk = list(self.source.incremental_snapshot(
                table, cursor_col, last_val, self._snap_chunk_size
            ))
            if not chunk:
                break

            self._flush(chunk, offset=None)
            total += len(chunk)

            last_val = str(chunk[-1].after.get(cursor_col, last_val))
            self.offset_store.set(source_name, table, f"snap:{cursor_col}:{last_val}")
            logger.info("[%s/%s] Snapshot chunk: %d rows (total %d, cursor=%s)",
                        source_name, table, len(chunk), total, last_val)

        if not self._stop_flag.is_set():
            self.offset_store.set(source_name, table, "snap:done")
            logger.info("[%s/%s] Incremental snapshot complete (%d rows)", source_name, table, total)

    def _flush(self, batch: List[ChangeEvent], offset: Any):
        if not batch:
            return
        source_ts = min(
            (ev.timestamp.timestamp() for ev in batch if ev.timestamp is not None),
            default=None,
        )
        flush_start = time.time()
        try:
            if self._masker is not None:
                batch = self._masker.apply_batch(self.table, batch)
            self.sink.write_batch(batch)
            flush_duration_s = time.time() - flush_start
            flush_duration_ms = flush_duration_s * 1000
            if offset is not None:
                self.offset_store.set(self.source.name, self.table, offset)
            self.source.on_batch_committed(self.table, offset)
            self.status.record_flush(
                self.source.name, self.table, len(batch), offset,
                source_ts=source_ts, flush_duration_ms=flush_duration_ms,
            )
            # Adaptive batch tuning
            if self._tuner is not None:
                snap = self.status.snapshot()
                lag_s = next(
                    (w["lag_seconds"] for w in snap["workers"]
                     if w["source"] == self.source.name and w["table"] == self.table),
                    None,
                )
                new_size = self._tuner.tune(len(batch), flush_duration_s, lag_s)
                self.batch_size = new_size
                self.status.set_batch_size(self.source.name, self.table, new_size)
            logger.info("[%s/%s] Flushed %d events (batch_size=%d)",
                        self.source.name, self.table, len(batch), self.batch_size)
            # Transform Studio trigger
            if self._ts_trigger is not None:
                self._ts_trigger.trigger(self.source.name, self.table, len(batch))
            # Schema drift check (every N successful flushes)
            self._flush_count += 1
            if self._schema_store and self._flush_count % self._drift_every == 0:
                self._check_schema_drift()

        except Exception as exc:
            self.status.record_error(self.source.name, self.table, str(exc))
            logger.error(
                "[%s/%s] Flush failed (%d events): %s",
                self.source.name, self.table, len(batch), exc,
            )
            if self._dlq is not None:
                self._dlq.push(self.source.name, self.table, batch, str(exc))
            else:
                time.sleep(5)

    def _check_schema_drift(self):
        try:
            current = self.source.get_schema(self.table)
        except Exception:
            return

        stored = self._schema_store.get(self.source.name, self.table)
        if stored is None:
            self._schema_store.set(self.source.name, self.table, current)
            return

        stored_map = {c.name: c.data_type for c in stored}
        current_map = {c.name: c.data_type for c in current}

        added        = [c for c in current if c.name not in stored_map]
        removed      = [c for c in stored   if c.name not in current_map]
        type_changed = [c for c in current  if c.name in stored_map
                        and stored_map[c.name] != c.data_type]

        if not (added or removed or type_changed):
            return

        parts = []
        if added:
            parts.append(f"+{len(added)}: {', '.join(c.name for c in added)}")
        if removed:
            parts.append(f"-{len(removed)}: {', '.join(c.name for c in removed)}")
        if type_changed:
            parts.append(f"~{len(type_changed)} type change(s)")
        drift_msg = "; ".join(parts)

        logger.warning("[%s/%s] Schema drift: %s", self.source.name, self.table, drift_msg)
        self.status.set_drift(self.source.name, self.table, drift_msg)

        if self._drift_action == "auto_migrate" and added and hasattr(self.sink, "evolve_schema"):
            self.sink.evolve_schema(self.table, current)
            logger.info("[%s/%s] Auto-migrated %d new column(s)", self.source.name, self.table, len(added))

        if self._drift_action == "pause":
            self._stop_flag.set()
            self.status.set_state(self.source.name, self.table, "paused")

        self._schema_store.set(self.source.name, self.table, current)


class CDCEngine:
    def __init__(self, cfg: Dict[str, Any], status_store: Optional[StatusStore] = None):
        from core.config import get_dremio_config, get_options, get_source_configs
        _load_sources()

        options      = get_options(cfg)
        dremio_cfg   = get_dremio_config(cfg)
        source_cfgs  = get_source_configs(cfg)
        sink_mode    = options.get("sink_mode", "dremio").lower()

        self.status_store = status_store or StatusStore()
        self.offset_store = get_offset_store(options.get("offset_db_path", "./cdc_offsets.db"))
        self.schema_store = SchemaStore(options.get("schema_db_path", "./cdc_schemas.db"))
        self.dlq = DeadLetterQueue(
            db_path=options.get("dlq_db_path", "./cdc_dlq.db"),
            max_retries=options.get("dlq_max_retries", 3),
        )
        self._dlq_worker: Optional[DLQWorker] = None

        # ── Sink pool: one sink per unique target namespace ───────────────────
        # Each source can override target_namespace; sinks are shared when the
        # namespace is the same so we don't open redundant REST sessions.
        self._sinks: Dict[str, Any] = {}   # namespace -> sink instance

        def _get_sink(namespace: str):
            if namespace in self._sinks:
                return self._sinks[namespace]
            if sink_mode == "iceberg":
                from core.iceberg_sink import IcebergSink
                iceberg_cfg_copy = dict(cfg.get("iceberg", {}))
                iceberg_cfg_copy["target_namespace"] = namespace
                s = IcebergSink(iceberg_cfg=iceberg_cfg_copy, dremio_cfg=dremio_cfg)
            else:
                dremio_cfg_copy = dict(dremio_cfg)
                dremio_cfg_copy["target_namespace"] = namespace
                s = DremioSink(dremio_cfg_copy)
            self._sinks[namespace] = s
            return s

        global_ns = dremio_cfg.get("target_namespace", "cdc")
        if sink_mode == "iceberg":
            from core.iceberg_sink import IcebergSink
            iceberg_cfg = cfg.get("iceberg", {})
            self._sinks[global_ns] = IcebergSink(iceberg_cfg=iceberg_cfg, dremio_cfg=dremio_cfg)
            logger.info("Sink mode: Iceberg direct (Mode B)")
        else:
            self._sinks[global_ns] = DremioSink(dremio_cfg)
            logger.info("Sink mode: Dremio SQL (Mode A)")

        # Expose primary sink for DLQ worker (uses global namespace)
        self.sink = self._sinks[global_ns]

        self.workers: List[TableWorker] = []

        # Global Transform Studio trigger (shared across all workers)
        from core.ts_trigger import build_trigger
        ts_cfg = cfg.get("transform_studio", {})
        self._ts_trigger = build_trigger(ts_cfg)
        if self._ts_trigger:
            logger.info("Transform Studio integration enabled (pipeline: %s)", ts_cfg.get("pipeline_id"))

        for sc in source_cfgs:
            src_type = sc.get("type", "").lower()
            cls = _SOURCE_REGISTRY.get(src_type)
            if not cls:
                raise ValueError(f"Unknown source type '{src_type}'. "
                                 f"Available: {list(_SOURCE_REGISTRY)}")
            source = cls(name=sc["name"], cfg=sc)
            source.connect()

            # Per-source column masking config
            masking_rules = sc.get("masking", {})
            masker = MaskingEngine(masking_rules) if masking_rules else None

            # Use source-level namespace if set, else fall back to global
            source_ns = sc.get("target_namespace") or global_ns
            source_sink = _get_sink(source_ns)
            if source_ns != global_ns:
                logger.info("Source '%s' → namespace '%s'", sc["name"], source_ns)

            for table in source.tables:
                worker = TableWorker(
                    source, table, source_sink,
                    self.offset_store, self.status_store, options,
                    schema_store=self.schema_store,
                    dlq=self.dlq,
                )
                worker._masker = masker
                worker._ts_trigger = self._ts_trigger
                self.workers.append(worker)

    def start(self):
        for s in self._sinks.values():
            s.connect()
        self.status_store.set_engine_state("running")
        for w in self.workers:
            logger.info("Starting worker %s", w.name)
            w.start()
        self._dlq_worker = DLQWorker(
            self.dlq, self.sink,
            interval_s=60.0,
        )
        self._dlq_worker.start()
        logger.info("CDC engine running — %d worker(s)", len(self.workers))

    def stop(self):
        for w in self.workers:
            w.stop()
        if self._dlq_worker:
            self._dlq_worker.stop()
        for w in self.workers:
            w.join(timeout=10)
        self.status_store.set_engine_state("stopped")
        logger.info("CDC engine stopped")

    def join(self):
        """Block until all workers finish (or Ctrl-C)."""
        try:
            while any(w.is_alive() for w in self.workers):
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down…")
            self.stop()
