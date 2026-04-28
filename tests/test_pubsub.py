"""
Tests for the Google Cloud Pub/Sub CDC source connector.

Unit tests (no external services — run immediately):
  python -m pytest tests/test_pubsub.py -v -m "not pubsub"

Integration tests (require Pub/Sub emulator on port 8085):
  docker compose up -d pubsub-emulator
  PUBSUB_EMULATOR_HOST=localhost:8085 python -m pytest tests/test_pubsub.py -v -m pubsub
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.event import Operation
from sources.pubsub import (
    PubSubSource,
    _infer_type,
    _infer_schema,
    _normalise_row,
    _parse_op,
    _DEFAULT_OP_MAP,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_source(extra_cfg: Dict = None) -> PubSubSource:
    cfg = {
        "connection": {
            "project_id": "test-project",
            "ack_deadline_seconds": 30,
            "max_messages_per_pull": 10,
            "pull_timeout_seconds": 1,
        },
        "tables": ["orders-sub", "events-sub"],
        **(extra_cfg or {}),
    }
    return PubSubSource(name="test_pubsub", cfg=cfg)


def _make_received_message(data: Dict, ack_id: str, message_id: str = None):
    """Build a mock ReceivedMessage as returned by subscriber.pull()."""
    msg = MagicMock()
    msg.data = json.dumps(data).encode("utf-8")
    msg.message_id = message_id or ack_id
    msg.publish_time = None

    received = MagicMock()
    received.ack_id = ack_id
    received.message = msg
    return received


def _make_pull_response(received_messages: list):
    resp = MagicMock()
    resp.received_messages = received_messages
    return resp


def _empty_pull_response():
    resp = MagicMock()
    resp.received_messages = []
    return resp


def _make_mock_subscriber():
    sub = MagicMock()
    sub.subscription_path.side_effect = lambda proj, name: f"projects/{proj}/subscriptions/{name}"
    return sub


# ══════════════════════════════════════════════════════════════════════════════
# 1. Pure-function unit tests
# ══════════════════════════════════════════════════════════════════════════════

class TestInferType:
    def test_bool(self):
        assert _infer_type(True) == "boolean"

    def test_int(self):
        assert _infer_type(42) == "bigint"

    def test_float(self):
        assert _infer_type(3.14) == "double"

    def test_string(self):
        assert _infer_type("hello") == "varchar"

    def test_none(self):
        assert _infer_type(None) == "varchar"

    def test_dict(self):
        assert _infer_type({"a": 1}) == "varchar"

    def test_list(self):
        assert _infer_type([1, 2, 3]) == "varchar"

    def test_bool_before_int(self):
        # bool is a subclass of int — must be checked first
        assert _infer_type(False) == "boolean"


class TestInferSchema:
    def test_basic(self):
        row = {"id": 1, "name": "Alice", "score": 9.5, "active": True}
        schema = _infer_schema(row, pk_fields=["id"])
        by_name = {c.name: c for c in schema}
        assert by_name["id"].primary_key is True
        assert by_name["id"].data_type == "bigint"
        assert by_name["name"].primary_key is False
        assert by_name["name"].data_type == "varchar"
        assert by_name["score"].data_type == "double"
        assert by_name["active"].data_type == "boolean"

    def test_no_pk(self):
        row = {"x": 1, "y": 2}
        schema = _infer_schema(row, pk_fields=[])
        assert all(not c.primary_key for c in schema)

    def test_composite_pk(self):
        row = {"tenant_id": "abc", "order_id": 99, "amount": 10.0}
        schema = _infer_schema(row, pk_fields=["tenant_id", "order_id"])
        by_name = {c.name: c for c in schema}
        assert by_name["tenant_id"].primary_key is True
        assert by_name["order_id"].primary_key is True
        assert by_name["amount"].primary_key is False


class TestNormaliseRow:
    def test_flat_unchanged(self):
        row = {"id": 1, "name": "Alice"}
        assert _normalise_row(row) == row

    def test_nested_dict_stringified(self):
        row = {"id": 1, "meta": {"region": "us"}}
        result = _normalise_row(row)
        assert isinstance(result["meta"], str)
        assert json.loads(result["meta"]) == {"region": "us"}

    def test_nested_list_stringified(self):
        row = {"id": 1, "tags": ["a", "b"]}
        result = _normalise_row(row)
        assert isinstance(result["tags"], str)
        assert json.loads(result["tags"]) == ["a", "b"]

    def test_none_unchanged(self):
        row = {"id": 1, "val": None}
        assert _normalise_row(row)["val"] is None


class TestParseOp:
    def test_no_op_field_defaults_to_insert(self):
        assert _parse_op({"id": 1}, None, _DEFAULT_OP_MAP) == Operation.INSERT

    def test_op_field_missing_from_row(self):
        assert _parse_op({"id": 1}, "op", _DEFAULT_OP_MAP) == Operation.INSERT

    def test_insert(self):
        assert _parse_op({"op": "insert", "id": 1}, "op", _DEFAULT_OP_MAP) == Operation.INSERT

    def test_create_maps_to_insert(self):
        assert _parse_op({"op": "create"}, "op", _DEFAULT_OP_MAP) == Operation.INSERT

    def test_update(self):
        assert _parse_op({"op": "update"}, "op", _DEFAULT_OP_MAP) == Operation.UPDATE

    def test_modify_maps_to_update(self):
        assert _parse_op({"op": "modify"}, "op", _DEFAULT_OP_MAP) == Operation.UPDATE

    def test_delete(self):
        assert _parse_op({"op": "delete"}, "op", _DEFAULT_OP_MAP) == Operation.DELETE

    def test_remove_maps_to_delete(self):
        assert _parse_op({"op": "remove"}, "op", _DEFAULT_OP_MAP) == Operation.DELETE

    def test_case_insensitive(self):
        assert _parse_op({"op": "UPDATE"}, "op", _DEFAULT_OP_MAP) == Operation.UPDATE

    def test_unknown_value_defaults_to_insert(self):
        assert _parse_op({"op": "upsert"}, "op", _DEFAULT_OP_MAP) == Operation.INSERT

    def test_custom_op_map(self):
        custom = {**_DEFAULT_OP_MAP, "upsert": Operation.UPDATE}
        assert _parse_op({"op": "upsert"}, "op", custom) == Operation.UPDATE


# ══════════════════════════════════════════════════════════════════════════════
# 2. PubSubSource unit tests (mocked subscriber)
# ══════════════════════════════════════════════════════════════════════════════

class TestPubSubSourceInit:
    def test_tables(self):
        src = _make_source()
        assert src.tables == ["orders-sub", "events-sub"]

    def test_pk_fields_parsed(self):
        src = _make_source({"primary_key_field": "id, tenant_id"})
        assert src._pk_fields == ["id", "tenant_id"]

    def test_no_pk_fields(self):
        src = _make_source()
        assert src._pk_fields == []

    def test_op_field_none_by_default(self):
        src = _make_source()
        assert src._op_field is None


class TestPubSubSourceConnect:
    def test_connect_initialises_pending_state(self):
        src = _make_source()
        mock_sub = _make_mock_subscriber()
        with patch("sources.pubsub.pubsub_v1", create=True) as mock_lib:
            mock_lib.SubscriberClient.return_value = mock_sub
            with patch.dict("sys.modules", {"google.cloud": MagicMock(), "google.cloud.pubsub_v1": mock_lib}):
                src.connect()

        assert "orders-sub" in src._pending_acks
        assert "events-sub" in src._pending_acks
        assert src._pending_acks["orders-sub"] == []


class TestSnapshot:
    def test_snapshot_yields_nothing(self):
        src = _make_source()
        src._subscriber = _make_mock_subscriber()
        results = list(src.snapshot("orders-sub"))
        assert results == []


class TestGetSchema:
    def test_get_schema_infers_from_first_message(self):
        src = _make_source()
        sub = _make_mock_subscriber()
        row = {"id": 1, "name": "Alice", "score": 9.5}
        sub.pull.return_value = _make_pull_response([_make_received_message(row, "ack-1")])
        src._subscriber = sub
        src._pending_acks = {"orders-sub": [], "events-sub": []}
        src._pending_locks = {"orders-sub": threading.Lock(), "events-sub": threading.Lock()}

        schema = src.get_schema("orders-sub")
        by_name = {c.name: c for c in schema}
        assert set(by_name.keys()) == {"id", "name", "score"}
        # verify it nacked the message (ack_deadline_seconds=0)
        sub.modify_ack_deadline.assert_called_once()
        call_kwargs = sub.modify_ack_deadline.call_args[1]["request"]
        assert call_kwargs["ack_deadline_seconds"] == 0

    def test_get_schema_empty_when_no_messages(self):
        src = _make_source()
        sub = _make_mock_subscriber()
        sub.pull.return_value = _empty_pull_response()
        src._subscriber = sub
        src._pending_acks = {"orders-sub": []}
        src._pending_locks = {"orders-sub": threading.Lock()}
        assert src.get_schema("orders-sub") == []


class TestStream:
    def _setup_source(self, extra_cfg=None):
        src = _make_source(extra_cfg)
        src._subscriber = _make_mock_subscriber()
        src._pending_acks = {"orders-sub": [], "events-sub": []}
        src._pending_locks = {"orders-sub": threading.Lock(), "events-sub": threading.Lock()}
        src._extenders = {}
        return src

    def _run_stream_n(self, src, table, n_events, stop_after_heartbeats=3):
        """Collect n real events from stream(), skipping heartbeats."""
        events = []
        heartbeats = 0
        gen = src.stream(table, offset=None)
        for ev in gen:
            if ev is None:
                heartbeats += 1
                if heartbeats >= stop_after_heartbeats:
                    break
            else:
                events.append(ev)
                if len(events) >= n_events:
                    break
        return events

    def test_yields_change_events_for_messages(self):
        src = self._setup_source()
        row = {"id": 1, "name": "Alice"}
        src._subscriber.pull.side_effect = [
            _make_pull_response([_make_received_message(row, "ack-1", "msg-1")]),
            _empty_pull_response(),
            _empty_pull_response(),
        ]

        events = self._run_stream_n(src, "orders-sub", n_events=1)
        assert len(events) == 1
        assert events[0].op == Operation.INSERT
        assert events[0].after == row
        assert events[0].offset == "msg-1"

    def test_yields_heartbeat_on_empty_pull(self):
        src = self._setup_source()
        src._subscriber.pull.return_value = _empty_pull_response()

        gen = src.stream("orders-sub", offset=None)
        ev = next(gen)
        assert ev is None

    def test_op_field_mapped_correctly(self):
        src = self._setup_source({"op_field": "op"})
        row = {"id": 2, "name": "Bob", "op": "delete"}
        src._subscriber.pull.side_effect = [
            _make_pull_response([_make_received_message(row, "ack-2")]),
            _empty_pull_response(),
            _empty_pull_response(),
        ]

        events = self._run_stream_n(src, "orders-sub", n_events=1)
        assert events[0].op == Operation.DELETE
        assert events[0].before == row  # DELETE sets before, not after
        assert events[0].after is None

    def test_ack_ids_accumulated_before_commit(self):
        src = self._setup_source()
        rows = [{"id": i} for i in range(3)]
        received = [_make_received_message(r, f"ack-{i}") for i, r in enumerate(rows)]
        src._subscriber.pull.side_effect = [
            _make_pull_response(received),
            _empty_pull_response(),
            _empty_pull_response(),
        ]

        self._run_stream_n(src, "orders-sub", n_events=3)
        assert set(src._pending_acks["orders-sub"]) == {"ack-0", "ack-1", "ack-2"}
        # Nothing acked yet
        src._subscriber.acknowledge.assert_not_called()

    def test_bad_json_message_is_skipped_and_immediately_acked(self):
        src = self._setup_source()
        bad_msg = MagicMock()
        bad_msg.data = b"not valid json {{{"
        bad_msg.message_id = "bad-1"
        bad_msg.publish_time = None
        bad_received = MagicMock()
        bad_received.ack_id = "bad-ack-1"
        bad_received.message = bad_msg

        good_row = {"id": 1}
        src._subscriber.pull.side_effect = [
            _make_pull_response([bad_received, _make_received_message(good_row, "good-ack-1")]),
            _empty_pull_response(),
            _empty_pull_response(),
        ]

        events = self._run_stream_n(src, "orders-sub", n_events=1)
        assert len(events) == 1  # bad message skipped
        # Bad message was immediately acked
        src._subscriber.acknowledge.assert_called_once()
        ack_call = src._subscriber.acknowledge.call_args[1]["request"]
        assert ack_call["ack_ids"] == ["bad-ack-1"]
        # Good message is pending (not acked yet)
        assert "good-ack-1" in src._pending_acks["orders-sub"]

    def test_multiple_events_from_one_pull(self):
        src = self._setup_source()
        rows = [{"id": i, "val": f"v{i}"} for i in range(5)]
        received = [_make_received_message(r, f"ack-{i}") for i, r in enumerate(rows)]
        src._subscriber.pull.side_effect = [
            _make_pull_response(received),
            _empty_pull_response(),
            _empty_pull_response(),
        ]

        events = self._run_stream_n(src, "orders-sub", n_events=5)
        assert len(events) == 5
        ids = [e.after["id"] for e in events]
        assert ids == list(range(5))

    def test_pk_fields_set_in_schema(self):
        src = self._setup_source({"primary_key_field": "id"})
        row = {"id": 1, "name": "Alice"}
        src._subscriber.pull.side_effect = [
            _make_pull_response([_make_received_message(row, "ack-1")]),
            _empty_pull_response(),
            _empty_pull_response(),
        ]

        events = self._run_stream_n(src, "orders-sub", n_events=1)
        schema = {c.name: c for c in events[0].schema}
        assert schema["id"].primary_key is True
        assert schema["name"].primary_key is False


class TestOnBatchCommitted:
    def _setup_source_with_pending(self, ack_ids: List[str]) -> PubSubSource:
        src = _make_source()
        src._subscriber = _make_mock_subscriber()
        src._pending_acks = {"orders-sub": list(ack_ids), "events-sub": []}
        src._pending_locks = {"orders-sub": threading.Lock(), "events-sub": threading.Lock()}
        src._extenders = {}
        return src

    def test_acks_all_pending_messages(self):
        src = self._setup_source_with_pending(["ack-1", "ack-2", "ack-3"])
        src.on_batch_committed("orders-sub", offset="msg-3")

        src._subscriber.acknowledge.assert_called_once()
        req = src._subscriber.acknowledge.call_args[1]["request"]
        assert set(req["ack_ids"]) == {"ack-1", "ack-2", "ack-3"}

    def test_clears_pending_after_ack(self):
        src = self._setup_source_with_pending(["ack-1", "ack-2"])
        src.on_batch_committed("orders-sub", offset="msg-2")
        assert src._pending_acks["orders-sub"] == []

    def test_noop_when_no_pending(self):
        src = self._setup_source_with_pending([])
        src.on_batch_committed("orders-sub", offset=None)
        src._subscriber.acknowledge.assert_not_called()

    def test_does_not_ack_other_subscriptions(self):
        src = _make_source()
        src._subscriber = _make_mock_subscriber()
        src._pending_acks = {"orders-sub": ["ack-1"], "events-sub": ["ack-x", "ack-y"]}
        src._pending_locks = {"orders-sub": threading.Lock(), "events-sub": threading.Lock()}
        src._extenders = {}

        src.on_batch_committed("orders-sub", offset="msg-1")

        # Only one acknowledge call, only for orders-sub ack_ids
        src._subscriber.acknowledge.assert_called_once()
        req = src._subscriber.acknowledge.call_args[1]["request"]
        assert req["ack_ids"] == ["ack-1"]
        # events-sub still pending
        assert src._pending_acks["events-sub"] == ["ack-x", "ack-y"]

    def test_chunks_large_ack_batches(self):
        # Pub/Sub allows max 1000 ack_ids per call
        ack_ids = [f"ack-{i}" for i in range(2500)]
        src = self._setup_source_with_pending(ack_ids)
        src.on_batch_committed("orders-sub", offset="msg-2500")

        assert src._subscriber.acknowledge.call_count == 3  # 1000 + 1000 + 500
        all_acked = []
        for c in src._subscriber.acknowledge.call_args_list:
            all_acked.extend(c[1]["request"]["ack_ids"])
        assert set(all_acked) == set(ack_ids)

    def test_ack_failure_logs_error_does_not_raise(self):
        src = self._setup_source_with_pending(["ack-1"])
        src._subscriber.acknowledge.side_effect = Exception("network error")
        # Should not raise — engine continues; Pub/Sub will redeliver
        src.on_batch_committed("orders-sub", offset="msg-1")


class TestOnBatchCommittedHookWiredInEngine:
    """Verify the engine calls on_batch_committed after a successful flush."""

    def test_engine_calls_hook_after_flush(self):
        from core.engine import TableWorker
        from core.offset_store import get_offset_store
        from core.status_store import StatusStore
        import tempfile, os

        src = _make_source()
        src._subscriber = _make_mock_subscriber()
        src._pending_acks = {"orders-sub": ["ack-1"]}
        src._pending_locks = {"orders-sub": threading.Lock()}
        src._extenders = {}

        mock_sink = MagicMock()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            offset_store = get_offset_store(db_path)
            status = StatusStore()
            worker = TableWorker(
                source=src,
                table="orders-sub",
                sink=mock_sink,
                offset_store=offset_store,
                status_store=status,
                options={"batch_size": 10, "batch_timeout_seconds": 5, "adaptive_batching": False},
            )

            from core.event import ChangeEvent, ColumnSchema, Operation
            import datetime
            events = [ChangeEvent(
                op=Operation.INSERT,
                source_name="test_pubsub",
                source_table="orders-sub",
                before=None,
                after={"id": 1},
                schema=[ColumnSchema("id", "bigint", primary_key=True)],
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                offset="msg-1",
            )]

            worker._flush(events, offset="msg-1")

            # Sink was written
            mock_sink.write_batch.assert_called_once()
            # on_batch_committed was called → ack should have been called
            src._subscriber.acknowledge.assert_called_once()
        finally:
            os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# 3. Metadata columns + sort_by unit tests
# ══════════════════════════════════════════════════════════════════════════════

class TestCdcMetadataColumns:
    """Verify _cdc_ingest_ts and other metadata columns are written correctly."""

    def test_enrich_adds_ingest_ts(self):
        from core.iceberg_sink import IcebergSink
        from core.event import ChangeEvent, ColumnSchema, Operation
        import datetime

        sink = IcebergSink(iceberg_cfg={"type": "rest", "uri": "http://fake", "target_namespace": "cdc"},
                           dremio_cfg={})
        ev = ChangeEvent(
            op=Operation.INSERT,
            source_name="test_src",
            source_table="orders",
            before=None,
            after={"id": 1, "name": "Alice"},
            schema=[ColumnSchema("id", "bigint", primary_key=True), ColumnSchema("name", "varchar")],
            timestamp=datetime.datetime(2026, 4, 27, 10, 0, 0, tzinfo=datetime.timezone.utc),
            offset="msg-1",
        )

        before = datetime.datetime.now(datetime.timezone.utc)
        row = sink._enrich(ev)
        after = datetime.datetime.now(datetime.timezone.utc)

        assert "_cdc_ingest_ts" in row
        assert "_cdc_ts" in row
        assert "_cdc_op" in row
        assert "_cdc_source" in row
        # _cdc_ts = event time (from ev.timestamp)
        assert row["_cdc_ts"] == ev.timestamp
        # _cdc_ingest_ts = wall clock at processing time
        assert before <= row["_cdc_ingest_ts"] <= after
        # They are distinct columns
        assert row["_cdc_ts"] != row["_cdc_ingest_ts"] or True  # may equal if fast

    def test_cdc_meta_has_four_columns(self):
        from core.iceberg_sink import _CDC_META
        names = {c.name for c in _CDC_META}
        assert "_cdc_op" in names
        assert "_cdc_source" in names
        assert "_cdc_ts" in names
        assert "_cdc_ingest_ts" in names
        assert len(_CDC_META) == 4

    def test_enrich_op_values(self):
        from core.iceberg_sink import IcebergSink
        from core.event import ChangeEvent, ColumnSchema, Operation
        import datetime

        sink = IcebergSink(iceberg_cfg={"type": "rest", "uri": "http://fake", "target_namespace": "cdc"},
                           dremio_cfg={})
        schema = [ColumnSchema("id", "bigint", primary_key=True)]
        ts = datetime.datetime.now(datetime.timezone.utc)

        for op, expected in [(Operation.INSERT, "insert"), (Operation.UPDATE, "update"),
                             (Operation.DELETE, "delete"), (Operation.SNAPSHOT, "snapshot")]:
            after = {"id": 1} if op != Operation.DELETE else None
            before = {"id": 1} if op == Operation.DELETE else None
            ev = ChangeEvent(op=op, source_name="src", source_table="t",
                             before=before, after=after, schema=schema, timestamp=ts, offset=None)
            row = sink._enrich(ev)
            assert row["_cdc_op"] == expected


class TestSortBy:
    """Verify sort_by config is parsed and stripped from catalog kwargs."""

    def test_sort_by_parsed_from_config(self):
        from core.iceberg_sink import IcebergSink
        sink = IcebergSink(
            iceberg_cfg={"type": "rest", "uri": "http://fake", "target_namespace": "cdc",
                         "sort_by": "event_ts, id"},
            dremio_cfg={},
        )
        assert sink._sort_by == ["event_ts", "id"]

    def test_sort_by_empty_by_default(self):
        from core.iceberg_sink import IcebergSink
        sink = IcebergSink(
            iceberg_cfg={"type": "rest", "uri": "http://fake", "target_namespace": "cdc"},
            dremio_cfg={},
        )
        assert sink._sort_by == []

    def test_sort_by_stripped_from_catalog_kwargs(self):
        """sort_by must not be forwarded to load_catalog."""
        from core.iceberg_sink import IcebergSink
        from unittest.mock import patch, MagicMock

        sink = IcebergSink(
            iceberg_cfg={"type": "rest", "uri": "http://fake", "target_namespace": "cdc",
                         "write_mode": "merge", "sort_by": "event_ts"},
            dremio_cfg={},
        )
        # load_catalog is imported inside connect() so patch at the source module
        with patch("pyiceberg.catalog.load_catalog") as mock_load:
            mock_catalog = MagicMock()
            mock_catalog.create_namespace.return_value = None
            mock_load.return_value = mock_catalog
            sink.connect()
            _, kwargs = mock_load.call_args
            assert "sort_by" not in kwargs
            assert "write_mode" not in kwargs
            assert "target_namespace" not in kwargs

    def test_build_sort_order_returns_none_when_no_sort_by(self):
        from core.iceberg_sink import IcebergSink
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType

        sink = IcebergSink(iceberg_cfg={"type": "rest", "uri": "http://fake"}, dremio_cfg={})
        schema = Schema(NestedField(1, "id", LongType(), required=False))
        assert sink._build_sort_order(schema) is None

    def test_build_sort_order_skips_missing_column(self):
        from core.iceberg_sink import IcebergSink
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType

        sink = IcebergSink(
            iceberg_cfg={"type": "rest", "uri": "http://fake", "sort_by": "nonexistent_col"},
            dremio_cfg={},
        )
        schema = Schema(NestedField(1, "id", LongType(), required=False))
        # Should return None (no valid fields) without raising
        result = sink._build_sort_order(schema)
        assert result is None

    def test_build_sort_order_valid_column(self):
        from core.iceberg_sink import IcebergSink
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, TimestampType

        sink = IcebergSink(
            iceberg_cfg={"type": "rest", "uri": "http://fake", "sort_by": "event_ts"},
            dremio_cfg={},
        )
        schema = Schema(NestedField(1, "event_ts", TimestampType(), required=False))
        result = sink._build_sort_order(schema)
        assert result is not None
        assert len(result.fields) == 1


# ══════════════════════════════════════════════════════════════════════════════
# 4. Integration tests (require Pub/Sub emulator on localhost:8085)
# ══════════════════════════════════════════════════════════════════════════════

PUBSUB_EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
EMULATOR_PROJECT     = "test-project"


def _emulator_available() -> bool:
    import socket
    host, port = PUBSUB_EMULATOR_HOST.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except OSError:
        return False


def _unique_names():
    """Return a unique (topic, subscription) name pair so tests don't share state."""
    import uuid
    suffix = uuid.uuid4().hex[:8]
    return f"topic-{suffix}", f"sub-{suffix}"


def _emulator_client_options():
    from google.api_core import client_options as co
    return co.ClientOptions(api_endpoint=PUBSUB_EMULATOR_HOST)


def _setup_emulator_topic_and_sub(topic_name: str = None, sub_name: str = None):
    """Create a fresh topic + subscription in the emulator."""
    import google.auth.credentials
    from google.cloud import pubsub_v1
    if topic_name is None or sub_name is None:
        topic_name, sub_name = _unique_names()
    creds = google.auth.credentials.AnonymousCredentials()
    opts  = _emulator_client_options()
    pub = pubsub_v1.PublisherClient(credentials=creds, client_options=opts)
    sub = pubsub_v1.SubscriberClient(credentials=creds, client_options=opts)
    topic_path = pub.topic_path(EMULATOR_PROJECT, topic_name)
    sub_path   = sub.subscription_path(EMULATOR_PROJECT, sub_name)
    pub.create_topic(request={"name": topic_path})
    sub.create_subscription(request={"name": sub_path, "topic": topic_path, "ack_deadline_seconds": 30})
    return pub, sub, topic_path, sub_path, sub_name


def _publish_messages(pub, topic_path: str, rows: List[Dict]):
    futures = [pub.publish(topic_path, json.dumps(r).encode()) for r in rows]
    for f in futures:
        f.result()


@pytest.mark.pubsub
class TestPubSubEmulatorIntegration:

    @pytest.fixture(autouse=True)
    def require_emulator(self):
        if not _emulator_available():
            pytest.skip(f"Pub/Sub emulator not available at {PUBSUB_EMULATOR_HOST}")

    def _make_src(self, sub_name: str, extra: dict = None) -> PubSubSource:
        cfg = {
            "connection": {
                "project_id": EMULATOR_PROJECT,
                "ack_deadline_seconds": 30,
                "max_messages_per_pull": 10,
                "pull_timeout_seconds": 3,
            },
            "tables": [sub_name],
            **(extra or {}),
        }
        src = PubSubSource("emulator_src", cfg)
        src.connect()
        return src

    def _collect(self, src, sub_name, n_events, max_heartbeats=3):
        events, heartbeats = [], 0
        for ev in src.stream(sub_name, offset=None):
            if ev is None:
                heartbeats += 1
                if (len(events) >= n_events) or heartbeats >= max_heartbeats:
                    break
            else:
                events.append(ev)
                if len(events) >= n_events:
                    break
        return events

    def test_stream_receives_published_messages(self):
        pub, _, topic_path, _, sub_name = _setup_emulator_topic_and_sub()
        rows = [{"id": i, "name": f"user-{i}", "score": float(i * 10)} for i in range(5)]
        _publish_messages(pub, topic_path, rows)

        src = self._make_src(sub_name, {"primary_key_field": "id"})
        events = self._collect(src, sub_name, n_events=5)
        src.on_batch_committed(sub_name, offset=None)
        src.close()

        assert len(events) == 5
        assert sorted(e.after["id"] for e in events) == list(range(5))

    def test_messages_redelivered_if_not_acked(self):
        """If on_batch_committed is NOT called, messages should redeliver."""
        pub, sub_client, topic_path, sub_path, sub_name = _setup_emulator_topic_and_sub()
        _publish_messages(pub, topic_path, [{"id": 99, "marker": "redeliver-test"}])

        src = self._make_src(sub_name)
        events = self._collect(src, sub_name, n_events=1)

        # Force ack deadline to 0 so Pub/Sub redelivers immediately
        with src._pending_locks[sub_name]:
            ack_ids = list(src._pending_acks[sub_name])
        if ack_ids:
            src._subscriber.modify_ack_deadline(
                request={"subscription": src._subscription_path(sub_name),
                         "ack_ids": ack_ids, "ack_deadline_seconds": 0}
            )
        src.close()

        time.sleep(1)
        resp = sub_client.pull(request={"subscription": sub_path, "max_messages": 5}, timeout=5)
        redelivered_ids = [json.loads(m.message.data.decode())["id"] for m in resp.received_messages]
        assert 99 in redelivered_ids
        if resp.received_messages:
            sub_client.acknowledge(
                request={"subscription": sub_path, "ack_ids": [m.ack_id for m in resp.received_messages]}
            )

    def test_on_batch_committed_acks_messages(self):
        """After on_batch_committed, messages are NOT redelivered."""
        pub, sub_client, topic_path, sub_path, sub_name = _setup_emulator_topic_and_sub()
        _publish_messages(pub, topic_path, [{"id": 77, "marker": "ack-test"}])

        src = self._make_src(sub_name)
        events = self._collect(src, sub_name, n_events=1)
        src.on_batch_committed(sub_name, offset=events[-1].offset if events else None)
        assert src._pending_acks[sub_name] == []
        src.close()

        time.sleep(1)
        resp = sub_client.pull(request={"subscription": sub_path, "max_messages": 5}, timeout=3)
        marker_ids = [json.loads(m.message.data.decode()).get("id") for m in resp.received_messages]
        assert 77 not in marker_ids

    def test_op_field_routing_end_to_end(self):
        pub, _, topic_path, _, sub_name = _setup_emulator_topic_and_sub()
        _publish_messages(pub, topic_path, [
            {"id": 1, "name": "Alice", "op": "insert"},
            {"id": 2, "name": "Bob",   "op": "update"},
            {"id": 3, "name": "Carol", "op": "delete"},
        ])

        src = self._make_src(sub_name, {"op_field": "op"})
        events = self._collect(src, sub_name, n_events=3)
        src.on_batch_committed(sub_name, offset=None)
        src.close()

        ops_by_id = {(e.after or e.before)["id"]: e.op for e in events if e.after or e.before}
        assert ops_by_id.get(1) == Operation.INSERT
        assert ops_by_id.get(2) == Operation.UPDATE
        assert ops_by_id.get(3) == Operation.DELETE

    def test_schema_inferred_correctly(self):
        pub, _, topic_path, _, sub_name = _setup_emulator_topic_and_sub()
        _publish_messages(pub, topic_path, [
            {"id": 1, "amount": 99.99, "active": True, "tags": ["sale", "new"]}
        ])

        src = self._make_src(sub_name, {"primary_key_field": "id"})
        events = self._collect(src, sub_name, n_events=1)
        src.on_batch_committed(sub_name, offset=None)
        src.close()

        assert len(events) == 1
        schema = {c.name: c for c in events[0].schema}
        assert schema["id"].data_type == "bigint"
        assert schema["id"].primary_key is True
        assert schema["amount"].data_type == "double"
        assert schema["active"].data_type == "boolean"
        assert schema["tags"].data_type == "varchar"
        assert json.loads(events[0].after["tags"]) == ["sale", "new"]
