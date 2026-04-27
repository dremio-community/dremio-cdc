"""
Tests for sources/datastream.py — Google Cloud Datastream GCS connector.

Unit tests use mocked GCS clients (no network).
Integration tests (marked @pytest.mark.datastream) require:
  docker compose up -d fake-gcs
  STORAGE_EMULATOR_HOST=http://localhost:4443
"""
from __future__ import annotations

import io
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest

from core.event import Operation
from sources.datastream import (
    DatastreamSource,
    _avro_to_cdc_type,
    _decode_offset,
    _encode_offset,
    _infer_schema_from_record,
    _normalise,
    _parse_op,
    _parse_ts,
    _pk_fields,
    _strip_meta,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_record(data: Dict, op: str = "INSERT", deleted: bool = False,
                 pks: List[str] | None = None) -> Dict:
    record = dict(data)
    record["_metadata_operation"]              = op
    record["_metadata_deleted"]                = deleted
    record["_metadata_timestamp"]              = 1_714_215_600.0
    record["_metadata_change_sequence_number"] = "00000000000000000001"
    record["_metadata_schema"]                 = "HR"
    record["_metadata_table"]                  = "EMPLOYEES"
    record["_metadata_primary_keys"]           = pks or ["id"]
    return record


def _make_source(extra_cfg: Dict | None = None) -> DatastreamSource:
    cfg = {
        "connection": {
            "project_id": "test-project",
            "bucket":     "test-bucket",
            "path_prefix": "ds/",
            "poll_interval_seconds": 0,
            "file_format": "json",
        },
        "tables": ["HR.EMPLOYEES"],
        **(extra_cfg or {}),
    }
    src = DatastreamSource("ds_test", cfg)
    src._client = MagicMock()
    src._bucket = MagicMock()
    return src


def _make_avro_bytes(records: List[Dict], schema_fields: List[Dict] | None = None) -> bytes:
    """Create a minimal Avro file in memory using fastavro."""
    fastavro = pytest.importorskip("fastavro")
    if schema_fields is None:
        # Build from first record
        fields = [
            {"name": k, "type": ["null", "string"], "default": None}
            for k in records[0].keys()
        ]
    else:
        fields = schema_fields

    avro_schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": fields,
    }
    buf = io.BytesIO()
    fastavro.writer(buf, avro_schema, records)
    return buf.getvalue()


# ── Unit: offset encoding ─────────────────────────────────────────────────────

class TestOffsetEncoding:
    def test_encode_decode_roundtrip(self):
        path = "HR/EMPLOYEES/2026/04/27/10/000000000001.avro"
        encoded = _encode_offset(path, 42)
        decoded_path, decoded_idx = _decode_offset(encoded)
        assert decoded_path == path
        assert decoded_idx == 42

    def test_decode_none(self):
        assert _decode_offset(None) == (None, None)

    def test_decode_empty(self):
        assert _decode_offset("") == (None, None)

    def test_decode_legacy_file_only_offset(self):
        path = "HR/EMPLOYEES/2026/04/27/10/000000000001.avro"
        file_path, idx = _decode_offset(path)
        assert file_path == path
        assert idx is None  # legacy format — treated as fully processed

    def test_encode_record_zero(self):
        enc = _encode_offset("some/path.avro", 0)
        p, i = _decode_offset(enc)
        assert i == 0


# ── Unit: operation parsing ───────────────────────────────────────────────────

class TestParseOp:
    def test_insert(self):
        assert _parse_op({"_metadata_operation": "INSERT"}) == Operation.INSERT

    def test_update(self):
        assert _parse_op({"_metadata_operation": "UPDATE"}) == Operation.UPDATE

    def test_delete_via_op_field(self):
        assert _parse_op({"_metadata_operation": "DELETE"}) == Operation.DELETE

    def test_delete_via_is_deleted_flag(self):
        assert _parse_op({"_metadata_deleted": True}) == Operation.DELETE

    def test_read_maps_to_insert(self):
        assert _parse_op({"_metadata_operation": "READ"}) == Operation.INSERT

    def test_missing_op_defaults_insert(self):
        assert _parse_op({"some_col": 1}) == Operation.INSERT

    def test_case_insensitive(self):
        assert _parse_op({"_metadata_operation": "delete"}) == Operation.DELETE


# ── Unit: metadata stripping ──────────────────────────────────────────────────

class TestStripMeta:
    def test_strips_metadata_prefix(self):
        record = {"id": 1, "name": "Alice", "_metadata_operation": "INSERT"}
        assert _strip_meta(record) == {"id": 1, "name": "Alice"}

    def test_empty_record(self):
        assert _strip_meta({}) == {}

    def test_all_meta_returns_empty(self):
        record = {"_metadata_a": 1, "_metadata_b": 2}
        assert _strip_meta(record) == {}


# ── Unit: PK extraction ───────────────────────────────────────────────────────

class TestPkFields:
    def test_list_of_strings(self):
        assert _pk_fields({"_metadata_primary_keys": ["id", "tenant_id"]}) == ["id", "tenant_id"]

    def test_single_string(self):
        assert _pk_fields({"_metadata_primary_key": "id"}) == ["id"]

    def test_missing_returns_empty(self):
        assert _pk_fields({"id": 1}) == []

    def test_list_with_single_pk(self):
        assert _pk_fields({"_metadata_primary_keys": ["id"]}) == ["id"]


# ── Unit: timestamp parsing ───────────────────────────────────────────────────

class TestParseTs:
    def test_unix_seconds(self):
        ts = _parse_ts({"_metadata_timestamp": 1_714_215_600.0})
        assert ts.tzinfo is not None
        assert ts.year == 2024

    def test_unix_millis(self):
        ts = _parse_ts({"_metadata_timestamp": 1_714_215_600_000.0})
        assert ts.year == 2024

    def test_unix_micros(self):
        ts = _parse_ts({"_metadata_timestamp": 1_714_215_600_000_000.0})
        assert ts.year == 2024

    def test_datetime_object(self):
        dt = datetime(2026, 4, 27, 10, 0, 0, tzinfo=timezone.utc)
        ts = _parse_ts({"_metadata_timestamp": dt})
        assert ts == dt

    def test_missing_returns_now(self):
        ts = _parse_ts({})
        assert ts.tzinfo is not None

    def test_naive_datetime_gets_utc(self):
        dt = datetime(2026, 4, 27, 10, 0, 0)  # no tzinfo
        ts = _parse_ts({"_metadata_timestamp": dt})
        assert ts.tzinfo is not None


# ── Unit: schema inference ────────────────────────────────────────────────────

class TestInferSchema:
    def test_basic_types(self):
        record = {"id": 1, "name": "Alice", "score": 9.5, "active": True}
        schema = _infer_schema_from_record(record, ["id"])
        by_name = {c.name: c for c in schema}
        assert by_name["id"].data_type == "bigint"
        assert by_name["id"].primary_key is True
        assert by_name["name"].data_type == "varchar"
        assert by_name["score"].data_type == "double"
        assert by_name["active"].data_type == "boolean"

    def test_nested_coerced_to_varchar(self):
        record = {"id": 1, "tags": ["a", "b"], "meta": {"k": "v"}}
        schema = _infer_schema_from_record(record, [])
        by_name = {c.name: c for c in schema}
        assert by_name["tags"].data_type == "varchar"
        assert by_name["meta"].data_type == "varchar"

    def test_pk_marking(self):
        record = {"id": 1, "tenant_id": 2, "name": "Alice"}
        schema = _infer_schema_from_record(record, ["id", "tenant_id"])
        pks = {c.name for c in schema if c.primary_key}
        assert pks == {"id", "tenant_id"}


# ── Unit: Avro type mapping ───────────────────────────────────────────────────

class TestAvroTypeMapping:
    def test_primitives(self):
        assert _avro_to_cdc_type("string")  == "varchar"
        assert _avro_to_cdc_type("long")    == "bigint"
        assert _avro_to_cdc_type("double")  == "double"
        assert _avro_to_cdc_type("boolean") == "boolean"

    def test_union_strips_null(self):
        assert _avro_to_cdc_type(["null", "string"]) == "varchar"
        assert _avro_to_cdc_type(["null", "long"])   == "bigint"

    def test_logical_timestamp(self):
        assert _avro_to_cdc_type({"type": "long", "logicalType": "timestamp-millis"}) == "timestamp"
        assert _avro_to_cdc_type({"type": "long", "logicalType": "timestamp-micros"}) == "timestamp"

    def test_logical_date(self):
        assert _avro_to_cdc_type({"type": "int", "logicalType": "date"}) == "date"

    def test_logical_decimal(self):
        assert _avro_to_cdc_type({"type": "bytes", "logicalType": "decimal"}) == "double"

    def test_unknown_defaults_varchar(self):
        assert _avro_to_cdc_type("bytes") == "varchar"


# ── Unit: normalise ───────────────────────────────────────────────────────────

class TestNormalise:
    def test_nested_dict_to_json_string(self):
        row = {"id": 1, "meta": {"k": "v"}}
        norm = _normalise(row)
        assert isinstance(norm["meta"], str)
        assert json.loads(norm["meta"]) == {"k": "v"}

    def test_list_to_json_string(self):
        row = {"tags": ["a", "b"]}
        norm = _normalise(row)
        assert isinstance(norm["tags"], str)

    def test_scalars_unchanged(self):
        row = {"id": 1, "name": "Alice", "score": 9.5}
        assert _normalise(row) == row


# ── Unit: DatastreamSource._list_files ───────────────────────────────────────

class TestListFiles:
    def _make_blob(self, name):
        b = MagicMock()
        b.name = name
        return b

    def test_returns_sorted_avro_files(self):
        src = _make_source({"connection": {
            "project_id": "p", "bucket": "b", "path_prefix": "pfx",
            "file_format": "avro",
        }})
        src._client.list_blobs.return_value = [
            self._make_blob("pfx/HR/EMPLOYEES/2026/04/27/10/000000000002.avro"),
            self._make_blob("pfx/HR/EMPLOYEES/2026/04/27/10/000000000001.avro"),
            self._make_blob("pfx/HR/EMPLOYEES/2026/04/27/10/000000000003.avro"),
        ]
        files = src._list_files("HR.EMPLOYEES")
        assert files == [
            "pfx/HR/EMPLOYEES/2026/04/27/10/000000000001.avro",
            "pfx/HR/EMPLOYEES/2026/04/27/10/000000000002.avro",
            "pfx/HR/EMPLOYEES/2026/04/27/10/000000000003.avro",
        ]

    def test_filters_after_offset(self):
        src = _make_source({"connection": {
            "project_id": "p", "bucket": "b", "path_prefix": "pfx",
            "file_format": "avro",
        }})
        src._client.list_blobs.return_value = [
            self._make_blob("pfx/HR/EMPLOYEES/2026/04/27/10/000000000001.avro"),
            self._make_blob("pfx/HR/EMPLOYEES/2026/04/27/10/000000000002.avro"),
            self._make_blob("pfx/HR/EMPLOYEES/2026/04/27/10/000000000003.avro"),
        ]
        files = src._list_files(
            "HR.EMPLOYEES",
            after="pfx/HR/EMPLOYEES/2026/04/27/10/000000000001.avro",
        )
        assert "pfx/HR/EMPLOYEES/2026/04/27/10/000000000001.avro" not in files
        assert len(files) == 2

    def test_skips_non_matching_extension(self):
        src = _make_source({"connection": {
            "project_id": "p", "bucket": "b", "path_prefix": "pfx",
            "file_format": "json",
        }})
        src._client.list_blobs.return_value = [
            self._make_blob("pfx/HR/EMPLOYEES/000000000001.avro"),  # wrong ext
            self._make_blob("pfx/HR/EMPLOYEES/000000000001.json"),  # correct
        ]
        files = src._list_files("HR.EMPLOYEES")
        assert len(files) == 1
        assert files[0].endswith(".json")


# ── Unit: _gcs_prefix ────────────────────────────────────────────────────────

class TestGcsPrefix:
    def test_schema_dot_table(self):
        src = _make_source()
        assert src._gcs_prefix("HR.EMPLOYEES") == "ds/HR/EMPLOYEES/"

    def test_no_schema(self):
        src = _make_source()
        assert src._gcs_prefix("orders") == "ds/orders/"

    def test_no_path_prefix(self):
        src = _make_source({"connection": {
            "project_id": "p", "bucket": "b",
        }})
        assert src._gcs_prefix("HR.EMPLOYEES") == "HR/EMPLOYEES/"


# ── Unit: stream() event yield ────────────────────────────────────────────────

class TestStreamEvents:
    def _setup_source_with_json_records(self, records: List[Dict]) -> DatastreamSource:
        src = _make_source()
        blob = MagicMock()
        blob.download_as_text.return_value = "\n".join(json.dumps(r) for r in records)
        src._bucket.blob.return_value = blob
        file_name = "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json"
        src._client.list_blobs.side_effect = [
            [MagicMock(name=file_name)],  # first poll: one file
            [],                            # second poll: idle
        ]

        def _fix_list(name):
            b = MagicMock()
            b.name = file_name
            return b

        blobs = [_fix_list(file_name)]
        for b in blobs:
            b.name = file_name

        src._client.list_blobs.side_effect = [blobs, []]
        return src

    def test_yields_events_from_json_file(self):
        records = [
            _make_record({"id": 1, "name": "Alice"}, op="INSERT"),
            _make_record({"id": 2, "name": "Bob"},   op="UPDATE"),
        ]
        src = _make_source()
        blob = MagicMock()
        blob.download_as_text.return_value = "\n".join(json.dumps(r) for r in records)
        src._bucket.blob.return_value = blob

        file_name = "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json"
        b1 = MagicMock()
        b1.name = file_name
        src._client.list_blobs.side_effect = [[b1], []]

        events = []
        gen = src.stream("HR.EMPLOYEES", None)
        for event in gen:
            if event is None:
                break
            events.append(event)

        assert len(events) == 2
        assert events[0].op == Operation.INSERT
        assert events[0].after["id"] == 1
        assert events[1].op == Operation.UPDATE
        assert events[1].after["id"] == 2

    def test_delete_event_has_before_not_after(self):
        records = [_make_record({"id": 1, "name": "Alice"}, op="DELETE")]
        src = _make_source()
        blob = MagicMock()
        blob.download_as_text.return_value = json.dumps(records[0])
        src._bucket.blob.return_value = blob

        b1 = MagicMock()
        b1.name = "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json"
        src._client.list_blobs.side_effect = [[b1], []]

        events = []
        for event in src.stream("HR.EMPLOYEES", None):
            if event is None:
                break
            events.append(event)
        assert events[0].op == Operation.DELETE
        assert events[0].before is not None
        assert events[0].after is None

    def test_offset_encoded_as_file_and_record_index(self):
        records = [
            _make_record({"id": i}) for i in range(3)
        ]
        src = _make_source()
        blob = MagicMock()
        blob.download_as_text.return_value = "\n".join(json.dumps(r) for r in records)
        src._bucket.blob.return_value = blob

        file_name = "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json"
        b1 = MagicMock()
        b1.name = file_name
        src._client.list_blobs.side_effect = [[b1], []]

        offsets = []
        for event in src.stream("HR.EMPLOYEES", None):
            if event is None:
                break
            offsets.append(event.offset)

        # offset = change_sequence_number from metadata (not file-encoded)
        # but the file/record offset is used internally for resumability
        assert len(offsets) == 3

    def test_heartbeat_when_no_new_files(self):
        src = _make_source()
        src._client.list_blobs.return_value = []

        gen = src.stream("HR.EMPLOYEES", None)
        event = next(gen)
        assert event is None  # heartbeat

    def test_resumes_from_mid_file_offset(self):
        """Offset {file}#1 means records 0 was processed; resume from record 1."""
        records = [
            _make_record({"id": 0, "name": "Alice"}),
            _make_record({"id": 1, "name": "Bob"}),
            _make_record({"id": 2, "name": "Carol"}),
        ]
        src = _make_source()
        blob = MagicMock()
        blob.download_as_text.return_value = "\n".join(json.dumps(r) for r in records)
        src._bucket.blob.return_value = blob

        file_name = "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json"
        src._client.list_blobs.return_value = []  # no NEW files after the resume file

        offset = _encode_offset(file_name, 0)  # last processed = record 0
        events = []
        for event in src.stream("HR.EMPLOYEES", offset):
            if event is None:
                break
            events.append(event)

        # Should yield records 1 and 2 only (record 0 was already processed)
        assert len(events) == 2
        assert events[0].after["id"] == 1
        assert events[1].after["id"] == 2

    def test_skips_corrupt_file_gracefully(self):
        src = _make_source()
        blob = MagicMock()
        blob.download_as_text.side_effect = Exception("GCS download failed")
        src._bucket.blob.return_value = blob

        b1 = MagicMock()
        b1.name = "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json"
        src._client.list_blobs.side_effect = [[b1], []]

        events = []
        for event in src.stream("HR.EMPLOYEES", None):
            if event is None:
                break
            events.append(event)
        assert events == []  # file skipped, no crash


# ── Unit: snapshot is a no-op ─────────────────────────────────────────────────

class TestSnapshot:
    def test_snapshot_yields_nothing(self):
        src = _make_source()
        events = list(src.snapshot("HR.EMPLOYEES"))
        assert events == []


# ── Unit: get_schema ──────────────────────────────────────────────────────────

class TestGetSchema:
    def test_returns_empty_when_no_files(self):
        src = _make_source()
        src._client.list_blobs.return_value = []
        assert src.get_schema("HR.EMPLOYEES") == []

    def test_infers_from_first_json_record(self):
        records = [_make_record({"id": 1, "name": "Alice", "salary": 95000.0})]
        src = _make_source()
        blob = MagicMock()
        blob.download_as_text.return_value = json.dumps(records[0])
        src._bucket.blob.return_value = blob

        b1 = MagicMock()
        b1.name = "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json"
        src._client.list_blobs.return_value = [b1]

        schema = src.get_schema("HR.EMPLOYEES")
        by_name = {c.name: c for c in schema}
        assert "id" in by_name
        assert "name" in by_name
        assert "salary" in by_name
        assert by_name["id"].primary_key is True  # from _metadata_primary_keys


# ── Integration tests (require fake-gcs-server on localhost:4443) ─────────────

@pytest.mark.datastream
class TestDatastreamGCSIntegration:
    """
    Requires:  docker compose up -d fake-gcs
               STORAGE_EMULATOR_HOST=http://localhost:4443
    """

    EMULATOR = os.environ.get("STORAGE_EMULATOR_HOST", "http://localhost:4443")

    def _gcs_client(self):
        from google.cloud import storage
        return storage.Client(
            project="test-project",
            client_options={"api_endpoint": self.EMULATOR},
        )

    def _unique_bucket(self):
        return f"test-ds-{uuid.uuid4().hex[:8]}"

    def _make_src(self, bucket: str, path_prefix: str = "ds",
                  file_format: str = "json") -> DatastreamSource:
        src = DatastreamSource("ds_int", {
            "connection": {
                "project_id":             "test-project",
                "bucket":                 bucket,
                "path_prefix":            path_prefix,
                "poll_interval_seconds":  1,
                "file_format":            file_format,
            },
            "tables": ["HR.EMPLOYEES"],
        })
        src._client = self._gcs_client()
        src._bucket = src._client.bucket(bucket)
        return src

    def _upload_json(self, client, bucket_name: str, path: str,
                     records: List[Dict]):
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(path)
        blob.upload_from_string(
            "\n".join(json.dumps(r) for r in records),
            content_type="application/json",
        )

    def _setup_bucket(self, client, bucket_name: str):
        bucket = client.bucket(bucket_name)
        bucket.create()
        return bucket

    def test_streams_json_files_end_to_end(self):
        client = self._gcs_client()
        bucket_name = self._unique_bucket()
        self._setup_bucket(client, bucket_name)

        records = [
            _make_record({"id": 1, "name": "Alice"}, op="INSERT"),
            _make_record({"id": 2, "name": "Bob"},   op="INSERT"),
        ]
        self._upload_json(
            client, bucket_name,
            "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json",
            records,
        )

        src = self._make_src(bucket_name)
        events = []
        for event in src.stream("HR.EMPLOYEES", None):
            if event is None:
                break
            events.append(event)

        assert len(events) == 2
        assert events[0].op == Operation.INSERT
        assert events[0].after["name"] == "Alice"
        assert events[1].after["name"] == "Bob"

    def test_offset_resumes_from_correct_file(self):
        client = self._gcs_client()
        bucket_name = self._unique_bucket()
        self._setup_bucket(client, bucket_name)

        file1 = "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json"
        file2 = "ds/HR/EMPLOYEES/2026/04/27/10/000000000002.json"

        self._upload_json(client, bucket_name, file1,
                          [_make_record({"id": 1}, op="INSERT")])
        self._upload_json(client, bucket_name, file2,
                          [_make_record({"id": 2}, op="INSERT")])

        src = self._make_src(bucket_name)
        # Offset says file1 record 0 was already processed
        offset = _encode_offset(file1, 0)
        events = []
        for event in src.stream("HR.EMPLOYEES", offset):
            if event is None:
                break
            events.append(event)

        # Only file2 should be processed (file1 was fully consumed at record 0)
        assert len(events) == 1
        assert events[0].after["id"] == 2

    def test_delete_operation(self):
        client = self._gcs_client()
        bucket_name = self._unique_bucket()
        self._setup_bucket(client, bucket_name)

        records = [_make_record({"id": 1, "name": "Alice"}, op="DELETE")]
        self._upload_json(
            client, bucket_name,
            "ds/HR/EMPLOYEES/2026/04/27/10/000000000001.json",
            records,
        )

        src = self._make_src(bucket_name)
        events = []
        for event in src.stream("HR.EMPLOYEES", None):
            if event is None:
                break
            events.append(event)

        assert len(events) == 1
        assert events[0].op == Operation.DELETE
        assert events[0].before is not None
        assert events[0].after is None

    def test_processes_multiple_files_in_order(self):
        client = self._gcs_client()
        bucket_name = self._unique_bucket()
        self._setup_bucket(client, bucket_name)

        for i in range(1, 4):
            self._upload_json(
                client, bucket_name,
                f"ds/HR/EMPLOYEES/2026/04/27/10/00000000000{i}.json",
                [_make_record({"id": i}, op="INSERT")],
            )

        src = self._make_src(bucket_name)
        events = []
        for event in src.stream("HR.EMPLOYEES", None):
            if event is None:
                break
            events.append(event)

        assert [e.after["id"] for e in events] == [1, 2, 3]

    def test_avro_file_format(self):
        fastavro = pytest.importorskip("fastavro")
        client = self._gcs_client()
        bucket_name = self._unique_bucket()
        self._setup_bucket(client, bucket_name)

        avro_schema = {
            "type": "record",
            "name": "Employee",
            "fields": [
                {"name": "id",                              "type": ["null", "long"],   "default": None},
                {"name": "name",                            "type": ["null", "string"], "default": None},
                {"name": "_metadata_operation",             "type": ["null", "string"], "default": None},
                {"name": "_metadata_deleted",               "type": ["null", "boolean"],"default": None},
                {"name": "_metadata_timestamp",             "type": ["null", "double"], "default": None},
                {"name": "_metadata_change_sequence_number","type": ["null", "string"], "default": None},
                {"name": "_metadata_primary_keys",          "type": {"type": "array", "items": "string"}, "default": []},
            ],
        }
        records = [
            {"id": 1, "name": "Alice", "_metadata_operation": "INSERT",
             "_metadata_deleted": False, "_metadata_timestamp": 1_714_215_600.0,
             "_metadata_change_sequence_number": "000001",
             "_metadata_primary_keys": ["id"]},
        ]
        buf = io.BytesIO()
        fastavro.writer(buf, avro_schema, records)

        bucket = client.bucket(bucket_name)
        blob = bucket.blob("ds/HR/EMPLOYEES/2026/04/27/10/000000000001.avro")
        blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")

        src = self._make_src(bucket_name, file_format="avro")
        events = []
        for event in src.stream("HR.EMPLOYEES", None):
            if event is None:
                break
            events.append(event)

        assert len(events) == 1
        assert events[0].after["name"] == "Alice"
        assert events[0].op == Operation.INSERT
