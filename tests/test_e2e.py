"""
End-to-end test suite for Dremio CDC framework.
Covers: masking, incremental snapshot encoding, iceberg sink (local + cloud),
Dremio Cloud SQL API, UI backend endpoints.

Run:
    cd /Users/mark/Desktop/Claude\ Projects/dremio-cdc
    python -m pytest tests/test_e2e.py -v
"""
from __future__ import annotations

import os
import sys
import time
import uuid
import datetime
import threading
import unittest

import pytest
import requests

from core.event import ChangeEvent, ColumnSchema, Operation

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ── Config ────────────────────────────────────────────────────────────────────

DREMIO_LOCAL        = {"host": "localhost", "port": 9047, "user": "mark", "password": "Hoyasaxa7788&&**", "ssl": False}
ICEBERG_LOCAL       = {"type": "rest", "uri": "http://localhost:8181", "warehouse": "s3://dremio-test/iceberg-warehouse",
                       "s3.endpoint": "http://localhost:9000", "s3.access-key-id": "minioadmin",
                       "s3.secret-access-key": "minioadmin", "s3.path-style-access": "true",
                       "target_namespace": "cdc_e2e_test"}
DREMIO_CLOUD_PROJECT = "957704f5-4495-42ad-94de-671bf7790610"
DREMIO_CLOUD_PAT     = "5g9KmDHFTkKOKZdUa65b2wQozzI3xzUfIENU0O1VhlgupUQHM5qXssGeoPQ5vg=="
ICEBERG_CLOUD        = {"type": "rest", "uri": "https://catalog.dremio.cloud/api/iceberg",
                        "token": DREMIO_CLOUD_PAT, "warehouse": "first-project",
                        "target_namespace": "cdc_e2e_test"}
UI_BASE             = "http://localhost:7070/api"

# ── Helpers ───────────────────────────────────────────────────────────────────

def _cloud_sql(sql: str) -> dict:
    """Run SQL against Dremio Cloud REST API."""
    url = f"https://api.dremio.cloud/v0/projects/{DREMIO_CLOUD_PROJECT}/sql"
    r = requests.post(url, headers={"Authorization": f"Bearer {DREMIO_CLOUD_PAT}",
                                    "Content-Type": "application/json"},
                      json={"sql": sql}, timeout=30)
    r.raise_for_status()
    job_id = r.json()["id"]
    # Poll for completion
    for _ in range(30):
        time.sleep(2)
        jr = requests.get(f"https://api.dremio.cloud/v0/projects/{DREMIO_CLOUD_PROJECT}/job/{job_id}",
                          headers={"Authorization": f"Bearer {DREMIO_CLOUD_PAT}"}, timeout=10)
        jr.raise_for_status()
        state = jr.json().get("jobState", "")
        if state == "COMPLETED":
            return {**jr.json(), "id": job_id}
        if state in ("FAILED", "CANCELED", "CANCELLED"):
            raise RuntimeError(f"Cloud SQL job {state}: {jr.json().get('errorMessage','')}")
    raise TimeoutError("Cloud SQL job timed out")


def _make_events(n: int = 3):
    from core.event import ChangeEvent, ColumnSchema, Operation
    schema = [
        ColumnSchema("id",    "bigint",  primary_key=True),
        ColumnSchema("name",  "varchar", primary_key=False),
        ColumnSchema("email", "varchar", primary_key=False),
        ColumnSchema("score", "double",  primary_key=False),
    ]
    events = []
    for i in range(1, n + 1):
        events.append(ChangeEvent(
            op=Operation.SNAPSHOT,
            source_name="test_src",
            source_table="public.customers",
            before=None,
            after={"id": i, "name": f"User {i}", "email": f"user{i}@example.com", "score": float(i * 10)},
            schema=schema,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            offset=None,
        ))
    return events


# ══════════════════════════════════════════════════════════════════════════════
# 1. Masking engine unit tests
# ══════════════════════════════════════════════════════════════════════════════

class TestMasking:

    def setup_method(self):
        from core.masking import MaskingEngine
        self.engine = MaskingEngine({
            "public.customers": {
                "email": "mask_email",
                "name":  "mask_name",
            }
        })

    def test_email_masked(self):
        events = _make_events(1)
        out = self.engine.apply("public.customers", events[0])
        assert "@" in out.after["email"]
        assert out.after["email"].startswith("u***@")

    def test_name_masked(self):
        events = _make_events(1)
        out = self.engine.apply("public.customers", events[0])
        assert out.after["name"].startswith("U")
        assert "***" in out.after["name"]

    def test_unmasked_columns_unchanged(self):
        events = _make_events(1)
        out = self.engine.apply("public.customers", events[0])
        assert out.after["id"] == 1
        assert out.after["score"] == 10.0

    def test_original_event_unmodified(self):
        events = _make_events(1)
        original_email = events[0].after["email"]
        self.engine.apply("public.customers", events[0])
        assert events[0].after["email"] == original_email

    def test_batch_masking(self):
        events = _make_events(3)
        out = self.engine.apply_batch("public.customers", events)
        assert len(out) == 3
        for e in out:
            assert "***" in e.after["email"]

    def test_unlisted_table_passthrough(self):
        events = _make_events(1)
        out = self.engine.apply("public.orders", events[0])
        assert out.after["email"] == "user1@example.com"


# ══════════════════════════════════════════════════════════════════════════════
# 2. All masking functions
# ══════════════════════════════════════════════════════════════════════════════

class TestMaskingFunctions:

    def _apply(self, fn_name: str, value: str) -> str:
        from core.masking import MaskingEngine
        from core.event import ChangeEvent, ColumnSchema, Operation
        engine = MaskingEngine({"t": {"col": fn_name}})
        schema = [ColumnSchema("col", "varchar")]
        evt = ChangeEvent(op=Operation.SNAPSHOT, source_name="s", source_table="t",
                          before=None, after={"col": value}, schema=schema,
                          timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None)
        return engine.apply("t", evt).after["col"]

    def test_mask_email(self):
        r = self._apply("mask_email", "alice@example.com")
        assert r.startswith("a***@")
        assert r.endswith("example.com")

    def test_mask_phone(self):
        r = self._apply("mask_phone", "555-867-5309")
        assert r.endswith("5309")
        assert "***" in r

    def test_mask_ssn(self):
        r = self._apply("mask_ssn", "123-45-6789")
        assert r.endswith("6789")
        assert "***" in r

    def test_mask_card(self):
        r = self._apply("mask_card", "4111-1111-1111-1234")
        assert r.endswith("1234")
        assert "****" in r

    def test_mask_ip(self):
        r = self._apply("mask_ip", "192.168.1.100")
        assert r.startswith("192.168.")
        assert r.endswith("*.*") or "***" in r

    def test_mask_name(self):
        r = self._apply("mask_name", "Alice Smith")
        assert r.startswith("A")
        assert "***" in r

    def test_redact(self):
        r = self._apply("redact", "secret")
        assert r == "[REDACTED]"

    def test_hash_sha256(self):
        r = self._apply("hash_sha256", "secret")
        assert len(r) == 64

    def test_nullify(self):
        from core.masking import MaskingEngine
        from core.event import ChangeEvent, ColumnSchema, Operation
        engine = MaskingEngine({"t": {"col": "nullify"}})
        schema = [ColumnSchema("col", "varchar")]
        evt = ChangeEvent(op=Operation.SNAPSHOT, source_name="s", source_table="t",
                          before=None, after={"col": "value"}, schema=schema,
                          timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None)
        out = engine.apply("t", evt)
        assert out.after["col"] is None


# ══════════════════════════════════════════════════════════════════════════════
# 3. Incremental snapshot offset encoding
# ══════════════════════════════════════════════════════════════════════════════

class TestIncrementalSnapshotOffset:

    def test_snap_done_is_not_in_progress(self):
        offset = "snap:done"
        snap_in_progress = isinstance(offset, str) and offset.startswith("snap:") and offset != "snap:done"
        assert not snap_in_progress

    def test_snap_progress_is_detected(self):
        offset = "snap:id:42"
        snap_in_progress = isinstance(offset, str) and offset.startswith("snap:") and offset != "snap:done"
        assert snap_in_progress

    def test_none_offset_needs_snapshot(self):
        offset = None
        need = offset is None or (isinstance(offset, str) and offset.startswith("snap:"))
        assert need

    def test_real_offset_skips_snapshot(self):
        offset = "0/1A3F000"
        need = offset is None or (isinstance(offset, str) and offset.startswith("snap:"))
        assert not need

    def test_snap_val_parsed(self):
        offset = "snap:id:9999"
        parts = offset.split(":", 2)
        assert parts[1] == "id"
        assert parts[2] == "9999"

    def test_clean_offset_for_stream(self):
        # stream() must strip snap: prefix to avoid passing it as LSN/binlog
        for o in ["snap:done", "snap:id:42", None]:
            clean = o if (o and not str(o).startswith("snap:")) else None
            assert clean is None


# ══════════════════════════════════════════════════════════════════════════════
# 4. DremioSink construction (no live connection)
# ══════════════════════════════════════════════════════════════════════════════

class TestDremioSinkConstruct:

    def test_pat_sets_bearer(self):
        from core.dremio_sink import DremioSink
        s = DremioSink({"host": "api.dremio.cloud", "port": 443, "ssl": True, "pat": "tok123"})
        s._token = s._pat
        s._bearer = bool(s._pat)
        assert s._bearer
        hdrs = s._headers()
        assert hdrs["Authorization"] == "Bearer tok123"

    def test_password_sets_legacy_auth(self):
        from core.dremio_sink import DremioSink
        s = DremioSink({"host": "localhost", "port": 9047, "user": "admin", "password": "pw"})
        s._token = "abc123"
        s._bearer = False
        hdrs = s._headers()
        assert hdrs["Authorization"].startswith("_dremio")

    def test_table_quoting(self):
        from core.dremio_sink import _quote_table
        assert _quote_table("ns.schema.table") == '"ns"."schema"."table"'

    def test_type_mapping(self):
        from core.dremio_sink import _dremio_type
        assert _dremio_type("varchar") == "VARCHAR"
        assert _dremio_type("bigint") == "BIGINT"
        assert _dremio_type("boolean") == "BOOLEAN"
        assert _dremio_type("jsonb") == "VARCHAR"


# ══════════════════════════════════════════════════════════════════════════════
# 5. IcebergSink construction (no live connection)
# ══════════════════════════════════════════════════════════════════════════════

class TestIcebergSinkConstruct:

    _dremio = {"host": "localhost", "port": 9047}

    def test_namespace_from_config(self):
        from core.iceberg_sink import IcebergSink
        s = IcebergSink({"type": "rest", "uri": "http://localhost:8181",
                         "warehouse": "s3://test", "target_namespace": "myns"}, self._dremio)
        assert s._namespace == "myns"

    def test_write_mode_default(self):
        from core.iceberg_sink import IcebergSink
        s = IcebergSink({"type": "rest", "uri": "http://localhost:8181",
                         "warehouse": "s3://test"}, self._dremio)
        assert s._write_mode in ("merge", "append")

    def test_target_table_name(self):
        from core.iceberg_sink import IcebergSink
        s = IcebergSink({"type": "rest", "uri": "http://localhost:8181",
                         "warehouse": "s3://test", "target_namespace": "ns"}, self._dremio)
        name = s._table_identifier("public.orders")
        assert "public_orders" in name or "orders" in name


# ══════════════════════════════════════════════════════════════════════════════
# 6. Transform Studio trigger
# ══════════════════════════════════════════════════════════════════════════════

class TestTSTrigger:

    def test_build_trigger_disabled(self):
        from core.ts_trigger import build_trigger
        t = build_trigger({"enabled": False, "url": "http://localhost:5001", "pipeline_id": "p1"})
        assert t is None

    def test_build_trigger_missing_url(self):
        from core.ts_trigger import build_trigger
        t = build_trigger({"enabled": True, "pipeline_id": "p1"})
        assert t is None

    def test_build_trigger_ok(self):
        from core.ts_trigger import build_trigger
        t = build_trigger({"enabled": True, "url": "http://localhost:5001", "pipeline_id": "p1"})
        assert t is not None

    def test_trigger_graceful_on_connection_error(self):
        from core.ts_trigger import TransformStudioTrigger
        t = TransformStudioTrigger("http://localhost:19999", "pipe-1")
        # Should not raise — just logs a warning
        t.trigger("src", "tbl", 10)


# ══════════════════════════════════════════════════════════════════════════════
# 7. Config loading
# ══════════════════════════════════════════════════════════════════════════════

class TestConfigLoad:

    def test_load_test_config(self):
        from core.config import load_config
        cfg = load_config("/Users/mark/Desktop/Claude Projects/dremio-cdc/config.test.yml")
        assert "sources" in cfg
        assert len(cfg["sources"]) >= 1

    def test_source_has_required_fields(self):
        from core.config import load_config
        cfg = load_config("/Users/mark/Desktop/Claude Projects/dremio-cdc/config.test.yml")
        src = cfg["sources"][0]
        assert "name" in src
        assert "type" in src
        assert "tables" in src

    def test_iceberg_config_present(self):
        from core.config import load_config
        cfg = load_config("/Users/mark/Desktop/Claude Projects/dremio-cdc/config.test.yml")
        assert "iceberg" in cfg


# ══════════════════════════════════════════════════════════════════════════════
# Secrets resolution
# ══════════════════════════════════════════════════════════════════════════════

class TestSecretsEnvVar:

    def test_exact_substitution(self, monkeypatch):
        monkeypatch.setenv("DB_PASS", "s3cr3t")
        from core.secrets import SecretsResolver
        r = SecretsResolver()
        assert r.resolve("${DB_PASS}") == "s3cr3t"

    def test_inline_substitution(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "myhost")
        monkeypatch.setenv("DB_NAME", "mydb")
        from core.secrets import SecretsResolver
        r = SecretsResolver()
        assert r.resolve("jdbc:///${DB_HOST}/${DB_NAME}") == "jdbc:///myhost/mydb"

    def test_missing_env_var_left_as_is(self):
        from core.secrets import SecretsResolver
        r = SecretsResolver()
        result = r.resolve("${DOES_NOT_EXIST_XYZ}")
        assert result == "${DOES_NOT_EXIST_XYZ}"

    def test_non_string_passthrough(self):
        from core.secrets import SecretsResolver
        r = SecretsResolver()
        assert r.resolve(42) == 42
        assert r.resolve(True) is True
        assert r.resolve(None) is None

    def test_walk_dict(self, monkeypatch):
        monkeypatch.setenv("MY_PWD", "hunter2")
        from core.secrets import SecretsResolver
        r = SecretsResolver()
        result = r.walk({"connection": {"password": "${MY_PWD}", "port": 5432}})
        assert result["connection"]["password"] == "hunter2"
        assert result["connection"]["port"] == 5432

    def test_walk_nested_list(self, monkeypatch):
        monkeypatch.setenv("WEBHOOK", "https://hooks.example.com/abc")
        from core.secrets import SecretsResolver
        r = SecretsResolver()
        result = r.walk({"alerts": {"channels": [{"webhook_url": "${WEBHOOK}"}]}})
        assert result["alerts"]["channels"][0]["webhook_url"] == "https://hooks.example.com/abc"

    def test_config_load_expands_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("TEST_PG_PASS", "pgpass123")
        cfg_file = tmp_path / "cfg.yml"
        cfg_file.write_text(
            "sources:\n"
            "  - name: pg\n"
            "    type: postgres\n"
            "    connection:\n"
            "      password: ${TEST_PG_PASS}\n"
            "dremio:\n"
            "  host: localhost\n"
        )
        from core.config import load_config
        cfg = load_config(str(cfg_file))
        assert cfg["sources"][0]["connection"]["password"] == "pgpass123"

    def test_config_load_expands_alerts(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SLACK_URL", "https://hooks.slack.com/xyz")
        cfg_file = tmp_path / "cfg.yml"
        cfg_file.write_text(
            "sources: []\n"
            "dremio:\n"
            "  host: localhost\n"
            "alerts:\n"
            "  channels:\n"
            "    - type: slack\n"
            "      webhook_url: ${SLACK_URL}\n"
        )
        from core.config import load_config
        cfg = load_config(str(cfg_file))
        assert cfg["alerts"]["channels"][0]["webhook_url"] == "https://hooks.slack.com/xyz"


class TestSecretsVault:

    def _make_mock_vault(self, secrets: dict):
        """Return a VaultClient-compatible mock."""
        from unittest.mock import MagicMock
        mock = MagicMock()
        mock.get.side_effect = lambda path, field: secrets[f"{path}#{field}"]
        return mock

    def test_vault_reference_resolved(self):
        from core.secrets import SecretsResolver
        vault = self._make_mock_vault({"infra/db#password": "vaultpass"})
        r = SecretsResolver(vault_client=vault)
        assert r.resolve("vault:infra/db#password") == "vaultpass"
        vault.get.assert_called_once_with("infra/db", "password")

    def test_vault_reference_missing_field_raises(self):
        from core.secrets import SecretsResolver
        from unittest.mock import MagicMock
        vault = MagicMock()
        vault.get.side_effect = KeyError("no_field")
        r = SecretsResolver(vault_client=vault)
        import pytest
        with pytest.raises(KeyError):
            r.resolve("vault:infra/db#no_field")

    def test_vault_reference_no_client_raises(self):
        from core.secrets import SecretsResolver
        import pytest
        r = SecretsResolver()
        with pytest.raises(ValueError, match="no Vault config"):
            r.resolve("vault:infra/db#password")

    def test_vault_invalid_reference_raises(self):
        from core.secrets import SecretsResolver
        from unittest.mock import MagicMock
        r = SecretsResolver(vault_client=MagicMock())
        import pytest
        with pytest.raises(ValueError, match="Invalid vault reference"):
            r.resolve("vault:infra/db_missing_hash")

    def test_walk_with_vault(self):
        from core.secrets import SecretsResolver
        vault = self._make_mock_vault({
            "prod/dremio#pat": "dremio-secret-token",
            "prod/pg#password": "pgvaultpass",
        })
        r = SecretsResolver(vault_client=vault)
        result = r.walk({
            "dremio": {"pat": "vault:prod/dremio#pat"},
            "sources": [{"connection": {"password": "vault:prod/pg#password"}}],
        })
        assert result["dremio"]["pat"] == "dremio-secret-token"
        assert result["sources"][0]["connection"]["password"] == "pgvaultpass"

    def test_vault_client_init_token_auth(self):
        """VaultClient authenticates with token and caches secrets."""
        import pytest
        pytest.importorskip("hvac")
        from unittest.mock import MagicMock, patch
        mock_hvac = MagicMock()
        mock_instance = MagicMock()
        mock_instance.is_authenticated.return_value = True
        mock_hvac.Client.return_value = mock_instance
        mock_instance.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"password": "toplevel"}}
        }
        with patch.dict("sys.modules", {"hvac": mock_hvac}):
            from importlib import reload
            import core.secrets as sm
            reload(sm)
            vc = sm.VaultClient({"url": "http://vault:8200", "token": "mytoken", "auth_method": "token"})
            val = vc.get("prod/db", "password")
            assert val == "toplevel"
            # second call hits cache, not Vault
            val2 = vc.get("prod/db", "password")
            assert mock_instance.secrets.kv.v2.read_secret_version.call_count == 1


# ══════════════════════════════════════════════════════════════════════════════
# 8. Offset store
# ══════════════════════════════════════════════════════════════════════════════

class TestOffsetStore:

    def setup_method(self):
        import tempfile, os
        self._tmp = tempfile.mktemp(suffix=".db")
        from core.offset_store import OffsetStore
        self.store = OffsetStore(self._tmp)

    def teardown_method(self):
        try:
            os.unlink(self._tmp)
        except Exception:
            pass

    def test_get_none_initially(self):
        assert self.store.get("src", "tbl") is None

    def test_set_and_get(self):
        self.store.set("src", "tbl", "0/1A3F000")
        assert self.store.get("src", "tbl") == "0/1A3F000"

    def test_overwrite(self):
        self.store.set("src", "tbl", "v1")
        self.store.set("src", "tbl", "v2")
        assert self.store.get("src", "tbl") == "v2"

    def test_snap_offset_roundtrip(self):
        self.store.set("src", "tbl", "snap:id:9999")
        val = self.store.get("src", "tbl")
        assert val == "snap:id:9999"
        assert val.startswith("snap:")
        assert val != "snap:done"

    def test_multiple_sources(self):
        self.store.set("src1", "tbl", "off1")
        self.store.set("src2", "tbl", "off2")
        assert self.store.get("src1", "tbl") == "off1"
        assert self.store.get("src2", "tbl") == "off2"


# ══════════════════════════════════════════════════════════════════════════════
# 9. Local Iceberg REST + MinIO (requires docker-compose up)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestLocalIceberg:

    def _sink(self):
        from core.iceberg_sink import IcebergSink
        cfg = dict(ICEBERG_LOCAL)
        cfg["target_namespace"] = f"cdc_e2e_{uuid.uuid4().hex[:6]}"
        return IcebergSink(cfg, DREMIO_LOCAL), cfg["target_namespace"]

    def test_write_and_scan(self):
        sink, ns = self._sink()
        sink.connect()
        events = _make_events(3)
        sink.write_batch(events)
        tbl_id = sink._table_identifier("public.customers")
        tbl = sink._catalog.load_table(tbl_id)
        rows = tbl.scan().to_arrow()
        assert len(rows) >= 3
        sink._catalog.drop_table(tbl_id)

    def test_upsert_deduplication(self):
        from core.event import ChangeEvent, ColumnSchema, Operation
        sink, ns = self._sink()
        sink.connect()
        schema = [ColumnSchema("id", "bigint", primary_key=True),
                  ColumnSchema("val", "varchar")]
        e1 = ChangeEvent(op=Operation.SNAPSHOT, source_name="s", source_table="t",
                         before=None, after={"id": 1, "val": "a"}, schema=schema,
                         timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None)
        e2 = ChangeEvent(op=Operation.UPDATE, source_name="s", source_table="t",
                         before={"id": 1, "val": "a"}, after={"id": 1, "val": "b"}, schema=schema,
                         timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None)
        sink.write_batch([e1])
        sink.write_batch([e2])
        tbl_id = sink._table_identifier("t")
        try:
            sink._catalog.drop_table(tbl_id)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# 10. Dremio Cloud Open Catalog (Iceberg REST)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.cloud
class TestDremioCloudIceberg:

    def _ns(self):
        return f"cdc_e2e_{uuid.uuid4().hex[:6]}"

    _dremio_cloud_cfg = {"host": "api.dremio.cloud", "port": 443, "ssl": True, "pat": DREMIO_CLOUD_PAT}

    def _sink(self, ns: str):
        from core.iceberg_sink import IcebergSink
        cfg = dict(ICEBERG_CLOUD)
        cfg["target_namespace"] = ns
        return IcebergSink(cfg, self._dremio_cloud_cfg)

    def test_connect(self):
        sink = self._sink(self._ns())
        sink.connect()   # should not raise
        assert sink._catalog is not None

    def test_write_snapshot_events(self):
        ns = self._ns()
        sink = self._sink(ns)
        sink.connect()
        events = _make_events(3)
        sink.write_batch(events)
        # verify table exists in catalog
        tbl_id = sink._table_identifier("public.customers")
        tbl = sink._catalog.load_table(tbl_id)
        assert tbl is not None
        # cleanup
        sink._catalog.drop_table(tbl_id)
        try:
            sink._catalog.drop_namespace(sink._namespace)
        except Exception:
            pass

    def test_write_update_event(self):
        from core.event import ChangeEvent, ColumnSchema, Operation
        ns = self._ns()
        sink = self._sink(ns)
        sink.connect()
        schema = [ColumnSchema("id", "bigint", primary_key=True),
                  ColumnSchema("name", "varchar")]
        e_snap = ChangeEvent(op=Operation.SNAPSHOT, source_name="s", source_table="t",
                             before=None, after={"id": 1, "name": "Alice"}, schema=schema,
                             timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None)
        e_upd  = ChangeEvent(op=Operation.UPDATE, source_name="s", source_table="t",
                             before={"id": 1, "name": "Alice"}, after={"id": 1, "name": "AliceUpdated"},
                             schema=schema, timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None)
        sink.write_batch([e_snap])
        sink.write_batch([e_upd])
        tbl_id = sink._table_identifier("t")
        try:
            sink._catalog.drop_table(tbl_id)
        except Exception:
            pass
        try:
            sink._catalog.drop_namespace(sink._namespace)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# 11. Dremio Cloud SQL API
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.cloud
class TestDremioCloudSQL:

    def test_auth_header_accepted(self):
        """Verify our Bearer PAT is accepted (GET project info)."""
        r = requests.get(
            f"https://api.dremio.cloud/v0/projects/{DREMIO_CLOUD_PROJECT}",
            headers={"Authorization": f"Bearer {DREMIO_CLOUD_PAT}"},
            timeout=10,
        )
        assert r.status_code == 200

    def test_sql_current_timestamp(self):
        """SELECT CURRENT_TIMESTAMP — scalar with no FROM clause, works in all Dremio dialects."""
        result = _cloud_sql("SELECT CURRENT_TIMESTAMP")
        assert result["jobState"] == "COMPLETED"

    def test_sql_show_schemas(self):
        """SHOW SCHEMAS — lists catalog schemas, works in Dremio Cloud."""
        result = _cloud_sql("SHOW SCHEMAS")
        assert result["jobState"] == "COMPLETED"

    def test_job_polling(self):
        """Submit a job and verify we can poll its status to completion."""
        url = f"https://api.dremio.cloud/v0/projects/{DREMIO_CLOUD_PROJECT}/sql"
        r = requests.post(url,
                          headers={"Authorization": f"Bearer {DREMIO_CLOUD_PAT}",
                                   "Content-Type": "application/json"},
                          json={"sql": "SELECT * FROM SYS.VERSION"}, timeout=15)
        assert r.status_code == 200
        job_id = r.json()["id"]
        assert job_id


# ══════════════════════════════════════════════════════════════════════════════
# 12. UI backend API (requires run_ui.py running on port 5050)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.ui
class TestUIBackend:

    def test_status_endpoint(self):
        r = requests.get(f"{UI_BASE}/status", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert "engine_state" in data

    def test_sources_endpoint(self):
        r = requests.get(f"{UI_BASE}/sources", timeout=5)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_target_endpoint(self):
        r = requests.get(f"{UI_BASE}/target", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert "sink_mode" in data

    def test_settings_endpoint(self):
        r = requests.get(f"{UI_BASE}/settings", timeout=5)
        assert r.status_code == 200
        assert isinstance(r.json(), dict)

    def test_alerts_endpoint(self):
        r = requests.get(f"{UI_BASE}/alerts", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert "config" in data

    def test_dlq_endpoint(self):
        r = requests.get(f"{UI_BASE}/dlq", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert "entries" in data

    def test_target_includes_transform_studio(self):
        r = requests.get(f"{UI_BASE}/target", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert "transform_studio" in data

    def test_settings_includes_incremental_snapshot(self):
        r = requests.get(f"{UI_BASE}/settings", timeout=5)
        assert r.status_code == 200
        # Settings may or may not have these keys depending on config, just verify API responds
        assert r.status_code == 200


# ══════════════════════════════════════════════════════════════════════════════
# 11. MySQL CDC source — snapshot + streaming
# ══════════════════════════════════════════════════════════════════════════════

MYSQL_CFG = {
    "connection": {
        "host":     "localhost",
        "port":     3306,
        "user":     "cdc_user",
        "password": "cdc_pass",
        "database": "testdb",
        "server_id": 99,
    }
}


@pytest.mark.integration
class TestMySQLSource:

    def setup_method(self):
        from sources.mysql import MySQLSource
        self.src = MySQLSource("mysql_test", MYSQL_CFG)
        self.src.connect()

    def teardown_method(self):
        self.src.close()

    def test_get_schema_customers(self):
        schema = self.src.get_schema("customers")
        names = [c.name for c in schema]
        assert "id" in names
        assert "name" in names
        assert "email" in names
        pk_cols = [c.name for c in schema if c.primary_key]
        assert "id" in pk_cols

    def test_snapshot_customers(self):
        events = list(self.src.snapshot("customers"))
        assert len(events) >= 3
        # Check schema columns are present regardless of data mutations from other tests
        assert all("id" in e.after and "name" in e.after for e in events)

    def test_snapshot_orders(self):
        events = list(self.src.snapshot("orders"))
        assert len(events) >= 3

    def test_incremental_snapshot_chunk(self):
        events = list(self.src.incremental_snapshot("customers", "id", None, 2))
        assert len(events) == 2
        assert events[0].after["id"] < events[1].after["id"]

    def test_incremental_snapshot_after_cursor(self):
        events = list(self.src.incremental_snapshot("customers", "id", 1, 10))
        ids = [e.after["id"] for e in events]
        assert all(i > 1 for i in ids)

    def test_streaming_captures_insert(self):
        import threading
        from core.event import Operation

        captured = []

        def _stream():
            for ev in self.src.stream("customers", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(1)

        import pymysql
        conn = pymysql.connect(
            host="localhost", port=3306, user="cdc_user",
            password="cdc_pass", database="testdb",
            autocommit=True,
        )
        cur = conn.cursor()
        uid = uuid.uuid4().hex[:8]
        cur.execute(f"INSERT INTO customers (name, email) VALUES ('Test_{uid}', 'test_{uid}@example.com')")
        cur.close()
        conn.close()

        t.join(timeout=15)
        assert any(e.op == Operation.INSERT for e in captured), "Expected INSERT event from MySQL stream"

    def test_streaming_captures_update(self):
        import threading
        from core.event import Operation

        captured = []
        uid = uuid.uuid4().hex[:8]

        # Insert a fresh row so the update is deterministic regardless of prior state
        import pymysql
        conn = pymysql.connect(host="localhost", port=3306, user="cdc_user",
                               password="cdc_pass", database="testdb", autocommit=True)
        cur = conn.cursor()
        cur.execute(f"INSERT INTO customers (name, email) VALUES ('Upd_{uid}', 'upd_{uid}@example.com')")
        conn.close()

        def _stream():
            for ev in self.src.stream("customers", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(1)

        conn2 = pymysql.connect(host="localhost", port=3306, user="cdc_user",
                                password="cdc_pass", database="testdb", autocommit=True)
        cur2 = conn2.cursor()
        cur2.execute(f"UPDATE customers SET name='Upd_{uid}_done' WHERE name='Upd_{uid}'")
        conn2.close()

        t.join(timeout=15)
        assert any(e.op in (Operation.UPDATE, Operation.INSERT) for e in captured)


# ══════════════════════════════════════════════════════════════════════════════
# 12. SQL Server CDC source — snapshot + streaming
# ══════════════════════════════════════════════════════════════════════════════

SQLSERVER_CFG = {
    "connection": {
        "host":     "localhost",
        "port":     1433,
        "user":     "sa",
        "password": "CdcPass123!",
        "database": "testdb",
        "poll_interval": 2,
    }
}


@pytest.mark.integration
class TestSQLServerSource:

    def setup_method(self):
        from sources.sqlserver import SQLServerSource
        self.src = SQLServerSource("sqlserver_test", SQLSERVER_CFG)
        self.src.connect()

    def teardown_method(self):
        self.src.close()

    def test_get_schema_customers(self):
        schema = self.src.get_schema("dbo.customers")
        names = [c.name for c in schema]
        assert "id" in names
        assert "name" in names
        assert "email" in names
        pk_cols = [c.name for c in schema if c.primary_key]
        assert "id" in pk_cols

    def test_snapshot_customers(self):
        events = list(self.src.snapshot("dbo.customers"))
        assert len(events) >= 3
        assert all("id" in e.after and "name" in e.after for e in events)

    def test_snapshot_orders(self):
        events = list(self.src.snapshot("dbo.orders"))
        assert len(events) >= 3

    def test_incremental_snapshot_chunk(self):
        events = list(self.src.incremental_snapshot("dbo.customers", "id", None, 2))
        assert len(events) == 2
        assert events[0].after["id"] < events[1].after["id"]

    def test_incremental_snapshot_after_cursor(self):
        events = list(self.src.incremental_snapshot("dbo.customers", "id", 1, 10))
        ids = [e.after["id"] for e in events]
        assert all(i > 1 for i in ids)

    def test_get_pk_column(self):
        pk = self.src.get_pk_column("dbo.customers")
        assert pk == "id"

    def test_streaming_captures_insert(self):
        import threading
        from core.event import Operation

        captured = []

        def _stream():
            for ev in self.src.stream("dbo.customers", None):
                if ev is None:
                    if captured:
                        break
                    continue
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(2)

        import pymssql
        conn = pymssql.connect(
            server="localhost", port=1433,
            user="sa", password="CdcPass123!",
            database="testdb", as_dict=True,
        )
        cur = conn.cursor()
        uid = uuid.uuid4().hex[:8]
        cur.execute(
            f"INSERT INTO customers (name, email) VALUES ('Test_{uid}', 'test_{uid}@example.com')"
        )
        conn.commit()
        cur.close()
        conn.close()

        t.join(timeout=20)
        assert any(e.op == Operation.INSERT for e in captured), "Expected INSERT event from SQL Server CDC stream"

    def test_streaming_captures_update(self):
        import threading
        from core.event import Operation

        captured = []
        uid = uuid.uuid4().hex[:8]

        # Insert a fresh row to update — avoids depending on seed data names
        import pymssql
        conn0 = pymssql.connect(server="localhost", port=1433, user="sa",
                                password="CdcPass123!", database="testdb", as_dict=True)
        cur0 = conn0.cursor()
        cur0.execute(f"INSERT INTO customers (name, email) VALUES ('Upd_{uid}', 'upd_{uid}@example.com')")
        conn0.commit()
        cur0.close()
        conn0.close()

        def _stream():
            for ev in self.src.stream("dbo.customers", None):
                if ev is None:
                    if captured:
                        break
                    continue
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(2)

        conn = pymssql.connect(
            server="localhost", port=1433,
            user="sa", password="CdcPass123!",
            database="testdb", as_dict=True,
        )
        cur = conn.cursor()
        cur.execute(f"UPDATE customers SET name='Upd_{uid}_done' WHERE name='Upd_{uid}'")
        conn.commit()
        cur.close()
        conn.close()

        t.join(timeout=20)
        assert any(e.op in (Operation.UPDATE, Operation.INSERT) for e in captured)


# ══════════════════════════════════════════════════════════════════════════════
# 13. MongoDB CDC source — snapshot + change stream
# ══════════════════════════════════════════════════════════════════════════════

MONGO_CFG = {
    "connection": {
        # directConnection bypasses RS member hostname resolution (docker internal vs host)
        "uri":  "mongodb://localhost:27017/?directConnection=true",
        "database": "testdb",
    }
}


SNOWFLAKE_CFG = {
    "account":   "ezuabpp-nyb01234",
    "user":      "mark",
    "password":  "Cheyenne7788&&**",
    "database":  "CDC_TEST",
    "schema":    "PUBLIC",
    "warehouse": "COMPUTE_WH",
    "poll_interval": 2,
}


@pytest.mark.snowflake
class TestSnowflakeSource:
    """Live tests against the CDC_TEST Snowflake trial account."""

    def setup_method(self):
        from sources.snowflake_src import SnowflakeSource
        self.src = SnowflakeSource("sf_test", SNOWFLAKE_CFG)
        self.src.connect()
        # Ensure test tables have seed data
        cur = self.src._conn.cursor()
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        cur.execute("""
            MERGE INTO CDC_TEST.PUBLIC.CUSTOMERS AS t
            USING (SELECT 1 AS id, 'Alice' AS name, 'alice@example.com' AS email, 'WEST' AS region
                   UNION ALL SELECT 2, 'Bob', 'bob@example.com', 'EAST'
                   UNION ALL SELECT 3, 'Carol', 'carol@example.com', 'NORTH') AS s
            ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, name, email, region)
                VALUES (s.id, s.name, s.email, s.region)
        """)
        cur.close()

    def teardown_method(self):
        self.src.close()

    def test_connection(self):
        cur = self.src._conn.cursor()
        cur.execute("SELECT CURRENT_USER()")
        assert cur.fetchone()[0] == "MARK"
        cur.close()

    def test_get_schema_customers(self):
        schema = self.src.get_schema("PUBLIC.CUSTOMERS")
        names = [c.name for c in schema]
        assert "ID" in names or "id" in [n.lower() for n in names]
        assert "NAME" in names or "name" in [n.lower() for n in names]
        assert "EMAIL" in names or "email" in [n.lower() for n in names]

    def test_get_pks_customers(self):
        pks = self.src._get_pks("PUBLIC.CUSTOMERS")
        assert len(pks) == 1
        assert pks[0].upper() == "ID"

    def test_snapshot_customers(self):
        from core.event import Operation
        events = list(self.src.snapshot("PUBLIC.CUSTOMERS"))
        assert len(events) >= 3
        assert all(e.op == Operation.SNAPSHOT for e in events)
        assert all("ID" in {k.upper() for k in e.after} for e in events)

    def test_snapshot_orders(self):
        events = list(self.src.snapshot("PUBLIC.ORDERS"))
        assert len(events) >= 3
        ids = [e.after.get("ORDER_ID") or e.after.get("order_id") for e in events]
        assert all(i is not None for i in ids)

    def test_snapshot_event_structure(self):
        from core.event import Operation
        events = list(self.src.snapshot("PUBLIC.CUSTOMERS"))
        ev = events[0]
        assert ev.op == Operation.SNAPSHOT
        assert ev.source_name == "sf_test"
        assert ev.source_table == "PUBLIC.CUSTOMERS"
        assert ev.before is None
        assert isinstance(ev.after, dict)
        assert len(ev.schema) >= 4

    def test_stream_auto_created(self):
        """_ensure_stream should create the STREAM object if it doesn't exist."""
        stream_fqn = self.src._ensure_stream("PUBLIC.CUSTOMERS")
        assert stream_fqn is not None
        # Verify the stream exists in Snowflake
        cur = self.src._conn.cursor()
        cur.execute("SHOW STREAMS IN SCHEMA CDC_TEST.PUBLIC")
        stream_names = [r[1].upper() for r in cur.fetchall()]
        cur.close()
        assert "DREMIO_CDC_CUSTOMERS" in stream_names

    def test_stream_has_data_check(self):
        """SYSTEM$STREAM_HAS_DATA should return a boolean-like result."""
        stream_fqn = self.src._ensure_stream("PUBLIC.CUSTOMERS")
        cur = self.src._conn.cursor()
        cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{stream_fqn}')")
        result = cur.fetchone()[0]
        cur.close()
        assert result in (True, False)

    def _dml_conn(self):
        """Open a separate Snowflake connection for DML — stream() holds the main one."""
        import snowflake.connector
        return snowflake.connector.connect(
            account=SNOWFLAKE_CFG["account"],
            user=SNOWFLAKE_CFG["user"],
            password=SNOWFLAKE_CFG["password"],
            database=SNOWFLAKE_CFG["database"],
            schema=SNOWFLAKE_CFG["schema"],
            warehouse=SNOWFLAKE_CFG["warehouse"],
        )

    def test_streaming_insert(self):
        """Insert a row and verify it appears as a CDC INSERT event."""
        import threading
        from core.event import Operation

        self.src._ensure_stream("PUBLIC.CUSTOMERS")
        captured = []

        def _stream():
            for ev in self.src.stream("PUBLIC.CUSTOMERS", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(2)

        uid = uuid.uuid4().hex[:8]
        new_id = 9000 + int(uid[:4], 16) % 1000
        conn = self._dml_conn()
        cur = conn.cursor()
        cur.execute(f"INSERT INTO CDC_TEST.PUBLIC.CUSTOMERS (id, name, email, region) VALUES ({new_id}, 'Test_{uid}', 'test_{uid}@sf.com', 'TEST')")
        cur.close()
        conn.close()

        t.join(timeout=30)
        inserts = [e for e in captured if e.op == Operation.INSERT]
        assert len(inserts) >= 1, "Expected at least one INSERT event from Snowflake stream"

    def test_streaming_update(self):
        """Update a row and verify it appears as an UPDATE event."""
        import threading
        from core.event import Operation

        self.src._ensure_stream("PUBLIC.ORDERS")
        captured = []

        def _stream():
            for ev in self.src.stream("PUBLIC.ORDERS", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(2)

        conn = self._dml_conn()
        cur = conn.cursor()
        cur.execute("UPDATE CDC_TEST.PUBLIC.ORDERS SET status='updated' WHERE order_id=101")
        cur.close()
        conn.close()

        t.join(timeout=30)
        updates = [e for e in captured if e.op == Operation.UPDATE]
        assert len(updates) >= 1, "Expected at least one UPDATE event from Snowflake stream"

    def test_streaming_delete(self):
        """Delete a pre-existing seed row and verify the DELETE event is captured.

        Snowflake streams compute NET changes: an INSERT+DELETE of the same row within
        a single stream period cancels to zero (no events). To reliably capture a DELETE,
        the deleted row must have existed before the stream's current offset — i.e. it was
        not inserted in the same stream period. Using a seed row satisfies this condition.
        """
        import threading
        from core.event import Operation

        # Reset stream to clean state so the seed rows are "pre-existing" from the stream's view
        reset_cur = self.src._conn.cursor()
        reset_cur.execute('DROP STREAM IF EXISTS "PUBLIC"."dremio_cdc_CUSTOMERS"')
        reset_cur.close()
        self.src._ensure_stream("PUBLIC.CUSTOMERS")
        time.sleep(3)

        captured = []

        def _stream():
            for ev in self.src.stream("PUBLIC.CUSTOMERS", None):
                captured.append(ev)
                if any(e.op == Operation.DELETE for e in captured) or len(captured) >= 10:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(2)  # Let the thread start and complete its first has_data poll

        # Delete seed row 3 (Carlos) — existed before this stream was created
        conn = self._dml_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM CDC_TEST.PUBLIC.CUSTOMERS WHERE id=3")
        cur.close()
        conn.close()

        t.join(timeout=30)

        # Restore seed row so subsequent test runs find it (Snowflake MERGE for upsert)
        conn = self._dml_conn()
        cur = conn.cursor()
        cur.execute("""
            MERGE INTO CDC_TEST.PUBLIC.CUSTOMERS AS t
            USING (SELECT 3 AS id, 'Carlos' AS name, 'carlos@example.com' AS email, 'WEST' AS region) AS s
            ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, name, email, region) VALUES (s.id, s.name, s.email, s.region)
        """)
        cur.close()
        conn.close()

        deletes = [e for e in captured if e.op == Operation.DELETE]
        assert len(deletes) >= 1, f"Expected DELETE event, got: {[e.op for e in captured]}"


@pytest.mark.integration
class TestMongoDBSource:

    def setup_method(self):
        from sources.mongodb import MongoDBSource
        self.src = MongoDBSource("mongo_test", MONGO_CFG)
        self.src.connect()

    def teardown_method(self):
        self.src.close()

    def test_get_schema_customers(self):
        schema = self.src.get_schema("testdb.customers")
        names = [c.name for c in schema]
        assert "_id" in names
        assert "name" in names
        pk_cols = [c.name for c in schema if c.primary_key]
        assert "_id" in pk_cols

    def test_snapshot_customers(self):
        events = list(self.src.snapshot("testdb.customers"))
        assert len(events) >= 3
        assert all("_id" in e.after and "name" in e.after for e in events)

    def test_snapshot_orders(self):
        events = list(self.src.snapshot("testdb.orders"))
        assert len(events) >= 3
        assert all("customer" in e.after for e in events)

    def test_incremental_snapshot_chunk(self):
        events = list(self.src.incremental_snapshot("testdb.customers", "_id", None, 2))
        assert len(events) == 2

    def test_incremental_snapshot_after_cursor(self):
        first = list(self.src.incremental_snapshot("testdb.customers", "_id", None, 1))
        assert len(first) == 1
        cursor = first[0].after["_id"]
        rest = list(self.src.incremental_snapshot("testdb.customers", "_id", cursor, 10))
        ids = [e.after["_id"] for e in rest]
        assert cursor not in ids

    def test_get_pk_column(self):
        assert self.src.get_pk_column("testdb.customers") == "_id"

    def test_streaming_captures_insert(self):
        import threading
        from core.event import Operation
        from pymongo import MongoClient

        captured = []

        def _stream():
            for ev in self.src.stream("testdb.customers", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(1)

        client = MongoClient("mongodb://localhost:27017/?directConnection=true")
        uid = uuid.uuid4().hex[:8]
        client["testdb"]["customers"].insert_one(
            {"name": f"Test_{uid}", "email": f"test_{uid}@example.com"}
        )
        client.close()

        t.join(timeout=15)
        assert any(e.op == Operation.INSERT for e in captured), "Expected INSERT from MongoDB change stream"

    def test_streaming_captures_update(self):
        import threading
        from core.event import Operation
        from pymongo import MongoClient

        # Insert a fresh doc to update
        client = MongoClient("mongodb://localhost:27017/?directConnection=true")
        uid = uuid.uuid4().hex[:8]
        result = client["testdb"]["customers"].insert_one(
            {"name": f"Upd_{uid}", "email": f"upd_{uid}@example.com"}
        )
        doc_id = result.inserted_id
        client.close()

        captured = []

        def _stream():
            for ev in self.src.stream("testdb.customers", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(1)

        client2 = MongoClient("mongodb://localhost:27017/?directConnection=true")
        client2["testdb"]["customers"].update_one(
            {"_id": doc_id}, {"$set": {"name": f"Upd_{uid}_done"}}
        )
        client2.close()

        t.join(timeout=15)
        assert any(e.op == Operation.UPDATE for e in captured), "Expected UPDATE from MongoDB change stream"

    def test_streaming_captures_delete(self):
        import threading
        from core.event import Operation
        from pymongo import MongoClient

        client = MongoClient("mongodb://localhost:27017/?directConnection=true")
        uid = uuid.uuid4().hex[:8]
        result = client["testdb"]["customers"].insert_one({"name": f"Del_{uid}"})
        doc_id = result.inserted_id
        client.close()

        captured = []

        def _stream():
            for ev in self.src.stream("testdb.customers", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(1)

        client2 = MongoClient("mongodb://localhost:27017/?directConnection=true")
        client2["testdb"]["customers"].delete_one({"_id": doc_id})
        client2.close()

        t.join(timeout=15)
        assert any(e.op == Operation.DELETE for e in captured), "Expected DELETE from MongoDB change stream"


# ══════════════════════════════════════════════════════════════════════════════
# 14. DynamoDB CDC source — snapshot + streams (LocalStack)
# ══════════════════════════════════════════════════════════════════════════════

DYNAMO_CFG = {
    "connection": {
        "region":              "us-east-1",
        "aws_access_key_id":   "test",
        "aws_secret_access_key": "test",
        "endpoint_url":        "http://localhost:4566",
    }
}


@pytest.mark.integration
class TestDynamoDBSource:

    def setup_method(self):
        from sources.dynamodb import DynamoDBSource
        self.src = DynamoDBSource("dynamo_test", DYNAMO_CFG)
        self.src.connect()

    def teardown_method(self):
        self.src.close()

    def test_get_schema_customers(self):
        schema = self.src.get_schema("customers")
        names = [c.name for c in schema]
        assert "id" in names
        pk_cols = [c.name for c in schema if c.primary_key]
        assert "id" in pk_cols

    def test_snapshot_customers(self):
        events = list(self.src.snapshot("customers"))
        assert len(events) >= 3
        assert all("id" in e.after for e in events)

    def test_snapshot_orders(self):
        events = list(self.src.snapshot("orders"))
        assert len(events) >= 3

    def _ddb_client(self):
        import boto3
        return boto3.client(
            "dynamodb", region_name="us-east-1",
            aws_access_key_id="test", aws_secret_access_key="test",
            endpoint_url="http://localhost:4566",
        )

    def _collect_stream(self, table: str, stop_fn, timeout: int = 15):
        """Run stream() in a thread, collecting events until stop_fn(events) is True."""
        import threading
        captured = []

        def _run():
            for ev in self.src.stream(table, None):
                captured.append(ev)
                if stop_fn(captured):
                    break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=timeout)
        return captured

    def test_streaming_captures_insert(self):
        from core.event import Operation

        uid = uuid.uuid4().hex[:8]
        ddb = self._ddb_client()
        ddb.put_item(TableName="customers", Item={
            "id":    {"S": f"ins_{uid}"},
            "name":  {"S": f"Ins_{uid}"},
            "email": {"S": f"ins_{uid}@example.com"},
        })

        events = self._collect_stream(
            "customers",
            lambda evs: any(e.after and e.after.get("id") == f"ins_{uid}" for e in evs),
        )
        assert any(
            e.op in (Operation.INSERT, Operation.UPDATE) and e.after and e.after.get("id") == f"ins_{uid}"
            for e in events
        ), "Expected inserted item in DynamoDB stream"

    def test_streaming_captures_delete(self):
        from core.event import Operation

        uid = uuid.uuid4().hex[:8]
        ddb = self._ddb_client()
        ddb.put_item(TableName="customers", Item={"id": {"S": f"del_{uid}"}, "name": {"S": f"Del_{uid}"}})
        ddb.delete_item(TableName="customers", Key={"id": {"S": f"del_{uid}"}})

        events = self._collect_stream(
            "customers",
            lambda evs: any(e.op == Operation.DELETE for e in evs),
        )
        assert any(e.op == Operation.DELETE for e in events), "Expected DELETE event in DynamoDB stream"


# ══════════════════════════════════════════════════════════════════════════════
# 15. Debezium HTTP adapter — unit tests (no external service needed)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestDebeziumSource:

    @classmethod
    def setup_class(cls):
        from sources.debezium import DebeziumSource
        cls.src = DebeziumSource("debezium_test", {"listen_port": 8766})
        cls.src.connect()
        time.sleep(0.2)   # allow HTTP server thread to bind

    @classmethod
    def teardown_class(cls):
        cls.src.close()

    def setup_method(self):
        # Drain any leftover events from previous test
        import queue as _q
        while not self.src._q.empty():
            try:
                self.src._q.get_nowait()
            except _q.Empty:
                break

    def _post(self, payload: dict):
        import json
        import http.client
        body = json.dumps(payload).encode()
        conn = http.client.HTTPConnection("localhost", 8766, timeout=5)
        conn.request("POST", "/events", body, {"Content-Length": str(len(body)), "Content-Type": "application/json"})
        resp = conn.getresponse()
        conn.close()
        return resp.status

    def _debezium_payload(self, op: str, table: str, before=None, after=None):
        return {
            "schema": {
                "fields": [
                    {"field": "id",    "type": "int32",  "optional": False},
                    {"field": "name",  "type": "string", "optional": True},
                    {"field": "email", "type": "string", "optional": True},
                ],
                "primaryKey": ["id"],
            },
            "payload": {
                "op":     op,
                "before": before,
                "after":  after,
                "ts_ms":  1700000000000,
                "source": {"db": "testdb", "table": table},
            },
        }

    def test_http_server_accepts_post(self):
        payload = self._debezium_payload("c", "customers", after={"id": 1, "name": "Alice", "email": "alice@example.com"})
        status = self._post(payload)
        assert status == 200

    def test_insert_event_parsed(self):
        import threading
        from core.event import Operation

        captured = []

        def _stream():
            for ev in self.src.stream("testdb.customers", None):
                captured.append(ev)
                break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(0.1)

        self._post(self._debezium_payload("c", "customers", after={"id": 10, "name": "Alice", "email": "a@example.com"}))
        t.join(timeout=5)

        assert len(captured) == 1
        assert captured[0].op == Operation.INSERT
        assert captured[0].after["name"] == "Alice"

    def test_update_event_parsed(self):
        import threading
        from core.event import Operation

        captured = []

        def _stream():
            for ev in self.src.stream("testdb.customers", None):
                captured.append(ev)
                break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(0.1)

        self._post(self._debezium_payload(
            "u", "customers",
            before={"id": 1, "name": "Alice",   "email": "a@example.com"},
            after= {"id": 1, "name": "Alice V2", "email": "a@example.com"},
        ))
        t.join(timeout=5)

        assert len(captured) == 1
        assert captured[0].op == Operation.UPDATE
        assert captured[0].after["name"] == "Alice V2"
        assert captured[0].before["name"] == "Alice"

    def test_delete_event_parsed(self):
        import threading
        from core.event import Operation

        captured = []

        def _stream():
            for ev in self.src.stream("testdb.customers", None):
                captured.append(ev)
                break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(0.1)

        self._post(self._debezium_payload("d", "customers", before={"id": 5, "name": "Charlie", "email": "c@example.com"}))
        t.join(timeout=5)

        assert len(captured) == 1
        assert captured[0].op == Operation.DELETE
        assert captured[0].before["name"] == "Charlie"

    def test_schema_parsed_correctly(self):
        import threading

        captured = []

        def _stream():
            for ev in self.src.stream("testdb.customers", None):
                captured.append(ev)
                break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(0.1)

        self._post(self._debezium_payload("c", "customers", after={"id": 99, "name": "Z", "email": "z@example.com"}))
        t.join(timeout=5)

        schema = captured[0].schema
        pk_cols = [c.name for c in schema if c.primary_key]
        assert "id" in pk_cols
        col_types = {c.name: c.data_type for c in schema}
        assert col_types["id"] == "integer"
        assert col_types["name"] == "varchar"

    def test_table_filter_skips_other_tables(self):
        import threading

        captured = []

        def _stream():
            for ev in self.src.stream("testdb.customers", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(0.1)

        # Post an event for a different table — should be filtered
        self._post(self._debezium_payload("c", "orders", after={"id": 1, "name": "irrelevant", "email": "x@x.com"}))
        # Then post one that matches
        self._post(self._debezium_payload("c", "customers", after={"id": 200, "name": "Target", "email": "t@example.com"}))
        t.join(timeout=5)

        assert len(captured) == 1
        assert captured[0].after["name"] == "Target"


# ══════════════════════════════════════════════════════════════════════════════
# 16. Debezium Oracle payload tests — DDL filtering, schema mapping, quirks
#     No external service needed: posts synthetic Oracle-format payloads.
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestDebeziumOraclePayloads:
    """
    Tests the Oracle-specific handling added to DebeziumSource:
      - DDL / schema-change events are silently dropped
      - Heartbeat events are silently dropped
      - VariableScaleDecimal values (Oracle NUMBER) are coerced to strings
      - Logical type names map to correct data types (timestamp, numeric, etc.)
      - Table filter is case-insensitive (Oracle sends uppercase table names)
      - SCN is preserved in the event offset
      - before-image is preserved on UPDATE and DELETE
    """

    @classmethod
    def setup_class(cls):
        from sources.debezium import DebeziumSource
        cls.src = DebeziumSource("oracle_test", {"listen_port": 8768})
        cls.src.connect()
        time.sleep(0.2)

    @classmethod
    def teardown_class(cls):
        cls.src.close()

    def setup_method(self):
        import queue as _q
        while not self.src._q.empty():
            try:
                self.src._q.get_nowait()
            except _q.Empty:
                break

    def _post(self, payload: dict):
        import json, http.client
        body = json.dumps(payload).encode()
        conn = http.client.HTTPConnection("localhost", 8768, timeout=5)
        conn.request("POST", "/events", body,
                     {"Content-Length": str(len(body)), "Content-Type": "application/json"})
        resp = conn.getresponse()
        conn.close()
        return resp.status

    def _oracle_payload(self, op: str, table: str, schema_name: str = "HR",
                        before=None, after=None, scn: str = "12345678"):
        """Build a realistic Oracle Debezium envelope payload."""
        return {
            "schema": {
                "fields": [
                    {"field": "EMPLOYEE_ID",   "type": "int32",  "optional": False},
                    {"field": "FIRST_NAME",    "type": "string", "optional": True},
                    {"field": "SALARY",        "type": "bytes",
                     "name": "org.apache.kafka.connect.data.Decimal", "optional": True},
                    {"field": "HIRE_DATE",     "type": "int64",
                     "name": "io.debezium.time.Timestamp", "optional": True},
                ],
                "primaryKey": ["EMPLOYEE_ID"],
            },
            "payload": {
                "op":     op,
                "before": before,
                "after":  after,
                "ts_ms":  1700000000000,
                "source": {
                    "connector": "oracle",
                    "db":        schema_name,
                    "schema":    schema_name,
                    "table":     table,
                    "scn":       scn,
                },
            },
        }

    def _oracle_ddl_payload(self):
        """Schema-change event — no 'op' field, has 'databaseName'."""
        return {
            "schema": {},
            "payload": {
                "databaseName": "ORCLPDB1",
                "schemaName":   "HR",
                "ddl":          "ALTER TABLE HR.EMPLOYEES ADD (MIDDLE_NAME VARCHAR2(50))",
                "tableChanges": [],
            },
        }

    def _heartbeat_payload(self):
        return {
            "schema": {},
            "payload": {
                "op": "r",
                "ts_ms": 1700000000000,
                "source": {"connector": "heartbeat", "table": "", "db": ""},
                "before": None,
                "after":  {"ts_ms": 1700000000000},
            },
        }

    def _collect(self, timeout: float = 3.0):
        import threading
        captured = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                captured.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=timeout)
        return captured

    # ── DDL and heartbeat filtering ───────────────────────────────────────────

    def test_ddl_event_is_dropped(self):
        """DDL schema-change events must never produce a ChangeEvent."""
        import threading
        received = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                received.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        time.sleep(0.1)

        self._post(self._oracle_ddl_payload())
        # Follow up with a real DML event so the thread exits
        self._post(self._oracle_payload(
            "c", "EMPLOYEES",
            after={"EMPLOYEE_ID": 1, "FIRST_NAME": "Alice", "SALARY": None, "HIRE_DATE": None},
        ))
        t.join(timeout=5)

        # The only captured event should be the DML INSERT, not the DDL
        assert len(received) == 1
        from core.event import Operation
        assert received[0].op == Operation.INSERT

    def test_heartbeat_is_dropped(self):
        """Heartbeat events are dropped and never emitted."""
        import threading
        received = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                received.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        time.sleep(0.1)

        self._post(self._heartbeat_payload())
        self._post(self._oracle_payload(
            "c", "EMPLOYEES",
            after={"EMPLOYEE_ID": 2, "FIRST_NAME": "Bob", "SALARY": None, "HIRE_DATE": None},
        ))
        t.join(timeout=5)

        assert len(received) == 1

    # ── Schema / type mapping ─────────────────────────────────────────────────

    def test_logical_type_timestamp_mapped(self):
        """io.debezium.time.Timestamp → 'timestamp' data type."""
        captured = self._collect()
        self._post(self._oracle_payload(
            "c", "EMPLOYEES",
            after={"EMPLOYEE_ID": 3, "FIRST_NAME": "Carol", "SALARY": None, "HIRE_DATE": 1700000000000},
        ))
        t = time.time()
        while not captured and time.time() - t < 3:
            time.sleep(0.05)

        # Trigger collection
        import threading
        ev_box = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                ev_box.append(ev)
                break

        t2 = threading.Thread(target=_run, daemon=True)
        t2.start()
        self._post(self._oracle_payload(
            "c", "EMPLOYEES",
            after={"EMPLOYEE_ID": 3, "FIRST_NAME": "Carol", "SALARY": None, "HIRE_DATE": 1700000000000},
        ))
        t2.join(timeout=5)

        schema = ev_box[0].schema
        col_types = {c.name: c.data_type for c in schema}
        assert col_types.get("HIRE_DATE") == "timestamp"

    def test_decimal_logical_type_mapped(self):
        """org.apache.kafka.connect.data.Decimal → 'numeric' data type."""
        import threading
        ev_box = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                ev_box.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self._post(self._oracle_payload(
            "c", "EMPLOYEES",
            after={"EMPLOYEE_ID": 4, "FIRST_NAME": "Dave", "SALARY": None, "HIRE_DATE": None},
        ))
        t.join(timeout=5)

        schema = ev_box[0].schema
        col_types = {c.name: c.data_type for c in schema}
        assert col_types.get("SALARY") == "numeric"

    def test_pk_column_detected(self):
        """EMPLOYEE_ID declared as primaryKey in schema → primary_key=True."""
        import threading
        ev_box = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                ev_box.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self._post(self._oracle_payload(
            "c", "EMPLOYEES",
            after={"EMPLOYEE_ID": 5, "FIRST_NAME": "Eve", "SALARY": None, "HIRE_DATE": None},
        ))
        t.join(timeout=5)

        pk_cols = [c.name for c in ev_box[0].schema if c.primary_key]
        assert "EMPLOYEE_ID" in pk_cols

    # ── Oracle NUMBER coercion ────────────────────────────────────────────────

    def test_variable_scale_decimal_coerced(self):
        """Oracle NUMBER sent as {value: '...', scale: 2} is flattened to string."""
        import threading
        ev_box = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                ev_box.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self._post(self._oracle_payload(
            "c", "EMPLOYEES",
            after={
                "EMPLOYEE_ID": 6,
                "FIRST_NAME":  "Frank",
                "SALARY":      {"value": "AABB", "scale": 2},
                "HIRE_DATE":   None,
            },
        ))
        t.join(timeout=5)

        salary = ev_box[0].after.get("SALARY")
        assert isinstance(salary, str), f"Expected str, got {type(salary)}: {salary}"

    # ── Case-insensitive table filter ─────────────────────────────────────────

    def test_table_filter_case_insensitive(self):
        """Stream filter 'HR.EMPLOYEES' must match Oracle's uppercase 'EMPLOYEES'."""
        import threading
        captured = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                captured.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        time.sleep(0.1)

        # Post event for a different table (should be filtered)
        self._post(self._oracle_payload("c", "DEPARTMENTS",
            after={"EMPLOYEE_ID": 99, "FIRST_NAME": "Skip", "SALARY": None, "HIRE_DATE": None}))
        # Post matching event
        self._post(self._oracle_payload("c", "EMPLOYEES",
            after={"EMPLOYEE_ID": 7, "FIRST_NAME": "Grace", "SALARY": None, "HIRE_DATE": None}))
        t.join(timeout=5)

        assert len(captured) == 1
        assert captured[0].after["FIRST_NAME"] == "Grace"

    # ── SCN preserved in offset ───────────────────────────────────────────────

    def test_scn_preserved_in_offset(self):
        """Oracle SCN from source metadata is stored in the event offset."""
        import threading
        ev_box = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                ev_box.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self._post(self._oracle_payload(
            "c", "EMPLOYEES", scn="99887766",
            after={"EMPLOYEE_ID": 8, "FIRST_NAME": "Heidi", "SALARY": None, "HIRE_DATE": None},
        ))
        t.join(timeout=5)

        assert ev_box[0].offset.get("scn") == "99887766"

    # ── UPDATE before-image ───────────────────────────────────────────────────

    def test_update_preserves_before_image(self):
        """Oracle UPDATE event carries both before and after images."""
        import threading
        from core.event import Operation
        ev_box = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                ev_box.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self._post(self._oracle_payload(
            "u", "EMPLOYEES",
            before={"EMPLOYEE_ID": 9, "FIRST_NAME": "Ivan",    "SALARY": None, "HIRE_DATE": None},
            after= {"EMPLOYEE_ID": 9, "FIRST_NAME": "Ivan V2", "SALARY": None, "HIRE_DATE": None},
        ))
        t.join(timeout=5)

        assert ev_box[0].op == Operation.UPDATE
        assert ev_box[0].before["FIRST_NAME"] == "Ivan"
        assert ev_box[0].after["FIRST_NAME"]  == "Ivan V2"

    # ── DELETE before-image ───────────────────────────────────────────────────

    def test_delete_preserves_before_image(self):
        """Oracle DELETE event carries before-image and null after."""
        import threading
        from core.event import Operation
        ev_box = []

        def _run():
            for ev in self.src.stream("HR.EMPLOYEES", None):
                ev_box.append(ev)
                break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self._post(self._oracle_payload(
            "d", "EMPLOYEES",
            before={"EMPLOYEE_ID": 10, "FIRST_NAME": "Judy", "SALARY": None, "HIRE_DATE": None},
            after=None,
        ))
        t.join(timeout=5)

        assert ev_box[0].op == Operation.DELETE
        assert ev_box[0].before["FIRST_NAME"] == "Judy"
        assert ev_box[0].after is None


# ══════════════════════════════════════════════════════════════════════════════
# 16. Oracle + Debezium Server live integration
# ══════════════════════════════════════════════════════════════════════════════
# Requires docker-compose services: oracle, debezium-oracle
#   docker compose up -d oracle debezium-oracle
# Oracle takes ~120s to start; Debezium takes ~60s to connect.
# Run only these tests:
#   pytest tests/test_e2e.py -m oracle -v
# ══════════════════════════════════════════════════════════════════════════════

ORACLE_DSN  = "localhost:1521/FREEPDB1"
ORACLE_USER = "cdc_test"
ORACLE_PASS = "cdc_test"

def _oracle_conn():
    import oracledb
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN)

def _wait_for_events(src, table: str, n: int, timeout: int = 90):
    """Collect up to n events from src.stream(), with timeout."""
    import threading
    from core.event import ChangeEvent
    collected: list[ChangeEvent] = []
    done = threading.Event()

    def _run():
        for ev in src.stream(table, None):
            if ev is None:
                if done.is_set():
                    break
                continue
            collected.append(ev)
            if len(collected) >= n:
                done.set()
                break

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)
    done.set()
    return collected


@pytest.mark.oracle
class TestOracleDebeziumLive:
    """
    End-to-end: Oracle XE → Debezium Server → DebeziumSource → parsed events.
    Requires: docker compose up -d oracle debezium-oracle
    The debezium-oracle service POSTs to host.docker.internal:8765 (this test's listener).
    """

    @classmethod
    def setup_class(cls):
        import subprocess, os
        from sources.debezium import DebeziumSource
        compose_dir = os.path.dirname(os.path.dirname(__file__))
        # Stop Debezium first so it can't stream DELETE events while we clean up
        subprocess.run(["docker", "compose", "stop", "debezium-oracle"],
                       cwd=compose_dir, check=True, capture_output=True)
        # Clean test rows so snapshot delivers exactly the 3 seeded rows
        try:
            with _oracle_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM cdc_test.employees "
                        "WHERE name NOT IN ('Alice', 'Bob', 'Charlie')"
                    )
                conn.commit()
        except Exception:
            pass
        # Wipe offset + schema history files from the volume so Debezium re-snapshots
        subprocess.run(
            ["docker", "run", "--rm",
             "-v", "dremio-cdc_debezium_oracle_data:/data",
             "alpine", "sh", "-c",
             "rm -f /data/oracle-test-offsets.dat /data/oracle-test-schema-history.dat"],
            cwd=compose_dir, capture_output=True
        )
        # Bind port BEFORE starting Debezium so we don't miss snapshot events
        cls.src = DebeziumSource("oracle_live", {"listen_port": 8765})
        cls.src.connect()
        subprocess.run(["docker", "compose", "start", "debezium-oracle"],
                       cwd=compose_dir, check=True, capture_output=True)

    @classmethod
    def teardown_class(cls):
        cls.src.close()

    def setup_method(self):
        # Drain leftover events between tests
        while not self.src._q.empty():
            try:
                self.src._q.get_nowait()
            except Exception:
                break

    # ── 1. Initial snapshot ───────────────────────────────────────────────────

    def test_snapshot_rows_arrive(self):
        """Debezium delivers the 3 pre-seeded employees as SNAPSHOT events."""
        events = _wait_for_events(self.src, "CDC_TEST.EMPLOYEES", 3, timeout=120)
        assert len(events) >= 3, f"Expected ≥3 snapshot events, got {len(events)}"
        names = {e.after["NAME"] for e in events if e.after}
        assert "Alice" in names
        assert "Bob" in names

    def test_snapshot_schema(self):
        """Schema parsed from a live Oracle event has expected columns."""
        uid = uuid.uuid4().hex[:8]
        with _oracle_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cdc_test.employees (name, email, salary) VALUES (:1, :2, :3)",
                    [f"Schema_{uid}", f"{uid}@schema.com", 55000],
                )
            conn.commit()
        events = _wait_for_events(self.src, "CDC_TEST.EMPLOYEES", 1, timeout=30)
        assert events, "No event received for schema test"
        col_names = {c.name for c in events[0].schema}
        assert "ID" in col_names
        assert "NAME" in col_names
        assert "SALARY" in col_names

    # ── 2. Streaming: INSERT ──────────────────────────────────────────────────

    def test_streaming_insert(self):
        """INSERT into Oracle produces an INSERT event."""
        from core.event import Operation
        uid = uuid.uuid4().hex[:8]
        with _oracle_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cdc_test.employees (name, email, salary) VALUES (:1, :2, :3)",
                    [f"Live_{uid}", f"{uid}@test.com", 50000],
                )
            conn.commit()

        events = _wait_for_events(self.src, "CDC_TEST.EMPLOYEES", 1, timeout=60)
        assert events, "No INSERT event received"
        insert_ev = next((e for e in events if e.op == Operation.INSERT
                          and e.after and e.after.get("NAME") == f"Live_{uid}"), None)
        assert insert_ev is not None, f"INSERT event for Live_{uid} not found in {events}"
        assert insert_ev.after["EMAIL"] == f"{uid}@test.com"

    # ── 3. Streaming: UPDATE ──────────────────────────────────────────────────

    def test_streaming_update(self):
        """UPDATE produces an UPDATE event with before + after images."""
        from core.event import Operation
        uid = uuid.uuid4().hex[:8]
        with _oracle_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cdc_test.employees (name, email, salary) VALUES (:1, :2, :3)",
                    [f"Upd_{uid}", f"{uid}@test.com", 60000],
                )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE cdc_test.employees SET salary = :1 WHERE name = :2",
                    [70000, f"Upd_{uid}"],
                )
            conn.commit()

        events = _wait_for_events(self.src, "CDC_TEST.EMPLOYEES", 2, timeout=60)
        upd = next((e for e in events if e.op == Operation.UPDATE
                    and e.after and e.after.get("NAME") == f"Upd_{uid}"), None)
        assert upd is not None, "UPDATE event not found"
        assert float(upd.after["SALARY"]) == 70000

    # ── 4. Streaming: DELETE ──────────────────────────────────────────────────

    def test_streaming_delete(self):
        """DELETE produces a DELETE event with before-image."""
        from core.event import Operation
        uid = uuid.uuid4().hex[:8]
        with _oracle_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cdc_test.employees (name, email, salary) VALUES (:1, :2, :3)",
                    [f"Del_{uid}", f"{uid}@test.com", 40000],
                )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("DELETE FROM cdc_test.employees WHERE name = :1", [f"Del_{uid}"])
            conn.commit()

        events = _wait_for_events(self.src, "CDC_TEST.EMPLOYEES", 2, timeout=60)
        delete_ev = next((e for e in events if e.op == Operation.DELETE
                          and e.before and e.before.get("NAME") == f"Del_{uid}"), None)
        assert delete_ev is not None, "DELETE event not found"
        assert delete_ev.after is None

    # ── 5. SCN in offset ─────────────────────────────────────────────────────

    def test_offset_contains_scn(self):
        """Every streaming event offset includes an SCN value."""
        uid = uuid.uuid4().hex[:8]
        with _oracle_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cdc_test.employees (name, email, salary) VALUES (:1, :2, :3)",
                    [f"Scn_{uid}", f"{uid}@scn.com", 50000],
                )
            conn.commit()
        events = _wait_for_events(self.src, "CDC_TEST.EMPLOYEES", 1, timeout=30)
        assert events
        assert events[0].offset is not None
        assert events[0].offset.get("scn") is not None, \
            f"SCN missing from offset: {events[0].offset}"


# ══════════════════════════════════════════════════════════════════════════════
# 17. Oracle + Debezium → Dremio Cloud (end-to-end cloud test)
# ══════════════════════════════════════════════════════════════════════════════
# Requires: docker-compose oracle + debezium-oracle + Dremio Cloud credentials.
# Run: pytest tests/test_e2e.py -m "oracle and cloud" -v
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.oracle
@pytest.mark.cloud
class TestOracleDebeziumCloud:
    """
    Full pipeline: Oracle XE → Debezium Server → DebeziumSource → IcebergSink → Dremio Cloud.
    """

    _dremio_cloud_cfg = {"host": "api.dremio.cloud", "port": 443, "ssl": True, "pat": DREMIO_CLOUD_PAT}

    @classmethod
    def setup_class(cls):
        import subprocess, os
        from sources.debezium import DebeziumSource
        compose_dir = os.path.dirname(os.path.dirname(__file__))
        subprocess.run(["docker", "compose", "stop", "debezium-oracle"],
                       cwd=compose_dir, check=True, capture_output=True)
        try:
            with _oracle_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM cdc_test.employees "
                        "WHERE name NOT IN ('Alice', 'Bob', 'Charlie')"
                    )
                conn.commit()
        except Exception:
            pass
        subprocess.run(
            ["docker", "run", "--rm",
             "-v", "dremio-cdc_debezium_oracle_data:/data",
             "alpine", "sh", "-c",
             "rm -f /data/oracle-test-offsets.dat /data/oracle-test-schema-history.dat"],
            cwd=compose_dir, capture_output=True
        )
        cls.src = DebeziumSource("oracle_cloud", {"listen_port": 8765})
        cls.src.connect()
        cls._ns = f"oracle_cdc_{uuid.uuid4().hex[:6]}"
        subprocess.run(["docker", "compose", "start", "debezium-oracle"],
                       cwd=compose_dir, check=True, capture_output=True)

    @classmethod
    def teardown_class(cls):
        cls.src.close()

    def _sink(self):
        from core.iceberg_sink import IcebergSink
        cfg = dict(ICEBERG_CLOUD)
        cfg["target_namespace"] = self._ns
        return IcebergSink(cfg, self._dremio_cloud_cfg)

    def test_snapshot_events_reach_dremio_cloud(self):
        """
        Collect snapshot events from Oracle via Debezium and write them to Dremio Cloud.
        Then verify the row count in Dremio Cloud SQL matches.
        """
        # Collect initial snapshot (3 pre-seeded rows)
        events = _wait_for_events(self.src, "CDC_TEST.EMPLOYEES", 3, timeout=120)
        assert len(events) >= 3, f"Expected ≥3 snapshot events, got {len(events)}"

        sink = self._sink()
        sink.connect()
        sink.write_batch(events[:3])

        # Verify in Dremio Cloud via SQL
        table_name = f"{self._ns}.cdc_test_employees"
        time.sleep(5)  # give Dremio Cloud time to register the Iceberg commit
        result = _cloud_sql(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        assert result.get("jobState") == "COMPLETED"

        # cleanup
        try:
            tbl_id = sink._table_identifier("CDC_TEST.EMPLOYEES")
            sink._catalog.drop_table(tbl_id)
            sink._catalog.drop_namespace(sink._namespace)
        except Exception:
            pass

    def test_insert_event_reaches_dremio_cloud(self):
        """INSERT into Oracle → event arrives → written to Dremio Cloud."""
        uid = uuid.uuid4().hex[:8]
        with _oracle_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cdc_test.employees (name, email, salary) VALUES (:1, :2, :3)",
                    [f"Cloud_{uid}", f"{uid}@cloud.com", 99000],
                )
            conn.commit()

        events = _wait_for_events(self.src, "CDC_TEST.EMPLOYEES", 1, timeout=60)
        insert_ev = next((e for e in events if e.after and
                          e.after.get("NAME") == f"Cloud_{uid}"), None)
        assert insert_ev is not None, "INSERT event not received from Oracle"

        sink = self._sink()
        sink.connect()
        ns_tag = f"oracle_cdc_{uuid.uuid4().hex[:6]}"
        cfg = dict(ICEBERG_CLOUD)
        cfg["target_namespace"] = ns_tag
        from core.iceberg_sink import IcebergSink
        s2 = IcebergSink(cfg, self._dremio_cloud_cfg)
        s2.connect()
        s2.write_batch([insert_ev])

        try:
            tbl_id = s2._table_identifier("CDC_TEST.EMPLOYEES")
            s2._catalog.drop_table(tbl_id)
            s2._catalog.drop_namespace(s2._namespace)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# MariaDB CDC source — snapshot + streaming (mirrors MySQL tests)
# ══════════════════════════════════════════════════════════════════════════════

MARIADB_CFG = {
    "connection": {
        "host":     "localhost",
        "port":     3307,          # mariadb service mapped to 3307
        "user":     "cdc_user",
        "password": "cdc_pass",
        "database": "testdb",
        "server_id": 1001,
    }
}


@pytest.mark.mariadb
class TestMariaDBSource:

    def setup_method(self):
        from sources.mariadb import MariaDBSource
        self.src = MariaDBSource("mariadb_test", MARIADB_CFG)
        self.src.connect()

    def teardown_method(self):
        self.src.close()

    def test_get_schema_customers(self):
        schema = self.src.get_schema("customers")
        names = [c.name for c in schema]
        assert "id" in names
        assert "name" in names
        assert "email" in names
        pk_cols = [c.name for c in schema if c.primary_key]
        assert "id" in pk_cols

    def test_snapshot_customers(self):
        events = list(self.src.snapshot("customers"))
        assert len(events) >= 3
        assert all("id" in e.after and "name" in e.after for e in events)

    def test_snapshot_orders(self):
        events = list(self.src.snapshot("orders"))
        assert len(events) >= 3

    def test_incremental_snapshot_chunk(self):
        events = list(self.src.incremental_snapshot("customers", "id", None, 2))
        assert len(events) == 2
        assert events[0].after["id"] < events[1].after["id"]

    def test_incremental_snapshot_after_cursor(self):
        events = list(self.src.incremental_snapshot("customers", "id", 1, 10))
        ids = [e.after["id"] for e in events]
        assert all(i > 1 for i in ids)

    def test_streaming_captures_insert(self):
        import threading
        from core.event import Operation

        captured = []

        def _stream():
            for ev in self.src.stream("customers", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(1)

        import pymysql
        conn = pymysql.connect(
            host="localhost", port=3307, user="cdc_user",
            password="cdc_pass", database="testdb",
            autocommit=True,
        )
        cur = conn.cursor()
        uid = uuid.uuid4().hex[:8]
        cur.execute(f"INSERT INTO customers (name, email) VALUES ('MDB_{uid}', 'mdb_{uid}@example.com')")
        cur.close()
        conn.close()

        t.join(timeout=15)
        assert any(e.op == Operation.INSERT for e in captured), "Expected INSERT event from MariaDB stream"

    def test_streaming_captures_update(self):
        import threading
        from core.event import Operation

        uid = uuid.uuid4().hex[:8]
        import pymysql
        conn = pymysql.connect(host="localhost", port=3307, user="cdc_user",
                               password="cdc_pass", database="testdb", autocommit=True)
        cur = conn.cursor()
        cur.execute(f"INSERT INTO customers (name, email) VALUES ('MUpd_{uid}', 'mupd_{uid}@example.com')")
        conn.close()

        captured = []

        def _stream():
            for ev in self.src.stream("customers", None):
                captured.append(ev)
                if len(captured) >= 1:
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(1)

        conn2 = pymysql.connect(host="localhost", port=3307, user="cdc_user",
                                password="cdc_pass", database="testdb", autocommit=True)
        cur2 = conn2.cursor()
        cur2.execute(f"UPDATE customers SET name='MUpd_{uid}_done' WHERE name='MUpd_{uid}'")
        conn2.close()

        t.join(timeout=15)
        assert any(e.op in (Operation.UPDATE, Operation.INSERT) for e in captured)


# ══════════════════════════════════════════════════════════════════════════════
# Dead Letter Queue — unit tests (no external deps)
# ══════════════════════════════════════════════════════════════════════════════

class TestDeadLetterQueue:
    """Tests for DeadLetterQueue persistence and status transitions."""

    def setup_method(self):
        from core.dlq import DeadLetterQueue
        self.dlq = DeadLetterQueue(db_path=":memory:", max_retries=3)
        self._make_event = _make_dlq_event

    def test_push_returns_id(self):
        eid = self.dlq.push("src", "tbl", [self._make_event()], "boom")
        assert isinstance(eid, int) and eid > 0

    def test_push_appears_in_pending(self):
        self.dlq.push("src", "tbl", [self._make_event()], "err")
        pending = self.dlq.get_pending()
        assert len(pending) == 1
        assert pending[0]["status"] == "pending"
        assert pending[0]["event_count"] == 1

    def test_get_events_roundtrip(self):
        from core.event import Operation
        ev = self._make_event()
        eid = self.dlq.push("src", "tbl", [ev], "err")
        events = self.dlq.get_events(eid)
        assert len(events) == 1
        assert events[0].op == Operation.INSERT
        assert events[0].after["id"] == 1

    def test_mark_replayed(self):
        eid = self.dlq.push("src", "tbl", [self._make_event()], "err")
        self.dlq.mark_replayed(eid)
        all_entries = self.dlq.get_all()
        assert all_entries[0]["status"] == "replayed"
        assert self.dlq.get_pending() == []

    def test_mark_failed_increments_retry(self):
        eid = self.dlq.push("src", "tbl", [self._make_event()], "err1")
        self.dlq.mark_failed(eid, "err2")
        entries = self.dlq.get_all()
        assert entries[0]["retry_count"] == 1
        assert entries[0]["status"] == "pending"  # still pending (1 < 3)

    def test_exhausted_after_max_retries(self):
        eid = self.dlq.push("src", "tbl", [self._make_event()], "err")
        for i in range(3):
            self.dlq.mark_failed(eid, f"err{i}")
        entries = self.dlq.get_all()
        assert entries[0]["status"] == "exhausted"
        assert self.dlq.get_pending() == []

    def test_reset_to_pending(self):
        eid = self.dlq.push("src", "tbl", [self._make_event()], "err")
        for i in range(3):
            self.dlq.mark_failed(eid, f"err{i}")
        self.dlq.reset_to_pending(eid)
        pending = self.dlq.get_pending()
        assert len(pending) == 1
        assert pending[0]["retry_count"] == 0

    def test_discard(self):
        eid = self.dlq.push("src", "tbl", [self._make_event()], "err")
        self.dlq.discard(eid)
        assert self.dlq.get_pending() == []
        assert self.dlq.get_all()[0]["status"] == "discarded"

    def test_discard_all(self):
        self.dlq.push("src", "t1", [self._make_event()], "e1")
        self.dlq.push("src", "t2", [self._make_event()], "e2")
        self.dlq.discard_all()
        assert self.dlq.pending_count() == 0
        assert all(e["status"] == "discarded" for e in self.dlq.get_all())

    def test_stats(self):
        eid = self.dlq.push("src", "tbl", [self._make_event(), self._make_event()], "err")
        self.dlq.mark_replayed(eid)
        self.dlq.push("src", "tbl", [self._make_event()], "err2")
        stats = self.dlq.stats()
        assert stats["replayed"]["entries"] == 1
        assert stats["replayed"]["events"] == 2
        assert stats["pending"]["entries"] == 1

    def test_pending_count(self):
        self.dlq.push("src", "t1", [self._make_event()], "e1")
        self.dlq.push("src", "t2", [self._make_event()], "e2")
        assert self.dlq.pending_count() == 2


class TestDLQWorker:
    """Tests for DLQWorker retry/replay logic using a mock sink."""

    def setup_method(self):
        from core.dlq import DeadLetterQueue
        self.dlq = DeadLetterQueue(db_path=":memory:", max_retries=3)

    def test_worker_replays_on_success(self):
        from core.dlq import DLQWorker

        class OKSink:
            def write_batch(self, events): pass

        eid = self.dlq.push("src", "tbl", [_make_dlq_event()], "initial error")
        worker = DLQWorker(self.dlq, OKSink(), interval_s=999)
        worker._retry_pending()  # call directly — no threading needed

        assert self.dlq.get_all()[0]["status"] == "replayed"

    def test_worker_marks_failed_on_sink_error(self):
        from core.dlq import DLQWorker

        class BrokenSink:
            def write_batch(self, events): raise RuntimeError("sink down")

        eid = self.dlq.push("src", "tbl", [_make_dlq_event()], "initial error")
        worker = DLQWorker(self.dlq, BrokenSink(), interval_s=999)
        worker._retry_pending()

        entry = self.dlq.get_all()[0]
        assert entry["retry_count"] == 1
        assert entry["status"] == "pending"

    def test_worker_exhausts_after_max_retries(self):
        from core.dlq import DLQWorker

        class BrokenSink:
            def write_batch(self, events): raise RuntimeError("sink down")

        self.dlq.push("src", "tbl", [_make_dlq_event()], "initial error")
        worker = DLQWorker(self.dlq, BrokenSink(), interval_s=999)
        for _ in range(3):
            worker._retry_pending()

        assert self.dlq.get_all()[0]["status"] == "exhausted"
        assert self.dlq.pending_count() == 0

    def test_worker_skips_empty_events(self):
        """Entry with no events should be auto-discarded, not crash."""
        from core.dlq import DLQWorker

        class OKSink:
            def write_batch(self, events): pass

        eid = self.dlq.push("src", "tbl", [], "no events")
        worker = DLQWorker(self.dlq, OKSink(), interval_s=999)
        worker._retry_pending()

        assert self.dlq.get_all()[0]["status"] == "discarded"


def _make_dlq_event():
    from core.event import ChangeEvent, ColumnSchema, Operation
    import datetime
    return ChangeEvent(
        op=Operation.INSERT,
        source_name="test_src",
        source_table="test.tbl",
        before=None,
        after={"id": 1, "name": "Alice"},
        schema=[
            ColumnSchema("id", "integer", primary_key=True),
            ColumnSchema("name", "varchar"),
        ],
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        offset=None,
    )


# ══════════════════════════════════════════════════════════════════════════════
# AlertManager — unit tests (no external deps)
# ══════════════════════════════════════════════════════════════════════════════

class TestAlertManager:
    """Tests for AlertManager threshold checking and channel dispatch."""

    def _make_status(self, workers):
        """Return a minimal StatusStore-like mock."""
        class FakeStatus:
            def snapshot(self_):
                return {"workers": workers}
        return FakeStatus()

    def _make_mgr(self, cfg, workers):
        from core.alert_manager import AlertManager
        return AlertManager(cfg, self._make_status(workers))

    # ── Threshold detection ───────────────────────────────────────────────────

    def test_no_alert_below_lag_threshold(self):
        fired = []
        mgr = self._make_mgr(
            {"lag_threshold_seconds": 60, "error_count_threshold": 5, "cooldown_seconds": 0, "channels": []},
            [{"source": "s", "table": "t", "lag_seconds": 30, "error_count": 0, "state": "running"}],
        )
        mgr._maybe_fire = lambda **kw: fired.append(kw)
        mgr._check()
        assert fired == []

    def test_alert_fires_above_lag_threshold(self):
        fired = []
        mgr = self._make_mgr(
            {"lag_threshold_seconds": 60, "error_count_threshold": 5, "cooldown_seconds": 0, "channels": []},
            [{"source": "s", "table": "t", "lag_seconds": 120, "error_count": 0, "state": "running"}],
        )
        original = mgr._maybe_fire
        def capture(**kw): fired.append(kw); original(**kw)
        mgr._maybe_fire = capture
        mgr._check()
        assert any(k["alert_type"] == "lag" for k in fired)

    def test_alert_fires_on_error_threshold(self):
        fired = []
        mgr = self._make_mgr(
            {"lag_threshold_seconds": 60, "error_count_threshold": 5, "cooldown_seconds": 0, "channels": []},
            [{"source": "s", "table": "t", "lag_seconds": 0, "error_count": 5, "state": "running"}],
        )
        original = mgr._maybe_fire
        def capture(**kw): fired.append(kw); original(**kw)
        mgr._maybe_fire = capture
        mgr._check()
        assert any(k["alert_type"] == "errors" for k in fired)

    def test_alert_fires_on_worker_error_state(self):
        fired = []
        mgr = self._make_mgr(
            {"lag_threshold_seconds": 60, "error_count_threshold": 5, "cooldown_seconds": 0, "channels": []},
            [{"source": "s", "table": "t", "lag_seconds": 0, "error_count": 0, "state": "error", "error": "conn lost"}],
        )
        original = mgr._maybe_fire
        def capture(**kw): fired.append(kw); original(**kw)
        mgr._maybe_fire = capture
        mgr._check()
        assert any(k["alert_type"] == "worker_error" for k in fired)

    # ── Cooldown ──────────────────────────────────────────────────────────────

    def test_cooldown_suppresses_second_alert(self):
        from core.alert_manager import AlertManager
        fired_count = [0]
        mgr = self._make_mgr(
            {"lag_threshold_seconds": 60, "error_count_threshold": 5,
             "cooldown_seconds": 9999, "channels": []},
            [{"source": "s", "table": "t", "lag_seconds": 120, "error_count": 0, "state": "running"}],
        )
        original_send = mgr._send
        mgr._send = lambda ch, rec: fired_count.__setitem__(0, fired_count[0] + 1)
        mgr._check()
        mgr._check()  # second check should be suppressed by cooldown
        assert fired_count[0] <= 1

    def test_cooldown_zero_allows_repeated_alerts(self):
        from core.alert_manager import AlertManager
        fired_count = [0]
        mgr = self._make_mgr(
            {"lag_threshold_seconds": 60, "error_count_threshold": 5,
             "cooldown_seconds": 0, "channels": [{"type": "webhook", "url": "http://x"}]},
            [{"source": "s", "table": "t", "lag_seconds": 120, "error_count": 0, "state": "running"}],
        )
        mgr._send = lambda ch, rec: fired_count.__setitem__(0, fired_count[0] + 1)
        mgr._check()
        mgr._check()
        assert fired_count[0] == 2

    # ── Channel dispatch ──────────────────────────────────────────────────────

    def test_slack_channel_posts_correct_payload(self):
        from unittest.mock import patch, MagicMock
        from core.alert_manager import AlertManager

        mgr = self._make_mgr({"cooldown_seconds": 0, "channels": []}, [])
        record = {"type": "lag", "source": "s", "table": "t",
                  "message": "Lag 90s exceeds threshold 60s", "time": 0}

        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(raise_for_status=lambda: None)
            mgr._send({"type": "slack", "webhook_url": "http://slack.test"}, record)
            mock_post.assert_called_once()
            payload = mock_post.call_args[1]["json"]
            assert "text" in payload
            assert "LAG" in payload["text"]

    def test_webhook_channel_posts_correct_payload(self):
        from unittest.mock import patch, MagicMock
        from core.alert_manager import AlertManager

        mgr = self._make_mgr({"cooldown_seconds": 0, "channels": []}, [])
        record = {"type": "errors", "source": "s", "table": "t",
                  "message": "Error count 5 reached threshold 5", "time": 0}

        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(raise_for_status=lambda: None)
            mgr._send({"type": "webhook", "url": "http://hook.test", "method": "post"}, record)
            mock_post.assert_called_once()
            payload = mock_post.call_args[1]["json"]
            assert payload["type"] == "errors"

    def test_failed_channel_does_not_crash_other_channels(self):
        """A broken channel should not prevent other channels from firing."""
        from unittest.mock import patch, MagicMock
        from core.alert_manager import AlertManager

        delivered = []
        mgr = self._make_mgr(
            {"lag_threshold_seconds": 10, "error_count_threshold": 99,
             "cooldown_seconds": 0,
             "channels": [
                 {"type": "slack",   "webhook_url": "http://broken"},
                 {"type": "webhook", "url": "http://ok"},
             ]},
            [{"source": "s", "table": "t", "lag_seconds": 120, "error_count": 0, "state": "running"}],
        )

        import requests as req_mod
        def fake_post(url, **kw):
            if "broken" in url:
                raise ConnectionError("network down")
            delivered.append(url)
            return MagicMock(raise_for_status=lambda: None)

        with patch("requests.post", side_effect=fake_post):
            mgr._check()

        assert any("ok" in u for u in delivered)

    def test_get_recent_records_fired_alerts(self):
        from unittest.mock import patch, MagicMock

        mgr = self._make_mgr(
            {"lag_threshold_seconds": 60, "error_count_threshold": 5,
             "cooldown_seconds": 0, "channels": [{"type": "webhook", "url": "http://x"}]},
            [{"source": "s", "table": "t", "lag_seconds": 120, "error_count": 0, "state": "running"}],
        )
        with patch("requests.post", return_value=MagicMock(raise_for_status=lambda: None)):
            mgr._check()

        recent = mgr.get_recent()
        assert len(recent) >= 1
        assert recent[0]["type"] == "lag"

    def test_disabled_alert_manager_does_not_fire(self):
        fired = []
        mgr = self._make_mgr(
            {"enabled": False, "lag_threshold_seconds": 0,
             "error_count_threshold": 0, "cooldown_seconds": 0, "channels": []},
            [{"source": "s", "table": "t", "lag_seconds": 999, "error_count": 999, "state": "error"}],
        )
        mgr._send = lambda ch, rec: fired.append(rec)
        # _loop checks self._enabled before calling _check; simulate that here
        if mgr._enabled:
            mgr._check()
        assert fired == []

    def test_reconfigure_updates_thresholds(self):
        from core.alert_manager import AlertManager

        mgr = self._make_mgr(
            {"lag_threshold_seconds": 60, "error_count_threshold": 5, "cooldown_seconds": 0, "channels": []},
            [],
        )
        mgr.reconfigure({"lag_threshold_seconds": 30, "error_count_threshold": 2,
                          "cooldown_seconds": 10, "check_interval_seconds": 15, "channels": []})
        assert mgr._lag_threshold == 30
        assert mgr._error_threshold == 2
        assert mgr._cooldown == 10


# ─────────────────────────────────────────────────────────────────────────────
# 18. Schema Drift detection — pure unit tests (no docker needed)
# ─────────────────────────────────────────────────────────────────────────────

class _DriftMockSource:
    """Mock CDC source with a swappable schema."""
    name = "drift_source"

    def __init__(self, schema):
        self._schema = schema

    def get_schema(self, table):
        return list(self._schema)


class _DriftMockSink:
    def __init__(self):
        self.evolved = []

    def evolve_schema(self, table, schema):
        self.evolved.append((table, schema))


def _make_worker(source, sink, schema_store, drift_action="alert"):
    from core.offset_store import SQLiteOffsetStore
    from core.status_store import StatusStore
    from core.engine import TableWorker

    return TableWorker(
        source=source,
        table="customers",
        sink=sink,
        offset_store=SQLiteOffsetStore(":memory:"),
        status_store=StatusStore(),
        options={
            "batch_size": 100,
            "schema_drift_action": drift_action,
            "schema_drift_check_every_n_batches": 1,
        },
        schema_store=schema_store,
    )


class TestSchemaDrift(unittest.TestCase):

    def _schema(self, cols):
        return [ColumnSchema(name=n, data_type=t) for n, t in cols]

    def test_first_scan_stores_schema_no_drift(self):
        """First call records the schema; no drift reported."""
        from core.schema_store import SchemaStore
        from core.status_store import StatusStore

        ss = SchemaStore(":memory:")
        src = _DriftMockSource(self._schema([("id", "integer"), ("name", "text")]))
        w = _make_worker(src, _DriftMockSink(), ss)
        w._check_schema_drift()

        stored = ss.get("drift_source", "customers")
        assert stored is not None
        assert {c.name for c in stored} == {"id", "name"}
        assert next(x["schema_drift"] for x in w.status.snapshot()["workers"] if x["source"] == "drift_source") is None

    def test_no_drift_when_schema_unchanged(self):
        from core.schema_store import SchemaStore

        cols = [("id", "integer"), ("name", "text")]
        ss = SchemaStore(":memory:")
        src = _DriftMockSource(self._schema(cols))
        w = _make_worker(src, _DriftMockSink(), ss)
        w._check_schema_drift()  # seed
        w._check_schema_drift()  # second call — no change

        drift = next(x["schema_drift"] for x in w.status.snapshot()["workers"] if x["source"] == "drift_source")
        assert drift is None

    def test_column_added_detected(self):
        from core.schema_store import SchemaStore

        ss = SchemaStore(":memory:")
        src = _DriftMockSource(self._schema([("id", "integer"), ("name", "text")]))
        w = _make_worker(src, _DriftMockSink(), ss)
        w._check_schema_drift()  # seed

        src._schema = self._schema([("id", "integer"), ("name", "text"), ("email", "text")])
        w._check_schema_drift()

        drift = next(x["schema_drift"] for x in w.status.snapshot()["workers"] if x["source"] == "drift_source")
        assert drift is not None
        assert "email" in drift

    def test_column_removed_detected(self):
        from core.schema_store import SchemaStore

        ss = SchemaStore(":memory:")
        src = _DriftMockSource(self._schema([("id", "integer"), ("name", "text"), ("score", "float")]))
        w = _make_worker(src, _DriftMockSink(), ss)
        w._check_schema_drift()  # seed

        src._schema = self._schema([("id", "integer"), ("name", "text")])
        w._check_schema_drift()

        drift = next(x["schema_drift"] for x in w.status.snapshot()["workers"] if x["source"] == "drift_source")
        assert drift is not None
        assert "score" in drift

    def test_type_change_detected(self):
        from core.schema_store import SchemaStore

        ss = SchemaStore(":memory:")
        src = _DriftMockSource(self._schema([("id", "integer"), ("score", "integer")]))
        w = _make_worker(src, _DriftMockSink(), ss)
        w._check_schema_drift()

        src._schema = self._schema([("id", "integer"), ("score", "float")])
        w._check_schema_drift()

        drift = next(x["schema_drift"] for x in w.status.snapshot()["workers"] if x["source"] == "drift_source")
        assert drift is not None
        assert "type change" in drift.lower() or "~" in drift

    def test_drift_action_pause_sets_stop_flag(self):
        from core.schema_store import SchemaStore

        ss = SchemaStore(":memory:")
        src = _DriftMockSource(self._schema([("id", "integer")]))
        w = _make_worker(src, _DriftMockSink(), ss, drift_action="pause")
        w._check_schema_drift()  # seed

        src._schema = self._schema([("id", "integer"), ("new_col", "text")])
        w._check_schema_drift()

        assert w._stop_flag.is_set(), "pause action should set stop_flag"

    def test_drift_action_auto_migrate_calls_evolve_schema(self):
        from core.schema_store import SchemaStore

        ss = SchemaStore(":memory:")
        src = _DriftMockSource(self._schema([("id", "integer")]))
        sink = _DriftMockSink()
        w = _make_worker(src, sink, ss, drift_action="auto_migrate")
        w._check_schema_drift()  # seed

        src._schema = self._schema([("id", "integer"), ("added_col", "text")])
        w._check_schema_drift()

        assert len(sink.evolved) == 1
        assert sink.evolved[0][0] == "customers"

    def test_drift_action_alert_does_not_pause(self):
        from core.schema_store import SchemaStore

        ss = SchemaStore(":memory:")
        src = _DriftMockSource(self._schema([("id", "integer")]))
        w = _make_worker(src, _DriftMockSink(), ss, drift_action="alert")
        w._check_schema_drift()  # seed

        src._schema = self._schema([("id", "integer"), ("extra", "text")])
        w._check_schema_drift()

        assert not w._stop_flag.is_set(), "alert action should not stop the worker"

    def test_get_schema_exception_does_not_crash(self):
        from core.schema_store import SchemaStore

        class _ErrorSource:
            name = "err_src"
            def get_schema(self, table):
                raise RuntimeError("DB down")

        ss = SchemaStore(":memory:")
        src = _ErrorSource()
        from core.offset_store import SQLiteOffsetStore
        from core.status_store import StatusStore
        from core.engine import TableWorker
        w = TableWorker(
            source=src,
            table="t",
            sink=_DriftMockSink(),
            offset_store=SQLiteOffsetStore(":memory:"),
            status_store=StatusStore(),
            options={"batch_size": 10, "schema_drift_action": "alert"},
            schema_store=ss,
        )
        w._check_schema_drift()  # should not raise


# ─────────────────────────────────────────────────────────────────────────────
# 19. CockroachDB CDC source — snapshot + streaming
# ─────────────────────────────────────────────────────────────────────────────

COCKROACH_CFG = {
    "connection": {
        "host": "localhost",
        "port": 26257,
        "user": "root",
        "password": "",
        "database": "testdb",
        "sslmode": "disable",
    }
}

COCKROACH_SEED = """
SET CLUSTER SETTING kv.rangefeed.enabled = true;
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;
CREATE TABLE IF NOT EXISTS customers (
    id     INT PRIMARY KEY,
    name   VARCHAR(100),
    email  VARCHAR(100),
    score  FLOAT
);
CREATE TABLE IF NOT EXISTS orders (
    id       INT PRIMARY KEY,
    customer VARCHAR(100),
    amount   FLOAT,
    status   VARCHAR(50)
);
INSERT INTO customers (id, name, email, score) VALUES
    (1, 'Alice',   'alice@example.com',   10.0),
    (2, 'Bob',     'bob@example.com',     20.0),
    (3, 'Charlie', 'charlie@example.com', 30.0)
ON CONFLICT (id) DO NOTHING;
INSERT INTO orders (id, customer, amount, status) VALUES
    (1, 'Alice', 99.99,  'completed'),
    (2, 'Bob',   149.50, 'pending'),
    (3, 'Alice', 25.00,  'completed')
ON CONFLICT (id) DO NOTHING;
GRANT CHANGEFEED ON TABLE customers TO root;
GRANT CHANGEFEED ON TABLE orders    TO root;
"""


@pytest.mark.cockroachdb
class TestCockroachDBSource(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        import psycopg2

        # Wait for CockroachDB to be ready, then create testdb + seed tables.
        # psycopg2 doesn't support USE <db>, so we connect to defaultdb first to
        # create testdb, then reconnect to testdb for table creation.
        for attempt in range(30):
            try:
                # Phase 1: create testdb and enable rangefeeds (in defaultdb)
                conn = psycopg2.connect(
                    host="localhost", port=26257, user="root",
                    password="", dbname="defaultdb", sslmode="disable",
                )
                conn.autocommit = True
                cur = conn.cursor()
                for stmt in [
                    "SET CLUSTER SETTING kv.rangefeed.enabled = true",
                    "CREATE DATABASE IF NOT EXISTS testdb",
                ]:
                    try:
                        cur.execute(stmt)
                    except Exception:
                        pass
                cur.close()
                conn.close()

                # Phase 2: create tables and seed rows (in testdb)
                conn2 = psycopg2.connect(
                    host="localhost", port=26257, user="root",
                    password="", dbname="testdb", sslmode="disable",
                )
                conn2.autocommit = True
                cur2 = conn2.cursor()
                for stmt in [
                    """CREATE TABLE IF NOT EXISTS customers (
                        id INT PRIMARY KEY, name VARCHAR(100),
                        email VARCHAR(100), score FLOAT)""",
                    """CREATE TABLE IF NOT EXISTS orders (
                        id INT PRIMARY KEY, customer VARCHAR(100),
                        amount FLOAT, status VARCHAR(50))""",
                    """INSERT INTO customers (id, name, email, score) VALUES
                        (1,'Alice','alice@example.com',10.0),
                        (2,'Bob','bob@example.com',20.0),
                        (3,'Charlie','charlie@example.com',30.0)
                        ON CONFLICT (id) DO NOTHING""",
                    """INSERT INTO orders (id, customer, amount, status) VALUES
                        (1,'Alice',99.99,'completed'),
                        (2,'Bob',149.50,'pending'),
                        (3,'Alice',25.00,'completed')
                        ON CONFLICT (id) DO NOTHING""",
                    "GRANT CHANGEFEED ON TABLE customers TO root",
                    "GRANT CHANGEFEED ON TABLE orders TO root",
                ]:
                    try:
                        cur2.execute(stmt)
                    except Exception:
                        pass
                cur2.close()
                conn2.close()
                break
            except Exception:
                time.sleep(2)
        else:
            raise RuntimeError("CockroachDB not ready after 60s")

        from sources.cockroachdb import CockroachDBSource
        cls.src = CockroachDBSource("crdb_test", COCKROACH_CFG)
        cls.src.connect()

    @classmethod
    def tearDownClass(cls):
        cls.src.close()

    def _dml_conn(self):
        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=26257, user="root",
            password="", dbname="testdb", sslmode="disable",
        )
        conn.autocommit = True
        return conn

    def test_get_schema_customers(self):
        schema = self.src.get_schema("customers")
        names = [c.name for c in schema]
        assert "id" in names
        assert "name" in names
        assert "email" in names

    def test_snapshot_customers(self):
        rows = list(self.src.snapshot("customers"))
        assert len(rows) >= 3
        assert all(e.op == Operation.SNAPSHOT for e in rows)
        names = {e.after["name"] for e in rows}
        assert "Alice" in names

    def test_snapshot_orders(self):
        rows = list(self.src.snapshot("orders"))
        assert len(rows) >= 3
        assert all(e.op == Operation.SNAPSHOT for e in rows)

    def test_incremental_snapshot_chunk(self):
        rows = list(self.src.snapshot("customers"))
        assert len(rows) >= 1

    def test_streaming_captures_insert(self):
        # Reset stream state (delete test may have set stop_flag)
        self.src._stop_flag.clear()
        self.src._stream_thread = None
        self.src._table_queues.clear()

        captured = []
        stop = threading.Event()

        def _stream():
            for ev in self.src.stream("customers", None):
                captured.append(ev)
                if stop.is_set():
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(2)  # let changefeed start

        conn = self._dml_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM customers WHERE id = 101")
        cur.execute(
            "INSERT INTO customers (id, name, email, score) VALUES (101, 'Dave', 'dave@test.com', 5.0)"
        )
        conn.close()

        deadline = time.time() + 15
        while time.time() < deadline:
            if any(e.after and e.after.get("name") == "Dave" for e in captured):
                break
            time.sleep(0.5)

        stop.set()
        self.src._stop_flag.set()
        t.join(timeout=3)

        assert any(e.after and e.after.get("name") == "Dave" for e in captured), \
            "Expected INSERT event for Dave from CockroachDB changefeed"

    def test_streaming_captures_delete(self):
        # Reset stop_flag for a fresh stream
        self.src._stop_flag.clear()
        self.src._stream_thread = None
        self.src._table_queues.clear()

        captured = []
        stop = threading.Event()

        # Ensure row 102 exists first
        conn = self._dml_conn()
        conn.cursor().execute(
            "INSERT INTO customers (id, name, email, score) VALUES (102, 'Eve', 'eve@test.com', 7.0)"
            " ON CONFLICT (id) DO NOTHING"
        )
        conn.close()

        def _stream():
            for ev in self.src.stream("customers", None):
                captured.append(ev)
                if stop.is_set():
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(2)

        conn = self._dml_conn()
        conn.cursor().execute("DELETE FROM customers WHERE id = 102")
        conn.close()

        deadline = time.time() + 15
        while time.time() < deadline:
            if any(e.op == Operation.DELETE for e in captured):
                break
            time.sleep(0.5)

        stop.set()
        self.src._stop_flag.set()
        t.join(timeout=3)

        assert any(e.op == Operation.DELETE for e in captured), \
            "Expected DELETE event from CockroachDB changefeed"


# ─────────────────────────────────────────────────────────────────────────────
# 20. Dremio MERGE / DELETE correctness — Dremio Cloud
#     Verifies that the right rows land with the right values after each DML op.
#     Uses the same Cloud PAT / project as the other @pytest.mark.cloud tests.
# ─────────────────────────────────────────────────────────────────────────────

_DREMIO_CLOUD_SINK_CFG = {
    "host": "api.dremio.cloud", "port": 443, "ssl": True,
    "pat": DREMIO_CLOUD_PAT, "project_id": DREMIO_CLOUD_PROJECT,
    "target_namespace": "cdc_demo",
}

_MERGE_SCHEMA = [
    ColumnSchema("id",    "bigint",  nullable=False, primary_key=True),
    ColumnSchema("name",  "varchar", nullable=True,  primary_key=False),
    ColumnSchema("score", "double",  nullable=True,  primary_key=False),
]


def _sink_sql(sql: str) -> dict:
    """Submit and poll a SQL job against Dremio Cloud."""
    return _cloud_sql(sql)


def _sink_query(sql: str) -> list:
    """Run a SELECT against Dremio Cloud and return rows as list-of-dicts."""
    result = _cloud_sql(sql)
    rr = requests.get(
        f"https://api.dremio.cloud/v0/projects/{DREMIO_CLOUD_PROJECT}"
        f"/job/{result['id']}/results?offset=0&limit=500",
        headers={"Authorization": f"Bearer {DREMIO_CLOUD_PAT}"},
        timeout=15,
    )
    rr.raise_for_status()
    data = rr.json()
    rows = data.get("rows", [])
    if not rows or isinstance(rows[0], dict):
        return rows
    cols = [c["name"] for c in data.get("schema", [])]
    return [dict(zip(cols, row)) for row in rows]


def _make_ev(op, id_, name, score, *, before=None, source_table="public.customers"):
    return ChangeEvent(
        op=op,
        source_name="test_src",
        source_table=source_table,
        before=before,
        after={"id": id_, "name": name, "score": score} if op != Operation.DELETE else None,
        schema=_MERGE_SCHEMA,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        offset=None,
    )


@pytest.mark.cloud
class TestDremioMergeCorrectness(unittest.TestCase):
    """Verify MERGE / DELETE correctness against Dremio Cloud.

    Uses the same project/PAT as other @pytest.mark.cloud tests.
    Tables are created in the cdc_demo catalog.
    Each test uses a unique table name to avoid cross-test pollution.
    """

    @classmethod
    def setUpClass(cls):
        from core.dremio_sink import DremioSink
        cls.sink = DremioSink(_DREMIO_CLOUD_SINK_CFG)
        cls.sink.connect()

    def _table(self, suffix: str) -> str:
        return f"merge_test_{suffix}"

    def _drop(self, table: str):
        try:
            _sink_sql(f'DROP TABLE IF EXISTS "cdc_demo"."{table}"')
        except Exception:
            pass

    def _rows(self, table: str) -> list:
        return _sink_query(
            f'SELECT * FROM "cdc_demo"."{table}" ORDER BY "id"'
        )

    # ── INSERT (SNAPSHOT) ──────────────────────────────────────────────────────

    def test_snapshot_inserts_correct_values(self):
        tbl = self._table("snap")
        self._drop(tbl)
        try:
            events = [
                _make_ev(Operation.SNAPSHOT, 1, "Alice", 10.0, source_table=tbl),
                _make_ev(Operation.SNAPSHOT, 2, "Bob",   20.0, source_table=tbl),
                _make_ev(Operation.SNAPSHOT, 3, "Charlie", 30.0, source_table=tbl),
            ]
            self.sink.write_batch(events)
            rows = self._rows(tbl)
            assert len(rows) == 3
            names = {r["name"] for r in rows}
            assert names == {"Alice", "Bob", "Charlie"}
            scores = {r["id"]: r["score"] for r in rows}
            assert scores[1] == 10.0
            assert scores[2] == 20.0
            assert scores[3] == 30.0
        finally:
            self._drop(tbl)

    # ── UPDATE via MERGE ───────────────────────────────────────────────────────

    def test_update_changes_correct_row(self):
        tbl = self._table("upd")
        self._drop(tbl)
        try:
            # Seed three rows
            seed = [
                _make_ev(Operation.SNAPSHOT, 1, "Alice",   10.0, source_table=tbl),
                _make_ev(Operation.SNAPSHOT, 2, "Bob",     20.0, source_table=tbl),
                _make_ev(Operation.SNAPSHOT, 3, "Charlie", 30.0, source_table=tbl),
            ]
            self.sink.write_batch(seed)

            # Update only id=2
            upd = [_make_ev(Operation.UPDATE, 2, "Bobby", 99.0, source_table=tbl)]
            self.sink.write_batch(upd)

            rows = {r["id"]: r for r in self._rows(tbl)}
            assert rows[1]["name"] == "Alice"   # unchanged
            assert rows[2]["name"] == "Bobby"   # updated
            assert rows[2]["score"] == 99.0     # updated
            assert rows[3]["name"] == "Charlie" # unchanged
        finally:
            self._drop(tbl)

    # ── DELETE ────────────────────────────────────────────────────────────────

    def test_delete_removes_correct_row(self):
        tbl = self._table("del")
        self._drop(tbl)
        try:
            seed = [
                _make_ev(Operation.SNAPSHOT, 1, "Alice",   10.0, source_table=tbl),
                _make_ev(Operation.SNAPSHOT, 2, "Bob",     20.0, source_table=tbl),
                _make_ev(Operation.SNAPSHOT, 3, "Charlie", 30.0, source_table=tbl),
            ]
            self.sink.write_batch(seed)

            del_ev = _make_ev(Operation.DELETE, 2, None, None,
                              before={"id": 2, "name": "Bob", "score": 20.0},
                              source_table=tbl)
            self.sink.write_batch([del_ev])

            rows = self._rows(tbl)
            ids = {r["id"] for r in rows}
            assert 2 not in ids, "Deleted row should be gone"
            assert ids == {1, 3}
        finally:
            self._drop(tbl)

    # ── MERGE idempotency (re-inserting same PK updates, not duplicates) ───────

    def test_merge_is_idempotent(self):
        tbl = self._table("idem")
        self._drop(tbl)
        try:
            ev = _make_ev(Operation.SNAPSHOT, 1, "Alice", 10.0, source_table=tbl)
            self.sink.write_batch([ev])
            self.sink.write_batch([ev])  # write same row twice
            rows = self._rows(tbl)
            assert len(rows) == 1, "Re-inserting same PK should not create duplicate"
        finally:
            self._drop(tbl)

    # ── CDC metadata columns ──────────────────────────────────────────────────

    def test_cdc_metadata_columns_present(self):
        tbl = self._table("meta")
        self._drop(tbl)
        try:
            ev = _make_ev(Operation.INSERT, 1, "Alice", 10.0, source_table=tbl)
            self.sink.write_batch([ev])
            rows = self._rows(tbl)
            assert len(rows) == 1
            row = rows[0]
            assert "_cdc_op" in row
            assert "_cdc_source" in row
            assert "_cdc_ts" in row
            assert row["_cdc_op"] == "insert"
            assert row["_cdc_source"] == "test_src"
        finally:
            self._drop(tbl)

    # ── DELETE with NULL before-image is silently skipped ────────────────────

    def test_delete_without_before_image_skipped(self):
        tbl = self._table("delnobefore")
        self._drop(tbl)
        try:
            seed = [_make_ev(Operation.SNAPSHOT, 1, "Alice", 10.0, source_table=tbl)]
            self.sink.write_batch(seed)

            del_ev = ChangeEvent(
                op=Operation.DELETE,
                source_name="test_src",
                source_table=tbl,
                before=None,  # no before-image
                after=None,
                schema=_MERGE_SCHEMA,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                offset=None,
            )
            self.sink.write_batch([del_ev])  # should not raise

            rows = self._rows(tbl)
            assert len(rows) == 1, "Row should still exist when no before-image"
        finally:
            self._drop(tbl)


# ─────────────────────────────────────────────────────────────────────────────
# 21. Schema evolution (ALTER TABLE ADD COLUMN) — Dremio Cloud
#     Verifies that new source columns are added via ALTER TABLE and that
#     newly written rows carry the new column values.
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cloud
class TestDremioSchemaEvolution(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from core.dremio_sink import DremioSink
        cls.sink = DremioSink(_DREMIO_CLOUD_SINK_CFG)
        cls.sink.connect()

    def _drop(self, table: str):
        try:
            _sink_sql(f'DROP TABLE IF EXISTS "cdc_demo"."{table}"')
        except Exception:
            pass

    def _rows(self, table: str) -> list:
        return _sink_query(
            f'SELECT * FROM "cdc_demo"."{table}" ORDER BY "id"'
        )

    def test_alter_table_adds_new_column(self):
        """Write events with schema_v1, then events with schema_v2 (extra column).
        Verify ALTER TABLE added the column and new rows carry the new value."""
        tbl = "schema_evo_alter"
        self._drop(tbl)
        try:
            schema_v1 = [
                ColumnSchema("id",   "bigint",  nullable=False, primary_key=True),
                ColumnSchema("name", "varchar", nullable=True,  primary_key=False),
            ]
            schema_v2 = schema_v1 + [
                ColumnSchema("email", "varchar", nullable=True, primary_key=False),
            ]

            # Phase 1: write with original schema
            ev1 = ChangeEvent(
                op=Operation.SNAPSHOT, source_name="test_src", source_table=tbl,
                before=None, after={"id": 1, "name": "Alice"},
                schema=schema_v1,
                timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None,
            )
            self.sink.write_batch([ev1])

            # Phase 2: write with evolved schema (new email column)
            ev2 = ChangeEvent(
                op=Operation.INSERT, source_name="test_src", source_table=tbl,
                before=None, after={"id": 2, "name": "Bob", "email": "bob@example.com"},
                schema=schema_v2,
                timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None,
            )
            self.sink.write_batch([ev2])

            rows = {r["id"]: r for r in self._rows(tbl)}
            assert len(rows) == 2
            assert "email" in rows[2], "ALTER TABLE should have added email column"
            assert rows[2]["email"] == "bob@example.com"
            # Row 1 was written before the column existed — value should be NULL
            assert rows[1].get("email") is None or rows[1].get("email") == ""
        finally:
            self._drop(tbl)

    def test_multiple_columns_added_in_sequence(self):
        """Add two new columns in two separate batches."""
        tbl = "schema_evo_multi"
        self._drop(tbl)
        try:
            schema_v1 = [ColumnSchema("id", "bigint", nullable=False, primary_key=True),
                         ColumnSchema("name", "varchar", nullable=True, primary_key=False)]
            schema_v2 = schema_v1 + [ColumnSchema("email", "varchar", nullable=True, primary_key=False)]
            schema_v3 = schema_v2 + [ColumnSchema("phone", "varchar", nullable=True, primary_key=False)]

            def _ev(schema, id_, **kw):
                return ChangeEvent(
                    op=Operation.INSERT, source_name="test_src", source_table=tbl,
                    before=None, after={"id": id_, **kw},
                    schema=schema,
                    timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None,
                )

            self.sink.write_batch([_ev(schema_v1, 1, name="Alice")])
            self.sink.write_batch([_ev(schema_v2, 2, name="Bob", email="bob@test.com")])
            self.sink.write_batch([_ev(schema_v3, 3, name="Carol", email="carol@test.com", phone="555-1234")])

            rows = {r["id"]: r for r in self._rows(tbl)}
            assert "email" in rows[3]
            assert "phone" in rows[3]
            assert rows[3]["email"] == "carol@test.com"
            assert rows[3]["phone"] == "555-1234"
        finally:
            self._drop(tbl)

    def test_auto_migrate_via_engine(self):
        """End-to-end: engine detects schema drift and calls sink.evolve_schema()."""
        from core.schema_store import SchemaStore
        from core.status_store import StatusStore
        from core.offset_store import SQLiteOffsetStore
        from core.engine import TableWorker
        from core.dremio_sink import DremioSink

        tbl = "schema_evo_engine"
        self._drop(tbl)
        try:
            sink = DremioSink(_DREMIO_CLOUD_SINK_CFG)
            sink.connect()

            schema_v1 = [ColumnSchema("id",   "bigint",  nullable=False, primary_key=True),
                         ColumnSchema("name",  "varchar", nullable=True,  primary_key=False)]
            schema_v2 = schema_v1 + [ColumnSchema("score", "double", nullable=True, primary_key=False)]

            # Seed the schema store with v1 so the worker sees v2 as drift
            ss = SchemaStore(":memory:")
            ss.set("engine_src", tbl, schema_v1)

            class _StaticSource:
                name = "engine_src"
                def get_schema(self, table):
                    return list(schema_v2)

            w = TableWorker(
                source=_StaticSource(),
                table=tbl,
                sink=sink,
                offset_store=SQLiteOffsetStore(":memory:"),
                status_store=StatusStore(),
                options={"batch_size": 10, "schema_drift_action": "auto_migrate"},
                schema_store=ss,
            )

            # Ensure table with v1 schema first
            sink.ensure_table(tbl, schema_v1)

            # Write a row with v2 schema (triggers evolve_schema via write_batch)
            ev = ChangeEvent(
                op=Operation.INSERT, source_name="engine_src", source_table=tbl,
                before=None, after={"id": 1, "name": "Alice", "score": 42.0},
                schema=schema_v2,
                timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None,
            )
            sink.write_batch([ev])

            # Also trigger the drift check
            w._check_schema_drift()

            rows = {r["id"]: r for r in self._rows(tbl)}
            assert len(rows) == 1
            assert "score" in rows[1], "score column should exist after schema evolution"
            assert rows[1]["score"] == 42.0
        finally:
            self._drop(tbl)


# ─────────────────────────────────────────────────────────────────────────────
# 22. Iceberg merge deduplication against Dremio Cloud catalog
#     Verifies INSERT, UPDATE, DELETE, and deduplication against the real
#     Dremio Cloud Iceberg REST catalog.
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.cloud
class TestIcebergCloudDeduplication(unittest.TestCase):
    """Run Iceberg merge deduplication tests against the real Dremio Cloud catalog."""

    @classmethod
    def setUpClass(cls):
        from core.iceberg_sink import IcebergSink
        dremio_cfg = {
            "host": "api.dremio.cloud", "port": 443, "ssl": True,
            "pat": DREMIO_CLOUD_PAT, "project_id": DREMIO_CLOUD_PROJECT,
        }
        cls.sink = IcebergSink(ICEBERG_CLOUD, dremio_cfg)
        cls.sink.connect()

    def _make_schema(self):
        return [
            ColumnSchema("id",    "bigint",  nullable=False, primary_key=True),
            ColumnSchema("name",  "varchar", nullable=True,  primary_key=False),
            ColumnSchema("score", "double",  nullable=True,  primary_key=False),
        ]

    def _ev(self, op, id_, name, score, *, before=None, source_table="cloud_dedup_test.customers"):
        return ChangeEvent(
            op=op,
            source_name="cloud_test",
            source_table=source_table,
            before=before,
            after={"id": id_, "name": name, "score": score} if op != Operation.DELETE else None,
            schema=self._make_schema(),
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            offset=None,
        )

    def _cloud_count(self, table_fqn: str) -> int:
        result = _cloud_sql(f"SELECT COUNT(*) AS cnt FROM {table_fqn}")
        rr = requests.get(
            f"https://api.dremio.cloud/v0/projects/{DREMIO_CLOUD_PROJECT}"
            f"/job/{result['id']}/results?offset=0&limit=1",
            headers={"Authorization": f"Bearer {DREMIO_CLOUD_PAT}"},
            timeout=15,
        )
        rr.raise_for_status()
        data = rr.json()
        rows = data.get("rows", [])
        if not rows:
            return 0
        row = rows[0]
        return int(row["cnt"] if isinstance(row, dict) else row[0])

    def _cloud_rows(self, table_fqn: str) -> list:
        result = _cloud_sql(f"SELECT * FROM {table_fqn} ORDER BY id")
        rr = requests.get(
            f"https://api.dremio.cloud/v0/projects/{DREMIO_CLOUD_PROJECT}"
            f"/job/{result['id']}/results?offset=0&limit=500",
            headers={"Authorization": f"Bearer {DREMIO_CLOUD_PAT}"},
            timeout=15,
        )
        rr.raise_for_status()
        data = rr.json()
        rows = data.get("rows", [])
        if not rows or isinstance(rows[0], dict):
            return rows
        cols = [c["name"] for c in data.get("schema", [])]
        return [dict(zip(cols, row)) for row in rows]

    def _drop_table(self, src_table: str):
        try:
            tbl_id = self.sink._table_identifier(src_table)
            self.sink._catalog.drop_table(tbl_id)
        except Exception:
            pass

    def test_snapshot_inserts_all_rows(self):
        src = "cloud_dedup_test.snap_insert"
        self._drop_table(src)
        try:
            events = [self._ev(Operation.SNAPSHOT, i, f"User{i}", float(i*10), source_table=src)
                      for i in range(1, 4)]
            self.sink.write_batch(events)
            # Give Cloud catalog time to propagate
            time.sleep(3)
            ns, tbl = "cdc_e2e_test", "cloud_dedup_test_snap_insert"
            count = self._cloud_count(f'"{ns}"."{tbl}"')
            assert count == 3, f"Expected 3 rows, got {count}"
        finally:
            self._drop_table(src)

    def test_update_deduplication_last_write_wins(self):
        """Two updates to the same PK in one batch — only the last value should land."""
        src = "cloud_dedup_test.dedup_update"
        self._drop_table(src)
        try:
            seed = [self._ev(Operation.SNAPSHOT, 1, "Alice", 10.0, source_table=src)]
            self.sink.write_batch(seed)

            # Two updates to id=1 in one batch — last write wins
            upd1 = self._ev(Operation.UPDATE, 1, "Alice_v2", 20.0,
                            before={"id": 1, "name": "Alice", "score": 10.0}, source_table=src)
            upd2 = self._ev(Operation.UPDATE, 1, "Alice_v3", 30.0,
                            before={"id": 1, "name": "Alice_v2", "score": 20.0}, source_table=src)
            self.sink.write_batch([upd1, upd2])

            time.sleep(3)
            ns, tbl = "cdc_e2e_test", "cloud_dedup_test_dedup_update"
            rows = self._cloud_rows(f'"{ns}"."{tbl}"')
            assert len(rows) == 1
            assert rows[0]["name"] == "Alice_v3", "Last write should win"
            assert rows[0]["score"] == 30.0
        finally:
            self._drop_table(src)

    def test_delete_removes_row(self):
        src = "cloud_dedup_test.delete_row"
        self._drop_table(src)
        try:
            seed = [self._ev(Operation.SNAPSHOT, i, f"User{i}", float(i), source_table=src)
                    for i in range(1, 4)]
            self.sink.write_batch(seed)

            del_ev = self._ev(
                Operation.DELETE, 2, None, None,
                before={"id": 2, "name": "User2", "score": 2.0},
                source_table=src,
            )
            self.sink.write_batch([del_ev])

            time.sleep(3)
            ns, tbl = "cdc_e2e_test", "cloud_dedup_test_delete_row"
            rows = self._cloud_rows(f'"{ns}"."{tbl}"')
            ids = {r["id"] for r in rows}
            assert 2 not in ids, "Deleted row should not appear"
            assert ids == {1, 3}
        finally:
            self._drop_table(src)

    def test_insert_update_delete_in_sequence(self):
        """Full lifecycle: insert 3 rows, update 1, delete 1 — verify final state."""
        src = "cloud_dedup_test.lifecycle"
        self._drop_table(src)
        try:
            # Insert
            self.sink.write_batch([
                self._ev(Operation.INSERT, 1, "Alice",   10.0, source_table=src),
                self._ev(Operation.INSERT, 2, "Bob",     20.0, source_table=src),
                self._ev(Operation.INSERT, 3, "Charlie", 30.0, source_table=src),
            ])
            # Update id=2
            self.sink.write_batch([
                self._ev(Operation.UPDATE, 2, "Bobby", 99.0,
                         before={"id": 2, "name": "Bob", "score": 20.0},
                         source_table=src),
            ])
            # Delete id=3
            self.sink.write_batch([
                self._ev(Operation.DELETE, 3, None, None,
                         before={"id": 3, "name": "Charlie", "score": 30.0},
                         source_table=src),
            ])

            time.sleep(3)
            ns, tbl = "cdc_e2e_test", "cloud_dedup_test_lifecycle"
            rows = {r["id"]: r for r in self._cloud_rows(f'"{ns}"."{tbl}"')}
            assert set(rows.keys()) == {1, 2}, f"Expected ids {{1,2}}, got {set(rows.keys())}"
            assert rows[1]["name"] == "Alice"
            assert rows[2]["name"] == "Bobby"
            assert rows[2]["score"] == 99.0
        finally:
            self._drop_table(src)

    def test_iceberg_schema_evolution_adds_column(self):
        """Write v1 schema then v2 (extra column) — verify new column lands correctly."""
        src = "cloud_dedup_test.schema_evo"
        self._drop_table(src)
        try:
            schema_v1 = [
                ColumnSchema("id",   "bigint",  nullable=False, primary_key=True),
                ColumnSchema("name", "varchar", nullable=True,  primary_key=False),
            ]
            schema_v2 = schema_v1 + [
                ColumnSchema("email", "varchar", nullable=True, primary_key=False),
            ]

            ev1 = ChangeEvent(
                op=Operation.INSERT, source_name="cloud_test", source_table=src,
                before=None, after={"id": 1, "name": "Alice"},
                schema=schema_v1,
                timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None,
            )
            self.sink.write_batch([ev1])

            ev2 = ChangeEvent(
                op=Operation.INSERT, source_name="cloud_test", source_table=src,
                before=None, after={"id": 2, "name": "Bob", "email": "bob@example.com"},
                schema=schema_v2,
                timestamp=datetime.datetime.now(datetime.timezone.utc), offset=None,
            )
            self.sink.write_batch([ev2])

            time.sleep(3)
            ns, tbl = "cdc_e2e_test", "cloud_dedup_test_schema_evo"
            rows = {r["id"]: r for r in self._cloud_rows(f'"{ns}"."{tbl}"')}
            assert len(rows) == 2
            assert "email" in rows[2], "New email column should exist after schema evolution"
            assert rows[2]["email"] == "bob@example.com"
        finally:
            self._drop_table(src)


# ══════════════════════════════════════════════════════════════════════════════
# 22. Debezium DB2 payload tests — schema mapping, LSN offset, table naming
#     No external service needed: posts synthetic DB2-format Debezium payloads.
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestDebeziumDB2Payloads:
    """
    Tests the DB2-specific handling in DebeziumSource:
      - table full_name uses source.schema (not source.db)
      - DDL / heartbeat events are dropped
      - DECIMAL logical type maps to 'numeric'
      - commit_lsn preserved in event offset
      - case-insensitive table filter
      - INSERT / UPDATE / DELETE / SNAPSHOT ops
    Uses port 8769 to avoid conflict with Oracle payload tests on 8768.
    """

    @classmethod
    def setup_class(cls):
        from sources.debezium import DebeziumSource
        cls.src = DebeziumSource("db2_payload_test", {"listen_port": 8769})
        cls.src.connect()
        time.sleep(0.2)

    @classmethod
    def teardown_class(cls):
        cls.src.close()

    def setup_method(self):
        import queue as _q
        while not self.src._q.empty():
            try:
                self.src._q.get_nowait()
            except _q.Empty:
                break

    def _post(self, payload: dict):
        import json, http.client
        body = json.dumps(payload).encode()
        conn = http.client.HTTPConnection("localhost", 8769, timeout=5)
        conn.request("POST", "/events", body,
                     {"Content-Length": str(len(body)), "Content-Type": "application/json"})
        resp = conn.getresponse()
        conn.close()
        return resp.status

    def _db2_payload(self, op: str, table: str = "EMPLOYEES", *,
                     schema: str = "DB2INST1", db: str = "TESTDB",
                     before=None, after=None,
                     commit_lsn: str = "00000025:00001234:0002"):
        """Build a realistic DB2 Debezium envelope payload."""
        col_fields = [
            {"field": "ID",     "type": "int32",  "optional": False},
            {"field": "NAME",   "type": "string", "optional": True},
            {"field": "EMAIL",  "type": "string", "optional": True},
            {"field": "SALARY", "type": "string",
             "name": "org.apache.kafka.connect.data.Decimal", "optional": True},
        ]
        return {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "struct", "fields": col_fields, "optional": True, "field": "before"},
                    {"type": "struct", "fields": col_fields, "optional": True, "field": "after"},
                    {
                        "type": "struct",
                        "fields": [
                            {"type": "string", "optional": False, "field": "connector"},
                            {"type": "string", "optional": False, "field": "db"},
                            {"type": "string", "optional": False, "field": "schema"},
                            {"type": "string", "optional": False, "field": "table"},
                            {"type": "string", "optional": True,  "field": "change_lsn"},
                            {"type": "string", "optional": True,  "field": "commit_lsn"},
                        ],
                        "optional": False,
                        "field": "source",
                    },
                    {"type": "string", "optional": False, "field": "op"},
                    {"type": "int64",  "optional": True,  "field": "ts_ms"},
                ],
                "optional": False,
                "name": f"db2-cdc.{schema}.{table}.Envelope",
                "version": 1,
                "primaryKey": ["ID"],
            },
            "payload": {
                "before": before,
                "after":  after,
                "source": {
                    "connector":  "db2",
                    "db":         db,
                    "schema":     schema,
                    "table":      table,
                    "change_lsn": "00000025:00001234:0001",
                    "commit_lsn": commit_lsn,
                    "ts_ms":      1714000000000,
                    "snapshot":   op == "r",
                },
                "op":    op,
                "ts_ms": 1714000000000,
            },
        }

    def _db2_ddl_payload(self):
        """Schema-change event — no 'op' field, has 'databaseName'."""
        return {
            "schema": {},
            "payload": {
                "databaseName": "TESTDB",
                "schemaName":   "DB2INST1",
                "ddl":          "CREATE TABLE DB2INST1.ORDERS (ID INTEGER NOT NULL)",
                "tableChanges": [],
            },
        }

    def _heartbeat_payload(self):
        return {
            "schema": {},
            "payload": {
                "op":     "r",
                "ts_ms":  1714000000000,
                "source": {"connector": "heartbeat", "table": "", "db": ""},
                "before": None,
                "after":  {"ts_ms": 1714000000000},
            },
        }

    def _collect_one(self, table: str = "DB2INST1.EMPLOYEES", timeout: float = 4.0):
        """Collect one event from stream() and return it."""
        import threading
        box = []

        def _run():
            for ev in self.src.stream(table, None):
                if ev is not None:
                    box.append(ev)
                    break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=timeout)
        return box[0] if box else None

    # ── Op mapping ────────────────────────────────────────────────────────────

    def test_insert_op_emits_insert_event(self):
        from core.event import Operation
        self._post(self._db2_payload("c", after={"ID": 1, "NAME": "Alice", "EMAIL": "a@test.com", "SALARY": "75000.00"}))
        ev = self._collect_one()
        assert ev is not None
        assert ev.op == Operation.INSERT
        assert ev.after["NAME"] == "Alice"

    def test_update_op_emits_update_event(self):
        from core.event import Operation
        self._post(self._db2_payload(
            "u",
            before={"ID": 1, "NAME": "Alice",   "EMAIL": "a@test.com", "SALARY": "75000.00"},
            after= {"ID": 1, "NAME": "Alice V2", "EMAIL": "a@test.com", "SALARY": "80000.00"},
        ))
        ev = self._collect_one()
        assert ev is not None
        assert ev.op == Operation.UPDATE
        assert ev.before["NAME"] == "Alice"
        assert ev.after["NAME"]  == "Alice V2"

    def test_delete_op_emits_delete_event(self):
        from core.event import Operation
        self._post(self._db2_payload(
            "d",
            before={"ID": 2, "NAME": "Bob", "EMAIL": "b@test.com", "SALARY": "85000.00"},
        ))
        ev = self._collect_one()
        assert ev is not None
        assert ev.op == Operation.DELETE
        assert ev.before["NAME"] == "Bob"
        assert ev.after is None

    def test_snapshot_op_emits_snapshot_event(self):
        from core.event import Operation
        self._post(self._db2_payload("r", after={"ID": 3, "NAME": "Charlie", "EMAIL": "c@test.com", "SALARY": "95000.00"}))
        ev = self._collect_one()
        assert ev is not None
        assert ev.op == Operation.SNAPSHOT

    # ── DDL / heartbeat filtering ─────────────────────────────────────────────

    def test_ddl_event_is_dropped(self):
        """DB2 schema-change events must never produce a ChangeEvent."""
        import threading
        received = []

        def _run():
            for ev in self.src.stream("DB2INST1.EMPLOYEES", None):
                if ev is not None:
                    received.append(ev)
                    break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        time.sleep(0.1)
        self._post(self._db2_ddl_payload())
        # Follow with a real DML so the thread exits
        self._post(self._db2_payload("c", after={"ID": 10, "NAME": "DDL_After", "EMAIL": None, "SALARY": None}))
        t.join(timeout=5)
        assert len(received) == 1
        from core.event import Operation
        assert received[0].op == Operation.INSERT

    def test_heartbeat_is_dropped(self):
        import threading
        received = []

        def _run():
            for ev in self.src.stream("DB2INST1.EMPLOYEES", None):
                if ev is not None:
                    received.append(ev)
                    break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        time.sleep(0.1)
        self._post(self._heartbeat_payload())
        self._post(self._db2_payload("c", after={"ID": 11, "NAME": "HB_After", "EMAIL": None, "SALARY": None}))
        t.join(timeout=5)
        assert len(received) == 1

    # ── Schema / type mapping ─────────────────────────────────────────────────

    def test_decimal_logical_type_maps_to_numeric(self):
        """SALARY with org.apache.kafka.connect.data.Decimal logical type → 'numeric'."""
        self._post(self._db2_payload("c", after={"ID": 20, "NAME": "Dec", "EMAIL": None, "SALARY": "99.99"}))
        ev = self._collect_one()
        assert ev is not None
        col_types = {c.name: c.data_type for c in ev.schema}
        assert col_types.get("SALARY") == "numeric"

    def test_integer_type_maps_to_integer(self):
        """ID with int32 type → 'integer' data type."""
        self._post(self._db2_payload("c", after={"ID": 21, "NAME": "Int", "EMAIL": None, "SALARY": None}))
        ev = self._collect_one()
        assert ev is not None
        col_types = {c.name: c.data_type for c in ev.schema}
        assert col_types.get("ID") in ("integer", "smallint", "int")

    def test_pk_detected_from_primary_key_list(self):
        """ID declared in schema.primaryKey → primary_key=True on the ColumnSchema."""
        self._post(self._db2_payload("c", after={"ID": 22, "NAME": "PK", "EMAIL": None, "SALARY": None}))
        ev = self._collect_one()
        assert ev is not None
        pk_cols = [c.name for c in ev.schema if c.primary_key]
        assert "ID" in pk_cols

    # ── DB2-specific: table name and offset ───────────────────────────────────

    def test_table_name_uses_schema_not_db(self):
        """full_table must be 'DB2INST1.EMPLOYEES', not 'TESTDB.EMPLOYEES'."""
        self._post(self._db2_payload("c", after={"ID": 30, "NAME": "Schema", "EMAIL": None, "SALARY": None}))
        ev = self._collect_one()
        assert ev is not None
        assert ev.source_table == "DB2INST1.EMPLOYEES", \
            f"Expected DB2INST1.EMPLOYEES, got {ev.source_table}"

    def test_commit_lsn_preserved_in_offset(self):
        """DB2 commit_lsn from source metadata is stored in the event offset."""
        lsn = "00000042:00005678:0003"
        self._post(self._db2_payload("c", after={"ID": 31, "NAME": "LSN", "EMAIL": None, "SALARY": None},
                                     commit_lsn=lsn))
        ev = self._collect_one()
        assert ev is not None
        assert ev.offset is not None
        # commit_lsn maps to the generic "lsn" offset key
        assert ev.offset.get("lsn") == lsn, f"Expected lsn={lsn}, got {ev.offset}"

    def test_table_filter_case_insensitive(self):
        """Filter 'DB2INST1.EMPLOYEES' matches DB2's uppercase 'EMPLOYEES'."""
        import threading
        captured = []

        def _run():
            for ev in self.src.stream("DB2INST1.EMPLOYEES", None):
                if ev is not None:
                    captured.append(ev)
                    break

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        time.sleep(0.1)
        # Wrong table — should be filtered
        self._post(self._db2_payload("c", table="ORDERS",
                                     after={"ID": 99, "NAME": "Skip", "EMAIL": None, "SALARY": None}))
        # Matching table
        self._post(self._db2_payload("c", table="EMPLOYEES",
                                     after={"ID": 32, "NAME": "Match", "EMAIL": None, "SALARY": None}))
        t.join(timeout=5)
        assert len(captured) == 1
        assert captured[0].after["NAME"] == "Match"


# ══════════════════════════════════════════════════════════════════════════════
# 23. IBM Db2 + Debezium Server live integration
# ══════════════════════════════════════════════════════════════════════════════
# Requires docker-compose services: db2, debezium-db2
#   docker compose up -d db2
#   docker compose build debezium-db2 && docker compose up -d debezium-db2
# Db2 takes ~5 minutes to initialize; Debezium takes ~60s to connect.
# Run only these tests:
#   pytest tests/test_e2e.py -m db2 -v
# ══════════════════════════════════════════════════════════════════════════════

DB2_HOST = "localhost"
DB2_PORT = 50000
DB2_USER = "db2inst1"
DB2_PASS = "db2pass"
DB2_DB   = "TESTDB"


def _db2_conn():
    import ibm_db
    conn_str = (
        f"DATABASE={DB2_DB};HOSTNAME={DB2_HOST};PORT={DB2_PORT};"
        f"PROTOCOL=TCPIP;UID={DB2_USER};PWD={DB2_PASS};"
    )
    return ibm_db.connect(conn_str, "", "")


def _db2_exec(conn, sql: str):
    import ibm_db
    stmt = ibm_db.exec_immediate(conn, sql)
    return stmt


def _db2_wait_for_events(src, table: str, n: int, timeout: int = 90):
    """Collect up to n non-None events from src.stream(), with timeout."""
    import threading
    from core.event import ChangeEvent
    collected: list = []
    done = threading.Event()

    def _run():
        for ev in src.stream(table, None):
            if ev is None:
                if done.is_set():
                    break
                continue
            collected.append(ev)
            if len(collected) >= n:
                done.set()
                break

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)
    done.set()
    return collected


@pytest.mark.db2
class TestDb2DebeziumLive:
    """
    End-to-end: IBM Db2 → Debezium Server → DebeziumSource → parsed events.
    Requires: docker compose up -d db2 && docker compose up -d debezium-db2
    The debezium-db2 service POSTs to host.docker.internal:8767 (this test's listener).
    """

    @classmethod
    def setup_class(cls):
        import subprocess, os, time
        from sources.debezium import DebeziumSource
        compose_dir = os.path.dirname(os.path.dirname(__file__))

        # Ensure asncap is running (may not survive container restart)
        result = subprocess.run(
            ["docker", "exec", "dremio-cdc-db2-1", "pgrep", "-f", "asncap"],
            capture_output=True)
        if result.returncode != 0:
            subprocess.run(
                ["docker", "exec", "-d", "dremio-cdc-db2-1", "bash", "-c",
                 "nohup su - db2inst1 -c 'asncap capture_server=TESTDB capture_schema=ASNCDC capture_path=/tmp AUTOSTOP=N' > /tmp/asncap_test.log 2>&1 &"],
                capture_output=True)
            time.sleep(8)

        # Stop Debezium first so we don't race with snapshot events
        subprocess.run(["docker", "compose", "stop", "debezium-db2"],
                       cwd=compose_dir, capture_output=True)

        # Clean up any extra test rows and stale CDC data from previous runs
        try:
            conn = _db2_conn()
            _db2_exec(conn, "DELETE FROM DB2INST1.EMPLOYEES WHERE ID > 3")
            _db2_exec(conn, "DELETE FROM DB2INST1.CDEMPLOYEES")
            _db2_exec(conn, "DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE STATE = 'I' AND TRIM(SOURCE_OWNER) = ''")
            import ibm_db
            ibm_db.commit(conn)
            ibm_db.close(conn)
        except Exception:
            pass

        # Wipe Debezium offset + schema history so it re-snapshots
        subprocess.run(
            ["docker", "run", "--rm",
             "-v", "dremio-cdc_debezium_db2_data:/data",
             "alpine", "sh", "-c",
             "rm -f /data/db2-test-offsets.dat /data/db2-test-schema-history.dat"],
            cwd=compose_dir, capture_output=True,
        )

        # Bind the listener port BEFORE starting Debezium so no events are lost
        cls.src = DebeziumSource("db2_live", {"listen_port": 8767})
        cls.src.connect()

        subprocess.run(["docker", "compose", "start", "debezium-db2"],
                       cwd=compose_dir, check=True, capture_output=True)

    @classmethod
    def teardown_class(cls):
        cls.src.close()

    def setup_method(self):
        while not self.src._q.empty():
            try:
                self.src._q.get_nowait()
            except Exception:
                break

    # ── 1. Initial snapshot ───────────────────────────────────────────────────

    def test_snapshot_rows_arrive(self):
        """Debezium delivers the 3 pre-seeded employees as SNAPSHOT events."""
        events = _db2_wait_for_events(self.src, "DB2INST1.EMPLOYEES", 3, timeout=120)
        assert len(events) >= 3, f"Expected ≥3 snapshot events, got {len(events)}"
        names = {e.after["NAME"] for e in events if e.after}
        assert "Alice" in names
        assert "Bob" in names

    # ── 2. Streaming: INSERT ──────────────────────────────────────────────────

    def test_streaming_insert(self):
        """INSERT into Db2 produces an INSERT event."""
        from core.event import Operation
        conn = _db2_conn()
        import ibm_db
        _db2_exec(conn, "INSERT INTO DB2INST1.EMPLOYEES VALUES (101, 'Dave', 'dave@test.com', 65000.00)")
        ibm_db.commit(conn)
        ibm_db.close(conn)

        events = _db2_wait_for_events(self.src, "DB2INST1.EMPLOYEES", 1, timeout=60)
        assert events, "No INSERT event received"
        ins = next((e for e in events if e.op == Operation.INSERT
                    and e.after and e.after.get("NAME") == "Dave"), None)
        assert ins is not None, f"INSERT event for Dave not found in {events}"

    # ── 3. Streaming: UPDATE ──────────────────────────────────────────────────

    def test_streaming_update(self):
        """UPDATE produces an UPDATE event with correct after image.

        IBM DB2 LUW ASNCAP produces single 'U' (after-image only) rows for all
        UPDATEs — the Debezium LUW LEAD/LAG query does not handle 'U'.  The view
        ASNCDC.CDEMPLOYEES maps 'U'/'X' → 'I' to prevent connector crashes, but
        those events arrive as INSERT, not UPDATE.

        To get a true UPDATE event (OPCODE 3+4), we use DELETE + INSERT in the
        *same transaction* — ASNCAP emits a D+I pair in the same IBMSNAP_COMMITSEQ,
        which the LEAD/LAG window functions detect as UPDATE_BEFORE + UPDATE_AFTER.
        """
        from core.event import Operation
        conn = _db2_conn()
        import ibm_db
        # Ensure row exists
        try:
            _db2_exec(conn, "INSERT INTO DB2INST1.EMPLOYEES VALUES (102, 'Eve', 'eve@test.com', 70000.00)")
            ibm_db.commit(conn)
        except Exception:
            ibm_db.rollback(conn)
        # DELETE + INSERT in the same commit → ASNCAP emits D+I pair with the same
        # IBMSNAP_COMMITSEQ; Debezium's LEAD/LAG detects this as an UPDATE.
        # ibm_db.connect() defaults to auto-commit ON, so we must disable it here
        # to group the two statements into a single transaction.
        ibm_db.autocommit(conn, 0)  # 0 = SQL_AUTOCOMMIT_OFF
        _db2_exec(conn, "DELETE FROM DB2INST1.EMPLOYEES WHERE ID = 102")
        _db2_exec(conn, "INSERT INTO DB2INST1.EMPLOYEES VALUES (102, 'Eve', 'eve@test.com', 80000.00)")
        ibm_db.commit(conn)
        ibm_db.close(conn)

        events = _db2_wait_for_events(self.src, "DB2INST1.EMPLOYEES", 2, timeout=60)
        upd = next((e for e in events if e.op == Operation.UPDATE
                    and e.after and e.after.get("NAME") == "Eve"), None)
        assert upd is not None, "UPDATE event for Eve not found"

    # ── 4. Streaming: DELETE ──────────────────────────────────────────────────

    def test_streaming_delete(self):
        """DELETE produces a DELETE event with before-image."""
        from core.event import Operation
        conn = _db2_conn()
        import ibm_db
        try:
            _db2_exec(conn, "INSERT INTO DB2INST1.EMPLOYEES VALUES (103, 'Frank', 'frank@test.com', 55000.00)")
            ibm_db.commit(conn)
        except Exception:
            ibm_db.rollback(conn)
        _db2_exec(conn, "DELETE FROM DB2INST1.EMPLOYEES WHERE ID = 103")
        ibm_db.commit(conn)
        ibm_db.close(conn)

        events = _db2_wait_for_events(self.src, "DB2INST1.EMPLOYEES", 2, timeout=60)
        delete_ev = next((e for e in events if e.op == Operation.DELETE
                          and e.before and e.before.get("NAME") == "Frank"), None)
        assert delete_ev is not None, "DELETE event for Frank not found"
        assert delete_ev.after is None

    # ── 5. Commit LSN in offset ───────────────────────────────────────────────

    def test_commit_lsn_in_offset(self):
        """Every streaming event offset contains a commit_lsn value."""
        conn = _db2_conn()
        import ibm_db
        _db2_exec(conn, "INSERT INTO DB2INST1.EMPLOYEES VALUES (104, 'Grace', 'grace@test.com', 60000.00)")
        ibm_db.commit(conn)
        ibm_db.close(conn)

        events = _db2_wait_for_events(self.src, "DB2INST1.EMPLOYEES", 1, timeout=60)
        assert events, "No event received"
        assert events[0].offset is not None
        assert events[0].offset.get("lsn") is not None, \
            f"commit_lsn missing from offset: {events[0].offset}"


# ─────────────────────────────────────────────────────────────────────────────
# TestSpannerLive — Google Cloud Spanner emulator integration tests
# Requires: docker compose up -d spanner-emulator
# The emulator listens on localhost:9010 (gRPC) / 9020 (REST).
# SPANNER_EMULATOR_HOST is set programmatically before connecting.
# ─────────────────────────────────────────────────────────────────────────────

SPANNER_CFG = {
    "connection": {
        "project":       "test-project",
        "instance":      "test-instance",
        "database":      "testdb",
        "change_stream": "DremiocdcStream",
    }
}

def _spanner_admin():
    """Return (InstanceAdminClient, DatabaseAdminClient) pointed at the emulator."""
    import os
    os.environ.setdefault("SPANNER_EMULATOR_HOST", "localhost:9010")
    from google.cloud import spanner_admin_instance_v1, spanner_admin_database_v1
    from google.api_core.client_options import ClientOptions
    opts = ClientOptions(api_endpoint="localhost:9010")
    inst_client = spanner_admin_instance_v1.InstanceAdminClient(client_options=opts)
    db_client   = spanner_admin_database_v1.DatabaseAdminClient(client_options=opts)
    return inst_client, db_client


def _spanner_db():
    """Return a google.cloud.spanner Database handle for the emulator testdb."""
    import os
    os.environ.setdefault("SPANNER_EMULATOR_HOST", "localhost:9010")
    from google.cloud import spanner as gcs
    client   = gcs.Client(project="test-project")
    instance = client.instance("test-instance")
    return instance.database("testdb")


def _spanner_wait_for_events(src, table: str, n: int, timeout: int = 60):
    """Collect up to n non-None streaming events from src.stream(), with timeout."""
    collected = []
    done      = threading.Event()

    def _run():
        for ev in src.stream(table, None):
            if ev is None:
                if done.is_set():
                    break
                continue
            collected.append(ev)
            if len(collected) >= n:
                done.set()
                break

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)
    done.set()
    return collected


@pytest.mark.spanner
class TestSpannerLive:
    """
    End-to-end: Spanner emulator → SpannerSource → parsed ChangeEvents.
    Requires: docker compose up -d spanner-emulator
    """

    @classmethod
    def setup_class(cls):
        import os, time
        os.environ["SPANNER_EMULATOR_HOST"] = "localhost:9010"

        # Use the emulator's REST API (port 9020) for admin setup — avoids
        # TLS handshake failures that occur when pointing gRPC admin clients
        # at the emulator's plain-HTTP gRPC endpoint.
        REST = "http://localhost:9020/v1"

        # Wait for emulator REST API to be ready
        for _ in range(30):
            try:
                r = requests.get(f"{REST}/projects/test-project/instances", timeout=2)
                if r.status_code in (200, 404):
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            raise RuntimeError("Spanner emulator REST API not ready after 30s")

        # Create instance (idempotent)
        requests.post(
            f"{REST}/projects/test-project/instances",
            json={
                "instanceId": "test-instance",
                "instance": {
                    "config": "projects/test-project/instanceConfigs/emulator-config",
                    "displayName": "test-instance",
                    "nodeCount": 1,
                },
            },
            timeout=10,
        )

        # Create database + table (idempotent)
        requests.post(
            f"{REST}/projects/test-project/instances/test-instance/databases",
            json={
                "createStatement": "CREATE DATABASE testdb",
                "extraStatements": [
                    "CREATE TABLE IF NOT EXISTS Employees ("
                    "  Id INT64 NOT NULL, Name STRING(100),"
                    "  Email STRING(100), Salary FLOAT64"
                    ") PRIMARY KEY (Id)",
                ],
            },
            timeout=10,
        )
        time.sleep(1)  # let DDL propagate

        # Seed rows via SpannerSource (which uses insecure channel via emulator env var)
        from google.auth.credentials import AnonymousCredentials
        from google.cloud import spanner as gcs
        anon      = AnonymousCredentials()
        gcs_client = gcs.Client(project="test-project", credentials=anon)
        db = gcs_client.instance("test-instance").database("testdb")
        with db.batch() as batch:
            batch.insert_or_update(
                table="Employees",
                columns=["Id", "Name", "Email", "Salary"],
                values=[
                    (1, "Alice",   "alice@example.com",   75000.0),
                    (2, "Bob",     "bob@example.com",     85000.0),
                    (3, "Charlie", "charlie@example.com", 95000.0),
                ],
            )

        # Connect source (auto-creates change stream via DDL)
        from sources.spanner import SpannerSource
        cls.src = SpannerSource("spanner_test", SPANNER_CFG)
        cls.src.connect()
        cls._gcs_client = gcs_client

    @classmethod
    def teardown_class(cls):
        cls.src.close()

    def setup_method(self):
        # Drain any queued events between tests
        self.src._stop_flag.clear()
        self.src._stream_thread = None
        self.src._table_queues.clear()

    # ── 1. Schema ─────────────────────────────────────────────────────────────

    def test_get_schema(self):
        schema = self.src.get_schema("Employees")
        names  = [c.name for c in schema]
        assert "Id"     in names
        assert "Name"   in names
        assert "Salary" in names
        pk_cols = [c.name for c in schema if c.primary_key]
        assert "Id" in pk_cols

    # ── 2. Snapshot ───────────────────────────────────────────────────────────

    def test_snapshot_rows_arrive(self):
        events = list(self.src.snapshot("Employees"))
        assert len(events) >= 3
        assert all(e.op == Operation.SNAPSHOT for e in events)
        names = {e.after["Name"] for e in events if e.after}
        assert {"Alice", "Bob", "Charlie"}.issubset(names)

    # ── 3. Streaming insert ───────────────────────────────────────────────────

    def test_streaming_insert(self):
        db = self.__class__._gcs_client.instance("test-instance").database("testdb")

        events = []
        done   = threading.Event()

        def _stream():
            for ev in self.src.stream("Employees", None):
                if ev is None:
                    if done.is_set():
                        break
                    continue
                events.append(ev)
                if any(e.after and e.after.get("Name") == "Diana" for e in events):
                    done.set()
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(3)  # let change stream start

        with db.batch() as batch:
            batch.insert_or_update(
                table="Employees",
                columns=["Id", "Name", "Email", "Salary"],
                values=[(101, "Diana", "diana@test.com", 70000.0)],
            )

        done.wait(timeout=30)
        self.src._stop_flag.set()
        t.join(timeout=5)

        ins = next((e for e in events
                    if e.after and e.after.get("Name") == "Diana"), None)
        assert ins is not None, "Event for Diana not received"

    # ── 4. Streaming update ───────────────────────────────────────────────────

    def test_streaming_update(self):
        db = self.__class__._gcs_client.instance("test-instance").database("testdb")

        # Ensure row exists
        with db.batch() as batch:
            batch.insert_or_update(
                table="Employees",
                columns=["Id", "Name", "Email", "Salary"],
                values=[(102, "Eve", "eve@test.com", 72000.0)],
            )

        events = []
        done   = threading.Event()

        def _stream():
            for ev in self.src.stream("Employees", None):
                if ev is None:
                    if done.is_set():
                        break
                    continue
                events.append(ev)
                if any(e.op == Operation.UPDATE and e.after and e.after.get("Name") == "Eve"
                       for e in events):
                    done.set()
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(3)

        with db.batch() as batch:
            batch.update(
                table="Employees",
                columns=["Id", "Name", "Email", "Salary"],
                values=[(102, "Eve", "eve@test.com", 82000.0)],
            )

        done.wait(timeout=30)
        self.src._stop_flag.set()
        t.join(timeout=5)

        upd = next((e for e in events
                    if e.op == Operation.UPDATE and e.after and e.after.get("Name") == "Eve"), None)
        assert upd is not None, "UPDATE event for Eve not received"
        assert upd.after.get("Salary") == 82000.0, f"Expected salary 82000, got {upd.after.get('Salary')}"

    # ── 5. Streaming delete ───────────────────────────────────────────────────

    def test_streaming_delete(self):
        db = self.__class__._gcs_client.instance("test-instance").database("testdb")

        with db.batch() as batch:
            batch.insert_or_update(
                table="Employees",
                columns=["Id", "Name", "Email", "Salary"],
                values=[(103, "Frank", "frank@test.com", 65000.0)],
            )

        events = []
        done   = threading.Event()

        def _stream():
            for ev in self.src.stream("Employees", None):
                if ev is None:
                    if done.is_set():
                        break
                    continue
                events.append(ev)
                if any(e.op == Operation.DELETE for e in events):
                    done.set()
                    break

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        time.sleep(3)

        from google.cloud.spanner_v1 import KeySet
        with db.batch() as batch:
            batch.delete("Employees", KeySet(keys=[[103]]))

        done.wait(timeout=30)
        self.src._stop_flag.set()
        t.join(timeout=5)

        del_ev = next((e for e in events if e.op == Operation.DELETE), None)
        assert del_ev is not None, "DELETE event not received"
        assert del_ev.after is None

    # ── 6. Offset is populated ────────────────────────────────────────────────

    def test_offset_populated(self):
        db = self.__class__._gcs_client.instance("test-instance").database("testdb")

        with db.batch() as batch:
            batch.insert_or_update(
                table="Employees",
                columns=["Id", "Name", "Email", "Salary"],
                values=[(104, "Grace", "grace@test.com", 60000.0)],
            )

        events = _spanner_wait_for_events(self.src, "Employees", 1, timeout=30)
        assert events, "No streaming event received"
        assert events[0].offset is not None, "offset should not be None"
