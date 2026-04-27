"""
Dremio CDC UI — Flask backend.

Manages config.yml, controls the CDCEngine lifecycle in a background thread,
and exposes a REST API consumed by the React frontend.

Start: python ui/backend/app.py --config config.yml [--port 7070]
"""
from __future__ import annotations

import copy
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

# Make sure the project root is on the path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from core.status_store import StatusStore
from core.alert_manager import AlertManager
from core.dlq import DeadLetterQueue

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Global state ─────────────────────────────────────────────────────────────

_config_path: Path = Path("config.yml")
_status_store = StatusStore()
_engine = None
_engine_thread: Optional[threading.Thread] = None
_engine_lock = threading.Lock()
_alert_manager: Optional[AlertManager] = None
_dlq: Optional[DeadLetterQueue] = None


# ── Config helpers ────────────────────────────────────────────────────────────

def _load_raw() -> dict:
    if _config_path.exists():
        with open(_config_path) as f:
            return yaml.safe_load(f) or {}
    return {}


def _save_raw(cfg: dict):
    with open(_config_path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)


# ── Engine lifecycle ──────────────────────────────────────────────────────────

def _engine_run(cfg: dict):
    global _engine
    from core.engine import CDCEngine
    try:
        _status_store.set_engine_state("starting")
        eng = CDCEngine(cfg, status_store=_status_store)
        with _engine_lock:
            _engine = eng
        eng.start()
        eng.join()
    except Exception as exc:
        _status_store.set_engine_state("error")
        logger.error("Engine error: %s", exc)
    finally:
        with _engine_lock:
            _engine = None


def _is_dremio_cloud(dremio_cfg: dict) -> bool:
    return bool(dremio_cfg.get("project_id") or "dremio.cloud" in dremio_cfg.get("host", ""))


def _dremio_headers(dremio_cfg: dict):
    """Authenticate with Dremio. Returns (headers, base_url, catalog_url)."""
    import requests as req
    scheme = "https" if dremio_cfg.get("ssl") else "http"
    base = f"{scheme}://{dremio_cfg.get('host','localhost')}:{dremio_cfg.get('port',9047)}"
    pat = dremio_cfg.get("pat", "")
    project_id = dremio_cfg.get("project_id", "")
    cloud = _is_dremio_cloud(dremio_cfg)

    if pat:
        headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}
    else:
        r = req.post(f"{base}/apiv2/login",
                     json={"userName": dremio_cfg.get("user",""), "password": dremio_cfg.get("password","")},
                     timeout=10)
        r.raise_for_status()
        headers = {"Authorization": f"_dremio{r.json()['token']}", "Content-Type": "application/json"}

    catalog_url = (f"{base}/v0/projects/{project_id}/catalog" if cloud
                   else f"{base}/api/v3/catalog")
    return headers, base, catalog_url


def _validate_dremio_namespace(dremio_cfg: dict, namespace: str) -> Optional[str]:
    """Return error string if namespace is invalid, None if OK or unreachable."""
    try:
        import requests as req
        headers, _, catalog_url = _dremio_headers(dremio_cfg)
        r = req.get(catalog_url, headers=headers, timeout=8)
        r.raise_for_status()
        items = r.json().get("data", [])
        by_name = {(item.get("path") or [None])[0]: item.get("containerType", item.get("type", "")) for item in items}
        if namespace not in by_name:
            sources = [n for n, t in by_name.items() if n and t == "SOURCE"]
            return f"Namespace '{namespace}' not found in Dremio. Available sources: {', '.join(sources) or 'none'}"
        ns_type = by_name[namespace]
        if ns_type == "SPACE":
            sources = [n for n, t in by_name.items() if n and t == "SOURCE"]
            return (f"'{namespace}' is a Dremio Space, which doesn't support CREATE TABLE. "
                    f"Use a writable source instead (e.g. {sources[0] if sources else 'hudi_local'}).")
    except Exception as exc:
        # Dremio unreachable — log a warning but don't block engine start.
        # The engine will surface connection errors when it first tries to write.
        logger.warning("Could not validate Dremio namespace (Dremio may be offline): %s", exc)
    return None


def _start_engine():
    global _engine_thread
    cfg = _load_raw()
    if not cfg.get("sources"):
        return {"error": "No sources configured"}, 400

    # Validate Dremio namespace exists before starting
    sink_mode = cfg.get("options", {}).get("sink_mode", "dremio")
    if sink_mode == "dremio":
        dremio_cfg = cfg.get("dremio", {})
        namespace = dremio_cfg.get("target_namespace", "")
        if namespace and dremio_cfg.get("host"):
            err = _validate_dremio_namespace(dremio_cfg, namespace)
            if err:
                return {"error": err}, 400

    with _engine_lock:
        if _engine_thread and _engine_thread.is_alive():
            # Allow restart if engine state is stopped/error (thread is winding down)
            state = _status_store.get_engine_state()
            if state not in ("stopped", "error", None):
                return {"error": "Engine already running"}, 409
            # Thread lingering after stop — join briefly then proceed
            _engine_thread.join(timeout=3)

    _engine_thread = threading.Thread(target=_engine_run, args=(cfg,), daemon=True)
    _engine_thread.start()
    return {"status": "starting"}, 200


def _stop_engine():
    global _engine_thread
    with _engine_lock:
        eng = _engine
    if eng:
        def _do_stop():
            global _engine_thread
            eng.stop()
            if _engine_thread:
                _engine_thread.join(timeout=10)
            _engine_thread = None
        threading.Thread(target=_do_stop, daemon=True).start()
        return {"status": "stopping"}, 200
    # Engine object gone but thread may still linger — clear it so Start works
    with _engine_lock:
        if _engine_thread and not _engine_thread.is_alive():
            _engine_thread = None
    return {"error": "Engine not running"}, 409


def _get_dlq() -> DeadLetterQueue:
    global _dlq
    if _dlq is None:
        cfg = _load_raw()
        db_path = cfg.get("options", {}).get("dlq_db_path", "./cdc_dlq.db")
        _dlq = DeadLetterQueue(db_path=db_path)
    return _dlq


def _ensure_alert_manager():
    """Start AlertManager if not already running (idempotent)."""
    global _alert_manager
    cfg = _load_raw()
    alerts_cfg = cfg.get("alerts", {})
    if _alert_manager is None:
        _alert_manager = AlertManager(alerts_cfg, _status_store)
        _alert_manager.start()
    else:
        _alert_manager.reconfigure(alerts_cfg)


# ── API: engine ───────────────────────────────────────────────────────────────

@app.post("/api/engine/start")
def api_engine_start():
    result, code = _start_engine()
    return jsonify(result), code


@app.post("/api/engine/stop")
def api_engine_stop():
    result, code = _stop_engine()
    return jsonify(result), code


@app.post("/api/engine/restart")
def api_engine_restart():
    _stop_engine()
    time.sleep(1)
    result, code = _start_engine()
    return jsonify(result), code


@app.get("/api/status")
def api_status():
    snap = _status_store.snapshot()
    snap["config_path"] = str(_config_path)
    cfg = _load_raw()
    global_ns  = cfg.get("dremio", {}).get("target_namespace", "")
    sink_mode  = cfg.get("options", {}).get("sink_mode", "dremio")
    snap["target_namespace"] = global_ns
    snap["sink_mode"] = sink_mode
    # Annotate each worker with its effective target namespace
    source_ns_map = {s["name"]: s.get("target_namespace", "") for s in cfg.get("sources", [])}
    for w in snap.get("workers", []):
        src_ns = source_ns_map.get(w.get("source", ""), "")
        w["target_namespace"] = src_ns or global_ns
    return jsonify(snap)


@app.get("/metrics")
def api_metrics():
    """Prometheus text-format metrics endpoint for scraping."""
    from flask import Response
    snap = _status_store.snapshot()
    lines: List[str] = []

    def emit(name: str, help_text: str, mtype: str, samples: List[str]):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {mtype}")
        lines.extend(samples)

    emit("dremio_cdc_engine_up", "1 if CDC engine is running", "gauge",
         [f"dremio_cdc_engine_up {1 if snap['engine_state'] == 'running' else 0}"])

    s = snap.get("summary", {})
    emit("dremio_cdc_total_events", "Total events written across all workers", "counter",
         [f"dremio_cdc_total_events {s.get('total_events', 0)}"])
    emit("dremio_cdc_total_errors", "Total flush errors across all workers", "counter",
         [f"dremio_cdc_total_errors {s.get('total_errors', 0)}"])
    emit("dremio_cdc_active_workers", "Number of workers in streaming state", "gauge",
         [f"dremio_cdc_active_workers {s.get('active_workers', 0)}"])

    workers = snap.get("workers", [])

    def lbl(w: dict) -> str:
        return f'source="{w["source"]}",table="{w["table"]}"'

    emit("dremio_cdc_events_written_total", "Total CDC events flushed to Dremio", "counter",
         [f'dremio_cdc_events_written_total{{{lbl(w)}}} {w["events_written"]}' for w in workers])

    emit("dremio_cdc_events_per_minute", "Rolling 60-second event rate", "gauge",
         [f'dremio_cdc_events_per_minute{{{lbl(w)}}} {w["events_per_minute"]}' for w in workers])

    emit("dremio_cdc_lag_seconds", "Seconds since last source event was processed", "gauge",
         [f'dremio_cdc_lag_seconds{{{lbl(w)}}} {w["lag_seconds"]}'
          for w in workers if w["lag_seconds"] is not None])

    emit("dremio_cdc_pipeline_lag_seconds", "Age of data at the moment it landed in the sink", "gauge",
         [f'dremio_cdc_pipeline_lag_seconds{{{lbl(w)}}} {w["pipeline_lag_seconds"]}'
          for w in workers if w["pipeline_lag_seconds"] is not None])

    emit("dremio_cdc_flush_duration_ms", "Last batch flush duration in milliseconds", "gauge",
         [f'dremio_cdc_flush_duration_ms{{{lbl(w)}}} {w["last_flush_duration_ms"]}' for w in workers])

    emit("dremio_cdc_error_count", "Total flush error count per worker", "counter",
         [f'dremio_cdc_error_count{{{lbl(w)}}} {w["error_count"]}' for w in workers])

    state_samples = []
    for w in workers:
        for st in ("idle", "snapshotting", "streaming", "paused", "error"):
            state_samples.append(
                f'dremio_cdc_worker_state{{source="{w["source"]}",table="{w["table"]}",state="{st}"}} '
                f'{1 if w["state"] == st else 0}'
            )
    emit("dremio_cdc_worker_state", "Worker state encoded as per-state gauge (1 = current state)", "gauge",
         state_samples)

    return Response("\n".join(lines) + "\n", mimetype="text/plain; version=0.0.4; charset=utf-8")


# ── API: config ───────────────────────────────────────────────────────────────

@app.get("/api/config")
def api_config_get():
    return jsonify(_load_raw())


@app.put("/api/config")
def api_config_put():
    body = request.json or {}
    _save_raw(body)
    return jsonify({"saved": True})


# ── API: sources ──────────────────────────────────────────────────────────────

@app.get("/api/sources")
def api_sources_list():
    cfg = _load_raw()
    return jsonify(cfg.get("sources", []))


@app.post("/api/sources")
def api_sources_add():
    src = request.json
    if not src or not src.get("name") or not src.get("type"):
        return jsonify({"error": "name and type required"}), 400
    cfg = _load_raw()
    sources = cfg.setdefault("sources", [])
    if any(s["name"] == src["name"] for s in sources):
        return jsonify({"error": f"Source '{src['name']}' already exists"}), 409
    sources.append(src)
    _save_raw(cfg)
    return jsonify(src), 201


@app.put("/api/sources/<name>")
def api_sources_update(name):
    src = request.json
    cfg = _load_raw()
    sources = cfg.get("sources", [])
    for i, s in enumerate(sources):
        if s["name"] == name:
            sources[i] = src
            _save_raw(cfg)
            return jsonify(src)
    return jsonify({"error": f"Source '{name}' not found"}), 404


@app.delete("/api/sources/<name>")
def api_sources_delete(name):
    cfg = _load_raw()
    before = len(cfg.get("sources", []))
    cfg["sources"] = [s for s in cfg.get("sources", []) if s["name"] != name]
    if len(cfg["sources"]) == before:
        return jsonify({"error": f"Source '{name}' not found"}), 404
    _save_raw(cfg)
    return jsonify({"deleted": name})


@app.post("/api/sources/test")
def api_sources_test():
    """Test a source connection and return available tables."""
    src = request.json
    if not src:
        return jsonify({"error": "No source config provided"}), 400
    src_type = src.get("type", "").lower()

    try:
        from core.engine import _load_sources, _SOURCE_REGISTRY
        _load_sources()
        cls = _SOURCE_REGISTRY.get(src_type)
        if not cls:
            return jsonify({"error": f"Unknown source type: {src_type}"}), 400

        source = cls(name=src.get("name", "test"), cfg=src)
        source.connect()

        tables = _get_source_tables(source, src_type, src)
        schema = _get_source_schema(source, src_type, src, tables)
        source.close()
        return jsonify({"ok": True, "tables": tables, "schema": schema})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 200


def _get_source_schema(source, src_type: str, cfg: dict, tables: List[str]) -> Dict[str, List[str]]:
    """Introspect columns for each table. Returns {table: [col, ...]}."""
    schema: Dict[str, List[str]] = {}
    try:
        if src_type == "postgres":
            with source._snap_conn.cursor() as cur:
                cur.execute("""
                    SELECT table_schema || '.' || table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog','information_schema')
                    ORDER BY table_schema, table_name, ordinal_position
                """)
                for tbl, col in cur.fetchall():
                    schema.setdefault(tbl, []).append(col)
            return schema

        if src_type in ("mysql", "mariadb"):
            conn_cfg = cfg.get("connection", {})
            import pymysql
            conn = pymysql.connect(
                host=conn_cfg.get("host", "localhost"),
                port=int(conn_cfg.get("port", 3306)),
                user=conn_cfg["user"],
                password=conn_cfg.get("password", ""),
            )
            with conn.cursor() as cur:
                for tbl in tables:
                    db_part, tbl_part = tbl.split(".", 1)
                    cur.execute(f"SHOW COLUMNS FROM `{db_part}`.`{tbl_part}`")
                    schema[tbl] = [r[0] for r in cur.fetchall()]
            conn.close()
            return schema

        if src_type == "mongodb":
            client = source._client
            for tbl in tables:
                db_name, col_name = tbl.split(".", 1)
                doc = client[db_name][col_name].find_one()
                if doc:
                    cols = [k for k in doc.keys() if k != "_id"]
                    schema[tbl] = ["_id"] + cols
                else:
                    schema[tbl] = []
            return schema

        if src_type == "dynamodb":
            conn_cfg = cfg.get("connection", {})
            import boto3
            kwargs = {"region_name": conn_cfg.get("region", "us-east-1")}
            if conn_cfg.get("endpoint_url"):
                kwargs["endpoint_url"] = conn_cfg["endpoint_url"]
            if conn_cfg.get("aws_access_key_id"):
                kwargs["aws_access_key_id"] = conn_cfg["aws_access_key_id"]
                kwargs["aws_secret_access_key"] = conn_cfg.get("aws_secret_access_key", "")
            ddb = boto3.client("dynamodb", **kwargs)
            for tbl in tables:
                resp = ddb.scan(TableName=tbl, Limit=1)
                if resp.get("Items"):
                    schema[tbl] = list(resp["Items"][0].keys())
                else:
                    desc = ddb.describe_table(TableName=tbl)
                    schema[tbl] = [k["AttributeName"] for k in desc["Table"]["AttributeDefinitions"]]
            return schema

        if src_type == "oracle":
            for tbl in tables:
                owner, tbl_name = tbl.upper().split(".", 1) if "." in tbl else ("HR", tbl.upper())
                schema[tbl] = [c.name for c in source.get_schema(tbl)]
            return schema

    except Exception as exc:
        logger.warning("Column introspection failed: %s", exc)
    return schema


def _get_typed_schema(source, src_type: str, cfg: dict, tables: List[str]) -> Dict[str, List[Dict]]:
    """Returns {table: [{name, raw_type, dremio_type}, ...]} for DDL generation."""
    from core.dremio_sink import _dremio_type
    schema: Dict[str, List[Dict]] = {}
    try:
        if src_type == "postgres":
            with source._snap_conn.cursor() as cur:
                cur.execute("""
                    SELECT table_schema || '.' || table_name, column_name, udt_name
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog','information_schema')
                      AND table_schema || '.' || table_name = ANY(%s)
                    ORDER BY table_schema, table_name, ordinal_position
                """, (tables,))
                for tbl, col, udt in cur.fetchall():
                    schema.setdefault(tbl, []).append({"name": col, "raw_type": udt, "dremio_type": _dremio_type(udt)})
            return schema

        if src_type in ("mysql", "mariadb"):
            conn_cfg = cfg.get("connection", {})
            import pymysql
            conn = pymysql.connect(
                host=conn_cfg.get("host", "localhost"),
                port=int(conn_cfg.get("port", 3306)),
                user=conn_cfg["user"],
                password=conn_cfg.get("password", ""),
            )
            with conn.cursor() as cur:
                for tbl in tables:
                    db_part, tbl_part = tbl.split(".", 1)
                    cur.execute(f"SHOW COLUMNS FROM `{db_part}`.`{tbl_part}`")
                    schema[tbl] = [{"name": r[0], "raw_type": r[1], "dremio_type": _dremio_type(r[1])} for r in cur.fetchall()]
            conn.close()
            return schema

        if src_type == "mongodb":
            client = source._client
            for tbl in tables:
                db_name, col_name = tbl.split(".", 1)
                doc = client[db_name][col_name].find_one()
                if doc:
                    schema[tbl] = [{"name": k, "raw_type": type(v).__name__, "dremio_type": "VARCHAR"} for k, v in doc.items()]
                else:
                    schema[tbl] = []
            return schema

        if src_type == "dynamodb":
            conn_cfg = cfg.get("connection", {})
            import boto3
            kwargs: Dict = {"region_name": conn_cfg.get("region", "us-east-1")}
            if conn_cfg.get("endpoint_url"):
                kwargs["endpoint_url"] = conn_cfg["endpoint_url"]
            if conn_cfg.get("aws_access_key_id"):
                kwargs["aws_access_key_id"] = conn_cfg["aws_access_key_id"]
                kwargs["aws_secret_access_key"] = conn_cfg.get("aws_secret_access_key", "")
            ddb = boto3.client("dynamodb", **kwargs)
            _ddb_type_map = {"S": "VARCHAR", "N": "DOUBLE", "B": "VARBINARY", "BOOL": "BOOLEAN"}
            for tbl in tables:
                desc = ddb.describe_table(TableName=tbl)
                schema[tbl] = [
                    {"name": a["AttributeName"], "raw_type": a["AttributeType"], "dremio_type": _ddb_type_map.get(a["AttributeType"], "VARCHAR")}
                    for a in desc["Table"]["AttributeDefinitions"]
                ]
            return schema

        if src_type == "oracle":
            for tbl in tables:
                cols = source.get_schema(tbl)
                schema[tbl] = [{"name": c.name, "raw_type": c.data_type, "dremio_type": _dremio_type(c.data_type)} for c in cols]
            return schema

    except Exception as exc:
        logger.warning("Typed schema introspection failed: %s", exc)
    return schema


@app.post("/api/sources/<name>/create-tables")
def api_create_tables(name: str):
    """Generate and optionally execute CREATE TABLE DDL in Dremio for a source's selected tables."""
    body = request.json or {}
    tables_filter: Optional[List[str]] = body.get("tables")
    dry_run: bool = body.get("dry_run", False)

    cfg = _load_raw()
    src_cfg = next((s for s in cfg.get("sources", []) if s["name"] == name), None)
    if not src_cfg:
        return jsonify({"error": f"Source '{name}' not found"}), 404

    dremio_cfg = cfg.get("dremio", {})
    src_type = src_cfg.get("type", "").lower()
    selected_tables: List[str] = tables_filter or src_cfg.get("tables", [])
    selected_columns: Dict[str, List[str]] = src_cfg.get("columns", {})

    if not selected_tables:
        return jsonify({"error": "No tables selected on this source"}), 400

    try:
        from core.engine import _load_sources, _SOURCE_REGISTRY
        _load_sources()
        cls = _SOURCE_REGISTRY.get(src_type)
        if not cls:
            return jsonify({"error": f"Unknown source type: {src_type}"}), 400
        source = cls(name=name, cfg=src_cfg)
        source.connect()
        typed_schema = _get_typed_schema(source, src_type, src_cfg, selected_tables)
        source.close()
    except Exception as exc:
        return jsonify({"error": f"Schema introspection failed: {exc}"}), 500

    from core.dremio_sink import DremioSink, _dremio_type, _quote, _quote_table, _CDC_META_COLS
    from core.event import ColumnSchema

    namespace = dremio_cfg.get("target_namespace", "cdc")

    sink: Optional[DremioSink] = None
    if not dry_run:
        try:
            sink = DremioSink(dremio_cfg)
            sink.connect()
        except Exception as exc:
            return jsonify({"error": f"Dremio connection failed: {exc}"}), 500

    results = []
    for table in selected_tables:
        typed_cols = typed_schema.get(table, [])
        # Filter to selected columns when user chose a subset
        col_filter = selected_columns.get(table)
        if col_filter:
            col_set = set(col_filter)
            typed_cols = [c for c in typed_cols if c["name"] in col_set]

        schema_cols = [ColumnSchema(c["name"], c["dremio_type"], nullable=True) for c in typed_cols]
        all_cols = schema_cols + list(_CDC_META_COLS)

        safe_name = table.replace(".", "_")
        target_path = f"{namespace}.{safe_name}"
        col_defs = ",\n  ".join(f"{_quote(c.name)} {_dremio_type(c.data_type)}" for c in all_cols)
        ddl = f"CREATE TABLE IF NOT EXISTS {_quote_table(target_path)} (\n  {col_defs}\n)"

        result: Dict = {"table": table, "target": target_path, "ddl": ddl, "status": "pending"}

        if not dry_run and sink:
            try:
                sink._sql(ddl)
                result["status"] = "created"
            except Exception as exc:
                err_str = str(exc)
                result["status"] = "exists" if "already exists" in err_str.lower() else "error"
                result["error"] = err_str

        results.append(result)

    return jsonify({"results": results, "dry_run": dry_run})


def _get_source_tables(source, src_type: str, cfg: dict) -> List[str]:
    """Introspect available tables/collections for a source."""
    try:
        if src_type == "postgres":
            with source._snap_conn.cursor() as cur:
                cur.execute("""
                    SELECT schemaname || '.' || tablename
                    FROM pg_tables
                    WHERE schemaname NOT IN ('pg_catalog','information_schema')
                    ORDER BY schemaname, tablename
                """)
                return [r[0] for r in cur.fetchall()]

        if src_type in ("mysql", "mariadb"):
            db = cfg.get("connection", {}).get("database", "")
            import pymysql
            conn = pymysql.connect(
                host=cfg["connection"].get("host", "localhost"),
                port=int(cfg["connection"].get("port", 3306)),
                user=cfg["connection"]["user"],
                password=cfg["connection"].get("password", ""),
                database=db,
            )
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                tables = [f"{db}.{r[0]}" for r in cur.fetchall()]
            conn.close()
            return tables

        if src_type == "mongodb":
            client = source._client
            tables = []
            for db_name in client.list_database_names():
                if db_name in ("admin", "local", "config"):
                    continue
                for col in client[db_name].list_collection_names():
                    tables.append(f"{db_name}.{col}")
            return tables

        if src_type == "dynamodb":
            import boto3
            conn_cfg = cfg.get("connection", {})
            kwargs = {"region_name": conn_cfg.get("region", "us-east-1")}
            if conn_cfg.get("endpoint_url"):
                kwargs["endpoint_url"] = conn_cfg["endpoint_url"]
            if conn_cfg.get("aws_access_key_id"):
                kwargs["aws_access_key_id"] = conn_cfg["aws_access_key_id"]
                kwargs["aws_secret_access_key"] = conn_cfg.get("aws_secret_access_key", "")
            ddb = boto3.client("dynamodb", **kwargs)
            tables = []
            paginator = ddb.get_paginator("list_tables")
            for page in paginator.paginate():
                tables.extend(page["TableNames"])
            return tables

        if src_type in ("sqlserver", "mssql"):
            import pymssql
            conn_cfg = cfg.get("connection", {})
            conn = pymssql.connect(
                server=conn_cfg.get("host", "localhost"),
                port=int(conn_cfg.get("port", 1433)),
                user=conn_cfg["user"],
                password=conn_cfg.get("password", ""),
                database=conn_cfg.get("database", ""),
                tds_version="7.0",
            )
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT TABLE_SCHEMA + '.' + TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE='BASE TABLE'
                      AND TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)
                tables = [r[0] for r in cur.fetchall()]
            conn.close()
            return tables

        if src_type == "oracle":
            conn_cfg = cfg.get("connection", {})
            try:
                import oracledb as cx
            except ImportError:
                import cx_Oracle as cx
            host = conn_cfg.get("host", "localhost")
            port = int(conn_cfg.get("port", 1521))
            svc  = conn_cfg.get("service_name") or conn_cfg.get("database") or "ORCL"
            dsn  = conn_cfg.get("dsn") or f"{host}:{port}/{svc}"
            conn = cx.connect(user=conn_cfg["user"], password=conn_cfg.get("password", ""), dsn=dsn)
            cur  = conn.cursor()
            cur.execute(
                "SELECT OWNER || '.' || TABLE_NAME FROM ALL_TABLES "
                "WHERE OWNER NOT IN ('SYS','SYSTEM','OUTLN','DBSNMP','XDB','CTXSYS','MDSYS','WMSYS') "
                "ORDER BY OWNER, TABLE_NAME"
            )
            tables = [r[0] for r in cur.fetchall()]
            cur.close()
            conn.close()
            return tables

        if src_type in ("pubsub", "datastream"):
            # No table introspection — tables are entered manually
            return []

    except Exception as exc:
        logger.warning("Table introspection failed: %s", exc)
    return []


# ── API: target / dremio config ───────────────────────────────────────────────

@app.get("/api/target")
def api_target_get():
    cfg = _load_raw()
    return jsonify({
        "dremio": cfg.get("dremio", {}),
        "iceberg": cfg.get("iceberg", {}),
        "sink_mode": cfg.get("options", {}).get("sink_mode", "dremio"),
        "transform_studio": cfg.get("transform_studio", {}),
    })


@app.put("/api/target")
def api_target_put():
    body = request.json or {}
    cfg = _load_raw()

    # Validate sink_mode compatibility with configured sources
    new_sink_mode = body.get("sink_mode", cfg.get("options", {}).get("sink_mode", "dremio"))
    if new_sink_mode == "dremio":
        _MODE_B_REQUIRED = {"pubsub", "spanner", "datastream"}
        incompatible = [
            s["name"] for s in cfg.get("sources", [])
            if s.get("type", "") in _MODE_B_REQUIRED
        ]
        if incompatible:
            return jsonify({
                "saved": False,
                "error": f"Mode A (Dremio SQL) is not compatible with source(s): {', '.join(incompatible)}. "
                         f"These sources require Mode B (Open Catalog) for correct operation."
            }), 400

    if "dremio" in body:
        cfg["dremio"] = body["dremio"]
    if "iceberg" in body:
        cfg["iceberg"] = body["iceberg"]
    if "sink_mode" in body:
        cfg.setdefault("options", {})["sink_mode"] = body["sink_mode"]
    if "transform_studio" in body:
        cfg["transform_studio"] = body["transform_studio"]
    _save_raw(cfg)
    return jsonify({"saved": True})


@app.post("/api/target/test")
def api_target_test():
    """Test the Dremio connection."""
    body = request.json or {}
    dremio_cfg = body.get("dremio", _load_raw().get("dremio", {}))
    try:
        headers, _, catalog_url = _dremio_headers(dremio_cfg)
        import requests as req
        r = req.get(catalog_url, headers=headers, timeout=8)
        r.raise_for_status()
        return jsonify({"ok": True, "version": r.headers.get("X-Dremio-Version", "unknown")})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


@app.get("/api/target/namespaces")
def api_target_namespaces():
    """List Dremio sources and spaces available as CDC targets."""
    cfg = _load_raw()
    dremio_cfg = cfg.get("dremio", {})
    if not dremio_cfg.get("host"):
        return jsonify({"ok": False, "error": "Dremio connection not configured"}), 400
    try:
        import requests as req
        headers, _, catalog_url = _dremio_headers(dremio_cfg)
        r = req.get(catalog_url, headers=headers, timeout=8)
        r.raise_for_status()
        namespaces = []
        for item in r.json().get("data", []):
            ns_type = item.get("containerType", item.get("type", ""))
            name = (item.get("path") or [None])[0]
            if name:
                namespaces.append({"name": name, "type": ns_type.lower()})
        return jsonify({"ok": True, "namespaces": namespaces})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


# ── API: saved target presets ─────────────────────────────────────────────────

def _targets_path() -> Path:
    return _config_path.parent / "targets.json"

def _load_targets() -> list:
    p = _targets_path()
    if p.exists():
        import json
        with open(p) as f:
            return json.load(f).get("targets", [])
    return []

def _save_targets(targets: list):
    import json
    with open(_targets_path(), "w") as f:
        json.dump({"targets": targets}, f, indent=2)


@app.get("/api/targets")
def api_targets_list():
    return jsonify({"targets": _load_targets()})


@app.post("/api/targets")
def api_targets_save():
    body = request.json or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    targets = _load_targets()
    targets = [t for t in targets if t.get("name") != name]  # replace if exists
    targets.append({
        "name": name,
        "dremio": body.get("dremio", {}),
        "iceberg": body.get("iceberg", {}),
        "sink_mode": body.get("sink_mode", "dremio"),
    })
    _save_targets(targets)
    return jsonify({"saved": True})


@app.delete("/api/targets/<name>")
def api_targets_delete(name: str):
    targets = [t for t in _load_targets() if t.get("name") != name]
    _save_targets(targets)
    return jsonify({"deleted": True})


@app.post("/api/targets/<name>/load")
def api_targets_load(name: str):
    """Copy a saved preset into the active config and return it."""
    targets = _load_targets()
    preset = next((t for t in targets if t.get("name") == name), None)
    if not preset:
        return jsonify({"error": f"Preset '{name}' not found"}), 404
    cfg = _load_raw()
    if "dremio" in preset:
        cfg["dremio"] = preset["dremio"]
    if "iceberg" in preset:
        cfg["iceberg"] = preset["iceberg"]
    cfg.setdefault("options", {})["sink_mode"] = preset.get("sink_mode", "dremio")
    _save_raw(cfg)
    return jsonify({"loaded": True, "target": preset})


@app.get("/api/mappings")
def api_mappings():
    """Return source table → Dremio target path for all configured sources."""
    cfg = _load_raw()
    sink_mode = cfg.get("options", {}).get("sink_mode", "dremio")
    if sink_mode == "iceberg":
        namespace = cfg.get("iceberg", {}).get("target_namespace", "cdc")
    else:
        namespace = cfg.get("dremio", {}).get("target_namespace", "cdc")

    mappings = []
    for src in cfg.get("sources", []):
        for table in src.get("tables", []):
            cols = src.get("columns", {}).get(table, [])
            mappings.append({
                "source_name": src["name"],
                "source_type": src.get("type", ""),
                "source_table": table,
                "target_path": f"{namespace}.{table.replace('.', '_')}",
                "columns": cols,
                "all_columns": not cols,
            })

    return jsonify({"mappings": mappings, "namespace": namespace, "sink_mode": sink_mode})


# ── API: settings ─────────────────────────────────────────────────────────────

@app.get("/api/settings")
def api_settings_get():
    cfg = _load_raw()
    return jsonify(cfg.get("options", {}))


@app.put("/api/settings")
def api_settings_put():
    body = request.json or {}
    cfg = _load_raw()
    cfg["options"] = body
    _save_raw(cfg)
    return jsonify({"saved": True})


# ── API: secrets ──────────────────────────────────────────────────────────────

@app.get("/api/secrets")
def api_secrets_get():
    cfg = _load_raw()
    return jsonify(cfg.get("secrets", {}))


@app.put("/api/secrets")
def api_secrets_put():
    body = request.json or {}
    cfg = _load_raw()
    if body:
        cfg["secrets"] = body
    elif "secrets" in cfg:
        del cfg["secrets"]
    _save_raw(cfg)
    return jsonify({"saved": True})


@app.get("/api/secrets/vault/list")
def api_secrets_vault_list():
    path = request.args.get("path", "")
    cfg = _load_raw()
    vault_cfg = cfg.get("secrets", {}).get("vault", {})
    if not vault_cfg:
        return jsonify({"ok": False, "error": "Vault not configured", "keys": []})
    try:
        from core.secrets import VaultClient
        vc = VaultClient(vault_cfg)
        mount = vault_cfg.get("mount", "secret")
        resp = vc._client.secrets.kv.v2.list_secrets(path=path or "/", mount_point=mount)
        keys = resp.get("data", {}).get("keys", [])
        return jsonify({"ok": True, "keys": keys})
    except (ImportError, SystemExit):
        return jsonify({"ok": False, "error": "hvac not installed", "keys": []})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "keys": []})


@app.post("/api/secrets/test")
def api_secrets_test():
    body = request.json or {}
    vault_cfg = body.get("vault", {})
    if not vault_cfg:
        return jsonify({"ok": False, "error": "No Vault configuration provided"})
    try:
        from core.secrets import VaultClient
        VaultClient(vault_cfg)
        return jsonify({"ok": True})
    except (ImportError, SystemExit):
        return jsonify({"ok": False, "error": "hvac library not installed — run: pip install hvac"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── API: alerts ──────────────────────────────────────────────────────────────

@app.get("/api/alerts")
def api_alerts_get():
    _ensure_alert_manager()
    cfg = _load_raw().get("alerts", {})
    recent = _alert_manager.get_recent() if _alert_manager else []
    return jsonify({"config": cfg, "recent": recent})


@app.put("/api/alerts")
def api_alerts_put():
    body = request.json or {}
    cfg = _load_raw()
    cfg["alerts"] = body
    _save_raw(cfg)
    if _alert_manager:
        _alert_manager.reconfigure(body)
    else:
        _ensure_alert_manager()
    return jsonify({"saved": True})


# ── API: dead letter queue ────────────────────────────────────────────────────

@app.get("/api/dlq")
def api_dlq_list():
    dlq = _get_dlq()
    return jsonify({"entries": dlq.get_all(), "stats": dlq.stats()})


@app.post("/api/dlq/<int:entry_id>/retry")
def api_dlq_retry(entry_id: int):
    _get_dlq().reset_to_pending(entry_id)
    return jsonify({"queued": entry_id})


@app.post("/api/dlq/retry-all")
def api_dlq_retry_all():
    _get_dlq().reset_all_exhausted()
    return jsonify({"ok": True})


@app.delete("/api/dlq/<int:entry_id>")
def api_dlq_discard(entry_id: int):
    _get_dlq().discard(entry_id)
    return jsonify({"discarded": entry_id})


@app.delete("/api/dlq")
def api_dlq_discard_all():
    _get_dlq().discard_all()
    return jsonify({"ok": True})


# ── API: offset management ────────────────────────────────────────────────────

@app.delete("/api/offsets/<source>/<path:table>")
def api_offset_reset(source, table):
    """Reset the replication offset for a table (forces re-snapshot on next start)."""
    cfg = _load_raw()
    options = cfg.get("options", {})
    from core.offset_store import OffsetStore
    store = OffsetStore(options.get("offset_db_path", "./cdc_offsets.db"))
    store.set(source, table, None)
    return jsonify({"reset": f"{source}/{table}"})


# ── SPA fallback ──────────────────────────────────────────────────────────────

_DIST = ROOT / "ui" / "frontend" / "dist"

@app.get("/")
@app.get("/<path:path>")
def spa(path=""):
    # Serve actual files from dist (JS/CSS/assets) if they exist
    candidate = _DIST / path if path else _DIST / "index.html"
    if path and candidate.exists() and candidate.is_file():
        return send_from_directory(str(_DIST), path)
    # All other routes → index.html (React Router handles them client-side)
    if (_DIST / "index.html").exists():
        return send_from_directory(str(_DIST), "index.html")
    return "<h2>UI not built. Run: cd ui/frontend && npm run build</h2>", 200


# ── Entry point ───────────────────────────────────────────────────────────────

def run_ui(config_path: str = "config.yml", port: int = 7070, open_browser: bool = True):
    global _config_path
    _config_path = Path(config_path)

    _ensure_alert_manager()

    if open_browser:
        import webbrowser
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()

    logger.info("Dremio CDC UI running at http://localhost:%d", port)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config.yml")
    p.add_argument("--port", type=int, default=7070)
    p.add_argument("--no-browser", action="store_true")
    args = p.parse_args()
    run_ui(args.config, args.port, open_browser=not args.no_browser)
