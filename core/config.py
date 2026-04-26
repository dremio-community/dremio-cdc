"""
YAML config loader and validation.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required: pip install pyyaml")


def _env(value: Any) -> Any:
    """Expand ${ENV_VAR} references in string config values."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        var = value[2:-1]
        return os.environ.get(var, value)
    return value


def load_config(path: str) -> Dict[str, Any]:
    with open(path) as f:
        cfg = yaml.safe_load(f)

    # expand env vars in connection blocks
    for source in cfg.get("sources", []):
        conn = source.get("connection", {})
        for k, v in conn.items():
            conn[k] = _env(v)

    dremio = cfg.get("dremio", {})
    for k, v in dremio.items():
        dremio[k] = _env(v)

    return cfg


def get_source_configs(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    return cfg.get("sources", [])


def get_dremio_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return cfg.get("dremio", {})


def get_options(cfg: Dict[str, Any]) -> Dict[str, Any]:
    defaults = {
        "batch_size": 500,
        "batch_timeout_seconds": 10,
        "snapshot_on_first_run": True,
        "offset_db_path": "./cdc_offsets.db",
        "log_level": "INFO",
    }
    defaults.update(cfg.get("options", {}))
    return defaults
