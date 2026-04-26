"""
YAML config loader with full secrets resolution.

Supports ${ENV_VAR} and vault:path#field references anywhere in the config.
See core/secrets.py for Vault setup details.
"""
from __future__ import annotations

from typing import Any, Dict, List

try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required: pip install pyyaml")

from core.secrets import build_resolver


def load_config(path: str) -> Dict[str, Any]:
    with open(path) as f:
        cfg = yaml.safe_load(f)

    resolver = build_resolver(cfg)
    cfg = resolver.walk(cfg)

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
