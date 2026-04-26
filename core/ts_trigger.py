"""
Transform Studio integration — fires a pipeline run after each successful CDC flush.

Config (under transform_studio: in config.yml):
  transform_studio:
    enabled: true
    url: http://localhost:5000
    pipeline_id: abc123
    token: optional_bearer_token   # Transform Studio API key
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TransformStudioTrigger:
    def __init__(self, url: str, pipeline_id: str, token: Optional[str] = None):
        self._url = url.rstrip("/")
        self._pipeline_id = pipeline_id
        self._token = token

    def trigger(self, source: str, table: str, event_count: int) -> None:
        try:
            import requests
            headers: dict = {"Content-Type": "application/json"}
            if self._token:
                headers["Authorization"] = f"Bearer {self._token}"
            resp = requests.post(
                f"{self._url}/api/pipelines/{self._pipeline_id}/run",
                json={"source": source, "table": table, "event_count": event_count},
                headers=headers,
                timeout=5,
            )
            resp.raise_for_status()
            logger.debug(
                "Transform Studio trigger fired for %s/%s (%d events)", source, table, event_count
            )
        except Exception as exc:
            logger.warning("Transform Studio trigger failed for %s/%s: %s", source, table, exc)


def build_trigger(cfg: dict) -> Optional[TransformStudioTrigger]:
    """Return a TransformStudioTrigger from config dict, or None if disabled/missing."""
    if not cfg.get("enabled", False):
        return None
    url = cfg.get("url", "").strip()
    pipeline_id = cfg.get("pipeline_id", "").strip()
    if not url or not pipeline_id:
        logger.warning("Transform Studio trigger enabled but url/pipeline_id not set — skipping")
        return None
    return TransformStudioTrigger(url, pipeline_id, token=cfg.get("token") or None)
