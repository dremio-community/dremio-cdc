"""
AlertManager — watches StatusStore and fires notifications when thresholds are exceeded.

Supported channels:
  slack   — POST to an Incoming Webhook URL
  webhook — POST (or any method) to an arbitrary HTTP endpoint
  email   — SMTP with optional STARTTLS

Config lives under the `alerts:` key in config.yml:

  alerts:
    enabled: true
    lag_threshold_seconds: 60
    error_count_threshold: 5
    cooldown_seconds: 300
    check_interval_seconds: 30
    channels:
      - type: slack
        webhook_url: https://hooks.slack.com/services/...
      - type: webhook
        url: https://my-endpoint.example.com/hook
        method: post
      - type: email
        smtp_host: smtp.gmail.com
        smtp_port: 587
        smtp_tls: true
        smtp_user: me@gmail.com
        smtp_password: secret
        from: me@gmail.com
        to: oncall@company.com
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional

from core.status_store import StatusStore

logger = logging.getLogger(__name__)


class AlertManager:
    def __init__(self, cfg: Dict[str, Any], status_store: StatusStore):
        self._cfg = cfg
        self._status = status_store
        self._enabled: bool = cfg.get("enabled", True)
        self._lag_threshold: float = float(cfg.get("lag_threshold_seconds", 60))
        self._error_threshold: int = int(cfg.get("error_count_threshold", 5))
        self._cooldown: float = float(cfg.get("cooldown_seconds", 300))
        self._interval: float = float(cfg.get("check_interval_seconds", 30))
        self._channels: List[Dict] = cfg.get("channels", [])

        self._last_fired: Dict[str, float] = {}
        self._recent: Deque[Dict] = deque(maxlen=200)
        self._lock = threading.Lock()

        self._stop_flag = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_flag.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="AlertManager")
        self._thread.start()
        logger.info("AlertManager started (interval=%ss)", self._interval)

    def stop(self):
        self._stop_flag.set()

    def reconfigure(self, cfg: Dict[str, Any]):
        """Hot-reload config without restarting the thread."""
        self._cfg = cfg
        self._enabled = cfg.get("enabled", True)
        self._lag_threshold = float(cfg.get("lag_threshold_seconds", 60))
        self._error_threshold = int(cfg.get("error_count_threshold", 5))
        self._cooldown = float(cfg.get("cooldown_seconds", 300))
        self._interval = float(cfg.get("check_interval_seconds", 30))
        self._channels = cfg.get("channels", [])

    # ── Internal loop ─────────────────────────────────────────────────────────

    def _loop(self):
        while not self._stop_flag.wait(self._interval):
            if self._enabled:
                try:
                    self._check()
                except Exception as exc:
                    logger.error("AlertManager check error: %s", exc)

    def _check(self):
        snap = self._status.snapshot()
        now = time.time()
        for w in snap.get("workers", []):
            src, tbl = w["source"], w["table"]

            lag = w.get("lag_seconds")
            if lag is not None and lag > self._lag_threshold:
                self._maybe_fire(
                    key=f"lag:{src}/{tbl}",
                    alert_type="lag",
                    worker=w,
                    message=f"Lag {lag:.1f}s exceeds threshold {self._lag_threshold:.0f}s",
                    now=now,
                )

            errs = w.get("error_count", 0)
            if errs >= self._error_threshold:
                self._maybe_fire(
                    key=f"errors:{src}/{tbl}",
                    alert_type="errors",
                    worker=w,
                    message=f"Error count {errs} reached threshold {self._error_threshold}",
                    now=now,
                )

            if w.get("state") == "error":
                self._maybe_fire(
                    key=f"state:{src}/{tbl}",
                    alert_type="worker_error",
                    worker=w,
                    message=f"Worker entered error state: {w.get('error', 'unknown')}",
                    now=now,
                )

    def _maybe_fire(self, key: str, alert_type: str, worker: dict, message: str, now: float):
        with self._lock:
            if now - self._last_fired.get(key, 0) < self._cooldown:
                return
            self._last_fired[key] = now

        record = {
            "time": now,
            "type": alert_type,
            "source": worker["source"],
            "table": worker["table"],
            "message": message,
        }
        with self._lock:
            self._recent.append(record)

        logger.warning("ALERT [%s] %s/%s — %s", alert_type, worker["source"], worker["table"], message)
        for channel in self._channels:
            try:
                self._send(channel, record)
            except Exception as exc:
                logger.error("Alert delivery failed (type=%s): %s", channel.get("type"), exc)

    # ── Channel dispatch ──────────────────────────────────────────────────────

    def _send(self, channel: dict, record: dict):
        ch_type = channel.get("type", "").lower()
        text = (
            f"[Dremio CDC] {record['type'].upper()}: {record['message']} "
            f"({record['source']}/{record['table']})"
        )

        if ch_type == "slack":
            import requests
            requests.post(
                channel["webhook_url"],
                json={"text": text},
                timeout=10,
            ).raise_for_status()

        elif ch_type == "webhook":
            import requests
            method = channel.get("method", "post").lower()
            getattr(requests, method)(
                channel["url"],
                json={**record, "text": text},
                timeout=10,
            ).raise_for_status()

        elif ch_type == "email":
            import smtplib
            from email.message import EmailMessage
            em = EmailMessage()
            em["Subject"] = f"[Dremio CDC Alert] {record['type']} — {record['source']}/{record['table']}"
            em["From"] = channel["from"]
            em["To"] = channel["to"]
            em.set_content(text)
            with smtplib.SMTP(channel.get("smtp_host", "localhost"), int(channel.get("smtp_port", 25))) as smtp:
                if channel.get("smtp_tls"):
                    smtp.starttls()
                if channel.get("smtp_user"):
                    smtp.login(channel["smtp_user"], channel.get("smtp_password", ""))
                smtp.send_message(em)

        else:
            logger.warning("Unknown alert channel type: %s", ch_type)

    # ── Query ─────────────────────────────────────────────────────────────────

    def get_recent(self) -> List[dict]:
        with self._lock:
            return list(self._recent)

    def get_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "lag_threshold_seconds": self._lag_threshold,
            "error_count_threshold": self._error_threshold,
            "cooldown_seconds": self._cooldown,
            "check_interval_seconds": self._interval,
            "channels": self._channels,
        }
