"""
Column-level masking / PII filtering applied before events reach the sink.

Config format (per source, in config.yml under each source entry):
  masking:
    public.users:
      email: mask_email      # a***@example.com
      ssn:   mask_ssn        # ***-**-6789
      phone: mask_phone      # ***-***-1234
      cc:    mask_card       # ****-****-****-1234
      ip:    mask_ip         # 192.168.*.*
      name:  mask_name       # J***

Generic functions also available: redact, hash_sha256, hash_md5, mask, nullify, tokenize
"""
from __future__ import annotations

import copy
import hashlib
import logging
import re
from typing import Any, Dict, Optional

from core.event import ChangeEvent

logger = logging.getLogger(__name__)

MASKING_FUNCTIONS = [
    "redact", "hash_sha256", "hash_md5", "mask", "nullify", "tokenize",
    "mask_email", "mask_phone", "mask_ssn", "mask_card", "mask_ip", "mask_name",
]

# ── Pattern-aware helpers ─────────────────────────────────────────────────────

def _mask_email(s: str) -> str:
    """a***@example.com — preserve first char and full domain."""
    m = re.match(r'^(.).*?(@.+)$', s)
    if m:
        return m.group(1) + "***" + m.group(2)
    return "***@***.***"


def _mask_phone(s: str) -> str:
    """Keep last 4 digits, mask the rest as ***-***-NNNN."""
    digits = re.sub(r'\D', '', s)
    if len(digits) >= 4:
        return "***-***-" + digits[-4:]
    return "***-***-****"


def _mask_ssn(s: str) -> str:
    """Keep last 4 digits: ***-**-NNNN."""
    digits = re.sub(r'\D', '', s)
    if len(digits) >= 4:
        return "***-**-" + digits[-4:]
    return "***-**-****"


def _mask_card(s: str) -> str:
    """Keep last 4 digits: ****-****-****-NNNN."""
    digits = re.sub(r'\D', '', s)
    if len(digits) >= 4:
        return "****-****-****-" + digits[-4:]
    return "****-****-****-****"


def _mask_ip(s: str) -> str:
    """Mask last two octets: 192.168.*.*"""
    parts = s.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.*.*"
    return "*.*.*.*"


def _mask_name(s: str) -> str:
    """Keep first initial only: J***"""
    s = s.strip()
    if s:
        return s[0].upper() + "***"
    return "***"


# ── Dispatch ──────────────────────────────────────────────────────────────────

def _apply_fn(fn: str, value: Any) -> Any:
    if value is None:
        return None
    s = str(value)
    if fn == "redact":
        return "[REDACTED]"
    if fn == "hash_sha256":
        return hashlib.sha256(s.encode()).hexdigest()
    if fn == "hash_md5":
        return hashlib.md5(s.encode()).hexdigest()  # noqa: S324
    if fn == "mask":
        return "***"
    if fn == "nullify":
        return None
    if fn == "tokenize":
        return "tok_" + hashlib.sha256(s.encode()).hexdigest()[:16]
    if fn == "mask_email":
        return _mask_email(s)
    if fn == "mask_phone":
        return _mask_phone(s)
    if fn == "mask_ssn":
        return _mask_ssn(s)
    if fn == "mask_card":
        return _mask_card(s)
    if fn == "mask_ip":
        return _mask_ip(s)
    if fn == "mask_name":
        return _mask_name(s)
    return value


def _mask_row(row: Optional[Dict[str, Any]], rules: Dict[str, str]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    out = dict(row)
    for col, fn in rules.items():
        if col in out:
            out[col] = _apply_fn(fn, out[col])
    return out


class MaskingEngine:
    """
    Applies per-column masking rules to ChangeEvent before/after payloads.

    rules: table_name -> {column_name -> function_name}
    """

    def __init__(self, rules: Dict[str, Dict[str, str]]):
        self._rules = rules

    def apply(self, table: str, event: ChangeEvent) -> ChangeEvent:
        rules = self._rules.get(table, {})
        if not rules:
            return event
        ev = copy.copy(event)
        ev.before = _mask_row(event.before, rules)
        ev.after  = _mask_row(event.after,  rules)
        return ev

    def apply_batch(self, table: str, batch: list) -> list:
        rules = self._rules.get(table, {})
        if not rules:
            return batch
        return [self.apply(table, ev) for ev in batch]
