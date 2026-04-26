"""
SQLite-backed schema store.
Persists the last-known column schema per (source, table) for drift detection.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import List, Optional

from core.event import ColumnSchema


class SchemaStore:
    def __init__(self, db_path: str = "./cdc_schemas.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._init()

    def _init(self):
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS schemas (
                    source_name  TEXT NOT NULL,
                    source_table TEXT NOT NULL,
                    schema_json  TEXT NOT NULL,
                    updated_at   TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (source_name, source_table)
                )
            """)
            self._conn.commit()

    def get(self, source_name: str, source_table: str) -> Optional[List[ColumnSchema]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT schema_json FROM schemas WHERE source_name=? AND source_table=?",
                (source_name, source_table),
            ).fetchone()
        if not row:
            return None
        return [ColumnSchema(**c) for c in json.loads(row[0])]

    def set(self, source_name: str, source_table: str, schema: List[ColumnSchema]):
        data = json.dumps([
            {"name": c.name, "data_type": c.data_type,
             "nullable": c.nullable, "primary_key": c.primary_key}
            for c in schema
        ])
        with self._lock:
            self._conn.execute(
                """INSERT INTO schemas (source_name, source_table, schema_json, updated_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(source_name, source_table)
                   DO UPDATE SET schema_json=excluded.schema_json,
                                 updated_at=excluded.updated_at""",
                (source_name, source_table, data),
            )
            self._conn.commit()
