"""
Offset store — persists the last-committed source position for each connector.

Supports two backends:
  SQLite  (default)  — single-process, zero config, path like ./cdc_offsets.db
  PostgreSQL         — multi-agent safe; set offset_db_path to a postgres:// DSN

Auto-detected from the value of options.offset_db_path in config.yml.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Optional


class SQLiteOffsetStore:
    def __init__(self, db_path: str = "./cdc_offsets.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._init()

    def _init(self):
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS offsets (
                    source_name  TEXT NOT NULL,
                    source_table TEXT NOT NULL,
                    offset_json  TEXT NOT NULL,
                    updated_at   TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (source_name, source_table)
                )
            """)
            self._conn.commit()

    def get(self, source_name: str, source_table: str) -> Optional[Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT offset_json FROM offsets WHERE source_name=? AND source_table=?",
                (source_name, source_table),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def set(self, source_name: str, source_table: str, offset: Any):
        with self._lock:
            self._conn.execute(
                """INSERT INTO offsets (source_name, source_table, offset_json, updated_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(source_name, source_table)
                   DO UPDATE SET offset_json=excluded.offset_json,
                                 updated_at=excluded.updated_at""",
                (source_name, source_table, json.dumps(offset)),
            )
            self._conn.commit()

    def all(self) -> dict:
        with self._lock:
            rows = self._conn.execute(
                "SELECT source_name, source_table, offset_json FROM offsets"
            ).fetchall()
        return {(r[0], r[1]): json.loads(r[2]) for r in rows}


class PostgresOffsetStore:
    """
    PostgreSQL-backed offset store.  Safe for multiple engine processes running
    in parallel — each write uses INSERT … ON CONFLICT DO UPDATE (upsert) so
    two workers updating different tables never block each other.
    """

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self):
        import psycopg2
        conn = psycopg2.connect(self._dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cdc_offsets (
                    source_name  TEXT NOT NULL,
                    source_table TEXT NOT NULL,
                    offset_val   TEXT,
                    updated_at   TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (source_name, source_table)
                )
            """)
        conn.close()

    def get(self, source_name: str, source_table: str) -> Optional[Any]:
        import psycopg2
        with self._lock:
            conn = psycopg2.connect(self._dsn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT offset_val FROM cdc_offsets "
                    "WHERE source_name=%s AND source_table=%s",
                    (source_name, source_table),
                )
                row = cur.fetchone()
            conn.close()
        return json.loads(row[0]) if row and row[0] else None

    def set(self, source_name: str, source_table: str, offset: Any):
        import psycopg2
        val = json.dumps(offset) if offset is not None else None
        with self._lock:
            conn = psycopg2.connect(self._dsn)
            with conn.cursor() as cur:
                if val is None:
                    cur.execute(
                        "DELETE FROM cdc_offsets "
                        "WHERE source_name=%s AND source_table=%s",
                        (source_name, source_table),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO cdc_offsets (source_name, source_table, offset_val)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (source_name, source_table)
                        DO UPDATE SET offset_val = EXCLUDED.offset_val,
                                      updated_at  = NOW()
                        """,
                        (source_name, source_table, val),
                    )
            conn.commit()
            conn.close()

    def all(self) -> dict:
        import psycopg2
        with self._lock:
            conn = psycopg2.connect(self._dsn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT source_name, source_table, offset_val FROM cdc_offsets"
                )
                rows = cur.fetchall()
            conn.close()
        return {(r[0], r[1]): json.loads(r[2]) if r[2] else None for r in rows}


# Backward-compatible alias — existing code that imports OffsetStore still works
OffsetStore = SQLiteOffsetStore


def get_offset_store(path_or_dsn: str):
    """Return the right store based on the configured path/DSN."""
    if path_or_dsn.startswith(("postgresql://", "postgres://")):
        return PostgresOffsetStore(path_or_dsn)
    return SQLiteOffsetStore(path_or_dsn)
