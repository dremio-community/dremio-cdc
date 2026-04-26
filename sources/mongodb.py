"""
MongoDB CDC source — uses MongoDB Change Streams (requires replica set or Atlas).
Supports insert / update / replace / delete operations.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_BSON_TYPES = {
    "string":    "varchar",
    "int":       "integer",
    "long":      "bigint",
    "double":    "double",
    "decimal":   "numeric",
    "bool":      "boolean",
    "date":      "timestamp",
    "timestamp": "timestamp",
    "objectId":  "varchar",
    "array":     "varchar",   # stored as JSON string
    "object":    "varchar",   # stored as JSON string
    "null":      "varchar",
}

_OP_MAP = {
    "insert":  Operation.INSERT,
    "update":  Operation.UPDATE,
    "replace": Operation.UPDATE,
    "delete":  Operation.DELETE,
}


def _flatten(doc: Dict, prefix: str = "") -> Dict:
    """Flatten nested MongoDB doc to dot-notation keys (1 level deep for simplicity)."""
    out = {}
    for k, v in doc.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out[key] = str(v)   # store nested docs as JSON string
        elif isinstance(v, list):
            out[key] = str(v)
        else:
            out[key] = v
    return out


def _infer_schema(doc: Dict) -> List[ColumnSchema]:
    cols = []
    for k, v in doc.items():
        if isinstance(v, bool):
            t = "boolean"
        elif isinstance(v, int):
            t = "bigint"
        elif isinstance(v, float):
            t = "double"
        elif isinstance(v, datetime):
            t = "timestamp"
        else:
            t = "varchar"
        cols.append(ColumnSchema(name=k, data_type=t, primary_key=(k == "_id")))
    return cols


class MongoDBSource(CDCSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._client = None

    def connect(self):
        try:
            from pymongo import MongoClient
        except ImportError:
            raise SystemExit("pymongo required: pip install pymongo")

        conn_cfg = self.cfg["connection"]
        uri = conn_cfg.get("uri") or (
            f"mongodb://{conn_cfg.get('user','')}:{conn_cfg.get('password','')}@"
            f"{conn_cfg.get('host','localhost')}:{conn_cfg.get('port',27017)}/"
            f"?authSource={conn_cfg.get('auth_source','admin')}"
        )
        self._client = __import__("pymongo").MongoClient(uri)
        logger.info("Connected to MongoDB %s", conn_cfg.get("host", "localhost"))

    def _db_col(self, table: str):
        parts = table.split(".", 1)
        db_name = parts[0] if len(parts) > 1 else self.cfg["connection"].get("database", "")
        col_name = parts[1] if len(parts) > 1 else parts[0]
        return self._client[db_name][col_name]

    def get_schema(self, table: str) -> List[ColumnSchema]:
        collection = self._db_col(table)
        sample = collection.find_one()
        if not sample:
            return [ColumnSchema("_id", "varchar", primary_key=True)]
        flat = _flatten(sample)
        return _infer_schema(flat)

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        collection = self._db_col(table)
        schema = self.get_schema(table)
        for doc in collection.find():
            doc["_id"] = str(doc["_id"])
            flat = _flatten(doc)
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name,
                source_table=table,
                before=None,
                after=flat,
                schema=_infer_schema(flat),
                timestamp=datetime.now(timezone.utc),
                offset=None,
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        from bson import ObjectId
        collection = self._db_col(table)
        query = {}
        if start_after is not None:
            try:
                query["_id"] = {"$gt": ObjectId(str(start_after))}
            except Exception:
                query[cursor_col] = {"$gt": start_after}
        for doc in collection.find(query).sort("_id", 1).limit(chunk_size):
            doc["_id"] = str(doc["_id"])
            flat = _flatten(doc)
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name,
                source_table=table,
                before=None,
                after=flat,
                schema=_infer_schema(flat),
                timestamp=datetime.now(timezone.utc),
                offset=None,
            )

    def get_pk_column(self, table: str) -> str:
        return "_id"

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        collection = self._db_col(table)
        # "snap:done" / "snap:..." means incremental snapshot finished — no resume token
        resume_token = offset if (offset and not str(offset).startswith("snap:")) else None

        pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "replace", "delete"]}}}]
        kwargs = {"resume_after": resume_token} if resume_token and not isinstance(resume_token, str) else {}

        with collection.watch(pipeline, **kwargs) as stream:
            for change in stream:
                op = _OP_MAP.get(change["operationType"], Operation.INSERT)
                doc_id = str(change["documentKey"]["_id"])

                if op == Operation.DELETE:
                    before = {"_id": doc_id}
                    after = None
                    schema = [ColumnSchema("_id", "varchar", primary_key=True)]
                else:
                    full_doc = change.get("fullDocument") or {}
                    full_doc["_id"] = str(full_doc.get("_id", doc_id))
                    flat = _flatten(full_doc)
                    schema = _infer_schema(flat)
                    before = None
                    after = flat

                yield ChangeEvent(
                    op=op,
                    source_name=self.name,
                    source_table=table,
                    before=before,
                    after=after,
                    schema=schema,
                    timestamp=datetime.fromtimestamp(
                        change["clusterTime"].time, tz=timezone.utc
                    ),
                    offset=change["_id"],   # resume token
                )

    def close(self):
        if self._client:
            self._client.close()
