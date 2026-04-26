"""
Amazon DynamoDB CDC source — reads changes via DynamoDB Streams.
Requires a DynamoDB table with StreamSpecification enabled (NEW_AND_OLD_IMAGES).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_DYNAMO_TYPES = {
    "S": "varchar",
    "N": "double",
    "B": "bytea",
    "BOOL": "boolean",
    "NULL": "varchar",
    "L": "varchar",   # list → JSON string
    "M": "varchar",   # map → JSON string
}

_OP_MAP = {
    "INSERT": Operation.INSERT,
    "MODIFY": Operation.UPDATE,
    "REMOVE": Operation.DELETE,
}


def _deserialise(item: Dict) -> Dict:
    """Convert DynamoDB attribute-value format to plain Python dict."""
    import json
    out = {}
    for k, v in item.items():
        if "S" in v:
            out[k] = v["S"]
        elif "N" in v:
            out[k] = float(v["N"]) if "." in v["N"] else int(v["N"])
        elif "BOOL" in v:
            out[k] = v["BOOL"]
        elif "NULL" in v:
            out[k] = None
        elif "L" in v:
            out[k] = json.dumps(v["L"])
        elif "M" in v:
            out[k] = json.dumps(v["M"])
        else:
            out[k] = str(v)
    return out


def _parse_ts(val) -> "datetime":
    if isinstance(val, datetime):
        return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
    return datetime.fromtimestamp(float(val or 0), tz=timezone.utc)


def _infer_schema(item: Dict, key_schema: List[Dict]) -> List[ColumnSchema]:
    pk_names = {k["AttributeName"] for k in key_schema}
    cols = []
    for name, val in item.items():
        if isinstance(val, bool):
            t = "boolean"
        elif isinstance(val, int):
            t = "bigint"
        elif isinstance(val, float):
            t = "double"
        else:
            t = "varchar"
        cols.append(ColumnSchema(name=name, data_type=t, primary_key=(name in pk_names)))
    return cols


class DynamoDBSource(CDCSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._ddb = None
        self._streams = None

    def connect(self):
        try:
            import boto3
        except ImportError:
            raise SystemExit("boto3 required: pip install boto3")

        conn_cfg = self.cfg["connection"]
        kwargs = {
            "region_name": conn_cfg.get("region", "us-east-1"),
        }
        if conn_cfg.get("aws_access_key_id"):
            kwargs["aws_access_key_id"]     = conn_cfg["aws_access_key_id"]
            kwargs["aws_secret_access_key"] = conn_cfg["aws_secret_access_key"]
        if conn_cfg.get("endpoint_url"):
            kwargs["endpoint_url"] = conn_cfg["endpoint_url"]

        self._ddb     = __import__("boto3").client("dynamodb", **kwargs)
        self._streams = __import__("boto3").client("dynamodbstreams", **kwargs)
        logger.info("Connected to DynamoDB (region %s)", conn_cfg.get("region", "us-east-1"))

    def _table_name(self, table: str) -> str:
        return table.split(".")[-1]   # allow "namespace.TableName" notation

    def get_schema(self, table: str) -> List[ColumnSchema]:
        tbl = self._table_name(table)
        desc = self._ddb.describe_table(TableName=tbl)["Table"]
        key_schema = desc["KeySchema"]
        attrs = {a["AttributeName"]: a["AttributeType"] for a in desc["AttributeDefinitions"]}
        return [
            ColumnSchema(
                name=a["AttributeName"],
                data_type=_DYNAMO_TYPES.get(attrs.get(a["AttributeName"], "S"), "varchar"),
                primary_key=True,
            )
            for a in key_schema
        ]

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        tbl = self._table_name(table)
        desc = self._ddb.describe_table(TableName=tbl)["Table"]
        key_schema = desc["KeySchema"]
        paginator = self._ddb.get_paginator("scan")
        for page in paginator.paginate(TableName=tbl):
            for item in page["Items"]:
                row = _deserialise(item)
                schema = _infer_schema(row, key_schema)
                yield ChangeEvent(
                    op=Operation.SNAPSHOT,
                    source_name=self.name,
                    source_table=table,
                    before=None,
                    after=row,
                    schema=schema,
                    timestamp=datetime.now(timezone.utc),
                    offset=None,
                )

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        tbl = self._table_name(table)
        desc = self._ddb.describe_table(TableName=tbl)["Table"]
        key_schema = desc["KeySchema"]
        stream_arn = desc.get("LatestStreamArn")
        if not stream_arn:
            raise RuntimeError(f"DynamoDB table {tbl} does not have streams enabled. "
                               "Enable with StreamSpecification: NEW_AND_OLD_IMAGES")

        # Get shards
        stream_desc = self._streams.describe_stream(StreamArn=stream_arn)["StreamDescription"]
        shards = stream_desc["Shards"]

        for shard in shards:
            shard_id = shard["ShardId"]
            saved_seq = (offset or {}).get(shard_id)

            if saved_seq:
                shard_iter = self._streams.get_shard_iterator(
                    StreamArn=stream_arn, ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    SequenceNumber=saved_seq,
                )["ShardIterator"]
            else:
                shard_iter = self._streams.get_shard_iterator(
                    StreamArn=stream_arn, ShardId=shard_id,
                    ShardIteratorType="TRIM_HORIZON",
                )["ShardIterator"]

            while shard_iter:
                resp = self._streams.get_records(ShardIterator=shard_iter, Limit=100)
                for record in resp.get("Records", []):
                    op = _OP_MAP.get(record["eventName"], Operation.INSERT)
                    dyn = record["dynamodb"]
                    new_img = _deserialise(dyn.get("NewImage", {})) or None
                    old_img = _deserialise(dyn.get("OldImage", {})) or None
                    row = new_img or old_img or {}
                    schema = _infer_schema(row, key_schema)
                    seq = dyn["SequenceNumber"]
                    yield ChangeEvent(
                        op=op,
                        source_name=self.name,
                        source_table=table,
                        before=old_img,
                        after=new_img,
                        schema=schema,
                        timestamp=_parse_ts(record["dynamodb"].get("ApproximateCreationDateTime", 0)),
                        offset={shard_id: seq},
                    )
                shard_iter = resp.get("NextShardIterator")

    def close(self):
        pass   # boto3 clients don't require explicit close
