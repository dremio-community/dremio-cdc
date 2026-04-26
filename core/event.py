"""
Core change event data model — source-agnostic representation of a CDC event.
Every source connector emits ChangeEvent objects; the Dremio sink consumes them.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class Operation(str, Enum):
    INSERT   = "insert"
    UPDATE   = "update"
    DELETE   = "delete"
    SNAPSHOT = "snapshot"   # initial full-table load row


@dataclass
class ColumnSchema:
    name: str
    data_type: str          # normalised SQL type: VARCHAR, INT, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP, etc.
    nullable: bool = True
    primary_key: bool = False


@dataclass
class ChangeEvent:
    op:           Operation
    source_name:  str               # connector instance name from config
    source_table: str               # fully-qualified source table (e.g. public.customers)
    before:       Optional[Dict[str, Any]]   # previous row values (UPDATE / DELETE)
    after:        Optional[Dict[str, Any]]   # new row values (INSERT / UPDATE / SNAPSHOT)
    schema:       List[ColumnSchema]
    timestamp:    datetime
    offset:       Any               # source-specific position (LSN, binlog coords, etc.)
    tx_id:        Optional[str] = None

    @property
    def primary_keys(self) -> List[str]:
        return [c.name for c in self.schema if c.primary_key]

    @property
    def row(self) -> Optional[Dict[str, Any]]:
        """The current row — after for INSERT/UPDATE/SNAPSHOT, before for DELETE."""
        return self.after if self.op != Operation.DELETE else self.before
