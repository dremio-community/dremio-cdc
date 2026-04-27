"""
Abstract base class for all CDC source connectors.
Every source implements this interface; the engine calls it uniformly.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema


class CDCSource(ABC):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        self.name = name
        self.cfg  = cfg

    @abstractmethod
    def connect(self):
        """Open connection to the source database."""

    @abstractmethod
    def get_schema(self, table: str) -> List[ColumnSchema]:
        """Return column definitions for a table."""

    @abstractmethod
    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        """
        Full-table scan yielding SNAPSHOT events.
        Called on first run (no saved offset) when snapshot_on_first_run=True.
        """

    @abstractmethod
    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        """
        Stream change events from offset onward.
        Must yield ChangeEvent objects indefinitely (blocking reads are fine).
        The engine commits offsets after each successful batch write to Dremio.
        """

    @abstractmethod
    def close(self):
        """Clean up connections."""

    # ── Optional: incremental snapshot support ────────────────────────────────

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        """
        Yield one chunk of snapshot rows where cursor_col > start_after,
        ordered by cursor_col, limited to chunk_size rows.

        Override in subclasses that support cursor-based pagination.
        Default raises NotImplementedError so the engine falls back to full snapshot.
        """
        raise NotImplementedError

    def on_batch_committed(self, table: str, offset: Any) -> None:
        """
        Called by the engine after a batch has been successfully written to the sink
        and the offset committed. Override in sources that need post-flush acking
        (e.g. Pub/Sub, Kinesis). Default is a no-op.
        """

    def get_pk_column(self, table: str) -> Optional[str]:
        """Return the best cursor column for incremental snapshot (first PK column)."""
        try:
            schema = self.get_schema(table)
            for col in schema:
                if col.primary_key:
                    return col.name
            return schema[0].name if schema else None
        except Exception:
            return None

    @property
    def tables(self) -> List[str]:
        return self.cfg.get("tables", [])
