# src/civic_ledger/projections/base.py

from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime, timezone

class Projection(ABC):
    """Base class for all civic projections."""

    def __init__(self, conn):
        # A synchronous psycopg3 connection
        self.conn = conn
        # Tracks the last global_position processed
        self._last_processed: Optional[int] = None
        # Tracks the recorded_at timestamp of the last processed event
        self._last_event_time: Optional[datetime] = None

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name used as key in projection_checkpoints."""
        pass

    @abstractmethod
    def handle_event(self, event: dict) -> None:
        """
        Update projection state based on a single event.
        Must be implemented by concrete projections.
        """
        pass

    def get_lag(self) -> int:
        """
        Return milliseconds between now and the recorded_at of the last processed event.
        If no event has been processed, return -1.
        """
        if self._last_event_time is None:
            return -1
        delta = datetime.now(timezone.utc) - self._last_event_time
        return int(delta.total_seconds() * 1000)
