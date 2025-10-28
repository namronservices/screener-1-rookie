"""Abstract interfaces for sourcing market data."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Sequence

from ..models import PreMarketSnapshot


logger = logging.getLogger(__name__)


class DataProvider(ABC):
    """Interface for fetching market data required by the screener."""

    @abstractmethod
    def fetch_snapshot(self, symbol: str, as_of: datetime) -> PreMarketSnapshot:
        """Load pre-market data and convert it into a :class:`PreMarketSnapshot`."""

    @abstractmethod
    def warm_cache(self, symbols: Sequence[str], as_of: datetime) -> None:
        """Optional hook to pre-fetch data for improved latency."""


class InMemoryProvider(DataProvider):
    """Test double that serves deterministic data from dictionaries."""

    def __init__(
        self,
        snapshots: dict[str, PreMarketSnapshot],
    ) -> None:
        self._snapshots = snapshots

    def fetch_snapshot(self, symbol: str, as_of: datetime) -> PreMarketSnapshot:
        try:
            return self._snapshots[symbol]
        except KeyError as exc:
            logger.error("Snapshot missing from in-memory provider", extra={"symbol": symbol})
            raise KeyError(f"Missing snapshot for symbol {symbol}") from exc

    def warm_cache(self, symbols: Sequence[str], as_of: datetime) -> None:  # pragma: no cover - no-op
        return None
