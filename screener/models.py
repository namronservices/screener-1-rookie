"""Domain models for the stock screener."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional


@dataclass(slots=True)
class PreMarketSnapshot:
    """Represents pre-market activity for a single symbol."""

    symbol: str
    timestamp: datetime
    last_price: float
    previous_close: float
    premarket_volume: int
    average_30_day_volume: int
    float_shares: int
    vwap: Optional[float]
    sma_20: Optional[float] = None
    sma_20_category: Optional[str] = None

    @property
    def gap_percent(self) -> float:
        """Percentage gap relative to the previous regular session close."""

        if self.previous_close == 0:
            return 0.0
        return (self.last_price - self.previous_close) / self.previous_close * 100

    @property
    def relative_volume(self) -> float:
        """Volume multiple compared to the 30-day average regular session volume."""

        if self.average_30_day_volume == 0:
            return 0.0
        return self.premarket_volume / self.average_30_day_volume

    @property
    def is_above_vwap(self) -> bool:
        if self.vwap is None:
            return True
        return self.last_price >= self.vwap

    @property
    def sma_20_percent_diff(self) -> float | None:
        """Percentage distance between the last price and the 20-day SMA."""

        if self.sma_20 is None or self.sma_20 == 0:
            return None
        return (self.last_price - self.sma_20) / self.sma_20 * 100


@dataclass(slots=True)
class ScreenerResult:
    """Outcome for a single symbol after applying all filters."""

    symbol: str
    snapshot: PreMarketSnapshot | None
    passed_filters: Mapping[str, bool]
    error: Exception | None = None

    def is_actionable(self) -> bool:
        """Whether the ticker satisfies every configured filter."""

        return self.error is None and bool(self.passed_filters) and all(
            self.passed_filters.values()
        )
