"""Analytics helpers for transforming raw market data into screenable signals."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import mean
from typing import Iterable, Sequence

from .models import PreMarketSnapshot


@dataclass(frozen=True)
class HistoricalBar:
    """Simplified representation of a historical OHLCV bar."""

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


def compute_vwap(bars: Sequence[HistoricalBar]) -> float | None:
    """Compute the volume weighted average price for the supplied bars."""

    cumulative_pv = 0.0
    cumulative_volume = 0
    for bar in bars:
        typical_price = (bar.high + bar.low + bar.close) / 3
        cumulative_pv += typical_price * bar.volume
        cumulative_volume += bar.volume
    if cumulative_volume == 0:
        return None
    return cumulative_pv / cumulative_volume


def build_snapshot(
    symbol: str,
    as_of: datetime,
    last_price: float,
    previous_close: float,
    premarket_volume: int,
    thirty_day_volume_samples: Iterable[int],
    float_shares: int,
    intraday_bars: Sequence[HistoricalBar] | None,
    daily_closes: Sequence[float] | None = None,
) -> PreMarketSnapshot:
    """Factory that assembles a :class:`PreMarketSnapshot` with derived metrics."""

    average_volume = int(mean(thirty_day_volume_samples)) if thirty_day_volume_samples else 0
    vwap = compute_vwap(intraday_bars) if intraday_bars else None
    closes = list(daily_closes) if daily_closes else []
    sma_20 = mean(closes[-20:]) if len(closes) >= 20 else None

    return PreMarketSnapshot(
        symbol=symbol,
        timestamp=as_of,
        last_price=last_price,
        previous_close=previous_close,
        premarket_volume=premarket_volume,
        average_30_day_volume=average_volume,
        float_shares=float_shares,
        vwap=vwap,
        sma_20=sma_20,
    )
