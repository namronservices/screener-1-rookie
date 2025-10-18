"""Implementation of :class:`DataProvider` using the `yfinance` package."""
from __future__ import annotations

from datetime import datetime
from functools import cached_property
from typing import Sequence

from ..analyzers import HistoricalBar, build_snapshot
from ..config import DataAcquisition
from ..models import PreMarketSnapshot
from .base import DataProvider

try:  # pragma: no cover - optional dependency
    import yfinance as yf
except Exception as exc:  # pragma: no cover - best effort import guard
    yf = None
    _IMPORT_ERROR = exc
else:  # pragma: no cover - no easy deterministic coverage
    _IMPORT_ERROR = None


class YFinanceProvider(DataProvider):
    """Loads pre-market data via Yahoo Finance."""

    def __init__(self, config: DataAcquisition) -> None:
        if yf is None:  # pragma: no cover - executed only without dependency
            raise RuntimeError(
                "yfinance is required for YFinanceProvider but could not be imported"
            ) from _IMPORT_ERROR
        self._config = config

    @cached_property
    def _session_tz(self):  # pragma: no cover - timezone conversion not deterministic
        import pytz

        return pytz.timezone(self._config.timezone)

    def warm_cache(self, symbols: Sequence[str], as_of: datetime) -> None:  # pragma: no cover - yfinance caches automatically
        return None

    def fetch_snapshot(self, symbol: str, as_of: datetime) -> PreMarketSnapshot:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="60d", interval="1d")
        if hist.empty:
            raise RuntimeError(f"No historical data returned for {symbol}")
        previous_close = float(hist["Close"].iloc[-1])
        average_volume_samples = hist["Volume"].tail(30).astype(int).tolist()

        intraday = ticker.history(period="5d", interval="5m")
        intraday = intraday.tz_convert(self._config.timezone)
        premarket_start = self._session_tz.localize(
            datetime.combine(as_of.date(), self._config.premarket_window_start)
        )
        premarket_end = self._session_tz.localize(
            datetime.combine(as_of.date(), self._config.premarket_window_end)
        )
        premarket_data = intraday.loc[premarket_start:premarket_end]
        if premarket_data.empty:
            raise RuntimeError(f"No premarket data returned for {symbol}")
        last_row = premarket_data.iloc[-1]
        last_price = float(last_row["Close"])
        premarket_volume = int(premarket_data["Volume"].sum())

        bars = [
            HistoricalBar(
                timestamp=index.to_pydatetime(),
                open=float(row["Open"]),
                high=float(row["High"]),
                low=float(row["Low"]),
                close=float(row["Close"]),
                volume=int(row["Volume"]),
            )
            for index, row in premarket_data.iterrows()
        ]

        # yfinance currently doesn't expose float shares reliably, so we fall back to
        # pulling it from the ticker info dictionary. This is cached by yfinance.
        info = ticker.get_info()
        float_shares = int(info.get("floatShares") or info.get("sharesOutstanding") or 0)

        return build_snapshot(
            symbol=symbol,
            as_of=as_of,
            last_price=last_price,
            previous_close=previous_close,
            premarket_volume=premarket_volume,
            thirty_day_volume_samples=average_volume_samples,
            float_shares=float_shares,
            intraday_bars=bars,
        )
