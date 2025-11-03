"""Implementation of :class:`DataProvider` using the `yfinance` package."""
from __future__ import annotations

import logging
from datetime import datetime
from functools import cached_property
from typing import Sequence

from ..analyzers import HistoricalBar, build_snapshot
from ..config import DataAcquisition
from ..models import PreMarketSnapshot
from .base import DataProvider

logger = logging.getLogger(__name__)

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
        logger.info(
            "Fetching yfinance snapshot",
            extra={"symbol": symbol, "as_of": as_of.isoformat()},
        )
        ticker = yf.Ticker(symbol)
        logger.debug(
            "Requesting yfinance historical data",
            extra={"symbol": symbol, "period": "60d", "interval": "1d"},
        )
        hist = ticker.history(period="60d", interval="1d")
        hist_columns = list(getattr(hist, "columns", []))
        logger.debug(
            "Received yfinance historical data",
            extra={
                "symbol": symbol,
                "rows": int(getattr(hist, "shape", (0, 0))[0]),
                "columns": hist_columns[:10],
                "columns_truncated": max(len(hist_columns) - 10, 0),
            },
        )
        if hist.empty:
            logger.error("No historical data returned", extra={"symbol": symbol})
            raise RuntimeError(f"No historical data returned for {symbol}")
        previous_close = float(hist["Close"].iloc[-1])
        average_volume_samples = hist["Volume"].tail(30).astype(int).tolist()
        daily_closes = hist["Close"].astype(float).tolist()

        logger.debug(
            "Requesting yfinance intraday data",
            extra={"symbol": symbol, "period": "5d", "interval": "5m"},
        )
        intraday = ticker.history(period="5d", interval="5m")
        intraday_columns = list(getattr(intraday, "columns", []))
        logger.debug(
            "Received yfinance intraday data",
            extra={
                "symbol": symbol,
                "rows": int(getattr(intraday, "shape", (0, 0))[0]),
                "columns": intraday_columns[:10],
                "columns_truncated": max(len(intraday_columns) - 10, 0),
            },
        )
        intraday = intraday.tz_convert(self._config.timezone)
        if as_of.tzinfo is None:
            session_local = self._session_tz.localize(as_of)
        else:
            session_local = as_of.astimezone(self._session_tz)
        premarket_start = self._session_tz.localize(
            datetime.combine(session_local.date(), self._config.premarket_window_start)
        )
        premarket_end = self._session_tz.localize(
            datetime.combine(session_local.date(), self._config.premarket_window_end)
        )
        effective_premarket_end = premarket_end
        if premarket_start <= session_local <= premarket_end:
            effective_premarket_end = session_local
        premarket_data = intraday.loc[premarket_start:effective_premarket_end]
        if premarket_data.empty:
            logger.error("No premarket data returned", extra={"symbol": symbol})
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
        logger.debug("Requesting yfinance ticker info", extra={"symbol": symbol})
        info = ticker.get_info()
        info_keys = list(info.keys()) if isinstance(info, dict) else None
        logger.debug(
            "Received yfinance ticker info",
            extra={
                "symbol": symbol,
                "keys": info_keys[:10] if info_keys is not None else None,
                "keys_truncated": max(len(info_keys) - 10, 0) if info_keys is not None and len(info_keys) > 10 else 0,
            },
        )
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
            daily_closes=daily_closes,
        )
