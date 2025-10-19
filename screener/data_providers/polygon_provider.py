"""Polygon.io backed :class:`DataProvider` implementation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Sequence

from ..analyzers import HistoricalBar, build_snapshot
from ..config import DataAcquisition
from ..models import PreMarketSnapshot
from .base import DataProvider

try:  # pragma: no cover - optional dependency
    from polygon import RESTClient
except Exception as exc:  # pragma: no cover - optional dependency guard
    RESTClient = None
    _IMPORT_ERROR = exc
else:  # pragma: no cover
    _IMPORT_ERROR = None


class PolygonProvider(DataProvider):
    """Fetches pre-market market data from the Polygon.io REST API."""

    def __init__(self, config: DataAcquisition) -> None:
        if RESTClient is None:  # pragma: no cover - executed when dependency missing
            raise RuntimeError(
                "polygon-api-client is required for PolygonProvider but could not be imported"
            ) from _IMPORT_ERROR

        self._config = config
        self._api_key = self._resolve_api_key()
        if not self._api_key:
            raise RuntimeError(
                "PolygonProvider requires an API key via provider_options['api_key'], "
                "provider_options['api_key_env'], or the POLYGON_API_KEY environment variable."
            )
        self._client = RESTClient(api_key=self._api_key)

    def _resolve_api_key(self) -> str | None:
        options = dict(self._config.provider_options or {})
        direct_key = options.get("api_key")
        if direct_key:
            return str(direct_key)

        import os

        env_var = options.get("api_key_env")
        if env_var:
            candidate = os.getenv(str(env_var))
            if candidate:
                return candidate

        return os.getenv("POLYGON_API_KEY")

    @cached_property
    def _session_tz(self):  # pragma: no cover - timezone conversion not deterministic
        import pytz

        return pytz.timezone(self._config.timezone)

    def warm_cache(self, symbols: Sequence[str], as_of: datetime) -> None:  # pragma: no cover - API handles caching
        return None

    def fetch_snapshot(self, symbol: str, as_of: datetime) -> PreMarketSnapshot:
        previous_close = self._fetch_previous_close(symbol)
        daily_volumes = self._fetch_daily_aggregates(symbol, as_of)

        intraday_bars = self._fetch_premarket_bars(symbol, as_of)
        if not intraday_bars:
            raise RuntimeError(f"No premarket data returned for {symbol}")
        premarket_volume = sum(bar.volume for bar in intraday_bars)
        last_price = intraday_bars[-1].close

        float_shares = self._fetch_float_shares(symbol)

        return build_snapshot(
            symbol=symbol,
            as_of=as_of,
            last_price=last_price,
            previous_close=previous_close,
            premarket_volume=premarket_volume,
            thirty_day_volume_samples=daily_volumes,
            float_shares=float_shares,
            intraday_bars=intraday_bars,
        )

    def _fetch_previous_close(self, symbol: str) -> float:
        response = self._call_previous_close(symbol)
        results = self._extract_results(response)
        if not results:
            raise RuntimeError(f"No previous close data returned for {symbol}")
        close = self._extract_field(results[0], "close", "c")
        if close is None:
            raise RuntimeError(f"Previous close payload missing close price for {symbol}")
        return float(close)

    def _call_previous_close(self, symbol: str):
        client = self._client

        if hasattr(client, "get_previous_close"):
            return client.get_previous_close(symbol, adjusted=True)

        stocks_client = getattr(client, "stocks", None)
        if stocks_client is not None and hasattr(stocks_client, "get_previous_close"):
            return stocks_client.get_previous_close(symbol, adjusted=True)

        for candidate_name in ("get_previous_close_v2", "get_previous_close_agg"):
            candidate = getattr(client, candidate_name, None)
            if callable(candidate):
                return candidate(symbol, adjusted=True)

        raise RuntimeError(
            "Polygon REST client does not expose a previous close endpoint compatible with this provider"
        )

    def _fetch_daily_aggregates(self, symbol: str, as_of: datetime) -> list[int]:
        start_date = (as_of - timedelta(days=60)).date().isoformat()
        end_date = as_of.date().isoformat()
        response = self._client.get_aggs(
            symbol,
            1,
            "day",
            start_date,
            end_date,
            adjusted=True,
            sort="asc",
            limit=200,
        )
        results = self._extract_results(response)
        if not results:
            raise RuntimeError(f"No historical daily data returned for {symbol}")
        volumes = []
        for entry in results[-30:]:
            volume = self._extract_field(entry, "volume", "v")
            volumes.append(int(volume or 0))
        return volumes

    def _fetch_premarket_bars(self, symbol: str, as_of: datetime) -> list[HistoricalBar]:
        premarket_start = self._session_tz.localize(
            datetime.combine(as_of.date(), self._config.premarket_window_start)
        )
        premarket_end = self._session_tz.localize(
            datetime.combine(as_of.date(), self._config.premarket_window_end)
        )

        premarket_start_utc = premarket_start.astimezone(timezone.utc)
        premarket_end_utc = premarket_end.astimezone(timezone.utc)
        start_utc = int(premarket_start_utc.timestamp() * 1000)
        end_utc = int(premarket_end_utc.timestamp() * 1000)

        response = self._client.get_aggs(
            symbol,
            5,
            "minute",
            premarket_start_utc.isoformat(),
            premarket_end_utc.isoformat(),
            adjusted=True,
            sort="asc",
            limit=5000,
        )
        results = self._extract_results(response)
        bars: list[HistoricalBar] = []
        for entry in results:
            timestamp_raw = self._extract_field(entry, "timestamp", "t")
            if timestamp_raw is None:
                continue
            timestamp = datetime.fromtimestamp(int(timestamp_raw) / 1000, tz=timezone.utc).astimezone(
                self._session_tz
            )
            if timestamp < premarket_start or timestamp > premarket_end:
                continue
            bars.append(
                HistoricalBar(
                    timestamp=timestamp,
                    open=float(self._extract_field(entry, "open", "o") or 0.0),
                    high=float(self._extract_field(entry, "high", "h") or 0.0),
                    low=float(self._extract_field(entry, "low", "l") or 0.0),
                    close=float(self._extract_field(entry, "close", "c") or 0.0),
                    volume=int(self._extract_field(entry, "volume", "v") or 0),
                )
            )
        return bars

    def _fetch_float_shares(self, symbol: str) -> int:
        response = self._client.get_ticker_details(symbol)
        result = self._ensure_mapping(response)
        candidates = [
            result.get("share_class_shares_outstanding"),
            result.get("weighted_shares_outstanding"),
            result.get("shares_outstanding"),
        ]
        for candidate in candidates:
            if candidate:
                return int(candidate)
        return 0

    def _extract_results(self, response) -> list:
        if response is None:
            return []
        results = getattr(response, "results", None)
        if results is None and isinstance(response, dict):
            results = response.get("results")
        if results is None:
            return []
        if isinstance(results, list):
            return results
        return list(results)

    def _ensure_mapping(self, entry) -> dict:
        if isinstance(entry, dict):
            return entry
        for method_name in ("to_dict", "dict", "model_dump"):
            method = getattr(entry, method_name, None)
            if callable(method):
                try:
                    candidate = method()
                except TypeError:
                    continue
                if isinstance(candidate, dict):
                    return candidate
        return {}

    def _extract_field(self, entry, *names):
        mapping = self._ensure_mapping(entry)
        for name in names:
            if hasattr(entry, name):
                value = getattr(entry, name)
                if value is not None and not callable(value):
                    return value
            if name in mapping and mapping[name] is not None:
                value = mapping[name]
                if not callable(value):
                    return value
        return None
