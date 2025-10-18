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
    import requests
except Exception as exc:  # pragma: no cover - optional dependency guard
    requests = None
    _IMPORT_ERROR = exc
else:  # pragma: no cover
    _IMPORT_ERROR = None


class PolygonProvider(DataProvider):
    """Fetches pre-market market data from the Polygon.io REST API."""

    _BASE_URL = "https://api.polygon.io"

    def __init__(self, config: DataAcquisition) -> None:
        if requests is None:  # pragma: no cover - executed when dependency missing
            raise RuntimeError(
                "requests is required for PolygonProvider but could not be imported"
            ) from _IMPORT_ERROR

        self._config = config
        self._session = requests.Session()
        self._api_key = self._resolve_api_key()
        if not self._api_key:
            raise RuntimeError(
                "PolygonProvider requires an API key via provider_options['api_key'], "
                "provider_options['api_key_env'], or the POLYGON_API_KEY environment variable."
            )

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
        payload = self._request(f"/v2/aggs/ticker/{symbol}/prev", params={"adjusted": "true"})
        results = payload.get("results") or []
        if not results:
            raise RuntimeError(f"No previous close data returned for {symbol}")
        return float(results[0]["c"])

    def _fetch_daily_aggregates(self, symbol: str, as_of: datetime) -> list[int]:
        start_date = (as_of - timedelta(days=60)).date().isoformat()
        end_date = as_of.date().isoformat()
        payload = self._request(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}",
            params={"adjusted": "true", "sort": "asc", "limit": 200},
        )
        results = payload.get("results") or []
        if not results:
            raise RuntimeError(f"No historical daily data returned for {symbol}")
        volumes = [int(entry.get("v", 0)) for entry in results[-30:]]
        return volumes

    def _fetch_premarket_bars(self, symbol: str, as_of: datetime) -> list[HistoricalBar]:
        premarket_start = self._session_tz.localize(
            datetime.combine(as_of.date(), self._config.premarket_window_start)
        )
        premarket_end = self._session_tz.localize(
            datetime.combine(as_of.date(), self._config.premarket_window_end)
        )

        start_utc = premarket_start.astimezone(timezone.utc).isoformat()
        end_utc = premarket_end.astimezone(timezone.utc).isoformat()

        payload = self._request(
            f"/v2/aggs/ticker/{symbol}/range/5/minute/{start_utc}/{end_utc}",
            params={"adjusted": "true", "sort": "asc", "limit": 5000},
        )
        results = payload.get("results") or []
        bars: list[HistoricalBar] = []
        for entry in results:
            timestamp = datetime.fromtimestamp(entry["t"] / 1000, tz=timezone.utc).astimezone(
                self._session_tz
            )
            if timestamp < premarket_start or timestamp > premarket_end:
                continue
            bars.append(
                HistoricalBar(
                    timestamp=timestamp,
                    open=float(entry["o"]),
                    high=float(entry["h"]),
                    low=float(entry["l"]),
                    close=float(entry["c"]),
                    volume=int(entry["v"]),
                )
            )
        return bars

    def _fetch_float_shares(self, symbol: str) -> int:
        payload = self._request(f"/v3/reference/tickers/{symbol}")
        result = payload.get("results") or {}
        candidates = [
            result.get("share_class_shares_outstanding"),
            result.get("weighted_shares_outstanding"),
            result.get("shares_outstanding"),
        ]
        for candidate in candidates:
            if candidate:
                return int(candidate)
        return 0

    def _request(self, path: str, params: dict[str, object] | None = None) -> dict:
        parameters = {"apiKey": self._api_key}
        if params:
            parameters.update(params)
        response = self._session.get(
            f"{self._BASE_URL}{path}", params=parameters, timeout=30
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Polygon request to {path} failed with status {response.status_code}: {response.text}"
            )
        payload = response.json()
        status = payload.get("status", "success").lower()
        if status not in {"ok", "success"}:
            message = payload.get("error") or payload.get("message")
            raise RuntimeError(f"Polygon request to {path} failed: {message or payload}")
        return payload
