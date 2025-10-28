"""Polygon.io backed :class:`DataProvider` implementation.

This module deliberately talks to the documented REST endpoints instead of the
auto-generated SDK so the behaviour mirrors the official HTTP reference:
https://polygon.io/docs/rest/stocks/overview
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from functools import cached_property
from typing import Iterable, Mapping, Sequence

import requests

from ..analyzers import HistoricalBar, build_snapshot
from ..config import DataAcquisition
from ..models import PreMarketSnapshot
from .base import DataProvider


logger = logging.getLogger(__name__)


class PolygonProvider(DataProvider):
    """Fetches pre-market market data from the Polygon.io REST API."""

    _API_BASE = "https://api.polygon.io"
    _DEFAULT_TIMEOUT = 10.0

    def __init__(self, config: DataAcquisition) -> None:
        self._config = config
        self._api_key = self._resolve_api_key()
        if not self._api_key:
            raise RuntimeError(
                "PolygonProvider requires an API key via provider_options['api_key'], "
                "provider_options['api_key_env'], or the POLYGON_API_KEY environment variable."
            )

        logger.info("Initialising Polygon provider")
        self._session = requests.Session()
        # Default API key parameter applied to all requests.
        self._session.params = {"apiKey": self._api_key}

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
    def _session_tz(self):  # pragma: no cover - timezone conversion depends on OS data
        try:
            from zoneinfo import ZoneInfo

            return ZoneInfo(self._config.timezone)
        except Exception:  # pragma: no cover - fallback used on older Python builds
            import pytz

            return pytz.timezone(self._config.timezone)

    def warm_cache(self, symbols: Sequence[str], as_of: datetime) -> None:  # pragma: no cover - REST API has no cache priming
        return None

    def discover_symbols(self, cap_size: str, limit: int) -> Sequence[str]:
        del cap_size, limit  # pragma: no cover - polygon discovery not implemented
        raise NotImplementedError(
            "Symbol discovery by market cap is not implemented for PolygonProvider"
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def fetch_snapshot(self, symbol: str, as_of: datetime) -> PreMarketSnapshot:
        logger.info(
            "Fetching Polygon snapshot",
            extra={"symbol": symbol, "as_of": as_of.isoformat()},
        )
        previous_close = self._fetch_previous_close(symbol, as_of)
        daily_volumes = self._fetch_thirty_day_volumes(symbol, as_of)

        intraday_bars = self._fetch_premarket_bars(symbol, as_of)
        if not intraday_bars:
            logger.error("No premarket bars returned", extra={"symbol": symbol})
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

    # ------------------------------------------------------------------
    # Polygon REST helpers
    # ------------------------------------------------------------------
    def _request(self, path: str, params: Mapping[str, object] | None = None) -> Mapping[str, object] | Iterable[object]:
        url = f"{self._API_BASE}{path if path.startswith('/') else '/' + path}"
        merged_params: dict[str, object] = dict(self._session.params)
        if params:
            merged_params.update({k: self._stringify_param(v) for k, v in params.items() if v is not None})

        logger.debug(
            "Calling Polygon API",
            extra={"url": url, "params": merged_params},
        )
        try:
            response = self._session.get(url, params=merged_params, timeout=self._DEFAULT_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network issues are environment specific
            # Try to collect as much useful context as possible for debugging.
            status = None
            text_snippet = None
            try:
                resp = getattr(exc, "response", None) or response
                if resp is not None:
                    status = getattr(resp, "status_code", None)
                    full_text = getattr(resp, "text", None)
                    if isinstance(full_text, str):
                        text_snippet = full_text[:1000].replace("\n", " ")
            except Exception:
                # best-effort only; don't obscure the original exception
                pass

            detail_parts = [
                f"path={path}",
                f"url={url}",
                f"params={merged_params}",
                f"status={status}" if status is not None else None,
                f"response_snippet={text_snippet}" if text_snippet else None,
                f"error={exc}",
            ]
            detail = ", ".join(p for p in detail_parts if p)
            logger.exception("Polygon request failed", extra={"detail": detail})
            raise RuntimeError(f"Polygon request failed: {detail}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            logger.exception(
                "Failed to decode Polygon response as JSON",
                extra={"url": url},
            )
            raise RuntimeError(f"Polygon response from {path} was not valid JSON") from exc

        logger.debug(
            "Polygon API response received: %s",
            payload,
            extra={
                "url": url,
                "status_code": response.status_code,
                "response_summary": self._summarize_response(payload),
            },
        )

        if isinstance(payload, Mapping):
            status = payload.get("status")
            if isinstance(status, str) and status.upper() == "ERROR":
                detail = payload.get("error") or payload.get("message") or "Unknown error"
                logger.error(
                    "Polygon API reported error",
                    extra={"url": url, "detail": detail},
                )
                raise RuntimeError(f"Polygon error for {path}: {detail}")
        return payload

    @staticmethod
    def _stringify_param(value: object) -> object:
        if isinstance(value, bool):
            return str(value).lower()
        return value

    @staticmethod
    def _coerce_results(payload: Mapping[str, object] | Iterable[object]) -> list[Mapping[str, object]]:
        if isinstance(payload, Mapping):
            results = payload.get("results")
            if results is None:
                return []
        else:
            results = payload

        if isinstance(results, Mapping):
            return [results]
        return [entry for entry in results if isinstance(entry, Mapping)]

    @staticmethod
    def _summarize_response(payload: Mapping[str, object] | Iterable[object]) -> Mapping[str, object]:
        if isinstance(payload, Mapping):
            keys = list(payload.keys())
            preview = keys[:10]
            summary: dict[str, object] = {
                "type": "mapping",
                "keys": preview,
            }
            if len(keys) > len(preview):
                summary["truncated_keys"] = len(keys) - len(preview)
            return summary
        if isinstance(payload, list):
            return {"type": "list", "length": len(payload)}
        if isinstance(payload, tuple):
            return {"type": "tuple", "length": len(payload)}
        return {"type": type(payload).__name__}

    @staticmethod
    def _safe_float(value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: object) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Previous close resolution
    # ------------------------------------------------------------------
    def _fetch_previous_close(self, symbol: str, as_of: datetime) -> float:
        as_of_date = self._as_date(as_of)

        payload = self._request(
            f"/v2/aggs/ticker/{symbol}/prev",
            {"adjusted": True, "timestamp": as_of_date.isoformat()},
        )
        for entry in self._coerce_results(payload):
            candidate = entry.get("close") or entry.get("c")
            close = self._safe_float(candidate)
            if close is not None:
                return close

        # Fallback by scanning recent trading sessions via daily aggregates.
        for entry in self._iter_recent_daily_aggs(symbol, as_of_date, days=15):
            session_date = self._extract_aggregate_date(entry)
            if session_date is None or session_date >= as_of_date:
                continue
            close = self._safe_float(entry.get("close") or entry.get("c"))
            if close is not None:
                return close

        logger.error("No previous close data returned", extra={"symbol": symbol})
        raise RuntimeError(f"No previous close data returned for {symbol}")

    def _iter_recent_daily_aggs(self, symbol: str, as_of: date, days: int) -> Iterable[Mapping[str, object]]:
        end_date = (as_of - timedelta(days=1)).isoformat()
        start_date = (as_of - timedelta(days=max(days, 1) + 30)).isoformat()
        payload = self._request(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}",
            {"adjusted": True, "sort": "desc", "limit": days + 30},
        )
        return self._coerce_results(payload)

    @staticmethod
    def _extract_aggregate_date(entry: Mapping[str, object]) -> date | None:
        timestamp = entry.get("timestamp") or entry.get("t")
        if timestamp is None:
            return None
        try:
            millis = int(timestamp)
        except (TypeError, ValueError):
            return None
        return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).date()

    # ------------------------------------------------------------------
    # Historical context
    # ------------------------------------------------------------------
    def _fetch_thirty_day_volumes(self, symbol: str, as_of: datetime) -> list[int]:
        as_of_date = self._as_date(as_of)
        start_date = (as_of_date - timedelta(days=120)).isoformat()
        payload = self._request(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{as_of_date.isoformat()}",
            {"adjusted": True, "sort": "asc", "limit": 180},
        )
        results = self._coerce_results(payload)
        if not results:
            logger.error("No historical daily data returned", extra={"symbol": symbol})
            raise RuntimeError(f"No historical daily data returned for {symbol}")

        volumes: list[int] = []
        for entry in results[-30:]:
            volume = self._safe_int(entry.get("volume") or entry.get("v"))
            volumes.append(volume or 0)
        return volumes

    # ------------------------------------------------------------------
    # Intraday bars
    # ------------------------------------------------------------------
    def _fetch_premarket_bars(self, symbol: str, as_of: datetime) -> list[HistoricalBar]:
        localized_as_of = self._ensure_timezone(as_of)
        session_local = localized_as_of.astimezone(self._session_tz)
        session_date = session_local.date()
        premarket_start = self._combine_with_timezone(session_date, self._config.premarket_window_start)
        premarket_end = self._combine_with_timezone(session_date, self._config.premarket_window_end)

        effective_premarket_end = premarket_end
        if premarket_start <= session_local <= premarket_end:
            effective_premarket_end = session_local

        start_utc = premarket_start.astimezone(timezone.utc)
        end_utc = effective_premarket_end.astimezone(timezone.utc)

        payload = self._request(
            f"/v2/aggs/ticker/{symbol}/range/1/minute/{self._format_epoch_millis(start_utc)}/{self._format_epoch_millis(end_utc)}",
            {"adjusted": True, "sort": "asc", "limit": 5000},
        )

        bars: list[HistoricalBar] = []
        for entry in self._coerce_results(payload):
            timestamp = entry.get("timestamp") or entry.get("t")
            if timestamp is None:
                continue
            try:
                millis = int(timestamp)
            except (TypeError, ValueError):
                continue
            utc_time = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
            local_time = utc_time.astimezone(self._session_tz)
            if local_time < premarket_start or local_time > effective_premarket_end:
                continue

            bars.append(
                HistoricalBar(
                    timestamp=local_time,
                    open=self._safe_float(entry.get("open") or entry.get("o")) or 0.0,
                    high=self._safe_float(entry.get("high") or entry.get("h")) or 0.0,
                    low=self._safe_float(entry.get("low") or entry.get("l")) or 0.0,
                    close=self._safe_float(entry.get("close") or entry.get("c")) or 0.0,
                    volume=self._safe_int(entry.get("volume") or entry.get("v")) or 0,
                )
            )
        return bars

    def _combine_with_timezone(self, session_date: date, session_time) -> datetime:
        naive = datetime.combine(session_date, session_time)
        tz = self._session_tz
        localize = getattr(tz, "localize", None)
        if callable(localize):  # pragma: no cover - exercised only when pytz is installed
            return localize(naive)
        return naive.replace(tzinfo=tz)

    # ------------------------------------------------------------------
    # Reference data
    # ------------------------------------------------------------------
    def _fetch_float_shares(self, symbol: str) -> int:
        payload = self._request(f"/v3/reference/tickers/{symbol}")
        if not isinstance(payload, Mapping):
            return 0
        result = payload.get("results")
        if not isinstance(result, Mapping):
            return 0

        for key in (
            "share_class_shares_outstanding",
            "weighted_shares_outstanding",
            "shares_outstanding",
        ):
            value = self._safe_int(result.get(key))
            if value:
                return value
        logger.warning("Float shares not reported", extra={"symbol": symbol})
        return 0

    # ------------------------------------------------------------------
    # Time handling helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _ensure_timezone(moment: datetime) -> datetime:
        if moment.tzinfo is None:
            return moment.replace(tzinfo=timezone.utc)
        return moment

    @staticmethod
    def _as_date(moment: datetime) -> date:
        return PolygonProvider._ensure_timezone(moment).astimezone(timezone.utc).date()

    @staticmethod
    def _format_epoch_millis(moment: datetime) -> str:
        moment = PolygonProvider._ensure_timezone(moment).astimezone(timezone.utc)
        epoch_seconds = moment.timestamp()
        millis = int(round(epoch_seconds * 1000))
        return str(millis)

