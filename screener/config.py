"""Configuration models for the daily stock screener.

These dataclasses capture both operational settings for running the screener and
thresholds that determine what qualifies as a high-quality ticker for
day-trading. The configuration is intentionally explicit so it can be serialized
from JSON/YAML or environment variables and later extended without breaking
changes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from .tickers import DEFAULT_TICKER_CSV, iter_catalog, select_symbols


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SymbolUniverse:
    """Definition of what tickers should be evaluated.

    Attributes
    ----------
    symbols:
        Primary list of tickers to screen. These are typically the small and
        mid-cap equities of interest to the desk.
    include_etfs:
        Some desks like to include ETFs in the scan as a liquidity proxy. The
        flag is provided for easy extension even though the default scan focuses
        on equities only.
    """

    symbols: Sequence[str]
    include_etfs: bool = False
    market_cap_from: int | None = None
    market_cap_to: int | None = None
    max_results: int | None = None
    catalog_path: Path | None = None

    def __post_init__(self) -> None:
        cleaned = tuple(dict.fromkeys(sym.strip().upper() for sym in self.symbols))
        if len(cleaned) == 0:
            raise ValueError("At least one symbol must be provided")
        if any(not sym for sym in cleaned):
            raise ValueError("Empty symbol detected after stripping whitespace")
        object.__setattr__(self, "symbols", cleaned)
        if self.market_cap_from is not None and self.market_cap_from < 0:
            raise ValueError("market_cap_from must be non-negative")
        if self.market_cap_to is not None and self.market_cap_to < 0:
            raise ValueError("market_cap_to must be non-negative")
        if (
            self.market_cap_from is not None
            and self.market_cap_to is not None
            and self.market_cap_from > self.market_cap_to
        ):
            raise ValueError("market_cap_from cannot exceed market_cap_to")
        if self.max_results is not None and self.max_results <= 0:
            raise ValueError("max_results must be positive when provided")
        catalog_path = self.catalog_path
        if catalog_path is not None and not isinstance(catalog_path, Path):
            object.__setattr__(self, "catalog_path", Path(catalog_path))


@dataclass(frozen=True)
class VolumeThresholds:
    """Relative and absolute liquidity requirements."""

    relative_to_30day_avg: float = 1.5
    absolute_pre_market_shares: int = 100_000

    def validate(self) -> None:
        if self.relative_to_30day_avg <= 0:
            raise ValueError("Volume threshold must be positive")
        if self.absolute_pre_market_shares <= 0:
            raise ValueError("Absolute volume threshold must be positive")


@dataclass(frozen=True)
class GapThresholds:
    """Criteria describing price displacement in the pre-market session."""

    minimum_gap_percent: float = 3.0
    require_above_vwap: bool = True

    def validate(self) -> None:
        if self.minimum_gap_percent < 0:
            raise ValueError("Gap percent must be non-negative")


@dataclass(frozen=True)
class TrendCriteria:
    """Thresholds for classifying price action relative to the 20-day SMA."""

    moderate_threshold_percent: float = 1.0
    strong_threshold_percent: float = 3.0
    preferred_categories: Sequence[str] = ("bullish", "bearish")

    _ALLOWED_CATEGORIES = {
        "bullish",
        "moderate bullish",
        "sideways",
        "moderate bearish",
        "bearish",
    }

    def validate(self) -> None:
        if not tuple(self.preferred_categories):
            raise ValueError("At least one preferred trend category must be specified")
        if self.moderate_threshold_percent < 0:
            raise ValueError("Trend moderate threshold must be non-negative")
        if self.strong_threshold_percent < 0:
            raise ValueError("Trend strong threshold must be non-negative")
        if self.strong_threshold_percent < self.moderate_threshold_percent:
            raise ValueError("Trend strong threshold cannot be below the moderate threshold")
        invalid = [
            category
            for category in self.preferred_categories
            if category.lower() not in self._ALLOWED_CATEGORIES
        ]
        if invalid:
            raise ValueError(
                "Unknown trend categories specified: " + ", ".join(sorted(set(invalid)))
            )


@dataclass(frozen=True)
class ScreenerCriteria:
    """Grouping of all filter thresholds applied by the screener."""

    volume: VolumeThresholds = field(default_factory=VolumeThresholds)
    gap: GapThresholds = field(default_factory=GapThresholds)
    minimum_float_shares: int = 10_000_000
    trend: TrendCriteria = field(default_factory=TrendCriteria)

    def validate(self) -> None:
        self.volume.validate()
        self.gap.validate()
        if self.minimum_float_shares <= 0:
            raise ValueError("Float threshold must be positive")
        self.trend.validate()


@dataclass(frozen=True)
class DataAcquisition:
    """Instructions for sourcing pre-market and historical data."""

    provider: str = "yfinance"
    cache_path: Path | None = None
    premarket_window_start: time = time(hour=4, minute=0)
    premarket_window_end: time = time(hour=9, minute=29)
    timezone: str = "US/Eastern"
    provider_options: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ScreenerConfig:
    """Primary configuration consumed by the application."""

    universe: SymbolUniverse
    criteria: ScreenerCriteria = field(default_factory=ScreenerCriteria)
    data: DataAcquisition = field(default_factory=DataAcquisition)
    max_concurrent_requests: int = 8

    def validate(self) -> None:
        self.universe
        self.criteria.validate()
        if self.max_concurrent_requests <= 0:
            raise ValueError("Concurrency must be positive")

    @classmethod
    def from_symbols(cls, symbols: Iterable[str], **overrides: object) -> "ScreenerConfig":
        """Helper for quick instantiation with minimal boilerplate."""

        universe = SymbolUniverse(tuple(symbols))
        return cls(universe=universe, **overrides)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> "ScreenerConfig":
        """Deserialize configuration from a nested mapping."""

        def ensure_mapping(value: object, label: str) -> Mapping[str, object]:
            if value is None:
                return {}
            if not isinstance(value, Mapping):
                raise TypeError(f"Expected '{label}' to be a mapping, got {type(value)!r}")
            return value

        def parse_time(value: object, fallback: time) -> time:
            if value is None:
                return fallback
            if isinstance(value, time):
                return value
            if isinstance(value, str):
                try:
                    return datetime.strptime(value, "%H:%M").time()
                except ValueError as exc:
                    raise ValueError(
                        f"Invalid time format '{value}'. Expected HH:MM"
                    ) from exc
            raise TypeError(
                f"Time values must be provided as HH:MM strings, got {type(value)!r}"
            )

        def parse_optional_int(value: object, label: str) -> int | None:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                candidate = int(value)
            else:
                text = str(value).strip()
                if not text:
                    return None
                try:
                    candidate = int(float(text))
                except ValueError as exc:
                    raise ValueError(f"{label} must be numeric, got {value!r}") from exc
            if candidate < 0:
                raise ValueError(f"{label} must be non-negative")
            return candidate

        universe_map = ensure_mapping(data.get("universe"), "universe")
        criteria_map = ensure_mapping(data.get("criteria"), "criteria")
        data_map = ensure_mapping(data.get("data"), "data")

        market_cap_from = parse_optional_int(
            universe_map.get("market_cap_from"), "universe.market_cap_from"
        )
        market_cap_to = parse_optional_int(
            universe_map.get("market_cap_to"), "universe.market_cap_to"
        )
        max_results = parse_optional_int(
            universe_map.get("max_results"), "universe.max_results"
        )
        if max_results is not None and max_results == 0:
            raise ValueError("universe.max_results must be positive when provided")

        tickers_source = universe_map.get("tickers_file")
        if tickers_source is None or tickers_source == "":
            catalog_path = DEFAULT_TICKER_CSV
        else:
            catalog_path = Path(str(tickers_source)).expanduser()

        raw_symbols = universe_map.get("symbols")
        if raw_symbols is not None:
            if isinstance(raw_symbols, str):
                raw_symbols = [raw_symbols]
            if not isinstance(raw_symbols, Iterable):
                raise TypeError("universe.symbols must be an iterable of strings")
            symbols = tuple(str(sym) for sym in raw_symbols)
        else:
            if market_cap_from is None or market_cap_to is None:
                raise ValueError(
                    "universe.market_cap_from and universe.market_cap_to must be provided when universe.symbols is omitted"
                )
            symbols = tuple(
                select_symbols(
                    path=catalog_path,
                    market_cap_from=market_cap_from,
                    market_cap_to=market_cap_to,
                )
            )

            max_catalog_check = max(50, max_results or 0)
            if max_catalog_check > 0:
                catalog_symbols: list[str] = []
                more_symbols_available = False
                for record in iter_catalog(catalog_path):
                    catalog_symbols.append(record.symbol)
                    if len(catalog_symbols) > max_catalog_check:
                        more_symbols_available = True
                        break

                if not more_symbols_available and len(symbols) < len(catalog_symbols):
                    logger.debug(
                        "Insufficient tickers after market-cap filtering; falling back to raw catalog order",
                        extra={
                            "catalog_path": str(catalog_path),
                            "filtered_count": len(symbols),
                            "catalog_count": len(catalog_symbols),
                        },
                    )
                    symbols = tuple(catalog_symbols)

        universe = SymbolUniverse(
            symbols=symbols,
            include_etfs=bool(universe_map.get("include_etfs", False)),
            market_cap_from=market_cap_from,
            market_cap_to=market_cap_to,
            max_results=max_results,
            catalog_path=catalog_path,
        )

        volume_defaults = VolumeThresholds()
        volume_map = ensure_mapping(criteria_map.get("volume"), "criteria.volume")
        volume = VolumeThresholds(
            relative_to_30day_avg=float(
                volume_map.get("relative_to_30day_avg", volume_defaults.relative_to_30day_avg)
            ),
            absolute_pre_market_shares=int(
                volume_map.get("absolute_pre_market_shares", volume_defaults.absolute_pre_market_shares)
            ),
        )

        gap_defaults = GapThresholds()
        gap_map = ensure_mapping(criteria_map.get("gap"), "criteria.gap")
        gap = GapThresholds(
            minimum_gap_percent=float(
                gap_map.get("minimum_gap_percent", gap_defaults.minimum_gap_percent)
            ),
            require_above_vwap=bool(
                gap_map.get("require_above_vwap", gap_defaults.require_above_vwap)
            ),
        )

        trend_defaults = TrendCriteria()
        trend_map = ensure_mapping(criteria_map.get("trend"), "criteria.trend")
        preferred_categories_value = trend_map.get(
            "preferred_categories", trend_defaults.preferred_categories
        )
        if preferred_categories_value is None:
            preferred_iterable = trend_defaults.preferred_categories
        elif isinstance(preferred_categories_value, str):
            preferred_iterable = (preferred_categories_value,)
        elif isinstance(preferred_categories_value, Iterable):
            preferred_iterable = tuple(str(cat) for cat in preferred_categories_value)
        else:
            raise TypeError(
                "criteria.trend.preferred_categories must be a string or iterable of strings"
            )

        trend = TrendCriteria(
            moderate_threshold_percent=float(
                trend_map.get(
                    "moderate_threshold_percent",
                    trend_defaults.moderate_threshold_percent,
                )
            ),
            strong_threshold_percent=float(
                trend_map.get(
                    "strong_threshold_percent", trend_defaults.strong_threshold_percent
                )
            ),
            preferred_categories=tuple(str(cat).strip().lower() for cat in preferred_iterable),
        )

        criteria = ScreenerCriteria(
            volume=volume,
            gap=gap,
            minimum_float_shares=int(
                criteria_map.get("minimum_float_shares", ScreenerCriteria().minimum_float_shares)
            ),
            trend=trend,
        )

        data_defaults = DataAcquisition()
        cache_value = data_map.get("cache_path")
        cache_path = Path(cache_value).expanduser() if cache_value else None

        provider_options_map = ensure_mapping(
            data_map.get("provider_options"), "data.provider_options"
        )

        data_config = DataAcquisition(
            provider=str(data_map.get("provider", data_defaults.provider)),
            cache_path=cache_path,
            premarket_window_start=parse_time(
                data_map.get("premarket_window_start"), data_defaults.premarket_window_start
            ),
            premarket_window_end=parse_time(
                data_map.get("premarket_window_end"), data_defaults.premarket_window_end
            ),
            timezone=str(data_map.get("timezone", data_defaults.timezone)),
            provider_options=dict(provider_options_map),
        )

        config = cls(
            universe=universe,
            criteria=criteria,
            data=data_config,
            max_concurrent_requests=int(data.get("max_concurrent_requests", 8)),
        )
        config.validate()
        return config
