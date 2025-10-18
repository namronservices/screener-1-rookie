"""Configuration models for the daily stock screener.

These dataclasses capture both operational settings for running the screener and
thresholds that determine what qualifies as a high-quality ticker for
day-trading. The configuration is intentionally explicit so it can be serialized
from JSON/YAML or environment variables and later extended without breaking
changes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import Iterable, Mapping, Sequence


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

    def __post_init__(self) -> None:
        cleaned = tuple(sym.strip().upper() for sym in self.symbols)
        if len(cleaned) == 0:
            raise ValueError("At least one symbol must be provided")
        if any(not sym for sym in cleaned):
            raise ValueError("Empty symbol detected after stripping whitespace")
        object.__setattr__(self, "symbols", cleaned)


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
class ScreenerCriteria:
    """Grouping of all filter thresholds applied by the screener."""

    volume: VolumeThresholds = field(default_factory=VolumeThresholds)
    gap: GapThresholds = field(default_factory=GapThresholds)
    minimum_float_shares: int = 10_000_000

    def validate(self) -> None:
        self.volume.validate()
        self.gap.validate()
        if self.minimum_float_shares <= 0:
            raise ValueError("Float threshold must be positive")


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

        universe_map = ensure_mapping(data.get("universe"), "universe")
        criteria_map = ensure_mapping(data.get("criteria"), "criteria")
        data_map = ensure_mapping(data.get("data"), "data")

        raw_symbols = universe_map.get("symbols", ())
        if isinstance(raw_symbols, str):
            raw_symbols = [raw_symbols]
        if not isinstance(raw_symbols, Iterable):
            raise TypeError("universe.symbols must be an iterable of strings")
        symbols = tuple(str(sym) for sym in raw_symbols)
        universe = SymbolUniverse(
            symbols=symbols,
            include_etfs=bool(universe_map.get("include_etfs", False)),
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

        criteria = ScreenerCriteria(
            volume=volume,
            gap=gap,
            minimum_float_shares=int(
                criteria_map.get("minimum_float_shares", ScreenerCriteria().minimum_float_shares)
            ),
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
