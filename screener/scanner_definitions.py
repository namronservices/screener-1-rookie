"""Predefined scanner bundles and their baseline query ingredients.

The repository's core screener focuses on pre-market gap and volume filters, but
many desks build higher-level scanners by composing a handful of reusable
"baseline" queries. This module documents the scanner catalogue requested by the
trading team and exposes structured data that downstream automations can consume
without hard-coding the combinations.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping, Sequence, Tuple

from .models import PreMarketSnapshot


@dataclass(frozen=True)
class BaselineQuery:
    """Atomic building block for a scanner condition."""

    key: str
    description: str
    parameters: Mapping[str, object] = field(default_factory=dict)

    def with_overrides(self, **overrides: object) -> "BaselineQuery":
        merged = dict(self.parameters)
        merged.update(overrides)
        return BaselineQuery(key=self.key, description=self.description, parameters=merged)


@dataclass(frozen=True)
class ScannerDefinition:
    """Named scanner composed from one or more baseline queries."""

    name: str
    group: str
    baselines: Tuple[BaselineQuery, ...]
    notes: str | None = None


@dataclass(frozen=True)
class BaselineOutcome:
    """Result of evaluating a baseline query against a snapshot."""

    key: str
    passed: bool
    reason: str


BASELINE_QUERIES: Dict[str, BaselineQuery] = {
    "premarket_gap": BaselineQuery(
        key="premarket_gap",
        description="Pre-market gap percentage with VWAP confirmation",
        parameters={"min_gap_percent": 3.0, "require_above_vwap": True},
    ),
    "after_hours_gap": BaselineQuery(
        key="after_hours_gap",
        description="After-hours price displacement relative to prior close",
        parameters={"min_gap_percent": 2.0, "session": "post"},
    ),
    "premarket_liquidity": BaselineQuery(
        key="premarket_liquidity",
        description="Pre-market participation filters",
        parameters={"min_relative_volume": 1.5, "min_absolute_volume": 100_000},
    ),
    "after_hours_liquidity": BaselineQuery(
        key="after_hours_liquidity",
        description="After-hours participation filters",
        parameters={"min_relative_volume": 1.2, "min_absolute_volume": 50_000},
    ),
    "earnings_upcoming": BaselineQuery(
        key="earnings_upcoming",
        description="Earnings scheduled within the next few sessions",
        parameters={"days_ahead": 1, "session": "after_close"},
    ),
    "post_earnings_followthrough": BaselineQuery(
        key="post_earnings_followthrough",
        description="Tracking price and volume behaviour after an earnings event",
        parameters={"days_since_report": 1, "min_gain_percent": 2.5},
    ),
    "multi_day_momentum": BaselineQuery(
        key="multi_day_momentum",
        description="Stacked green candles with higher highs",
        parameters={"green_days": 3, "require_higher_highs": True},
    ),
    "breakout": BaselineQuery(
        key="breakout",
        description="Price clearing recent resistance with volume",
        parameters={"lookback_days": 20, "volume_multiple": 1.5, "buffer_percent": 1.5},
    ),
    "double_breakout": BaselineQuery(
        key="double_breakout",
        description="Breakout validated on two different lookbacks",
        parameters={"first_lookback": 20, "second_lookback": 50, "min_volume_multiple": 2.0},
    ),
    "moving_average_cross": BaselineQuery(
        key="moving_average_cross",
        description="Fast MA crossing above a slower MA",
        parameters={"fast_window": 10, "slow_window": 20},
    ),
    "sma_cross": BaselineQuery(
        key="sma_cross",
        description="Simple moving-average cross",
        parameters={"fast_window": 20, "slow_window": 50},
    ),
    "golden_cross": BaselineQuery(
        key="golden_cross",
        description="50-day moving average crossing above the 200-day",
        parameters={"fast_window": 50, "slow_window": 200},
    ),
    "bullish_reversal": BaselineQuery(
        key="bullish_reversal",
        description="Oversold bounce reclaiming short-term resistance",
        parameters={"rsi_max": 35, "reclaim_days": 3, "min_wick_percent": 1.0},
    ),
    "volume_spike": BaselineQuery(
        key="volume_spike",
        description="Sudden increase in intraday volume",
        parameters={"min_multiple": 2.0},
    ),
    "relative_strength": BaselineQuery(
        key="relative_strength",
        description="Relative strength versus peers over the last N sessions",
        parameters={"lookback_days": 30, "percentile": 70},
    ),
    "intraday_momentum": BaselineQuery(
        key="intraday_momentum",
        description="Intraday gain over a short timeframe",
        parameters={"timeframe_minutes": 15, "min_gain_percent": 3.0},
    ),
    "short_squeeze": BaselineQuery(
        key="short_squeeze",
        description="Elevated short interest with supportive volume",
        parameters={"min_short_float_percent": 10, "max_days_to_cover": 4, "volume_multiple": 2.5},
    ),
}


def _get_baseline(key: str, overrides: Mapping[str, object] | None = None) -> BaselineQuery:
    try:
        base = BASELINE_QUERIES[key]
    except KeyError as exc:
        raise KeyError(f"Unknown baseline '{key}' referenced by scanner") from exc
    if not overrides:
        return base
    return base.with_overrides(**overrides)


def build_scanner_definitions() -> Dict[str, ScannerDefinition]:
    """Return scanner definitions requested by the trading desk."""

    scanners: Dict[str, ScannerDefinition] = {
        # Pre-Open Gainers
        "Gainers": ScannerDefinition(
            name="Gainers",
            group="Pre-Open Gainers",
            baselines=(
                _get_baseline("premarket_gap", {"min_gap_percent": 1.5}),
                _get_baseline("premarket_liquidity"),
            ),
        ),
        "After-Hours Gainers": ScannerDefinition(
            name="After-Hours Gainers",
            group="Pre-Open Gainers",
            baselines=(
                _get_baseline("after_hours_gap", {"min_gap_percent": 1.5}),
                _get_baseline("after_hours_liquidity"),
            ),
        ),
        "Gap-up": ScannerDefinition(
            name="Gap-up",
            group="Pre-Open Gainers",
            baselines=(
                _get_baseline("premarket_gap", {"min_gap_percent": 4.0}),
                _get_baseline("premarket_liquidity", {"min_absolute_volume": 150_000}),
            ),
        ),
        "Earnings Tonight": ScannerDefinition(
            name="Earnings Tonight",
            group="Pre-Open Gainers",
            baselines=(
                _get_baseline("earnings_upcoming", {"days_ahead": 0, "session": "after_close"}),
                _get_baseline("premarket_liquidity"),
            ),
            notes="Filters for names reporting after today's close.",
        ),
        "After Earnings": ScannerDefinition(
            name="After Earnings",
            group="Pre-Open Gainers",
            baselines=(
                _get_baseline("post_earnings_followthrough", {"days_since_report": 2}),
                _get_baseline("premarket_liquidity"),
            ),
        ),
        # Swing
        "Green Royal Flush": ScannerDefinition(
            name="Green Royal Flush",
            group="Swing",
            baselines=(
                _get_baseline("multi_day_momentum", {"green_days": 5}),
                _get_baseline("relative_strength", {"percentile": 80}),
                _get_baseline("volume_spike", {"min_multiple": 2.5}),
            ),
        ),
        "Pop Bull": ScannerDefinition(
            name="Pop Bull",
            group="Swing",
            baselines=(
                _get_baseline("multi_day_momentum", {"green_days": 2}),
                _get_baseline("breakout", {"buffer_percent": 0.5}),
            ),
        ),
        "Pop+Bull": ScannerDefinition(
            name="Pop+Bull",
            group="Swing",
            baselines=(
                _get_baseline("multi_day_momentum", {"green_days": 3}),
                _get_baseline("breakout"),
                _get_baseline("relative_strength", {"percentile": 75}),
            ),
        ),
        "Buy Into Eam": ScannerDefinition(
            name="Buy Into Eam",
            group="Swing",
            baselines=(
                _get_baseline("earnings_upcoming", {"days_ahead": 3}),
                _get_baseline("multi_day_momentum", {"green_days": 2}),
            ),
            notes="Positions building into the earnings catalyst.",
        ),
        "Strong After Eam": ScannerDefinition(
            name="Strong After Eam",
            group="Swing",
            baselines=(
                _get_baseline("post_earnings_followthrough", {"min_gain_percent": 3.0}),
                _get_baseline("breakout", {"volume_multiple": 2.0}),
            ),
        ),
        "Breakout Strong": ScannerDefinition(
            name="Breakout Strong",
            group="Swing",
            baselines=(
                _get_baseline("breakout", {"lookback_days": 30, "volume_multiple": 2.0}),
                _get_baseline("relative_strength", {"percentile": 75}),
            ),
        ),
        "Breakout x 2": ScannerDefinition(
            name="Breakout x 2",
            group="Swing",
            baselines=(
                _get_baseline("double_breakout"),
                _get_baseline("volume_spike", {"min_multiple": 2.5}),
            ),
        ),
        "Bullish Reversal": ScannerDefinition(
            name="Bullish Reversal",
            group="Swing",
            baselines=(
                _get_baseline("bullish_reversal"),
                _get_baseline("volume_spike", {"min_multiple": 1.5}),
            ),
        ),
        "Golden Cross": ScannerDefinition(
            name="Golden Cross",
            group="Swing",
            baselines=(_get_baseline("golden_cross"),),
        ),
        "Bullish SMA Cross": ScannerDefinition(
            name="Bullish SMA Cross",
            group="Swing",
            baselines=(_get_baseline("sma_cross"),),
        ),
        # Day
        "Heavy Buying": ScannerDefinition(
            name="Heavy Buying",
            group="Day",
            baselines=(
                _get_baseline("volume_spike", {"min_multiple": 3.0}),
                _get_baseline("premarket_liquidity"),
            ),
        ),
        "Relative Str30": ScannerDefinition(
            name="Relative Str30",
            group="Day",
            baselines=(
                _get_baseline("relative_strength", {"lookback_days": 30, "percentile": 80}),
            ),
        ),
        "Bullish Explosion": ScannerDefinition(
            name="Bullish Explosion",
            group="Day",
            baselines=(
                _get_baseline("intraday_momentum", {"min_gain_percent": 5.0}),
                _get_baseline("volume_spike", {"min_multiple": 2.5}),
            ),
        ),
        "Red Hot": ScannerDefinition(
            name="Red Hot",
            group="Day",
            baselines=(
                _get_baseline("multi_day_momentum", {"green_days": 4, "require_higher_highs": True}),
                _get_baseline("relative_strength", {"percentile": 85}),
            ),
        ),
        "Breakout": ScannerDefinition(
            name="Breakout",
            group="Day",
            baselines=(
                _get_baseline("breakout"),
                _get_baseline("volume_spike"),
            ),
        ),
        "Breakout Plus": ScannerDefinition(
            name="Breakout Plus",
            group="Day",
            baselines=(
                _get_baseline("breakout", {"lookback_days": 40, "buffer_percent": 0.5}),
                _get_baseline("volume_spike", {"min_multiple": 2.0}),
                _get_baseline("relative_strength", {"percentile": 75}),
            ),
        ),
        "M15 Gain": ScannerDefinition(
            name="M15 Gain",
            group="Day",
            baselines=(
                _get_baseline("intraday_momentum", {"timeframe_minutes": 15, "min_gain_percent": 2.5}),
                _get_baseline("volume_spike", {"min_multiple": 1.8}),
            ),
        ),
        "Bull Run": ScannerDefinition(
            name="Bull Run",
            group="Day",
            baselines=(
                _get_baseline("multi_day_momentum", {"green_days": 4}),
                _get_baseline("moving_average_cross", {"fast_window": 8, "slow_window": 21}),
            ),
        ),
        "Short Squeeze": ScannerDefinition(
            name="Short Squeeze",
            group="Day",
            baselines=(
                _get_baseline("short_squeeze"),
                _get_baseline("volume_spike", {"min_multiple": 3.0}),
            ),
        ),
    }

    return scanners


def validate_scanners(scanners: Mapping[str, ScannerDefinition] | None = None) -> None:
    """Verify that all referenced baseline keys are defined and non-empty."""

    scanners = scanners or build_scanner_definitions()
    if not scanners:
        raise ValueError("At least one scanner definition must be provided")

    for name, scanner in scanners.items():
        if not scanner.baselines:
            raise ValueError(f"Scanner '{name}' must include at least one baseline query")
        for baseline in scanner.baselines:
            if baseline.key not in BASELINE_QUERIES:
                raise ValueError(
                    f"Scanner '{name}' references undefined baseline '{baseline.key}'"
                )


def _format_failure_reason(label: str, checks: Sequence[tuple[bool, str]]) -> str:
    failures = [reason for passed, reason in checks if not passed]
    if not failures:
        return f"{label} satisfied"
    return f"{label} failed: {', '.join(failures)}"


def evaluate_baseline(snapshot: PreMarketSnapshot, baseline: BaselineQuery) -> BaselineOutcome:
    """Check whether the snapshot satisfies a baseline query."""

    params = baseline.parameters

    if baseline.key == "premarket_gap":
        min_gap_percent = float(params.get("min_gap_percent", 0.0))
        require_above_vwap = bool(params.get("require_above_vwap", False))
        meets_gap = snapshot.gap_percent >= min_gap_percent
        meets_vwap = (not require_above_vwap) or snapshot.is_above_vwap
        passed = meets_gap and meets_vwap
        reason = _format_failure_reason(
            "Gap", [
                (meets_gap, f"gap<{min_gap_percent:.2f}%"),
                (meets_vwap, "below VWAP"),
            ],
        )
        return BaselineOutcome(key=baseline.key, passed=passed, reason=reason)

    if baseline.key == "premarket_liquidity":
        min_rel_vol = float(params.get("min_relative_volume", 0.0))
        min_abs_vol = int(params.get("min_absolute_volume", 0))
        meets_rel = snapshot.relative_volume >= min_rel_vol
        meets_abs = snapshot.premarket_volume >= min_abs_vol
        passed = meets_rel and meets_abs
        reason = _format_failure_reason(
            "Liquidity", [
                (meets_rel, f"rel_vol<{min_rel_vol:.2f}"),
                (meets_abs, f"volume<{min_abs_vol:,}"),
            ],
        )
        return BaselineOutcome(key=baseline.key, passed=passed, reason=reason)

    return BaselineOutcome(
        key=baseline.key,
        passed=False,
        reason="baseline evaluation not supported by available snapshot data",
    )


def evaluate_scanner(snapshot: PreMarketSnapshot, scanner: ScannerDefinition) -> Tuple[BaselineOutcome, ...]:
    """Evaluate all baselines for a scanner against a snapshot."""

    outcomes = tuple(evaluate_baseline(snapshot, baseline) for baseline in scanner.baselines)
    return outcomes


__all__ = [
    "BaselineQuery",
    "ScannerDefinition",
    "BaselineOutcome",
    "BASELINE_QUERIES",
    "build_scanner_definitions",
    "validate_scanners",
    "evaluate_baseline",
    "evaluate_scanner",
]
