"""Filter functions that determine if a ticker is actionable."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Iterable

from .config import ScreenerCriteria
from .models import PreMarketSnapshot, ScreenerResult


@dataclass(frozen=True)
class Filter:
    """Callable wrapper representing a single screening rule."""

    name: str
    predicate: Callable[[PreMarketSnapshot], bool]

    def __call__(self, snapshot: PreMarketSnapshot) -> bool:
        return self.predicate(snapshot)


def build_filters(criteria: ScreenerCriteria) -> Iterable[Filter]:
    volume_rules = criteria.volume
    gap_rules = criteria.gap
    trend_rules = criteria.trend
    preferred_categories = {cat.lower() for cat in trend_rules.preferred_categories}

    yield Filter(
        name="float_liquidity",
        predicate=lambda snap: snap.float_shares >= criteria.minimum_float_shares,
    )
    yield Filter(
        name="relative_volume",
        predicate=lambda snap: snap.relative_volume >= volume_rules.relative_to_30day_avg,
    )
    yield Filter(
        name="absolute_volume",
        predicate=lambda snap: snap.premarket_volume >= volume_rules.absolute_pre_market_shares,
    )
    yield Filter(
        name="gap_size",
        predicate=lambda snap: abs(snap.gap_percent) >= gap_rules.minimum_gap_percent,
    )
    if gap_rules.require_above_vwap:
        yield Filter(
            name="above_vwap",
            predicate=lambda snap: snap.is_above_vwap,
        )

    def classify_trend(snapshot: PreMarketSnapshot) -> str:
        diff = snapshot.sma_20_percent_diff
        if diff is None:
            snapshot.sma_20_category = "unknown"
            return "unknown"

        moderate = trend_rules.moderate_threshold_percent
        strong = trend_rules.strong_threshold_percent

        if diff >= strong:
            category = "bullish"
        elif diff >= moderate:
            category = "moderate bullish"
        elif diff <= -strong:
            category = "bearish"
        elif diff <= -moderate:
            category = "moderate bearish"
        else:
            category = "sideways"

        snapshot.sma_20_category = category
        return category

    def trend_predicate(snapshot: PreMarketSnapshot) -> bool:
        category = classify_trend(snapshot)
        return category in preferred_categories

    yield Filter(
        name="sma_20_trend",
        predicate=trend_predicate,
    )


def apply_filters(snapshot: PreMarketSnapshot, filters: Iterable[Filter]) -> ScreenerResult:
    """Evaluate all filters and package results for downstream consumption."""

    outcomes: Dict[str, bool] = {}
    for filter_ in filters:
        outcomes[filter_.name] = filter_(snapshot)
    return ScreenerResult(symbol=snapshot.symbol, snapshot=snapshot, passed_filters=outcomes)
