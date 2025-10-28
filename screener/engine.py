"""Core orchestration logic for the screener."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Protocol

from .config import DataAcquisition, ScreenerConfig
from .filters import apply_filters, build_filters
from .models import ScreenerResult
from .data_providers.base import DataProvider


logger = logging.getLogger(__name__)


class ProviderFactory(Protocol):
    def __call__(self, config: DataAcquisition) -> DataProvider:
        ...


class ScreenerEngine:
    """Coordinates data fetching, filter evaluation and reporting."""

    def __init__(self, config: ScreenerConfig, provider_factory: ProviderFactory) -> None:
        self._config = config
        self._provider_factory = provider_factory

    def run(self, as_of: datetime | None = None) -> List[ScreenerResult]:
        """Execute the screening workflow for the configured universe."""

        as_of = as_of or datetime.now(tz=timezone.utc)
        logger.info("Running screener engine", extra={"as_of": as_of.isoformat()})

        provider = self._provider_factory(self._config.data)
        logger.debug("Warming provider cache", extra={"symbols": list(self._config.universe.symbols)})
        provider.warm_cache(self._config.universe.symbols, as_of)

        filters = tuple(build_filters(self._config.criteria))
        logger.debug(
            "Constructed filters",
            extra={"filter_count": len(filters)},
        )
        results: List[ScreenerResult] = []

        with ThreadPoolExecutor(max_workers=self._config.max_concurrent_requests) as pool:
            future_map = {
                pool.submit(provider.fetch_snapshot, symbol, as_of): symbol
                for symbol in self._config.universe.symbols
            }
            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    logger.debug("Awaiting snapshot", extra={"symbol": symbol})
                    snapshot = future.result()
                except Exception as exc:
                    logger.exception(
                        "Failed to fetch snapshot",
                        extra={"symbol": symbol},
                    )
                    # Instead of failing the entire screen we capture the failure in a
                    # synthetic result. Downstream systems can inspect the exception
                    # attribute to decide how to handle it.
                    result = ScreenerResult(
                        symbol=symbol,
                        snapshot=None,
                        passed_filters={},
                        error=exc,
                    )
                    results.append(result)
                    continue
                result = apply_filters(snapshot, filters)
                results.append(result)
                logger.info(
                    "Completed snapshot",
                    extra={"symbol": symbol, "actionable": result.is_actionable()},
                )
        return sorted(
            results,
            key=lambda r: (
                not r.is_actionable(),
                -(r.snapshot.gap_percent if r.snapshot else 0.0),
            ),
        )
