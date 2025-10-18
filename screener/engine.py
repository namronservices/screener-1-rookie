"""Core orchestration logic for the screener."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Protocol

from .config import DataAcquisition, ScreenerConfig
from .filters import apply_filters, build_filters
from .models import ScreenerResult
from .data_providers.base import DataProvider


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
        provider = self._provider_factory(self._config.data)
        provider.warm_cache(self._config.universe.symbols, as_of)

        filters = tuple(build_filters(self._config.criteria))
        results: List[ScreenerResult] = []

        with ThreadPoolExecutor(max_workers=self._config.max_concurrent_requests) as pool:
            future_map = {
                pool.submit(provider.fetch_snapshot, symbol, as_of): symbol
                for symbol in self._config.universe.symbols
            }
            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    snapshot = future.result()
                except Exception as exc:
                    # Instead of failing the entire screen we capture the failure in a
                    # synthetic result. Downstream systems can inspect the exception
                    # attribute to decide how to handle it. Logging is intentionally
                    # kept outside of this engine to allow the caller to plug any
                    # structured logging of their choice.
                    result = ScreenerResult(
                        symbol=symbol,
                        snapshot=None,
                        passed_filters={},
                        error=exc,
                    )
                    results.append(result)
                    continue
                results.append(apply_filters(snapshot, filters))
        return sorted(
            results,
            key=lambda r: (
                not r.is_actionable(),
                -(r.snapshot.gap_percent if r.snapshot else 0.0),
            ),
        )
