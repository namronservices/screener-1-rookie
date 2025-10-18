"""Factory utilities for wiring the screener components together."""
from __future__ import annotations

from typing import Callable, Dict

from .config import DataAcquisition
from .data_providers.base import DataProvider
from .data_providers.polygon_provider import PolygonProvider
from .data_providers.yfinance_provider import YFinanceProvider


def provider_factory_registry() -> Dict[str, Callable[[DataAcquisition], DataProvider]]:
    return {
        "yfinance": YFinanceProvider,
        "polygon": PolygonProvider,
    }


def resolve_provider_factory(name: str) -> Callable[[DataAcquisition], DataProvider]:
    registry = provider_factory_registry()
    try:
        return registry[name]
    except KeyError as exc:
        available = ", ".join(sorted(registry))
        raise KeyError(f"Unknown data provider '{name}'. Available: {available}") from exc
