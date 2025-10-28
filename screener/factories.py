"""Factory utilities for wiring the screener components together."""
from __future__ import annotations

import logging
from typing import Callable, Dict

from .config import DataAcquisition
from .data_providers.base import DataProvider
from .data_providers.polygon_provider import PolygonProvider
from .data_providers.yfinance_provider import YFinanceProvider


logger = logging.getLogger(__name__)


def provider_factory_registry() -> Dict[str, Callable[[DataAcquisition], DataProvider]]:
    return {
        "yfinance": YFinanceProvider,
        "polygon": PolygonProvider,
    }


def resolve_provider_factory(name: str) -> Callable[[DataAcquisition], DataProvider]:
    registry = provider_factory_registry()
    try:
        logger.info("Resolving provider factory", extra={"provider": name})
        return registry[name]
    except KeyError as exc:
        available = ", ".join(sorted(registry))
        logger.error(
            "Unknown data provider",
            extra={"requested": name, "available": available},
        )
        raise KeyError(f"Unknown data provider '{name}'. Available: {available}") from exc
