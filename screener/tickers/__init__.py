"""Utilities for working with the packaged ticker universe catalog."""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

logger = logging.getLogger(__name__)


DEFAULT_TICKER_CSV = Path(__file__).resolve().parent / "tickers.csv"


@dataclass(frozen=True)
class TickerRecord:
    """Representation of a single entry from the static ticker catalog."""

    symbol: str
    market_cap: int | None


def _parse_market_cap(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace(",", "")
    try:
        # Cast via float first to handle scientific notation if present.
        return int(float(normalized))
    except ValueError:
        logger.debug("Unable to parse market cap", extra={"value": value})
        return None


def iter_catalog(path: Path | None = None) -> Iterator[TickerRecord]:
    """Yield :class:`TickerRecord` entries from the packaged CSV dataset."""

    csv_path = path or DEFAULT_TICKER_CSV
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = str(row.get("ticker", "")).strip().upper()
            if not symbol:
                continue
            market_cap = _parse_market_cap(
                row.get("market_cap_num") if isinstance(row, dict) else None
            )
            yield TickerRecord(symbol=symbol, market_cap=market_cap)


def select_symbols(
    *,
    path: Path | None = None,
    market_cap_from: int | None = None,
    market_cap_to: int | None = None,
    limit: int | None = None,
) -> Sequence[str]:
    """Load ticker symbols filtered by market-cap bounds and optional limit."""

    filtered: list[str] = []
    for record in iter_catalog(path):
        if record.market_cap is None:
            continue
        if market_cap_from is not None and record.market_cap < market_cap_from:
            continue
        if market_cap_to is not None and record.market_cap > market_cap_to:
            continue
        filtered.append(record.symbol)
        if limit is not None and len(filtered) >= limit:
            break
    logger.debug(
        "Selected symbols from catalog",
        extra={
            "market_cap_from": market_cap_from,
            "market_cap_to": market_cap_to,
            "limit": limit,
            "selected": len(filtered),
        },
    )
    return tuple(filtered)


__all__ = ["DEFAULT_TICKER_CSV", "TickerRecord", "iter_catalog", "select_symbols"]
