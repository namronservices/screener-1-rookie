"""Output helpers for presenting screener results."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from .models import ScreenerResult


@dataclass(frozen=True)
class ReportRow:
    symbol: str
    gap_percent: float | None
    premarket_volume: int | None
    relative_volume: float | None
    float_shares: int | None
    notes: str


def summarize(result: ScreenerResult) -> ReportRow:
    if result.error:
        return ReportRow(
            symbol=result.symbol,
            gap_percent=None,
            premarket_volume=None,
            relative_volume=None,
            float_shares=None,
            notes=f"error: {result.error}",
        )
    assert result.snapshot is not None
    snapshot = result.snapshot
    failing = [name for name, passed in result.passed_filters.items() if not passed]
    notes = "PASS" if not failing else "Fail: " + ", ".join(sorted(failing))
    return ReportRow(
        symbol=snapshot.symbol,
        gap_percent=round(snapshot.gap_percent, 2),
        premarket_volume=snapshot.premarket_volume,
        relative_volume=round(snapshot.relative_volume, 2),
        float_shares=snapshot.float_shares,
        notes=notes,
    )


def render_table(rows: Sequence[ReportRow]) -> str:
    """Render a human-readable table suitable for console output or Slack."""

    headers = ["Symbol", "Gap %", "Premkt Vol", "Rel Vol", "Float", "Notes"]
    column_widths = [len(h) for h in headers]
    formatted_rows = []
    for row in rows:
        formatted = [
            row.symbol,
            "" if row.gap_percent is None else f"{row.gap_percent:.2f}",
            "" if row.premarket_volume is None else f"{row.premarket_volume:,}",
            "" if row.relative_volume is None else f"{row.relative_volume:.2f}",
            "" if row.float_shares is None else f"{row.float_shares:,}",
            row.notes,
        ]
        column_widths = [max(w, len(value)) for w, value in zip(column_widths, formatted)]
        formatted_rows.append(formatted)

    def format_row(row: Sequence[str]) -> str:
        return " | ".join(value.ljust(width) for value, width in zip(row, column_widths))

    separator = "-+-".join("-" * width for width in column_widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row(row) for row in formatted_rows)
    return "\n".join(lines)
