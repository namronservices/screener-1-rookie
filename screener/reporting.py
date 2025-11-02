"""Output helpers for presenting screener results."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from .models import PreMarketSnapshot, ScreenerResult


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


def _format_number(value: float | None, precision: int = 2) -> str:
    if value is None:
        return ""
    return f"{value:.{precision}f}"


def _format_int(value: int | None) -> str:
    if value is None:
        return ""
    return str(value)


def _format_snapshot(snapshot: PreMarketSnapshot | None) -> Mapping[str, str]:
    if snapshot is None:
        return {
            "timestamp": "",
            "last_price": "",
            "previous_close": "",
            "gap_percent": "",
            "premarket_volume": "",
            "average_30_day_volume": "",
            "relative_volume": "",
            "float_shares": "",
            "vwap": "",
        }

    return {
        "timestamp": snapshot.timestamp.isoformat(),
        "last_price": _format_number(snapshot.last_price),
        "previous_close": _format_number(snapshot.previous_close),
        "gap_percent": _format_number(snapshot.gap_percent),
        "premarket_volume": _format_int(snapshot.premarket_volume),
        "average_30_day_volume": _format_int(snapshot.average_30_day_volume),
        "relative_volume": _format_number(snapshot.relative_volume),
        "float_shares": _format_int(snapshot.float_shares),
        "vwap": _format_number(snapshot.vwap),
    }


def write_csv_report(results: Sequence[ScreenerResult], destination: Path) -> None:
    """Persist the complete screener results set as a CSV file."""

    filter_names = sorted({name for result in results for name in result.passed_filters})
    base_headers = [
        "symbol",
        "actionable",
        "error",
        "timestamp",
        "last_price",
        "previous_close",
        "gap_percent",
        "premarket_volume",
        "average_30_day_volume",
        "relative_volume",
        "float_shares",
        "vwap",
        "notes",
    ]
    filter_headers = [f"filter:{name}" for name in filter_names]
    headers = base_headers + filter_headers

    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for result in results:
            summary = summarize(result)
            snapshot_details = _format_snapshot(result.snapshot)
            row = {
                "symbol": result.symbol,
                "actionable": "YES" if result.is_actionable() else "NO",
                "error": "" if result.error is None else str(result.error),
                **snapshot_details,
                "notes": summary.notes,
            }
            for name in filter_names:
                passed = result.passed_filters.get(name)
                if passed is None:
                    value = ""
                else:
                    value = "PASS" if passed else "FAIL"
                row[f"filter:{name}"] = value
            writer.writerow(row)


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
