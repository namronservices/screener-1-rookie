"""CLI entry point for running the screener."""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Sequence

try:  # pragma: no cover - optional dependency
    import yaml
except Exception:  # pragma: no cover - safe fallback
    yaml = None

from .config import ScreenerConfig
from .engine import ScreenerEngine
from .factories import resolve_provider_factory
from .reporting import render_table, summarize, write_csv_report
from .scanner_definitions import build_scanner_definitions, validate_scanners


def _configure_logging() -> None:
    """Initialise an application wide logger configuration."""

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def load_config(path: Path) -> ScreenerConfig:
    loader = json.loads
    if path.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML is required to load YAML configuration files")
        loader = yaml.safe_load  # type: ignore[assignment]
    data = loader(path.read_text())
    if not isinstance(data, Mapping):
        raise TypeError("Configuration file must contain a mapping at the top level")
    return ScreenerConfig.from_dict(data)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily pre-market screener")
    parser.add_argument("--config", type=Path, required=True, help="Path to JSON/YAML configuration file")
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="Override timestamp in ISO format (UTC). Defaults to now.",
    )
    parser.add_argument(
        "--scanner",
        dest="scanners",
        action="append",
        default=None,
        help=(
            "Limit output to the named scanner(s). May be repeated or provided as a comma-separated list. "
            "Omit to use scanners declared in the config (if any)."
        ),
    )
    parser.add_argument(
        "--list-scanners",
        action="store_true",
        help="Print available scanners and exit."
    )
    return parser.parse_args(argv)


def _normalise_scanner_args(raw: Sequence[str] | None) -> tuple[str, ...]:
    if raw is None:
        return tuple()
    names: list[str] = []
    for value in raw:
        parts = [part.strip() for part in value.split(",") if part.strip()]
        names.extend(parts)
    return tuple(dict.fromkeys(names))


def _render_scanner_catalogue() -> str:
    catalogue = build_scanner_definitions()
    validate_scanners(catalogue)
    by_group: dict[str, list[str]] = {}
    for scanner in catalogue.values():
        by_group.setdefault(scanner.group, []).append(scanner.name)
    lines: list[str] = ["Available scanners:"]
    for group in sorted(by_group):
        names = ", ".join(sorted(by_group[group]))
        lines.append(f"- {group}: {names}")
    return "\n".join(lines)


def main(argv: Iterable[str] | None = None) -> int:
    _configure_logging()

    logger = logging.getLogger(__name__)
    args = parse_args(argv)

    if args.list_scanners:
        print(_render_scanner_catalogue())
        return 0

    logger.info("Loading configuration", extra={"config_path": str(args.config)})
    config = load_config(args.config)

    requested_scanners = _normalise_scanner_args(args.scanners) or config.scanners
    if requested_scanners:
        catalogue = build_scanner_definitions()
        validate_scanners(catalogue)
        missing = [name for name in requested_scanners if name not in catalogue]
        if missing:
            raise SystemExit(f"Unknown scanner(s): {', '.join(missing)}")
        logger.info("Scanners selected", extra={"scanners": list(requested_scanners)})

    factory = resolve_provider_factory(config.data.provider)
    engine = ScreenerEngine(config, factory)

    as_of = datetime.fromisoformat(args.as_of) if args.as_of else None
    logger.info(
        "Starting screener run",
        extra={"as_of": as_of.isoformat() if as_of else None},
    )
    results = engine.run(as_of=as_of)

    max_results = config.universe.max_results
    display_results = results if max_results is None else results[:max_results]
    rows = [summarize(result) for result in display_results]
    print(render_table(rows))

    if requested_scanners:
        print()
        print("Scanners:", ", ".join(requested_scanners))
        tickers = ", ".join(result.symbol for result in display_results)
        print(f"Tickers returned ({len(display_results)}): {tickers}")

    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_filename = f"screener-results-{timestamp}.csv"
    output_path = Path.cwd() / output_filename
    write_csv_report(results, output_path)
    logger.info("Wrote CSV report", extra={"path": str(output_path)})

    has_actionable = any(result.is_actionable() for result in results)
    return 0 if has_actionable else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
