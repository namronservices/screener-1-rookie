"""CLI entry point for running the screener."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping

try:  # pragma: no cover - optional dependency
    import yaml
except Exception:  # pragma: no cover - safe fallback
    yaml = None

from .config import ScreenerConfig
from .engine import ScreenerEngine
from .factories import resolve_provider_factory
from .reporting import render_table, summarize


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
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config(args.config)
    factory = resolve_provider_factory(config.data.provider)
    engine = ScreenerEngine(config, factory)

    as_of = datetime.fromisoformat(args.as_of) if args.as_of else None
    results = engine.run(as_of=as_of)
    rows = [summarize(result) for result in results]
    print(render_table(rows))
    has_actionable = any(result.is_actionable() for result in results)
    return 0 if has_actionable else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
