# Daily Pre-Market Screener

This project provides a production-ready Python codebase for screening small and
mid-cap equities ahead of the opening bell. The screener focuses on gapping
stocks with strong pre-market participation and sufficient liquidity to support
intra-day trading strategies.

## Features

- **Modular architecture** – Clear separation between configuration, data
  acquisition, analytics, filtering, and reporting layers for easy extension.
- **Configurable filters** – Thresholds for pre-market gap size, relative and
  absolute volume, and minimum float size can be tuned via JSON or YAML
  configuration files.
- **Pluggable data providers** – Default implementation pulls data from Yahoo
  Finance (`yfinance`), and an optional Polygon.io provider is available for
  desks with licensed market data. Additional providers can be registered
  without touching the core engine.
- **Static ticker universe** – A packaged CSV catalog is filtered by
  configurable market-cap bounds before evaluation, ensuring consistent inputs
  across runs while avoiding reliance on hand-curated lists.
- **Human-friendly reporting** – Results are rendered as a clean table that
  highlights failing criteria for each symbol.

## Project Layout

```
screener/
├── __main__.py              # CLI entry point
├── analyzers.py             # VWAP and snapshot builders
├── config.py                # Dataclasses and serialization helpers
├── data_providers/          # Provider interfaces and implementations
├── engine.py                # Orchestrates fetching, filtering, sorting
├── factories.py             # Provider factory registry
├── filters.py               # Screening rules
├── models.py                # Domain dataclasses
└── reporting.py             # Report summarization utilities
```

## Getting Started

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

   The default dependency set covers the Yahoo Finance provider and CLI. To use
   the Polygon.io integration you must also supply a valid API key (see below).

2. **Create a configuration file**

   Use [`config.example.yaml`](config.example.yaml) as a starting point. The
   config controls the ticker universe, filter thresholds, and data provider
   settings. The universe is derived from `screener/tickers/tickers.csv` and is
   trimmed to the configured market-cap band before any checks are executed.

3. **Run the screener**

   ```bash
   python -m screener --config path/to/config.yaml
   ```

   The command prints a ranked table sorted by actionable tickers first. The
   process exits with status code `0` when at least one ticker passes all
   filters, or `1` otherwise. Use the `--as-of` flag to back-test specific
   sessions (ISO8601 UTC timestamp).

### Using the Polygon.io provider

Set the `data.provider` field in the configuration to `"polygon"` and supply an
API key through `data.provider_options`. Keys can be embedded directly in the
config or referenced via an environment variable.

```yaml
data:
  provider: polygon
  provider_options:
    api_key_env: POLYGON_API_KEY  # or set api_key: "<token>"
```

Ensure the referenced environment variable is exported before running the
screener. The provider pulls historical aggregates, pre-market bars, and float
data through the Polygon v2/v3 REST APIs.

## Extending the Screener

- **Custom data providers** – Implement `DataProvider` and register it in
  `screener/factories.py`.
- **Additional filters** – Build new `Filter` objects in `screener/filters.py`
  or compose them dynamically based on the configuration.
- **Automated workflows** – The engine is designed to be called from Airflow,
  cron jobs, or chat bots. Consume the `ScreenerResult` objects directly for
  downstream automation.

## Testing Without Market Data

For deterministic tests, inject `InMemoryProvider` from
`screener/data_providers/base.py` into the `ScreenerEngine`. This allows you to
validate strategies and reporting logic without live market dependencies.
