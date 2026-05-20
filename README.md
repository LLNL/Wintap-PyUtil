<img width="200" src="https://user-images.githubusercontent.com/50601643/218871643-2d3af433-0923-4786-b5e5-24c6a72e803e.png">

# Wintap-PyUtil

Python, DuckDB, and DBT utilities for processing Wintap/Lintap telemetry data.

The canonical post-processing input is now a `raw_sensor` parquet dataset:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/*.parquet
```

## Minimum system requirements

- Python 3.10, 3.11, or 3.12
- [uv](https://docs.astral.sh/uv/)
- DuckDB-compatible parquet data

Install uv if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Getting started

Create the development environment and install the package in editable mode:

```bash
cd Wintap-PyUtil
uv sync --dev
```

Run commands through uv:

```bash
uv run rawtorolling --help
uv run rawtostdview --help
uv run ubersummary --help
```

Example Python usage:

```python
from wintappy.datautils.rawutil import init_db

connection = init_db()
print(connection.query('select 1'))
```

See `wintappy/examples/` for additional examples.

## DBT pipeline

An experimental DBT/DuckDB implementation of the Wintap ETL lives in:

```text
wintap_dbt/
```

It currently builds bronze, silver, and gold models from `raw_sensor` into a DuckDB database.

Run with the included sample-data defaults in `wintap_dbt/dbt_project.yml`:

```bash
make dbt-build
```

Or pass an explicit dataset and date range:

```bash
uv run dbt build --project-dir wintap_dbt --profiles-dir wintap_dbt \
  --vars '{dataset: /path/to/dataset, start_day: 20240904, end_day: 20240904}'
```

The default DBT profile writes to:

```text
wintap_dbt/target/wintap.duckdb
```

Override the database path with:

```bash
export WINTAP_DBT_DATABASE=/tmp/wintap.duckdb
```

Useful DBT targets:

```bash
make dbt-debug
make dbt-build
make dbt-test
make dbt-docs
```

## Development tasks

```bash
make fmt          # black + isort
make fmt-check    # formatting checks
make lint         # mypy + sqlfluff
make test         # pytest
make ci           # fmt-check + lint + test + dbt-build
make clean
```

## Legacy Python ETL commands

The pre-DBT Python/DuckDB pipeline remains available while DBT migration continues:

```bash
uv run rawtorolling -d <dataset> -s YYYYMMDD -e YYYYMMDD
uv run rawtostdview -d <dataset> -s YYYYMMDD -e YYYYMMDD
uv run ubersummary -d <dataset> -a stdview-YYYYMMDD-YYYYMMDD
```

## Release

LLNL-CODE-837816
