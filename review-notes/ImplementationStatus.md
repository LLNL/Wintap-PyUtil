# Implementation Status

This file captures what changed after the initial review/planning pass.

## Location

The review notes now live in the `Wintap-PyUtil` repository:

```text
Wintap-PyUtil/review-notes/
```

This avoids creating a separate documentation repository while keeping the data-pipeline planning material close to the canonical ETL code.

## DBT pipeline

A first DBT/DuckDB implementation now exists in:

```text
Wintap-PyUtil/wintap_dbt/
```

Implemented so far:

- DBT project skeleton and DuckDB profile.
- Bronze raw-source models for core `raw_sensor` events.
- Silver/detail models ported from `rawtostdview.sql`.
- Recursive `process_path` model ported from `process_path.sql`.
- Gold process summary models ported from `process_summary.sql`.
- `process_uber_summary` model with currently stubbed optional enrichment inputs.
- Basic DBT schema tests.
- `build_summary` monitoring view.
- Raw-source macros for:
  - raw event existence checks,
  - raw event aliases during transition,
  - source-column detection,
  - typed empty optional inputs.

Validated builds:

```text
ACME4 / older Windows raw_sensor sample
/home/ubuntu/data/lintap/lintap-dev/ACME4

LINTAP / newer Linux raw_sensor sample
/home/ubuntu/data/debug/parquet
```

The LINTAP build exposed schema drift that has now been handled in DBT:

- missing `raw_process_registry` -> typed empty model,
- missing `raw_imageload` -> typed empty model,
- missing `ProcessArgs` -> synthesized from `CommandLine`,
- missing `UniqueProcessKey` -> synthesized as `NULL`.

## uv conversion

`Wintap-PyUtil` has been converted to uv project management:

- Added `pyproject.toml`.
- Added `dbt-duckdb` as a project dependency.
- Updated `Makefile` to use `uv run`.
- Added DBT Makefile targets:
  - `make dbt-debug`
  - `make dbt-build`
  - `make dbt-test`
  - `make dbt-docs`
- Added `DBT_VARS` Makefile support.

Example DBT build:

```sh
cd Wintap-PyUtil

WINTAP_DBT_DATABASE=/tmp/debug-test.duckdb \
DBT_VARS='{dataset: /home/ubuntu/data/debug/parquet, start_day: 20260520, end_day: 20260520}' \
make dbt-build
```

## Sensor-side raw event naming cleanup

The .NET source has been updated so host metadata cache folders are named closer to canonical raw event names:

- `host_sensor` -> `host`
- `macip_sensor` -> `macip`

This means the raw writer naturally produces:

- `raw_host`
- `raw_macip`

instead of relying on downstream cleanup of `raw_host_sensor` / `raw_macip_sensor`.

Defensive compatibility mappings still exist for stale legacy files.

## Startup visibility

The .NET application startup output now preserves the existing ASP.NET hosting messages and adds data-path visibility:

```text
Wintap parquet data path: .../parquet
Wintap raw_sensor data path: .../parquet/raw_sensor
```

The same paths are also written to `WintapLogger`.

## Current limitations

Still not complete:

- DBT parquet export is not implemented; DBT currently builds to a DuckDB database.
- Optional enrichment inputs are represented as typed empty stubs unless/until label/Sigma/MITRE/LOLBAS source loading is wired into DBT.
- Old Python ETL commands still exist and are not yet wrappers around DBT.
- The legacy `merged` tooling still exists in code/docs, but is no longer the desired normal path.
- Already-collected LINTAP data may use `proto`; new source output uses the canonical `protoPK` partition.
