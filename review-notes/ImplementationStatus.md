# Implementation Status

This file captures what changed after the initial review/planning pass.

## Location

The review notes now live in the `Wintap-PyUtil` repository:

```text
Wintap-PyUtil/review-notes/
```

This avoids creating a separate documentation repository while keeping the data-pipeline planning material close to the canonical ETL code.

## DBT pipeline

A first DBT dual-engine implementation now exists in:

```text
Wintap-PyUtil/wintap_dbt/
```

Implemented so far:

- DBT project skeleton with DuckDB and Spark profiles.
- Bronze raw-source models for core `raw_sensor` events.
- Silver/detail models ported from `rawtostdview.sql`.
- Recursive `process_path` model ported from `process_path.sql`.
- Gold process summary models ported from `process_summary.sql`.
- `process_uber_summary` model with currently stubbed optional enrichment inputs.
- Basic DBT schema tests.
- `build_summary` monitoring view.
- Raw-source macros for:
  - raw event existence checks,
  - Spark raw event allow-listing with `WINTAP_DBT_AVAILABLE_RAW_EVENTS`,
  - raw event aliases during transition,
  - source-column detection,
  - typed empty optional inputs.

Validated builds and POCs:

```text
ACME4 / older Windows raw_sensor sample
/home/ubuntu/data/lintap/lintap-dev/ACME4

LINTAP / newer Linux raw_sensor sample
/home/ubuntu/data/debug/parquet

S3/Spark Connect POC sample
s3a://ilum-data/lintap/raw_sensor
sc://spark.acme.dev:15002
poc_s3_raw_process -> 728442 rows
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
- Replaced normal `DBT_VARS` usage with environment-driven config based on `WINTAP_DATA_ROOT` and derived `WINTAP_DBT_*` variables.

Example DBT build:

```sh
cd Wintap-PyUtil

source wintap-run.env
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

- Full Spark graph validation is not complete; next work is trying all available Bronze/Silver/Gold models against the S3 sample.
- DBT parquet export is not implemented; DBT currently builds to a DuckDB database or Spark catalog tables depending on target.
- Optional enrichment inputs are represented as typed empty stubs unless/until label/Sigma/MITRE/LOLBAS source loading is wired into DBT.
- Old Python ETL commands still exist and are not yet wrappers around DBT.
- The legacy `merged` tooling still exists in code/docs, but is no longer the desired normal path.
- Already-collected LINTAP data may use `proto`; new source output uses the canonical `protoPK` partition.
- Local DuckDB reads from `s3://ilum-data/...` require local credentials; Ilum Spark succeeds because object-storage credentials are supplied server-side by the connector.
