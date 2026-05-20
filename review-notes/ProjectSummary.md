# Project Summary

## Goal

Build and harden a robust, outsider-friendly Wintap/Lintap data pipeline.

The pipeline should start from canonical `raw_sensor` parquet and produce analysis-ready DuckDB/summary models. DBT is now the primary canonical ETL engine. Older Python and TeleTap ETL paths remain only as legacy/reference/development scaffolding unless explicitly revived.

Secondary goals:

- Clean up raw event and partition naming drift at the source.
- Make the pipeline easy to run manually against custom datasets.
- Keep developer tooling modern and reproducible with `uv`.
- Keep review and planning documentation in `Wintap-PyUtil` rather than a separate repository.

## Current canonical flow

```text
Wintap/Lintap sensor
  -> parquet/raw_sensor/<event>/dayPK=YYYYMMDD/hourPK=HH/[protoPK=tcp|udp]
  -> Wintap-PyUtil/wintap_dbt
  -> DuckDB analysis database
  -> SQL / notebooks / future parquet export
```

## Key decisions

- `raw_sensor` is the only canonical input to post-processing.
- `merged` is deprecated and should not be part of the normal pipeline.
- DBT in `Wintap-PyUtil/wintap_dbt` owns canonical ETL transformations.
- DBT currently builds to DuckDB; parquet export is future work.
- Canonical raw partition names are `dayPK`, `hourPK`, and `protoPK`.
- New raw naming drift should be fixed at the source instead of adding long-lived downstream aliases.
- DBT bronze models should assume new canonical data where source cleanup has been completed.
- The .NET startup should preserve existing ASP.NET Core lifetime messages and add data-path visibility only.
- `Wintap-PyUtil` uses `uv` and `pyproject.toml` as active project configuration.

## Implemented so far

- Review notes moved into `Wintap-PyUtil/review-notes`.
- DBT project added under `Wintap-PyUtil/wintap_dbt`.
- DBT bronze, silver, gold, and monitoring models added for the core pipeline.
- DBT schema tests added.
- Empty typed DBT stubs added for optional enrichment inputs.
- DBT builds validated on:
  - ACME4 Windows sample: `/home/ubuntu/data/lintap/lintap-dev/ACME4`
  - LINTAP Linux sample: `/home/ubuntu/data/debug/parquet`
- `Wintap-PyUtil` converted toward `uv` project management.
- Makefile DBT targets added:
  - `make dbt-debug`
  - `make dbt-build`
  - `make dbt-test`
  - `make dbt-docs`
- Makefile supports custom DBT vars through `DBT_VARS`.
- .NET source naming fixed so new metadata emits `raw_host` and `raw_macip`.
- .NET raw network partition output changed to canonical `protoPK`.
- Legacy `mergedtoraw.py` scripts removed.
- .NET startup output now includes parquet and `raw_sensor` data paths.

## Known current limitations

- DBT parquet export is not implemented yet.
- Optional enrichment sources are not fully wired into DBT; current models are typed empty stubs.
- Old Python console scripts (`rawtorolling`, `rawtostdview`, `ubersummary`) remain in the package as legacy/reference tools.
- Some documentation and analytics examples may still expect historical `stdview-*` parquet directories.
- New LINTAP/Wintap network data must use `protoPK=` partitions.

## Useful command pattern

```sh
cd Wintap-PyUtil

WINTAP_DBT_DATABASE=/tmp/wintap-debug.duckdb \
DBT_VARS='{dataset: /path/to/dataset, start_day: 20260520, end_day: 20260520}' \
make dbt-build
```

Then inspect:

```sh
duckdb /tmp/wintap-debug.duckdb
```

Useful tables/views include:

- `build_summary`
- `process`
- `process_file`
- `process_net_conn`
- `process_path`
- `process_summary`
- `process_uber_summary`
