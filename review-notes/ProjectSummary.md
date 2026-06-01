# Project Summary

## Goal

Build and harden a robust, outsider-friendly Wintap/Lintap data pipeline.

The pipeline should start from canonical `raw_sensor` parquet and produce analysis-ready DuckDB/Spark summary models. DBT is now the primary canonical ETL engine. Older Python and TeleTap ETL paths remain only as legacy/reference/development scaffolding unless explicitly revived.

Current POC milestone: dbt can read `raw_process` from S3 through the Ilum Spark connector and materialize a Spark table. We are ready to expand testing from the POC model to all available Bronze/Silver/Gold models.

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
  -> DuckDB analysis database or Spark catalog tables
  -> SQL / notebooks / future parquet export
```

## Key decisions

- `raw_sensor` is the only canonical input to post-processing.
- `merged` is deprecated and should not be part of the normal pipeline.
- DBT in `Wintap-PyUtil/wintap_dbt` owns canonical ETL transformations.
- DBT currently supports DuckDB and Spark targets; full-graph Spark compatibility is in progress.
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
  - S3/Spark POC sample: `s3a://ilum-data/lintap/raw_sensor` via `sc://spark.acme.dev:15002`
- `Wintap-PyUtil` converted toward `uv` project management.
- Makefile DBT targets added:
  - `make dbt-debug`
  - `make dbt-build`
  - `make dbt-test`
  - `make dbt-docs`
- Makefile validates environment-driven DBT config from `WINTAP_DATA_ROOT` and derived `WINTAP_DBT_*` variables.
- .NET source naming fixed so new metadata emits `raw_host` and `raw_macip`.
- .NET raw network partition output changed to canonical `protoPK`.
- Legacy `mergedtoraw.py` scripts removed.
- .NET startup output now includes parquet and `raw_sensor` data paths.

## Known current limitations

- DBT parquet export is not implemented yet.
- Full Spark compatibility for every Silver/Gold model is not complete; likely issues include remaining `group by all`, recursive `process_path`, and Linux/Windows schema drift.
- Optional enrichment sources are not fully wired into DBT; current models are typed empty stubs.
- Old Python console scripts (`rawtorolling`, `rawtostdview`, `ubersummary`) remain in the package as legacy/reference tools.
- Some documentation and analytics examples may still expect historical `stdview-*` parquet directories.
- New LINTAP/Wintap network data must use `protoPK=` partitions.

## Useful command pattern

```sh
cd Wintap-PyUtil

source wintap-run.env
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
