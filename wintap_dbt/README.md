# Wintap DBT Pipeline

Experimental DBT/DuckDB implementation of the Wintap post-processing pipeline.

## Scope

Input is canonical `raw_sensor` parquet:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/*.parquet
```

## Quick start with sample data

```sh
cd Wintap-PyUtil/wintap_dbt
DBT_PROFILES_DIR=. uvx --python 3.12 --from 'dbt-duckdb<1.10' dbt build \
  --vars '{dataset: /home/ubuntu/data/lintap/lintap-dev/ACME4, start_day: 20240904, end_day: 20240904}'
```

The default profile writes DuckDB state to:

```text
./target/wintap.duckdb
```

Override with:

```sh
export WINTAP_DBT_DATABASE=/tmp/wintap.duckdb
```

## Layers

- `models/bronze` — raw parquet scans and compatibility layer.
- `models/silver` — normalized detail tables and process paths.
- `models/gold` — process summaries and `process_uber_summary`.

## Current status

Phases 1-3 initial implementation:

- Bronze models for core raw events.
- Silver models ported from `rawtostdview.sql` and `process_path.sql`.
- Gold models ported from `process_summary.sql` and `uber_summary.sql`.
- Enrichment summaries are currently typed empty stubs unless/until label/lookup sources are wired into DBT.

## Known limitations

- This is not yet wired to a `wintap-etl` wrapper command.
- Parquet export is not implemented yet; DBT builds into DuckDB for validation.
- Optional missing raw event types still need robust empty typed source macros.
- Label/Sigma/MITRE/LOLBAS source loading is stubbed for now.
- The historical `merged` flow is intentionally not supported here.
