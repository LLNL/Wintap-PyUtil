# Wintap DBT Pipeline

DBT/DuckDB implementation of the canonical Wintap/Lintap post-processing pipeline.

## Scope

Input is canonical `raw_sensor` parquet:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/*.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp|udp/*.parquet
```

## Configuration

DBT configuration is environment-driven. The intended single source of truth is the run root:

```text
WINTAP_DATA_ROOT
```

The DBT-specific values are derived from that root in `../wintap-run.env`:

```text
WINTAP_DBT_DATASET=$WINTAP_DATA_ROOT/parquet
WINTAP_DBT_DATABASE=$WINTAP_DATA_ROOT/duckdb/wintap.duckdb
WINTAP_DBT_START_DAY=YYYYMMDD
WINTAP_DBT_END_DAY=YYYYMMDD
```

`dbt_project.yml` requires:

- `WINTAP_DBT_DATASET`
- `WINTAP_DBT_START_DAY`
- `WINTAP_DBT_END_DAY`

`profiles.yml` requires:

- `WINTAP_DBT_DATABASE`

## Running

Use the Makefile from the repository root:

```sh
cd Wintap-PyUtil
cp wintap-run.env.example wintap-run.env
# edit wintap-run.env
source wintap-run.env
make dbt-build
make dbt-test
make qa-pid-hash
```

## Layers

- `models/bronze` — raw parquet scans and compatibility layer.
- `models/silver` — normalized detail tables and process paths.
- `models/gold` — process summaries and `process_uber_summary`.
- `models/monitoring` — build/row-count monitoring.

## Current status

Implemented:

- Bronze models for core raw events.
- Silver models ported from legacy detail SQL and process-path logic.
- Gold models ported from legacy summary SQL.
- `process_uber_summary` with typed empty enrichment stubs.
- Schema tests for core keys and canonical network partitions.
- QA script for `pid_hash` primary/foreign-key orphan checks.

## Known limitations

- This is not yet wired to a `wintap-etl` wrapper command.
- Parquet export is not implemented yet; DBT builds into DuckDB.
- Label/Sigma/MITRE/LOLBAS source loading is stubbed for now.
- The historical `merged` flow is intentionally not supported here.
