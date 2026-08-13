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

With the Makefile, DBT-specific values are derived from that root when unset:

```text
WINTAP_DBT_DATASET=$WINTAP_DATA_ROOT/parquet
WINTAP_DBT_RAW_SENSOR_DATASET=$WINTAP_DBT_DATASET
WINTAP_DBT_DATABASE=$WINTAP_DATA_ROOT/duckdb/wintap.duckdb
WINTAP_DBT_START_DAY=<minimum discovered dayPK>
WINTAP_DBT_END_DAY=<maximum discovered dayPK>
PIDSTAT_DATA_PATH=$WINTAP_DATA_ROOT/pidstat
```

You can still export any of those variables to override the defaults. DBT itself requires `WINTAP_DBT_DATASET`, `WINTAP_DBT_START_DAY`, `WINTAP_DBT_END_DAY`, and `WINTAP_DBT_DATABASE`; the Makefile fills them from `WINTAP_DATA_ROOT` for normal runs.

For split I/O, keep `WINTAP_DBT_DATABASE` local and point `WINTAP_DBT_RAW_SENSOR_DATASET` at the parquet dataset root that contains `raw_sensor/`. If that raw dataset lives on S3, the dbt DuckDB profile now loads `httpfs` and configures an S3 secret from the usual AWS environment variables. For Garage/path-style endpoints, also set `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION` or `AWS_REGION`, and `DUCKDB_USE_SSL`.

Raw parquet scans also narrow their file globs to the requested `dayPK=` partitions from `WINTAP_DBT_START_DAY` through `WINTAP_DBT_END_DAY`, which helps when reading a small window out of a much larger remote dataset. The compile-time raw-event alias and optional-column checks use the same narrowed day partition set instead of probing the full event prefix.

If `WINTAP_DBT_START_HOUR` and/or `WINTAP_DBT_END_HOUR` are set, dbt further narrows raw scans to `hourPK=HH` partitions within the inclusive `start day/hour` to `end day/hour` window. When omitted, all hours in the requested day range are included.

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
make qa-dashboard
```

For the minimal workflow, only `WINTAP_DATA_ROOT` is required:

```sh
export WINTAP_DATA_ROOT=/path/to/run-root
make dbt-build
make qa-dashboard
```

Pidstat CSV data is optional. If `$PIDSTAT_DATA_PATH` or `$WINTAP_DATA_ROOT/pidstat` contains CSV files, DBT loads them into `pidstat_metrics`; otherwise the model is built as an empty typed table.

## Layers

- `models/bronze` — raw parquet scans and compatibility layer.
- `models/silver` — normalized detail tables and process paths.
- `models/gold` — process summaries and `process_uber_summary`.
- `models/monitoring` — build/row-count monitoring, TeleTap-style event/chart summaries, and dashboard inputs.

## Current status

Implemented:

- Bronze models for core raw events.
- Silver models ported from legacy detail SQL and process-path logic.
- Gold models ported from legacy summary SQL.
- `process_uber_summary` with typed empty enrichment stubs.
- Schema tests for core keys and canonical network partitions.
- Optional pidstat CSV loading into DBT.
- TeleTap-style summary/chart views in DBT monitoring models.
- QA script for `pid_hash` primary/foreign-key orphan checks.
- Marimo QA/overview dashboard.

## Known limitations

- This is not yet wired to a `wintap-etl` wrapper command.
- Parquet export is not implemented yet; DBT builds into DuckDB.
- Label/Sigma/MITRE/LOLBAS source loading is stubbed for now.
- The historical `merged` flow is intentionally not supported here.
