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
WINTAP_DBT_DATABASE=$WINTAP_DATA_ROOT/duckdb/wintap.duckdb
WINTAP_DBT_START_DAY=<minimum discovered dayPK>
WINTAP_DBT_END_DAY=<maximum discovered dayPK>
PIDSTAT_DATA_PATH=$WINTAP_DATA_ROOT/pidstat
```

You can still export any of those variables to override the defaults. DBT itself requires `WINTAP_DBT_DATASET`, `WINTAP_DBT_START_DAY`, `WINTAP_DBT_END_DAY`, and `WINTAP_DBT_DATABASE`; the Makefile fills them from `WINTAP_DATA_ROOT` for normal runs.

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

## Ilum / Spark proof of concept

This project can also be pointed at an Ilum-managed Spark service. The current
validated path uses Spark Connect/gRPC via the `spark` dbt target; the older
`ilum` target remains for future Thrift/Kyuubi testing.

The POC path is intentionally narrow: prove that dbt can read `raw_process` from
S3 and materialize a Spark table in the configured catalog/schema. That POC is
passing against:

```text
SPARK_REMOTE=sc://spark.acme.dev:15002
WINTAP_DBT_DATASET=s3a://ilum-data/lintap
WINTAP_DBT_START_DAY=20260530
WINTAP_DBT_END_DAY=20260530
```

See also:

```text
wintap_dbt/S3_POC_TESTS.md
wintap_dbt/STATUS_AND_NEXT_STEPS.md
```

Install the Spark adapter before running against Ilum:

```sh
uv sync --dev
```

Configure the dbt profile through environment variables. S3 credentials are not
stored in `profiles.yml`; Ilum should inject object-storage credentials into the
Spark driver/executors, for example through the `ilum-objectstorage` alias.

```sh
export WINTAP_DBT_TARGET=spark
export DBT_TARGET=spark
export WINTAP_DBT_DATASET=s3a://ilum-data/lintap
export WINTAP_DBT_START_DAY=20260530
export WINTAP_DBT_END_DAY=20260530
export WINTAP_DBT_SCHEMA=wintap_smoke
export WINTAP_DBT_INCLUDE_MONITORING=False
export WINTAP_DBT_AVAILABLE_RAW_EVENTS=raw_host,raw_process,raw_macip,raw_process_conn_incr,raw_process_file

export SPARK_CONNECT_HOST=spark.acme.dev
export SPARK_CONNECT_PORT=15002
export SPARK_REMOTE=sc://spark.acme.dev:15002

# For a future Thrift/Kyuubi endpoint instead, use target=ilum and set:
# export ILUM_KYUUBI_HOST=<kyuubi-service-host>
# export ILUM_KYUUBI_PORT=10009
# export ILUM_KYUUBI_METHOD=thrift
```

Run the end-to-end POC model directly:

```sh
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --select poc_s3_raw_process
```

Or through the Makefile:

```sh
make dbt-build DBT_TARGET=spark DBT_SELECT=poc_s3_raw_process
```

The POC model is:

```text
wintap_dbt/models/bronze/poc_s3_raw_process.sql
```

It reads:

```text
$WINTAP_DBT_DATASET/raw_sensor/raw_process
```

using Spark's Parquet data source and writes a managed table via dbt-spark.

### Trying broader model sets

The current S3 sample includes `raw_host`, `raw_process`, `raw_macip`,
`raw_process_conn_incr`, and `raw_process_file`. It does not include registry or
image-load data. Before trying broader Spark builds, set:

```sh
export WINTAP_DBT_AVAILABLE_RAW_EVENTS=raw_host,raw_process,raw_macip,raw_process_conn_incr,raw_process_file
```

Recommended staged commands are documented in `STATUS_AND_NEXT_STEPS.md`.

### Submitting through the Ilum REST API

A configurable helper script is included:

```sh
export ILUM_API_URL=https://<ilum-api>
export ILUM_API_TOKEN=<token-if-required>
export ILUM_GIT_REPO=https://github.com/LLNL/Wintap-PyUtil.git
export ILUM_GIT_BRANCH=grants-add-dbt
export ILUM_DBT_COMMAND="dbt build --project-dir wintap_dbt --profiles-dir wintap_dbt --target ilum --select poc_s3_raw_process"

python3 wintap_dbt/scripts/submit_ilum_dbt_job.py
```

If your Ilum deployment expects a different `POST /api/v1/jobs` payload, provide
it exactly with either:

```sh
export ILUM_JOB_PAYLOAD_FILE=/path/to/payload.json
# or
export ILUM_JOB_PAYLOAD_JSON='{"name":"wintap-dbt-poc", ...}'
```

### Spark compatibility notes

Adapter-dispatched macros now cover the first set of DuckDB/Spark differences:

- raw Parquet relation syntax: `parquet_scan(...)` for DuckDB vs `parquet.\`path\`` for Spark;
- compile-time raw file checks are skipped for Spark because dbt may not have S3 credentials;
- Win32 timestamp conversion uses Spark `timestamp_micros`;
- list aggregation uses Spark `collect_set`/`sort_array`;
- 10-second monitoring buckets use Spark `window(...).start`.

The full graph is closer to Spark-compatible but remains POC-grade. The most
likely follow-up work is replacing remaining `group by all` instances and
validating recursive process-path SQL on the target Spark version.

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
