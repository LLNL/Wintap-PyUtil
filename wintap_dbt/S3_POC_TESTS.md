# S3-backed dbt POC test matrix

This document records the current proof-of-concept permutations for reading Wintap raw Parquet from S3 and materializing dbt output with DuckDB or Spark.

## Test data

Current sample input:

```text
s3a://ilum-data/lintap/raw_sensor
```

Available raw event folders:

```text
raw_host
raw_process
raw_macip
raw_process_conn_incr
raw_process_file
```

The dbt project variable should point to the parent of `raw_sensor`:

```sh
export WINTAP_DBT_DATASET=s3a://ilum-data/lintap
```

The path macros also tolerate passing the raw root directly:

```sh
export WINTAP_DBT_DATASET=s3a://ilum-data/lintap/raw_sensor
```

## Common variables

```sh
export WINTAP_DBT_DATASET=s3a://ilum-data/lintap
export WINTAP_DBT_START_DAY=20260530
export WINTAP_DBT_END_DAY=20260530
export WINTAP_DBT_INCLUDE_MONITORING=False
```

The POC model is:

```text
wintap_dbt/models/bronze/poc_s3_raw_process.sql
```

It reads `raw_process` from S3 and materializes a table containing grouped process rows.

## Permutation 1: S3 -> Spark catalog/table

This is the currently validated end-to-end path. It uses the Spark Connect/gRPC endpoint from the Ilum Spark connector.

```sh
export WINTAP_DBT_SCHEMA=wintap_smoke
export SPARK_CONNECT_HOST=spark.acme.dev
export SPARK_CONNECT_PORT=15002
export SPARK_REMOTE=sc://spark.acme.dev:15002

uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --select poc_s3_raw_process
```

Verify the materialized table:

```sh
uv run --isolated --dev --project . dbt show \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --inline 'select count(*) as rows, sum(num_events) as source_rows from {{ target.schema }}.poc_s3_raw_process'
```

Expected current result:

```text
rows       = 728442
source_rows = 728442
```

A non-fatal Spark Connect warning may appear:

```text
[CANNOT_MODIFY_STATIC_CONFIG] Cannot modify the value of the static Spark config: "spark.sql.catalogImplementation"
```

## Permutation 2: S3 -> local DuckDB database

This path is implemented as a dbt target, but it requires local DuckDB to have credentials for `s3://ilum-data/...`. The Ilum Spark connector has server-side object-storage credentials, but those credentials are not automatically available to local DuckDB.

Target:

```text
duckdb_s3
```

Run:

```sh
export WINTAP_DBT_DATABASE=/tmp/wintap-s3.duckdb

uv run --isolated --dev --project . dbt debug \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target duckdb_s3

uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target duckdb_s3 \
  --select poc_s3_raw_process
```

### Credential requirements

DuckDB's `httpfs` extension uses local AWS/S3 credentials. Provide credentials with one of these mechanisms:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...        # if needed
export AWS_REGION=us-west-2         # or the region used by the bucket
```

or configure an AWS profile and select it:

```sh
export AWS_PROFILE=<profile-with-ilum-data-access>
export AWS_REGION=us-west-2
```

For S3-compatible endpoints, also set the endpoint variables expected by DuckDB/AWS tooling, for example:

```sh
export AWS_ENDPOINT_URL_S3=https://<s3-compatible-endpoint>
export AWS_S3_URL_STYLE=path
```

### Current local status

`dbt debug --target duckdb_s3` passes, confirming the profile and DuckDB adapter are valid.

The actual S3 read is currently blocked in this local environment because DuckDB does not have valid credentials for `ilum-data`. Spark succeeds because the Ilum Spark connector supplies object-storage credentials server-side.

Once local credentials are available, this target should exercise the same POC model and write the result to the local DuckDB file configured by `WINTAP_DBT_DATABASE`.

## Notes on S3 URI schemes

Spark expects `s3a://` paths.

DuckDB accepts S3 paths through `httpfs`; the project macros normalize `s3a://` to `s3://` for DuckDB targets while preserving `s3a://` for Spark targets.

## Trying all available models

Before expanding beyond the POC model, set the raw-event allow-list so optional missing event families use typed empty fallbacks instead of Spark trying to read absent S3 paths:

```sh
export WINTAP_DBT_AVAILABLE_RAW_EVENTS=raw_host,raw_process,raw_macip,raw_process_conn_incr,raw_process_file
```

Recommended staged test order:

```sh
# Bronze first.
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --select path:models/bronze

# Core silver next.
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --select process host host_ip process_conn_incr process_net_conn process_file files all_files

# Then process-centric gold summaries.
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --select process_summary process_file_summary process_net_summary process_uber_summary
```

Known likely failures when expanding are tracked in `STATUS_AND_NEXT_STEPS.md`.

## Current matrix

| Input | Target | Output | Status |
| --- | --- | --- | --- |
| S3 raw Parquet | `spark` | Spark catalog table | Passing |
| S3 raw Parquet | `duckdb_s3` | Local DuckDB table | Implemented; blocked until local S3 credentials are available |
