# Wintap dbt/Spark POC status and next steps

## Current milestone

A working POC milestone was committed as:

```text
7452604 Add Ilum Spark dbt POC milestone
```

That milestone demonstrates:

- dbt can use the Spark adapter against the Ilum Spark connector over Spark Connect/gRPC;
- Spark can read Wintap/Lintap raw Parquet from S3;
- dbt can materialize a table in the Spark catalog/schema;
- DuckDB/local development remains available as a dbt target.

## Current S3 sample dataset

The current uploaded sample is:

```text
s3a://ilum-data/lintap/raw_sensor
```

Available event folders:

```text
raw_host
raw_process
raw_macip
raw_process_conn_incr
raw_process_file
```

The dbt dataset variable should normally point to the parent of `raw_sensor`:

```sh
export WINTAP_DBT_DATASET=s3a://ilum-data/lintap
```

The path macros also accept the raw root directly:

```sh
export WINTAP_DBT_DATASET=s3a://ilum-data/lintap/raw_sensor
```

Known day partition in the current sample:

```text
20260530
```

## Validated POC command

```sh
export WINTAP_DBT_DATASET=s3a://ilum-data/lintap
export WINTAP_DBT_START_DAY=20260530
export WINTAP_DBT_END_DAY=20260530
export WINTAP_DBT_INCLUDE_MONITORING=False
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

Verification:

```sh
uv run --isolated --dev --project . dbt show \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --inline 'select count(*) as rows, sum(num_events) as source_rows from {{ target.schema }}.poc_s3_raw_process'
```

Expected current result:

```text
rows        728442
source_rows 728442
```

## Current target matrix

| Target | Input | Output | Status |
| --- | --- | --- | --- |
| `spark` | S3 raw Parquet | Spark catalog table | Passing for `poc_s3_raw_process`. Ready for broader model testing. |
| `duckdb_s3` | S3 raw Parquet | Local DuckDB table | Profile/debug passes. Actual S3 read requires local credentials for `ilum-data`. |
| `dev` | Local raw Parquet | Local DuckDB table | Existing local path. Not currently retested because local Parquet sample is unavailable. |
| `ilum` | S3 raw Parquet | Spark/Kyuubi table | Configured for Thrift/Kyuubi, but current live connector is Spark Connect/gRPC, so use `spark`. |

## Ready-to-try broader Spark build

The next goal is to try all available tables/models against the S3 sample using the `spark` target.

Because the current sample does **not** include all optional event families, set the available raw-event allow-list so Spark does not try to read missing S3 paths during optional model compilation/execution:

```sh
export WINTAP_DBT_AVAILABLE_RAW_EVENTS=raw_host,raw_process,raw_macip,raw_process_conn_incr,raw_process_file
```

Recommended staged order:

```sh
# 1. Confirm profile/connection.
uv run --isolated --dev --project . dbt debug \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark

# 2. Build only bronze models first.
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --select path:models/bronze

# 3. Build core silver models that depend on available raw events.
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --select process host host_ip process_conn_incr process_net_conn process_file files all_files

# 4. Then try gold summaries.
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark \
  --select process_summary process_file_summary process_net_summary process_uber_summary
```

After those pass, try the entire graph:

```sh
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target spark
```

## Expected issues when trying the full graph

These are known or likely compatibility issues that were intentionally delayed during the POC.

### 1. Remaining `group by all`

Several Silver/Gold models still use DuckDB-friendly `group by all`. Spark compatibility must be verified and likely replaced with explicit `group by` lists or target-dispatched helpers.

Find them with:

```sh
rg "group by all" wintap_dbt/models wintap_dbt/macros
```

### 2. Recursive process path

`process_path.sql` uses recursive CTEs and DuckDB-style list/struct expressions. Spark may need a rewrite using Spark arrays/structs or an alternate iterative strategy.

Delayed because it is not required for the minimal S3/Spark read/write proof.

### 3. Optional missing raw events

The current S3 sample lacks at least:

```text
raw_process_registry
raw_imageload / raw_image_load
```

The Spark target now supports:

```sh
WINTAP_DBT_AVAILABLE_RAW_EVENTS=...
```

to allow optional missing sources to compile to typed empty models where those models already have empty fallbacks.

### 4. Linux-vs-Windows schema drift

The current S3 sample is Linux/Lintap-like. It uses `CommandLine`; some legacy Windows models expect `ProcessArgs`. Bronze compatibility currently synthesizes `ProcessArgs` for Spark process rows, but more drift may appear in file/network/host columns.

### 5. DuckDB S3 credentials

Local DuckDB does not inherit Ilum Spark connector credentials. The `duckdb_s3` target is implemented, but the actual S3 read needs local credentials for `ilum-data`.

### 6. Monitoring models

Monitoring was disabled for the POC:

```sh
export WINTAP_DBT_INCLUDE_MONITORING=False
```

Re-enable only after core Bronze/Silver/Gold models are stable on Spark.

### 7. Pidstat

Pidstat CSV loading is optional and not part of the Spark/S3 POC. Leave it empty unless a Spark-readable pidstat path is provided.

### 8. Enrichment stubs

Label/Sigma/MITRE/LOLBAS inputs are still typed empty stubs. This is expected and should not block core process/network/file summaries.

## Delayed work

- Full external Parquet export from dbt outputs.
- `wintap-etl build` wrapper command.
- Full Spark compatibility rewrite for every model.
- Catalog-backed external table/source definitions for raw S3 inputs.
- Production Ilum REST job payload hardening.
- Backdated/incremental partition reprocessing.
- Complete schema inventory for every raw event and optional source.

## Practical next step

Run the staged Spark build with the S3 sample and fix failures in dependency order:

1. Bronze raw models.
2. Core Silver models.
3. Gold process/file/network summaries.
4. `process_uber_summary`.
5. Optional monitoring and tests.

Keep each fix adapter-dispatched where possible so DuckDB local development remains viable.
