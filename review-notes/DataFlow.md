# Data Flow: Raw Sensor Parquet to Analysis

## Executive summary

The canonical pipeline is now DBT-first:

```text
Wintap/Lintap sensor parquet
  -> canonical raw_sensor layout, local or S3
  -> DBT bronze models
  -> DBT silver models
  -> DBT gold models
  -> DuckDB analysis database or Spark catalog tables
  -> notebooks / SQL / future parquet export
```

Legacy Python ETL commands (`rawtorolling`, `rawtostdview`, `ubersummary`) and TeleTap scripts remain in the tree, but are no longer the preferred path for new processing.

Current Spark/S3 POC status: `poc_s3_raw_process` passes against `s3a://ilum-data/lintap/raw_sensor` using Spark Connect at `sc://spark.acme.dev:15002`. The next validation step is running all available Bronze/Silver/Gold model subsets against that same S3 sample.

## Stage 0: Sensor parquet production

In `wintap/`, the .NET runtime batches telemetry and writes parquet:

- `core/etl/extract/Serializer.cs`
  - Queues event objects from Esper/ETL serializers.
  - Periodically flushes queue contents to `ParquetWriter`.
- `core/etl/load/ParquetWriter.cs`
  - Writes per-sensor parquet files with `.parquet.active` during writes, then renames to `.parquet`.
- `core/etl/load/Merge.cs`
  - Consolidates per-sensor parquet files and materializes them through `RawSensorWriter`.
- `core/etl/load/RawSensorWriter.cs`
  - Writes directly to canonical `raw_sensor` with DuckDB:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/<file>.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp/<file>.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=udp/<file>.parquet
```

The old flat `merged` directory is deprecated. The legacy `mergedtoraw.py` scripts have been removed.

## Stage 1: Canonical raw input contract

`raw_sensor` is the only canonical input to post-processing.

Canonical partition columns:

| Partition | Meaning | Example |
| --- | --- | --- |
| `dayPK` | data capture date | `dayPK=20260520` |
| `hourPK` | data capture UTC hour | `hourPK=14` |
| `protoPK` | network protocol partition | `protoPK=tcp` |

Canonical core raw event names:

- `raw_host`
- `raw_macip`
- `raw_process`
- `raw_process_conn_incr`
- `raw_process_file`
- `raw_process_registry`
- `raw_imageload`

Known stale event aliases retained only where explicitly implemented:

- `raw_host_sensor` -> `raw_host`
- `raw_macip_sensor` -> `raw_macip`
- `raw_file` -> `raw_process_file`
- `raw_processstop` -> `raw_process`
- `raw_registry` -> `raw_process_registry`
Network protocol partitions are not aliased: DBT expects `protoPK=` for new data.

New source output should use canonical names rather than relying on these aliases.

## Stage 2: DBT bronze

DBT project location:

```text
Wintap-PyUtil/wintap_dbt/
```

Bronze models read `raw_sensor` parquet from local files, S3 through DuckDB/httpfs, or S3 through Spark. They normalize raw-source drift and own compatibility shims so downstream models can depend on stable columns and relations.

Implemented bronze behavior includes:

- raw event existence checks,
- raw event aliases during transition,
- optional typed empty `stg_raw_imageload`,
- optional typed empty `stg_raw_process_registry`,
- synthesized `ProcessArgs` from `CommandLine` when absent,
- synthesized `UniqueProcessKey` as `NULL` when absent,
- date filtering by `dayPK`.

## Stage 3: DBT silver

Silver models are normalized detail models, broadly replacing the old `rawtostdview.sql` detail-table phase.

Current silver outputs include:

- `host`
- `host_ip`
- `process`
- `process_conn_incr`
- `process_net_conn`
- `process_file`
- `process_registry`
- `process_image_load`
- `process_exe_file_summary`
- `files_tmp_v1`
- `files`
- `all_files`
- `process_path`

## Stage 4: DBT gold

Gold models are process-centric analyst-facing summaries, broadly replacing the old `process_summary.sql` and `ubersummary.py` phase.

Current gold outputs include:

- `process_registry_summary`
- `process_file_summary`
- `process_net_summary`
- `process_image_load_summary`
- `process_summary`
- `process_uber_summary`

Optional enrichment inputs currently build as typed empty stubs:

- `labels_graph_process_summary`
- `process_lolbas_summary`
- `process_mitre_summary`
- `sigma_labels_summary`

Future work should wire real label, LOLBAS/LOLC, MITRE, and Sigma inputs into DBT.

## Stage 5: Current output

The current official DBT output is a DuckDB database.

The output path is controlled by:

```text
WINTAP_DBT_DATABASE
```

Normal runs should define this through `wintap-run.env`.

Useful run command:

```sh
cd Wintap-PyUtil

source wintap-run.env
make dbt-build
```

Useful inspection queries:

```sql
select * from build_summary order by model_name;
select count(*) from process_summary;
select count(*) from process_uber_summary;
```

## Stage 6: Analytics consumption

`wintap-analytics` notebooks and workshop material consume analysis-ready objects such as:

- `process`
- `process_file`
- `process_net_conn`
- `process_path`
- `process_summary`
- `process_uber_summary`

Today, some analytics examples still expect published/local `stdview-*` parquet directories. DBT parquet export is not yet implemented, so those examples may need either:

- a DBT-built DuckDB database, or
- a future export wrapper that writes DBT outputs to expected parquet directories.

## Legacy Python ETL reference

The historical Wintap-PyUtil flow was:

```text
raw_sensor -> rawtorolling -> rolling -> rawtostdview -> stdview-* -> ubersummary -> process_uber_summary
```

Relevant legacy files:

- `wintappy/etlutils/rawtorolling.py`
- `wintappy/etlutils/rawtostdview.py`
- `wintappy/etlutils/ubersummary.py`
- `wintappy/datautils/rawtostdview.sql`
- `wintappy/datautils/process_summary.sql`
- `wintappy/datautils/uber_summary.sql`

These are useful as reference implementations and compatibility tools, but DBT should be used for new ETL work.

## TeleTap development scaffold

`Lintap/teletap` remains a narrower Linux-focused sanity-check and visualization scaffold:

```text
raw_sensor -> small local DuckDB tables -> summary/chart views -> Streamlit
```

It is not the canonical full pipeline and should not reintroduce `merged` as a required stage.
