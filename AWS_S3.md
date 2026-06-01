# S3 notes

## Legacy upload layout

Older S3 upload tooling used upload-time partitions:

```text
{root}/{event type}/uploadDPK=YYYYMMDD/uploadHPK=HH/{parquet files}
```

Filenames were generally:

```text
{hostname}+{event type}-{collected timestamp in UTC epoch seconds}.parquet
```

Those notes remain relevant for ingestion/download tooling, but the dbt pipeline expects the canonical `raw_sensor` layout below.

## Canonical dbt S3 layout

The dbt pipeline reads capture-time partitions under `raw_sensor`:

```text
s3a://<bucket>/<prefix>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/*.parquet
s3a://<bucket>/<prefix>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp|udp/*.parquet
```

Current POC data:

```text
s3a://ilum-data/lintap/raw_sensor
```

Available folders at time of writing:

```text
raw_host
raw_process
raw_macip
raw_process_conn_incr
raw_process_file
```

## Engine-specific S3 behavior

Spark uses `s3a://` through Hadoop/S3A. The current Spark Connect endpoint is:

```text
sc://spark.acme.dev:15002
```

DuckDB uses the `httpfs` extension and local AWS/S3 credentials. The project macros normalize `s3a://` to `s3://` for DuckDB targets, but local credentials are still required. Ilum Spark connector credentials are not automatically available to local DuckDB.

## Current TODOs

- Document bucket creation and permissions for `ilum-data`-style datasets.
- Document Ceph/S3-compatible endpoint settings.
- Decide whether upload-time partitions remain only an ingestion concern or need a documented conversion path to capture-time `raw_sensor`.
- Add a small diagram covering upload layout vs dbt layout.

