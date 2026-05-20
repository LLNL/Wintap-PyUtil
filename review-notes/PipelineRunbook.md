# Pipeline Runbook

This is the current manual runbook for the canonical DBT-based Wintap/Lintap ETL path.

## Inputs

A dataset directory containing canonical raw sensor parquet:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/<file>.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp/<file>.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=udp/<file>.parquet
```

`dayPK`, `hourPK`, and `protoPK` are the desired canonical Hive partition names.

DBT expects canonical `protoPK=` network partitions. Datasets that only contain `proto=` should be regenerated from the updated source output.

## Build into DuckDB

```sh
cd Wintap-PyUtil

WINTAP_DBT_DATABASE=/tmp/wintap-debug.duckdb \
DBT_VARS='{dataset: /path/to/dataset, start_day: 20260520, end_day: 20260520}' \
make dbt-build
```

The default DBT database path, if `WINTAP_DBT_DATABASE` is not set, is:

```text
Wintap-PyUtil/wintap_dbt/target/wintap.duckdb
```

## Run tests only

```sh
cd Wintap-PyUtil

WINTAP_DBT_DATABASE=/tmp/wintap-debug.duckdb \
DBT_VARS='{dataset: /path/to/dataset, start_day: 20260520, end_day: 20260520}' \
make dbt-test
```

## Inspect output

```sh
duckdb /tmp/wintap-debug.duckdb
```

Useful checks:

```sql
select * from build_summary order by model_name;
select count(*) from process;
select count(*) from process_summary;
select count(*) from process_uber_summary;
select * from process_summary limit 10;
```

## Generate DBT docs

```sh
cd Wintap-PyUtil
make dbt-docs
```

DBT docs artifacts are generated under:

```text
Wintap-PyUtil/wintap_dbt/target/
```

## Known validated examples

ACME4 Windows sample:

```sh
cd Wintap-PyUtil

WINTAP_DBT_DATABASE=/tmp/acme4.duckdb \
DBT_VARS='{dataset: /home/ubuntu/data/lintap/lintap-dev/ACME4, start_day: 20240904, end_day: 20240904}' \
make dbt-build
```

LINTAP Linux sample:

```sh
cd Wintap-PyUtil

WINTAP_DBT_DATABASE=/tmp/lintap.duckdb \
DBT_VARS='{dataset: /home/ubuntu/data/debug/parquet, start_day: 20260520, end_day: 20260520}' \
make dbt-build
```

Expected successful build shape at time of writing:

```text
PASS=46 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=46
```

## Legacy commands

These old commands remain in `pyproject.toml` for compatibility/reference, but DBT should be used for new ETL work:

- `rawtorolling`
- `rawtostdview`
- `ubersummary`

The legacy `mergedtoraw.py` scripts have been removed. New data should already be materialized as `raw_sensor` by the sensor/.NET side.
