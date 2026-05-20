# TeleTap / Lintap Scaffold Notes

## Purpose

`Lintap/teletap` is a lightweight local analysis scaffold for Linux/TeleTap data. It is useful for validating that raw parquet is being produced, loading a subset of raw event types into DuckDB, correlating counts with host resource usage, and plotting simple time-series in Streamlit.

It is **not** the canonical Wintap/Lintap ETL path.

The canonical full pipeline is DBT in:

```text
Wintap-PyUtil/wintap_dbt/
```

## Current stance

TeleTap should consume `raw_sensor` directly. The old `merged` conversion path is removed/deprecated and should not be required for new data.

Desired local shape:

```text
~/data/lintap/lintap-dev/raw_sensor/<event>/dayPK=YYYYMMDD/hourPK=HH/[protoPK=...]
```

## Files reviewed

| File | Purpose |
| --- | --- |
| `README.md` | Developer workflow for Lintap build, data copy, DuckDB load, and Streamlit visualization. Some data-loading details are historical. |
| `process-data.sh` | Runs local DuckDB SQL scripts against existing `raw_sensor`; no longer runs `mergedtoraw.py`. |
| `initdb.sql` | Defines macros for data paths and time/IP conversions. |
| `load-data.sql` | Loads raw process/file/network parquet into DuckDB tables. |
| `load-pidstat.sql` | Loads pidstat TSV/CSV data into a typed table. |
| `summary_ddl.sql` | Creates event summary and charting views. |
| `summary.sql` | Prints tables, `event_summary`, and joined chart data. |
| `grokdata.py` | Streamlit/Plotly visualization of the chart data. |

## Current local workflow

As implemented after removing `mergedtoraw.py`:

```sh
cd Lintap/teletap
./process-data.sh [sample.db] [--overwrite]
streamlit run grokdata.py
```

Expected source data:

```text
~/data/lintap/lintap-dev/raw_sensor/**/*.parquet
~/data/lintap/lintap-dev/pidstat/*.csv
```

Generated/used data:

```text
Lintap/teletap/sample.db  # default in grokdata.py
```

## DuckDB summary model

`load-data.sql` currently loads only:

- `raw_process`
- `raw_process_file`
- `raw_process_conn_incr`
- `pidstat_metrics`

`summary_ddl.sql` creates:

- `event_summary`
- `process_chart`
- `file_chart`
- `network_chart`
- `perf_chart`

`grokdata.py` joins chart views with:

```sql
select pf.*, procs:p.num_rows, file:f.num_rows, net:n.num_rows
from perf_chart pf
left outer join process_chart p on pf.time_chunk=p.time_chunk
left outer join file_chart f on pf.time_chunk=f.time_chunk
left outer join network_chart n on pf.time_chunk=n.time_chunk
order by all
```

The Streamlit app plots selectable metrics such as unique processes, max CPU/memory/read/write, process starts, file events, and network flows.

## Observed limitations and cleanup opportunities

- `process-data.sh` hard-codes `~/data/lintap/lintap-dev` indirectly through SQL macros.
- `initdb.sql` hard-codes the same path in the `dp()` macro.
- `grokdata.py` hard-codes `sample.db` and must be run from `Lintap/teletap` unless changed.
- `load-data.sql` only loads three raw telemetry types and ignores host/registry/image-load.
- The time conversions in `summary_ddl.sql` appear inconsistent:
  - `raw_process.eventtime` is converted with `win32_to_epoch`.
  - `raw_process_file` and `raw_process_conn_incr` chart views use `to_timestamp(eventtime)` directly, while `event_summary` uses `firstseen`/`lastseen` conversions for file/network. This should be verified against actual Lintap schemas.
- TeleTap scripts are useful for sensor-development feedback but should not duplicate full ETL logic now owned by DBT.

## Relationship to canonical ETL

TeleTap currently provides a quick raw-level sanity check and visualization path:

```text
raw_sensor -> small local DuckDB raw tables -> summary/chart views -> Streamlit
```

The canonical Wintap-PyUtil path is broader and DBT-based:

```text
raw_sensor -> DBT bronze -> DBT silver -> DBT gold -> DuckDB analysis database
```

For full pipeline validation, use `Wintap-PyUtil/wintap_dbt` rather than `Lintap/teletap`.
