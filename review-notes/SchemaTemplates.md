# Schema Template Inventory

`Wintap-PyUtil/wintappy/schema` contains empty parquet files used as schema stubs when optional data is missing. This was identified as important for making the pipeline robust and outsider-friendly.

Current dbt status: optional registry/image-load and enrichment handling is now mostly implemented as typed empty dbt SQL models rather than by copying parquet templates. For Spark/S3 partial datasets, use `WINTAP_DBT_AVAILABLE_RAW_EVENTS` to tell dbt which raw event folders exist.

## Current template files

```text
wintappy/schema/raw_sensor/raw_processstop.parquet
wintappy/schema/stdview/process_file_summary.parquet
wintappy/schema/stdview/process_image_load_summary.parquet
wintappy/schema/stdview/process_net_summary.parquet
wintappy/schema/stdview/process_registry_summary.parquet
```

## How templates are used today

`wintappy/datautils/rawutil.py` has two related mechanisms:

1. `validate_raw_views()` checks `raw_process` for process-stop-only columns such as `CPUCycleCount`. If absent, it creates an empty `raw_processstop.parquet` from the `raw_sensor/raw_processstop.parquet` template and recreates the raw view.
2. `run_sql_no_args()` parses SQL comments:

```sql
--# required
--# template: stdview
```

If a required `CREATE` fails because a dependent table/view is missing, it creates an empty table from:

```text
wintappy/schema/<template>/<object_name>.parquet
```

## Required SQL objects using templates

From `wintappy/datautils/process_summary.sql`:

| SQL object | Template file | Why needed |
| --- | --- | --- |
| `process_registry_summary` | `schema/stdview/process_registry_summary.parquet` | Allows `process_summary` to build when registry data is absent. |
| `process_file_summary` | `schema/stdview/process_file_summary.parquet` | Allows `process_summary` to build when file data is absent. |
| `process_net_summary` | `schema/stdview/process_net_summary.parquet` | Allows `process_summary` to build when network data is absent. |
| `process_image_load_summary` | `schema/stdview/process_image_load_summary.parquet` | Allows `process_summary` to build when image-load/DLL data is absent. |

From `rawutil.validate_raw_views()`:

| Template file | Trigger | Why needed |
| --- | --- | --- |
| `schema/raw_sensor/raw_processstop.parquet` | `raw_process` lacks `CPUCycleCount` | Some collections lack process stop files, but downstream `process` SQL references stop-specific columns. |

## Template schema summary

### `raw_sensor/raw_processstop.parquet`

Columns observed through DuckDB `DESCRIBE`:

- `PidHash`
- `ParentPidHash`
- `CPUCycleCount`
- `CPUUtilization`
- `CommitCharge`
- `CommitPeak`
- `ReadOperationCount`
- `WriteOperationCount`
- `ReadTransferKiloBytes`
- `WriteTransferKiloBytes`
- `HardFaultCount`
- `TokenElevationType`
- `ExitCode`
- `MessageType`
- `Hostname`
- `ActivityType`
- `EventTime`
- `ReceiveTime`
- `PID`
- `IncrType`
- `EventCount`
- `FirstSeenMs`
- `LastSeenMs`

### `stdview/process_registry_summary.parquet`

- `agent_id`
- `hostname`
- `pid_hash`
- `process_name`
- `reads`
- `writes`
- `createkeys`
- `deletekeys`
- `deletevalues`
- `first_seen`
- `total_activity_types`

Note: observed template output did not show `last_seen`, although `process_summary.sql` selects it. This should be verified; it may be a display/truncation issue or stale template.

### `stdview/process_file_summary.parquet`

- `agent_id`
- `Hostname`
- `process_name`
- `pid_hash`
- `Close_Events`
- `Create_Events`
- `Delete_Events`
- `Rename_Events`
- `SetInfo_Events`
- `Read_Bytes`
- `Read_Events`
- `Write_Bytes`
- `Write_Events`
- `num_raw_rows`
- `num_uniq_file_hash`
- `num_null_filename`
- `first_seen`
- `last_seen`

### `stdview/process_image_load_summary.parquet`

- `agent_id`
- `hostname`
- `pid_hash`
- `process_name`
- `dlls`
- `num_uniq_files`
- `first_seen`
- `last_seen`

### `stdview/process_net_summary.parquet`

Large schema including:

- identity: `os_family`, `pid_hash`, `process_name`, `agent_id`, `Hostname`
- counts/sizes: `conn_id_count`, `net_total_events`, `net_total_size`, `num_raw_rows`
- TCP metrics: `tcp_accept_count`, `tcp_connect_count`, `tcp_disconnect_count`, `tcp_reconnect_count`, `tcp_recv_count`, `tcp_recv_size`, `tcp_retransmit_count`, `tcp_send_count`, `tcp_send_size`, `tcp_tcpcopy_count`, `tcp_tcpcopy_size`
- UDP metrics: `udp_recv_count`, `udp_recv_size`, `udp_send_count`, `udp_send_size`
- times: `first_seen`, `last_seen`
- derived ratios/totals: `net_recv_size`, `net_send_size`, `net_rs_total`, `net_send_vs_recv`, `tcp_rs_total`, `tcp_send_vs_recv`, `udp_rs_total`, `udp_send_vs_recv`
- summary stats: `min_bytes`, `max_bytes`, `avg_bytes`, `min_packets`, `max_packets`, `avg_packets`, `sq_size`

## DBT migration recommendation

In DBT, prefer typed empty `select` models/macros over writing physical stub parquet files during build.

Example conceptual pattern:

```sql
{% if source_exists('raw_process_file') %}
  select ... from {{ ref('stg_raw_process_file') }}
{% else %}
  select
    cast(null as varchar) as agent_id,
    cast(null as varchar) as hostname,
    ...
  where false
{% endif %}
```

However, keeping the existing parquet templates as a source of truth during migration is useful. The DBT project can generate typed empty selects from a documented schema YAML instead of from parquet files.

## Follow-up tasks

1. Re-run schema extraction with full untruncated output and commit a machine-readable inventory.
2. Verify `process_registry_summary.parquet` includes every column selected by `process_summary.sql`, especially `last_seen`.
3. Decide whether template schemas should move into DBT `schema.yml` documentation.
4. Add CI tests that ensure template schemas match the models that use them.
