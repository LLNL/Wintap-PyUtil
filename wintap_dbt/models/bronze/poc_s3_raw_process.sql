{{ config(materialized='table', pre_hook="{{ configure_duckdb_s3_unsigned() }}") }}

select
    PidHash,
    Hostname,
    ProcessName,
    CommandLine as ProcessArgs,
    dayPK,
    hourPK,
    count(*) as num_events
from {{ parquet_relation('raw_process') }}
where {{ day_filter() }}
group by
    PidHash,
    Hostname,
    ProcessName,
    CommandLine,
    dayPK,
    hourPK
