select
    time_bucket(interval 10 seconds, to_timestamp(cast(eventtime as bigint))) as time_chunk,
    'file' as event_type,
    count(distinct processname) as uniq_process_name,
    count(distinct pid) as uniq_pid,
    count(distinct file_path) as uniq_files,
    sum(eventcount) as events,
    count(*) as num_rows
from {{ ref('stg_raw_process_file') }}
group by all
order by time_chunk
