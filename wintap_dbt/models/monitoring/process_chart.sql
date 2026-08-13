select
    time_bucket(interval 10 seconds, first_seen) as time_chunk,
    'process' as event_type,
    count(distinct process_name) as uniq_process_name,
    count(distinct os_pid) as uniq_pid,
    count(*) as num_rows
from {{ ref('process') }}
group by all
order by time_chunk
