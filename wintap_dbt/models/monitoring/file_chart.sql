select
    time_bucket(interval 10 seconds, min_event) as time_chunk,
    'file' as event_type,
    count(distinct process_name) as uniq_process_name,
    count(distinct os_pid) as uniq_pid,
    count(distinct filename) as uniq_files,
    sum(event_count) as events,
    count(*) as num_rows
from {{ ref('process_file') }}
group by all
order by time_chunk
