select
    {{ ten_second_bucket_expr('first_seen') }} as time_chunk,
    'process' as event_type,
    count(distinct process_name) as uniq_process_name,
    count(distinct os_pid) as uniq_pid,
    count(*) as num_rows
from {{ ref('process') }}
group by
    time_chunk,
    event_type
order by time_chunk
