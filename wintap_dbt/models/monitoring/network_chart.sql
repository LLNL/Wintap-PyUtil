select
    time_bucket(interval 10 seconds, incr_start) as time_chunk,
    'network' as event_type,
    count(distinct process_name) as uniq_process_name,
    count(distinct os_pid) as uniq_pid,
    count(distinct conn_id) as uniq_conn_id,
    count(distinct local_ip_addr) as uniq_local_ip,
    count(distinct remote_ip_addr) as uniq_remote_ip,
    sum(total_events) as events,
    count(*) as num_rows
from {{ ref('process_conn_incr') }}
group by all
order by time_chunk
