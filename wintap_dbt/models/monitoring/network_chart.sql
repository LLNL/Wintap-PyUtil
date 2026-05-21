select
    time_bucket(interval 10 seconds, to_timestamp(cast(eventtime as bigint))) as time_chunk,
    'network' as event_type,
    count(distinct processname) as uniq_process_name,
    count(distinct pid) as uniq_pid,
    count(distinct connid) as uniq_conn_id,
    count(distinct localipaddr) as uniq_local_ip,
    count(distinct remoteipaddr) as uniq_remote_ip,
    sum(eventcount) as events,
    count(*) as num_rows
from {{ ref('stg_raw_process_conn_incr') }}
group by all
order by time_chunk
