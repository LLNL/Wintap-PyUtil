select
    agentid agent_id,
    hostname,
    pidhash pid_hash,
    processname process_name,
    md5(concat_ws('||', hostname, lower(file_path))) file_id,
    file_hash file_hash,
    file_path filename,
    activitytype activity_type,
    sum(bytesrequested) bytes_requested,
    sum(eventcount) event_count,
    count(*) num_raw_rows,
    {{ win32_to_timestamp_expr('min(cast(firstseen as bigint))') }} first_seen,
    {{ win32_to_timestamp_expr('max(cast(lastseen as bigint))') }} last_seen,
    to_timestamp(min(cast(eventtime as bigint))) min_event,
    to_timestamp(max(cast(eventtime as bigint))) max_event
from {{ ref('stg_raw_process_file') }}
group by all
