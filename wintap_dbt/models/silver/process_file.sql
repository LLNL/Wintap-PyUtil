select
    agentid agent_id,
    hostname,
    pidhash pid_hash,
    any_value(pid) os_pid,
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
    {{ unix_or_timestamp_expr('min(eventtime)') }} min_event,
    {{ unix_or_timestamp_expr('max(eventtime)') }} max_event
from {{ ref('stg_raw_process_file') }}
group by all
