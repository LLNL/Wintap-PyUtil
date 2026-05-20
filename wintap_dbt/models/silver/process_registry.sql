select
    agentid agent_id,
    hosthame hostname,
    pidhash pid_hash,
    processname process_name,
    reg_path reg_path,
    reg_value reg_value,
    activitytype activity_type,
    reg_data reg_data,
    sum(eventcount) event_count,
    count(*) num_raw_rows,
    {{ win32_to_timestamp_expr('min(cast(firstseenms as bigint))') }} first_seen,
    {{ win32_to_timestamp_expr('max(cast(lastseenms as bigint))') }} last_seen,
    to_timestamp(min(cast(eventtime as bigint))) min_event,
    to_timestamp(max(cast(eventtime as bigint))) max_event
from {{ ref('stg_raw_process_registry') }}
group by all
