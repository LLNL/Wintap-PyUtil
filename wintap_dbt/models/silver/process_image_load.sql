select
    pidhash pid_hash,
    any_value(pid) os_pid,
    lower(filename) filename,
    any_value(agentid) agent_id,
    any_value(computername) hostname,
    any_value(processname) process_name,
    md5(concat_ws('||', computername, lower(filename))) file_id,
    any_value(md5) file_md5,
    max(buildtime) build_time,
    max(imagechecksum) checksum,
    max(defaultbase) default_base,
    max(imagebase) image_base,
    min(imagesize) min_image_size,
    max(imagesize) max_image_size,
    sum(if(upper(activitytype) = 'LOAD', 1, 0)) num_load,
    sum(if(upper(activitytype) = 'UNLOAD', 1, 0)) num_unload,
    {{ win32_to_timestamp_expr('min(cast(eventtime as bigint))') }} first_seen,
    {{ win32_to_timestamp_expr('max(cast(eventtime as bigint))') }} last_seen
from {{ ref('stg_raw_imageload') }}
group by all
