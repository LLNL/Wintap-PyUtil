select
    agent_id,
    hostname,
    pid_hash,
    any_value(os_pid) os_pid,
    process_name,
    {{ array_agg_distinct_sorted('filename') }} dlls,
    {{ array_length_expr(array_agg_distinct_sorted('filename')) }} num_uniq_files,
    min(first_seen) first_seen,
    max(last_seen) last_seen
from {{ ref('process_image_load') }}
group by all
