select
    agent_id,
    hostname,
    pid_hash,
    process_name,
    list_sort(list(distinct filename)) dlls,
    len(dlls) num_uniq_files,
    min(first_seen) first_seen,
    max(last_seen) last_seen
from {{ ref('process_image_load') }}
group by all
