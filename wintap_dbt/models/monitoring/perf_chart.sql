select
    {{ ten_second_bucket_expr('time') }} as time_chunk,
    'performance' as event_type,
    count(distinct command) as uniq_process_name,
    max(cpu_percent) as max_cpu,
    max(mem_percent) as max_mem,
    max(kb_read_per_sec) as max_read,
    max(kb_write_per_sec) as max_write,
    count(*) as num_rows
from {{ ref('pidstat_metrics') }}
group by
    time_chunk,
    event_type
order by time_chunk
