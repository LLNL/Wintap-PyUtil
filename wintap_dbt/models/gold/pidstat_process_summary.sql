select
    hostname,
    container_runtime,
    container_id,
    pid_ns_inode,
    pid,
    command,
    count(*) as samples,
    min(time) as first_seen,
    max(time) as last_seen,
    max(cpu_core_percent) as max_cpu_core_percent,
    avg(cpu_core_percent) as avg_cpu_core_percent,
    max(cpu_core_percent) as max_cpu_percent,
    avg(cpu_core_percent) as avg_cpu_percent,
    max(mem_percent) as max_mem_percent,
    avg(mem_percent) as avg_mem_percent,
    max(kb_read_per_sec) as max_kb_read_per_sec,
    avg(kb_read_per_sec) as avg_kb_read_per_sec,
    max(kb_write_per_sec) as max_kb_write_per_sec,
    avg(kb_write_per_sec) as avg_kb_write_per_sec
from {{ ref('pidstat_metrics') }}
group by all
