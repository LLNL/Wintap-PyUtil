select * from (
    select
        'process' as event_type,
        min(first_seen) as first_seen,
        max(last_seen) as last_seen,
        epoch(max(last_seen)) - epoch(min(first_seen)) as elapsed_seconds,
        count(distinct process_name) as uniq_process_name,
        count(distinct os_pid) as uniq_pid,
        cast(null as bigint) as uniq_files,
        cast(null as bigint) as uniq_conn_id,
        cast(null as bigint) as uniq_local_ip,
        cast(null as bigint) as uniq_remote_ip,
        cast(null as hugeint) as events,
        cast(null as real) as max_cpu,
        cast(null as real) as max_mem,
        cast(null as real) as max_read,
        cast(null as real) as max_write,
        count(*) as num_rows
    from {{ ref('process') }}
    group by all

    union by name

    select
        'file' as event_type,
        min(first_seen) as first_seen,
        max(last_seen) as last_seen,
        epoch(max(last_seen)) - epoch(min(first_seen)) as elapsed_seconds,
        count(distinct process_name) as uniq_process_name,
        count(distinct os_pid) as uniq_pid,
        count(distinct filename) as uniq_files,
        cast(null as bigint) as uniq_conn_id,
        cast(null as bigint) as uniq_local_ip,
        cast(null as bigint) as uniq_remote_ip,
        sum(event_count) as events,
        cast(null as real) as max_cpu,
        cast(null as real) as max_mem,
        cast(null as real) as max_read,
        cast(null as real) as max_write,
        count(*) as num_rows
    from {{ ref('process_file') }}
    group by all

    union by name

    select
        'network' as event_type,
        min(first_seen) as first_seen,
        max(last_seen) as last_seen,
        epoch(max(last_seen)) - epoch(min(first_seen)) as elapsed_seconds,
        count(distinct process_name) as uniq_process_name,
        count(distinct os_pid) as uniq_pid,
        cast(null as bigint) as uniq_files,
        count(distinct conn_id) as uniq_conn_id,
        count(distinct local_ip_addr) as uniq_local_ip,
        count(distinct remote_ip_addr) as uniq_remote_ip,
        sum(total_events) as events,
        cast(null as real) as max_cpu,
        cast(null as real) as max_mem,
        cast(null as real) as max_read,
        cast(null as real) as max_write,
        count(*) as num_rows
    from {{ ref('process_conn_incr') }}
    group by all

    union by name

    select
        'performance' as event_type,
        min(time) as first_seen,
        max(time) as last_seen,
        epoch(max(time)) - epoch(min(time)) as elapsed_seconds,
        count(distinct command) as uniq_process_name,
        count(distinct pid) as uniq_pid,
        cast(null as bigint) as uniq_files,
        cast(null as bigint) as uniq_conn_id,
        cast(null as bigint) as uniq_local_ip,
        cast(null as bigint) as uniq_remote_ip,
        cast(null as hugeint) as events,
        max(cpu_percent) as max_cpu,
        max(mem_percent) as max_mem,
        max(kb_read_per_sec) as max_read,
        max(kb_write_per_sec) as max_write,
        count(*) as num_rows
    from {{ ref('pidstat_metrics') }}
    group by all
)
order by event_type desc
