select
    agent_id,
    hostname,
    process_name,
    pid_hash,
    sum(case when activity_type = 'CLOSE' then event_count else 0 end) Close_Events,
    sum(case when activity_type = 'CREATE' then event_count else 0 end) Create_Events,
    sum(case when activity_type = 'DELETE' then event_count else 0 end) Delete_Events,
    sum(case when activity_type = 'RENAME' then event_count else 0 end) Rename_Events,
    sum(case when activity_type = 'SETINFO' then event_count else 0 end) SetInfo_Events,
    sum(case when activity_type = 'READ' then bytes_Requested else 0 end) Read_Bytes,
    sum(case when activity_type = 'READ' then event_count else 0 end) Read_Events,
    sum(case when activity_type = 'WRITE' then bytes_Requested else 0 end) Write_Bytes,
    sum(case when activity_type = 'WRITE' then event_count else 0 end) Write_Events,
    sum(num_raw_rows) num_raw_rows,
    count(distinct file_hash) num_uniq_file_hash,
    sum(case when filename is null then 1 else 0 end) num_null_filename,
    min(first_seen) first_seen,
    max(last_seen) last_seen
from {{ ref('process_file') }}
group by all
