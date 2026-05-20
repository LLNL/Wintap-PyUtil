select
    agent_id,
    hostname,
    pid_hash,
    process_name,
    sum(case when activity_type = 'READ' then event_count else 0 end) reads,
    sum(case when activity_type = 'WRITE' then event_count else 0 end) writes,
    sum(case when activity_type = 'CREATEKEY' then event_count else 0 end) createkeys,
    sum(case when activity_type = 'DELETEKEY' then event_count else 0 end) deletekeys,
    sum(case when activity_type = 'DELETEVALUE' then event_count else 0 end) deletevalues,
    min(first_seen) first_seen,
    max(last_seen) last_seen,
    sum(event_count) total_activity_types
from {{ ref('process_registry') }}
group by all
