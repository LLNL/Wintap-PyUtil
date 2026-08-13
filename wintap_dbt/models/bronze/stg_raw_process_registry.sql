{% if first_existing_raw_event(['raw_process_registry', 'raw_registry']) is not none %}
select *, count(*) as num_dups
from {{ raw_scan_for(['raw_process_registry', 'raw_registry']) }}
where {{ partition_filter() }}
group by all
{% else %}
select
    cast(null as varchar) AgentId,
    cast(null as varchar) ActivityType,
    cast(null as varchar) ProcessName,
    cast(null as varchar) Reg_Data,
    cast(null as integer) EventCount,
    cast(null as bigint) FirstSeenMs,
    cast(null as bigint) LastSeenMs,
    cast(null as integer) PID,
    cast(null as varchar) PidHash,
    cast(null as varchar) HostHame,
    cast(null as varchar) Reg_Path,
    cast(null as varchar) Reg_Value,
    cast(null as varchar) Reg_Id_Hash,
    cast(null as varchar) MessageType,
    cast(null as bigint) EventTime,
    cast(null as bigint) dayPK,
    cast(null as varchar) hourPK,
    cast(0 as bigint) num_dups
where false
{% endif %}
