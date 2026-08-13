{% if first_existing_raw_event(['raw_imageload', 'raw_image_load']) is not none %}
select *, count(*) as num_dups
from {{ raw_scan_for(['raw_imageload', 'raw_image_load']) }}
where {{ partition_filter() }}
group by all
{% else %}
select
    cast(null as varchar) FileName,
    cast(null as bigint) BuildTime,
    cast(null as integer) ImageChecksum,
    cast(null as integer) ImageSize,
    cast(null as integer) PID,
    cast(null as varchar) DefaultBase,
    cast(null as varchar) ImageBase,
    cast(null as varchar) MD5,
    cast(null as varchar) PidHash,
    cast(null as varchar) ProcessName,
    cast(null as varchar) MessageType,
    cast(null as varchar) ActivityType,
    cast(null as bigint) EventTime,
    cast(null as varchar) ComputerName,
    cast(null as varchar) AgentId,
    cast(null as varchar) ActivityId,
    cast(null as varchar) CorrelationId,
    cast(null as bigint) dayPK,
    cast(null as varchar) hourPK,
    cast(0 as bigint) num_dups
where false
{% endif %}
