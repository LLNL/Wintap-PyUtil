{% if target.type == 'spark' %}
select
    *,
    count(*) over (
        partition by PidHash, Hostname, ProcessName, ProcessArgs, dayPK, hourPK
    ) as num_dups
from {{ raw_scan_for(['raw_process']) }}
where {{ day_filter() }}
{% else %}
select
    *,
    {% if not raw_column_exists(['raw_process'], 'ProcessArgs') -%}
    commandline as ProcessArgs,
    {% endif -%}
    {% if not raw_column_exists(['raw_process'], 'UniqueProcessKey') -%}
    cast(null as varchar) as UniqueProcessKey,
    {% endif -%}
    count(*) as num_dups
from {{ raw_scan_for(['raw_process']) }}
where {{ day_filter() }}
group by all
{% endif %}
