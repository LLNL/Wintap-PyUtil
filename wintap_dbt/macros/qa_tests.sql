{% test no_forbidden_pid_hash_values(model, column_name) %}

select *
from {{ model }}
where lower(cast({{ column_name }} as varchar)) in ('fixmepid', 'fixparentpidhash')
   or lower(cast({{ column_name }} as varchar)) like 'unknown-%'

{% endtest %}
