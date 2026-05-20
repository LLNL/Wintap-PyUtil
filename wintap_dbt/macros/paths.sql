{% macro dataset_path() -%}
    {{ var('dataset') }}
{%- endmacro %}

{% macro raw_sensor_path(event_type) -%}
    {{ var('dataset') }}/raw_sensor/{{ event_type }}
{%- endmacro %}

{% macro raw_sensor_glob(event_type) -%}
    {{ raw_sensor_path(event_type) }}/**/*.parquet
{%- endmacro %}

{% macro day_filter(alias='') -%}
    {%- set prefix = alias ~ '.' if alias else '' -%}
    cast({{ prefix }}dayPK as bigint) between {{ var('start_day') }} and {{ var('end_day') }}
{%- endmacro %}
