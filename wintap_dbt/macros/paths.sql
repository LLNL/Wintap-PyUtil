{% macro dataset_path() -%}
    {%- set dataset = var('dataset') | string -%}
    {%- if target.type == 'duckdb' and dataset.startswith('s3a://') -%}
        {{ dataset.replace('s3a://', 's3://', 1) }}
    {%- else -%}
        {{ dataset }}
    {%- endif -%}
{%- endmacro %}

{% macro raw_sensor_root() -%}
    {%- set dataset = dataset_path() | string -%}
    {%- if dataset.rstrip('/').endswith('/raw_sensor') -%}
        {{ dataset.rstrip('/') }}
    {%- else -%}
        {{ dataset.rstrip('/') }}/raw_sensor
    {%- endif -%}
{%- endmacro %}

{% macro raw_sensor_path(event_type) -%}
    {{ raw_sensor_root() }}/{{ event_type }}
{%- endmacro %}

{% macro raw_sensor_glob(event_type) -%}
    {{ raw_sensor_path(event_type) }}/**/*.parquet
{%- endmacro %}

{% macro day_filter(alias='') -%}
    {%- set prefix = alias ~ '.' if alias else '' -%}
    cast({{ prefix }}dayPK as bigint) between {{ var('start_day') }} and {{ var('end_day') }}
{%- endmacro %}
