{% macro raw_event_path(event_type) -%}
    {{ raw_sensor_path(event_type) }}
{%- endmacro %}

{% macro raw_event_exists(event_type) -%}
    {%- if not execute -%}
        {{ return(true) }}
    {%- endif -%}
    {%- set sql -%}
        select count(*) as num_files
        from glob('{{ raw_sensor_glob(event_type) }}')
    {%- endset -%}
    {%- set results = run_query(sql) -%}
    {%- if results is none or results|length == 0 -%}
        {{ return(false) }}
    {%- endif -%}
    {{ return(results.columns[0].values()[0] > 0) }}
{%- endmacro %}

{% macro first_existing_raw_event(event_types) -%}
    {%- for event_type in event_types -%}
        {%- if raw_event_exists(event_type) -%}
            {{ return(event_type) }}
        {%- endif -%}
    {%- endfor -%}
    {{ return(none) }}
{%- endmacro %}

{% macro raw_scan_for(event_types) -%}
    {%- set event_type = first_existing_raw_event(event_types) -%}
    {%- if event_type is none -%}
        {{ exceptions.raise_compiler_error('No raw_sensor event parquet files found for aliases: ' ~ event_types | join(', ')) }}
    {%- endif -%}
    {{ parquet_relation(event_type) }}
{%- endmacro %}

{% macro raw_column_exists(event_types, column_name) -%}
    {%- if not execute -%}
        {{ return(true) }}
    {%- endif -%}
    {%- set event_type = first_existing_raw_event(event_types) -%}
    {%- if event_type is none -%}
        {{ return(false) }}
    {%- endif -%}
    {%- set sql -%}
        select column_name
        from (describe select * from {{ parquet_relation(event_type) }})
        where lower(column_name) = lower('{{ column_name }}')
        limit 1
    {%- endset -%}
    {%- set results = run_query(sql) -%}
    {{ return(results is not none and results|length > 0) }}
{%- endmacro %}
