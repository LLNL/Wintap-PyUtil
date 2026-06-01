{% macro raw_event_path(event_type) -%}
    {{ raw_sensor_path(event_type) }}
{%- endmacro %}

{% macro raw_event_exists(event_type) -%}
    {{ return(adapter.dispatch('raw_event_exists', 'wintap_dbt')(event_type)) }}
{%- endmacro %}

{% macro duckdb__raw_event_exists(event_type) -%}
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

{% macro spark__raw_event_exists(event_type) -%}
    {#
      Avoid compile-time filesystem checks in Spark/Ilum. S3 access is owned by
      the Spark driver, and dbt compilation may not have equivalent credentials.

      For broader Spark builds, set WINTAP_DBT_AVAILABLE_RAW_EVENTS to a
      comma-separated allow-list so optional missing event families compile to
      typed empty models instead of path reads that will fail at execution time.
      Example: raw_host,raw_process,raw_macip,raw_process_conn_incr,raw_process_file
    #}
    {%- set available = env_var('WINTAP_DBT_AVAILABLE_RAW_EVENTS', '') -%}
    {%- if available | trim == '' -%}
        {{ return(true) }}
    {%- endif -%}
    {%- set normalized = [] -%}
    {%- for item in available.split(',') -%}
        {%- do normalized.append(item | trim | lower) -%}
    {%- endfor -%}
    {{ return(event_type | lower in normalized) }}
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
    {{ return(adapter.dispatch('raw_column_exists', 'wintap_dbt')(event_types, column_name)) }}
{%- endmacro %}

{% macro duckdb__raw_column_exists(event_types, column_name) -%}
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

{% macro spark__raw_column_exists(event_types, column_name) -%}
    {#
      POC default: assume optional compatibility columns already exist. This
      keeps compilation independent from Spark-side S3 permissions. If a source
      lacks ProcessArgs/UniqueProcessKey, set WINTAP_DBT_SPARK_ASSUME_RAW_COLUMNS=false
      and implement catalog-backed schema introspection, or patch the source table.
    #}
    {{ return(env_var('WINTAP_DBT_SPARK_ASSUME_RAW_COLUMNS', 'true') | lower in ['1', 'true', 'yes', 'y']) }}
{%- endmacro %}
