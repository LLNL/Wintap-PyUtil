{# Scan one raw event type with hive partitions and union-by-name enabled. #}
{% macro parquet_relation(event_type) -%}
    parquet_scan({{ raw_sensor_partition_globs_sql(event_type) }}, hive_partitioning=1, union_by_name=true)
{%- endmacro %}

{# Return a real raw scan when the event directory exists, else a typed empty select. #}
{% macro optional_empty_raw_model(event_type, empty_select_sql) -%}
    {%- set path = raw_sensor_path(event_type) -%}
    {%- set exists = modules.os.path.isdir(path) -%}
    {%- if exists -%}
        select *
        from {{ parquet_relation(event_type) }}
        where {{ partition_filter() }}
        group by all
    {%- else -%}
        {{ empty_select_sql }}
    {%- endif -%}
{%- endmacro %}
