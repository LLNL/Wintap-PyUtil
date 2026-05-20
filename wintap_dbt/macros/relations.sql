{% macro parquet_relation(event_type) -%}
    parquet_scan('{{ raw_sensor_glob(event_type) }}', hive_partitioning=1, union_by_name=true)
{%- endmacro %}

{% macro optional_empty_raw_model(event_type, empty_select_sql) -%}
    {%- set path = raw_sensor_path(event_type) -%}
    {%- set exists = modules.os.path.isdir(path) -%}
    {%- if exists -%}
        select *
        from {{ parquet_relation(event_type) }}
        where {{ day_filter() }}
        group by all
    {%- else -%}
        {{ empty_select_sql }}
    {%- endif -%}
{%- endmacro %}
