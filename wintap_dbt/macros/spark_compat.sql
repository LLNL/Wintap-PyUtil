{% macro relation_columns(relation_sql, except=[]) -%}
    {{ return(adapter.dispatch('relation_columns', 'wintap_dbt')(relation_sql, except)) }}
{%- endmacro %}

{% macro duckdb__relation_columns(relation_sql, except=[]) -%}
    {%- if not execute -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set sql -%}
        describe select * from {{ relation_sql }}
    {%- endset -%}
    {%- set results = run_query(sql) -%}
    {%- set cols = [] -%}
    {%- if results is not none -%}
        {%- for col in results.columns[0].values() -%}
            {%- if col | lower not in except | map('lower') | list -%}
                {%- do cols.append(col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(cols) }}
{%- endmacro %}

{% macro spark__relation_columns(relation_sql, except=[]) -%}
    {%- if not execute -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set sql -%}
        describe query select * from {{ relation_sql }}
    {%- endset -%}
    {%- set results = run_query(sql) -%}
    {%- set cols = [] -%}
    {%- if results is not none -%}
        {%- for col in results.columns[0].values() -%}
            {%- if col and not (col | string).startswith('#') and col | lower not in except | map('lower') | list -%}
                {%- do cols.append(col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(cols) }}
{%- endmacro %}

{% macro dedup_raw_select(relation_sql, where_sql=None) -%}
    {{ adapter.dispatch('dedup_raw_select', 'wintap_dbt')(relation_sql, where_sql) }}
{%- endmacro %}

{% macro duckdb__dedup_raw_select(relation_sql, where_sql=None) -%}
    select *, count(*) as num_dups
    from {{ relation_sql }}
    {%- if where_sql %}
    where {{ where_sql }}
    {%- endif %}
    group by all
{%- endmacro %}

{% macro spark__dedup_raw_select(relation_sql, where_sql=None) -%}
    {#
      POC-friendly Spark path: do not perform compile-time S3 schema
      introspection. Preserve the raw rows and provide a placeholder duplicate
      count so downstream models can compile. Exact deduplication can be added
      per event type after the Ilum end-to-end path is validated.
    #}
    select *, cast(1 as bigint) as num_dups
    from {{ relation_sql }}
    {%- if where_sql %}
    where {{ where_sql }}
    {%- endif %}
{%- endmacro %}

{% macro array_agg_distinct_sorted(expr) -%}
    {{ adapter.dispatch('array_agg_distinct_sorted', 'wintap_dbt')(expr) }}
{%- endmacro %}

{% macro duckdb__array_agg_distinct_sorted(expr) -%}
    list_sort(list(distinct {{ expr }}))
{%- endmacro %}

{% macro spark__array_agg_distinct_sorted(expr) -%}
    sort_array(collect_set({{ expr }}))
{%- endmacro %}

{% macro real_type() -%}
    {{ adapter.dispatch('real_type', 'wintap_dbt')() }}
{%- endmacro %}

{% macro duckdb__real_type() -%}real{%- endmacro %}
{% macro spark__real_type() -%}float{%- endmacro %}
