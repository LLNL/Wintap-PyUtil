{% macro ten_second_bucket_expr(ts) -%}
    {{ adapter.dispatch('ten_second_bucket_expr', 'wintap_dbt')(ts) }}
{%- endmacro %}

{% macro duckdb__ten_second_bucket_expr(ts) -%}
    time_bucket(interval 10 seconds, {{ ts }})
{%- endmacro %}

{% macro spark__ten_second_bucket_expr(ts) -%}
    window({{ ts }}, '10 seconds').start
{%- endmacro %}
