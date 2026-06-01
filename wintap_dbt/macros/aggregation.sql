{% macro array_length_expr(expr) -%}
    {{ adapter.dispatch('array_length_expr', 'wintap_dbt')(expr) }}
{%- endmacro %}

{% macro duckdb__array_length_expr(expr) -%}
    len({{ expr }})
{%- endmacro %}

{% macro spark__array_length_expr(expr) -%}
    size({{ expr }})
{%- endmacro %}
