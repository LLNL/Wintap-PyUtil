{% macro string_array_type() -%}
    {{ adapter.dispatch('string_array_type', 'wintap_dbt')() }}
{%- endmacro %}

{% macro duckdb__string_array_type() -%}varchar[]{%- endmacro %}
{% macro spark__string_array_type() -%}array<string>{%- endmacro %}
