{% macro int_to_ip_expr(i) -%}
    concat_ws('.', {{ i }} >> 24, {{ i }} >> 16 & 255, {{ i }} >> 8 & 255, {{ i }} & 255)
{%- endmacro %}
