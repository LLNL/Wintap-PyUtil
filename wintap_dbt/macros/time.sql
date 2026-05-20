{% macro win32_to_epoch_expr(wts) -%}
    ({{ wts }} / 1e7 - 11644473600)
{%- endmacro %}

{% macro to_timestamp_micros_expr(epoch_seconds) -%}
    (to_timestamp(cast(floor({{ epoch_seconds }}) as bigint)) + to_microseconds(cast(floor(({{ epoch_seconds }} - floor({{ epoch_seconds }})) * 1e6) as bigint)))
{%- endmacro %}

{% macro win32_to_timestamp_expr(wts) -%}
    {{ to_timestamp_micros_expr(win32_to_epoch_expr(wts)) }}
{%- endmacro %}
