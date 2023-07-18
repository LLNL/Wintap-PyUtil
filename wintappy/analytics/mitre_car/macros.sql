{%- macro limit_search_days(search_day_pk) -%}
    {{ search_day_pk|default("20230501", true) }}
{%- endmacro -%}

{%- macro select_fallback(columns) -%}
    {{ columns|default("pid_hash", true) }}
{%- endmacro -%}
