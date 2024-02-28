{%- macro limit_search_days(search_day_pk) -%}
    {% if search_day_pk is defined and search_day_pk != None %}
    AND daypk = {{ search_day_pk|default(20230501, true) }}
    {% endif %} 
{%- endmacro -%}

{%- macro select_fallback(columns) -%}
    {{ columns|default("pid_hash, first_seen", true) }}
{%- endmacro -%}
