{% macro full_path(agg_level, table_name, data_path=var('data_path'), data_set=var('data_set')) %}
  {% if table_name.endswith('.parquet') %}
    -- If the path ends with .parquet, just used the name
    {% set parquet_path = table_name %}
  {% else %}
    -- Otherwise, use the hive partiton style
    {% set parquet_path =  table_name ~ '/**/*.parquet' %}
  {% endif %}

  {% set full_path = data_path ~ '/' ~ data_set ~ '/' ~ agg_level ~ '/' ~ parquet_path %}
  {{ return(full_path) }}
{% endmacro %}
