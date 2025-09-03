{% macro export_path(table_name, data_path=var('data_path'), data_set=var('data_set')) %}
  {% set  agg_level="xdr" %}
  {% set full_path = data_path ~ '/' ~ data_set ~ '/' ~ agg_level ~ '/' ~ table_name ~ '.parquet' %}
  {{ return(full_path) }}
{% endmacro %}
