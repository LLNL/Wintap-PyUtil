{% macro configure_duckdb_s3_unsigned() -%}
    {%- if target.type == 'duckdb' and env_var('WINTAP_DBT_DUCKDB_UNSIGNED_S3', 'false') | lower in ['1', 'true', 'yes', 'y'] -%}
        {%- do run_query("set s3_region='" ~ env_var('AWS_REGION', env_var('AWS_DEFAULT_REGION', 'us-west-2')) ~ "'") -%}
        {%- do run_query("set s3_access_key_id=''") -%}
        {%- do run_query("set s3_secret_access_key=''") -%}
        {%- do run_query("set s3_session_token=''") -%}
    {%- endif -%}
{%- endmacro %}
