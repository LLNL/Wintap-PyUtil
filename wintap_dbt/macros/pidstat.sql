{% macro pidstat_data_path() -%}
    {{ env_var('PIDSTAT_DATA_PATH', env_var('WINTAP_DATA_ROOT') ~ '/pidstat') }}
{%- endmacro %}

{% macro pidstat_csv_glob() -%}
    {%- set path = pidstat_data_path() -%}
    {%- if '*' in path -%}
        {{ path }}
    {%- else -%}
        {{ path }}/**/*.csv
    {%- endif -%}
{%- endmacro %}

{% macro pidstat_data_exists() -%}
    {%- if not execute -%}
        {{ return(true) }}
    {%- endif -%}
    {%- set sql -%}
        select count(*) as num_files
        from glob('{{ pidstat_csv_glob() }}')
    {%- endset -%}
    {%- set results = run_query(sql) -%}
    {%- if results is none or results|length == 0 -%}
        {{ return(false) }}
    {%- endif -%}
    {{ return(results.columns[0].values()[0] > 0) }}
{%- endmacro %}
