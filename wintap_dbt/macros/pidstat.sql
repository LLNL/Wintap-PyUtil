{% macro pidstat_data_path() -%}
    {{ env_var('PIDSTAT_DATA_PATH', env_var('WINTAP_DATA_ROOT', '') ~ '/pidstat') }}
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
    {{ return(adapter.dispatch('pidstat_data_exists', 'wintap_dbt')()) }}
{%- endmacro %}

{% macro duckdb__pidstat_data_exists() -%}
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

{% macro spark__pidstat_data_exists() -%}
    {{ return(env_var('PIDSTAT_DATA_PATH', '') != '') }}
{%- endmacro %}

{% macro csv_relation(path_glob, options={}) -%}
    {{ adapter.dispatch('csv_relation', 'wintap_dbt')(path_glob, options) }}
{%- endmacro %}

{% macro duckdb__csv_relation(path_glob, options={}) -%}
read_csv(
    '{{ path_glob }}',
    delim='\t',
    header=false,
    auto_detect=false,
    ignore_errors=true,
    filename=true,
    columns={
        'date_col': 'VARCHAR',
        'Time': 'VARCHAR',
        'UID': 'VARCHAR',
        'PID': 'VARCHAR',
        '%usr': 'VARCHAR',
        '%system': 'VARCHAR',
        '%guest': 'VARCHAR',
        '%wait': 'VARCHAR',
        '%CPU': 'VARCHAR',
        'CPU': 'VARCHAR',
        'minflt/s': 'VARCHAR',
        'majflt/s': 'VARCHAR',
        'VSZ': 'VARCHAR',
        'RSS': 'VARCHAR',
        '%MEM': 'VARCHAR',
        'kB_rd/s': 'VARCHAR',
        'kB_wr/s': 'VARCHAR',
        'kB_ccwr/s': 'VARCHAR',
        'iodelay': 'VARCHAR',
        'cswch/s': 'VARCHAR',
        'nvcswch/s': 'VARCHAR',
        'Command': 'VARCHAR'
    }
)
{%- endmacro %}

{% macro spark__csv_relation(path_glob, options={}) -%}
csv.`{{ path_glob }}`
{%- endmacro %}
