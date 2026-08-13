{{ config(materialized='view') }}

select
    file_id,
    hostname,
    filename,
    process_num_rows,
    cast(null as integer) dll_num_rows,
    cast(null as integer) file_num_rows,
    min_process_started,
    max_process_term,
    cast(null as timestamp) dll_first_seen,
    cast(null as timestamp) dll_last_seen,
    cast(null as timestamp) file_first_seen,
    cast(null as timestamp) file_last_seen
from {{ ref('process_exe_file_summary') }}
union
select
    file_id,
    hostname,
    filename,
    null as process_num_rows,
    count(*) as dll_num_rows,
    null as file_num_rows,
    null as min_process_started,
    null as max_process_term,
    min(first_seen) dll_first_seen,
    max(last_seen) dll_last_seen,
    null as file_first_seen,
    null as file_last_seen
from {{ ref('process_image_load') }}
group by all
union
select
    file_id,
    hostname,
    filename,
    null as process_num_rows,
    null as dll_num_rows,
    count(*) as file_num_rows,
    null as min_processstarted,
    null as max_processterm,
    null as dll_first_seen,
    null as dll_last_seen,
    min(first_seen) file_first_seen,
    max(last_seen) file_last_seen
from {{ ref('process_file') }}
group by all
