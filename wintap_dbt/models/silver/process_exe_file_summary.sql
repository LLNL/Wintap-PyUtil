{{ config(materialized='view') }}

select
    'process' as source,
    hostname,
    filename,
    file_id,
    min(process_started) min_process_started,
    max(process_term) max_process_term,
    count(*) as process_num_rows
from {{ ref('process') }}
group by all
