select
    filename,
    count(distinct hostname) num_hosts,
    sum(process_num_rows) process_num_rows,
    sum(dll_num_rows) dll_num_rows,
    sum(file_num_rows) file_num_rows
from {{ ref('files') }}
group by all
