select
    file_id,
    hostname,
    filename,
    sum(process_num_rows) process_num_rows,
    sum(dll_num_rows) dll_num_rows,
    sum(file_num_rows) file_num_rows,
    min(min_process_started) min_process_started,
    max(max_process_term) max_process_term,
    min(dll_first_seen) dll_first_seen,
    max(dll_last_seen) dll_last_seen,
    min(file_first_seen) file_first_seen,
    max(file_last_seen) file_last_seen
from {{ ref('files_tmp_v1') }}
group by all
