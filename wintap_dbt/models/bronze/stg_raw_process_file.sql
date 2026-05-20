select *, count(*) as num_dups
from {{ parquet_relation('raw_process_file') }}
where {{ day_filter() }}
group by all
