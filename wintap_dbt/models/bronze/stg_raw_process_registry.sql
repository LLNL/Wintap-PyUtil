select *, count(*) as num_dups
from {{ parquet_relation('raw_process_registry') }}
where {{ day_filter() }}
group by all
