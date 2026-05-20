select *, count(*) as num_dups
from {{ parquet_relation('raw_process') }}
where {{ day_filter() }}
group by all
