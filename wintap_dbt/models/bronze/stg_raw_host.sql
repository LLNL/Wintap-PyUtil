select *, count(*) as num_dups
from {{ parquet_relation('raw_host') }}
where {{ day_filter() }}
group by all
