select *, count(*) as num_dups
from {{ parquet_relation('raw_host') }}
where {{ partition_filter() }}
group by all
