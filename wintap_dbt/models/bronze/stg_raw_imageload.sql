select *, count(*) as num_dups
from {{ parquet_relation('raw_imageload') }}
where {{ day_filter() }}
group by all
