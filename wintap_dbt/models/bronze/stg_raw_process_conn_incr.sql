select *, count(*) as num_dups
from {{ parquet_relation('raw_process_conn_incr') }}
where {{ day_filter() }}
  and lower(protoPK) in ('tcp', 'udp')
group by all
