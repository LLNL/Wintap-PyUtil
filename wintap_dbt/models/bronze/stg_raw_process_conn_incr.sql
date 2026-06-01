{{ dedup_raw_select(parquet_relation('raw_process_conn_incr'), day_filter() ~ " and lower(protoPK) in ('tcp', 'udp')") }}
