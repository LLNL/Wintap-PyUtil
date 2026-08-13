select *, count(*) as num_dups
from {{ raw_scan_for(['raw_macip', 'raw_macip_sensor']) }}
where {{ partition_filter() }}
group by all
