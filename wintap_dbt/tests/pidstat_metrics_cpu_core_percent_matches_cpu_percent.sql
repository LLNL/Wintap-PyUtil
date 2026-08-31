select *
from {{ ref('pidstat_metrics') }}
where cpu_core_percent is distinct from cpu_percent
