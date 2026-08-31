select
    *,
    cpu_percent as cpu_core_percent
from {{ ref('stg_pidstat_metrics') }}
