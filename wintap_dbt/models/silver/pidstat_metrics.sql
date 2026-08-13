select *
from {{ ref('stg_pidstat_metrics') }}
