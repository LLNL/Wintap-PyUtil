{%- from 'car_macros.sql' import limit_search_days, select_fallback -%}

-- RDP Connection Detection
SELECT {{ select_fallback( columns ) }}
FROM process_net_conn
WHERE
    (
        local_port = '3389'
        OR remote_port = '3389'
    )
    AND daypk = {{ limit_search_days( search_day_pk ) }}
