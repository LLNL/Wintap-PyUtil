{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Windows Remote Management (WinRM)

SELECT {{ select_fallback( columns ) }}
FROM process_net_conn
WHERE
    local_port = '5985'
    OR local_port = '5986'
    OR remote_port = '5985'
    OR remote_port = '5986'
    {{ limit_search_days( search_day_pk ) }}
