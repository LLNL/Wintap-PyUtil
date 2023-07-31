{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Clearing Windows Logs with Wevtutil

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    args LIKE '%cl%'
    AND
    (
        args LIKE '%System%'
        OR args LIKE '%Security%'
        OR args LIKE '%Setup%'
        OR args LIKE '%Application%'
    )
    AND daypk = {{ limit_search_days( search_day_pk ) }}
