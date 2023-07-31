{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Local Permission Group Discovery

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'net.exe'
    AND (
        args LIKE '%net% user%'
        OR args LIKE '%net% group%'
        OR args LIKE '%net% localgroup%'
        OR args LIKE '%get-localgroup%'
        OR args LIKE '%get-ADPrincipalGroupMembership%'
    )
    AND daypk = {{ limit_search_days( search_day_pk ) }}
