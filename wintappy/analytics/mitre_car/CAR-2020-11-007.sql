{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Network Share Connection Removal

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    (
        ( 
            process_name = 'net.exe'
            AND 
            args LIKE '%delete%'
        )
        OR args LIKE '%Remove-SmbShare%'
        OR args LIKE '%Remove-FileShare%'
    )
    {{ limit_search_days( search_day_pk ) }}
