{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Create local admin accounts using net exe

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name IN ('net.exe', 'net1.exe')
    AND (
        args LIKE '%-localgroup%'
        OR args LIKE '%/add%'
        OR args LIKE '%user%'
    )
    AND daypk = {{ limit_search_days( search_day_pk ) }}
