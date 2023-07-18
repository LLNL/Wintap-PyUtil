{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Detecting Tampering of Windows Defender Command Prompt

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    
    process_name = 'sc.exe'
    AND (
        args LIKE '%config%'
        OR args LIKE '%stop%'
        OR args LIKE '%query%'
    )
    AND daypk = {{ search_day_pk|default(20230501, true) }}
