{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- RunDLL32.exe monitoring
SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'rundll32.exe'
    AND daypk = {{ limit_search_days( search_day_pk ) }}
