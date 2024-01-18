{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Compiled HTML Access

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'hh.exe'
    {{ limit_search_days( search_day_pk ) }}
