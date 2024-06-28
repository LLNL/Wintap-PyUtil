{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Create Remote Process via WMIC
SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'wmic.exe'
    AND args LIKE '% process call create %'
    {{ limit_search_days( search_day_pk ) }}
