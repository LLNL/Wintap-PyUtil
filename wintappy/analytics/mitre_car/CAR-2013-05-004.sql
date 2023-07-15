{%- from 'car_macros.sql' import limit_search_days, select_fallback -%}

-- Execution with AT
SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'at.exe'
    AND daypk = {{ limit_search_days( search_day_pk ) }}
