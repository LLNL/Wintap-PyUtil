{%- from 'car_macros.sql' import limit_search_days, select_fallback -%}

-- Processes Spawning cmd.exe

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'cmd.exe'
    AND daypk = {{ limit_search_days( search_day_pk ) }}
