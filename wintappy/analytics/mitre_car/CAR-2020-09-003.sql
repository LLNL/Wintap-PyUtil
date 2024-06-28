{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Indicator Blocking - Driver Unloaded

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'fltmc.exe'
    AND args LIKE '%unload%'
    {{ limit_search_days( search_day_pk ) }}
