{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Lsass Process Dump via Procdump

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name LIKE 'procdump%.exe'
    AND args LIKE '%lsass%'
    {{ limit_search_days( search_day_pk ) }}
