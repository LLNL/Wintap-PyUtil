{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Squiblydoo

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_path LIKE '%regsvr32.exe'
    AND args LIKE '%scrobj.dll'
    {{ limit_search_days( search_day_pk ) }}
