{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Certutil exe certificate extraction

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'certutil.exe'
    AND args LIKE '% -exportPFX %'
    AND daypk = {{ limit_search_days( search_day_pk ) }}
