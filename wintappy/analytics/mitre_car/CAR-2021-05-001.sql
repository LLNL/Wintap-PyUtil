{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Attempt To Add Certificate To Untrusted Store

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'certutil.exe'
    AND args LIKE '%addstore%'
    {{ limit_search_days( search_day_pk ) }}
