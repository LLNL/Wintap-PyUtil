{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- CertUtil Download With URLCache and Split Arguments

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'certutil.exe'
    AND args LIKE '%urlcache%'
    AND args LIKE '%split%'
    {{ limit_search_days( search_day_pk ) }}
