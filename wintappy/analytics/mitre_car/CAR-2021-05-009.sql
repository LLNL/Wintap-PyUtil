{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- CertUtil With Decode Argument

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'certutil.exe'
    AND args LIKE '%decode%'
    AND daypk = {{ limit_search_days( search_day_pk ) }}