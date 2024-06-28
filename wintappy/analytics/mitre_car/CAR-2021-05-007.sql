{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- CertUtil Download With VerifyCtl and Split Arguments

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'certutil.exe'
    AND args LIKE '%verifyctl%'
    AND args LIKE '%split%'
    {{ limit_search_days( search_day_pk ) }}
