{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- BCDEdit Failure Recovery Modification

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'bcdedit.exe'
    AND args LIKE '%recoveryenabled%'
    {{ limit_search_days( search_day_pk ) }}
