{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Component Object Model Hijacking

SELECT {{ select_fallback( columns ) }}
FROM process_registry
WHERE
    reg_path LIKE '%Software\Classes\CLSID%'
    {{ limit_search_days( search_day_pk ) }}
