{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Registry Edit from Screensaver

SELECT {{ select_fallback( columns ) }}
FROM process_registry
WHERE
    (
        reg_path LIKE
        '%Software\Policies\Microsoft\Windows\Control Panel\Desktop\SCRNSAVE.EXE'
    )
    {{ limit_search_days( search_day_pk ) }}
