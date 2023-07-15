{%- from 'car_macros.sql' import limit_search_days, select_fallback -%}

-- AppInit DLLs

SELECT {{ select_fallback( columns ) }}
FROM process_registry
WHERE
    (
        reg_path LIKE
        '%\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Windows\Appinit_Dlls\%'
        OR reg_path LIKE
        '%\SOFTWARE\Wow6432Node\Microsoft\Windows NT\CurrentVersion\Windows\Appinit_Dlls\%'
    )
    AND daypk = {{ limit_search_days( search_day_pk ) }}
