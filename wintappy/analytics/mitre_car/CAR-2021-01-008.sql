{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Disable UAC

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name = 'cmd.exe'
    AND (
        args LIKE '%reg.exe%HKLM\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Policies\\System%REG_DWORD /d 0%'
    )
    {{ limit_search_days( search_day_pk ) }}
