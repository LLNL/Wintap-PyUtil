{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Common Windows Process Masquerading

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name IN ('smss.exe', 'wininit.exe', 'taskhost.exe', 'lasass.exe', 'winlogon.exe', 'csrss.exe', 'services.exe', )
    AND process_path NOT LIKE 'C:\Windows\System32\%.exe'
    AND process_path NOT LIKE 'c:\windows\system32\%.exe'
    AND process_path NOT LIKE 'systemroot\system32\%.exe'
    AND process_path NOT LIKE '%systemroot%\system32\%.exe'
    {{ limit_search_days( search_day_pk ) }}
