{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Create Service In Suspicious File Path

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_path LIKE '%.exe'
    AND process_path NOT LIKE 'C:\\Windows%'
    AND process_path NOT LIKE '%windir%'
    AND process_path NOT LIKE 'C:\\Program File%'
    AND process_path NOT LIKE 'C:\\Programdata%'
    AND process_path NOT LIKE 'c:\\windows%'
    AND process_path NOT LIKE 'c:\\Program File%'
    AND process_path NOT LIKE 'c:\\Programdata%'
    {{ limit_search_days( search_day_pk ) }}
