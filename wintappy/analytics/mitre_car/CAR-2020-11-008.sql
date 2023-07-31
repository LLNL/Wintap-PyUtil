{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- MSBuild and msxsl

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    process_name IN ('MSBuild.exe', 'msbuild.exe', 'msxsl.exe')
    AND process_path NOT LIKE '%Microsoft Visual Studio%'
    AND process_path NOT LIKE '%microsoft visual studio%'
    AND daypk = {{ limit_search_days( search_day_pk ) }}
