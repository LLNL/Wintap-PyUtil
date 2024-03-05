{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Clear Powershell Console Command History

SELECT {{ select_fallback( columns ) }}
FROM process
WHERE
    (
        args LIKE '%rm (Get-PSReadlineOption).HistorySavePath%'
        OR args LIKE '%del (Get-PSReadlineOption).HistorySavePath%'
        OR args LIKE '%Set-PSReadlineOption –HistorySaveStyle SaveNothing%'
        OR args LIKE '%Remove-Item (Get-PSReadlineOption).HistorySavePath%'
        OR args LIKE '%del%Microsoft\Windows\Powershell\PSReadline\ConsoleHost_history.txt'
    )
    {{ limit_search_days( search_day_pk ) }}
