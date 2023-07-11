-- Indicator Blocking - Driver Unloaded
SELECT pid_hash
FROM process
WHERE
    process_name = 'fltmc.exe'
    AND args LIKE '%unload%'
