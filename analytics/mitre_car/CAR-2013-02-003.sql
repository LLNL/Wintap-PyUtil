-- Processes Spawning cmd.exe
SELECT pid_hash
FROM process
WHERE
    process_name = 'cmd.exe'
