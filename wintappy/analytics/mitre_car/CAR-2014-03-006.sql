-- RunDLL32.exe monitoring
SELECT pid_hash
FROM process
WHERE process_name = 'rundll32.exe'
