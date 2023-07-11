-- Powershell Execution
SELECT
    child.pid_hash,
    child.parent_pid_hash
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND parent.process_name != 'explorer.exe'
    AND child.process_name = 'powershell.exe'
