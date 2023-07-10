-- Command Launched from WinLogon
SELECT
    child.pid_hash,
    parent.pid_hash
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND parent.process_name = 'winlogon.exe'
    AND child.process_name = 'cmd.exe'