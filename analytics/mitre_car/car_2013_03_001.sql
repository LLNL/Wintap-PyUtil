-- Reg.exe called from Command Shell
SELECT p.pid_hash AS pid_hash
FROM process AS p
WHERE
    p.parent_pid_hash IN
    (
        SELECT child.pid_hash
        FROM process AS child,
            process AS parent
        WHERE
            parent.pid_hash = child.parent_pid_hash
            AND parent.process_name != 'explorer.exe'
            AND child.process_name = 'cmd.exe'
    )
    AND p.process_name = 'reg.exe'
