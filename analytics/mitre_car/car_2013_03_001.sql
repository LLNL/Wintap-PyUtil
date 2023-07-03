SELECT
    p.process_name,
    p.command_line
FROM process AS p
WHERE
    p.parent_pid_hash IN
    (
        SELECT child.pid_hash
        FROM raw_process_may AS child,
            raw_process_may AS parent
        WHERE
            parent.pid_hash = child.parent_pid_hash
            AND parent.process_name != 'explorer.exe'
            AND child.process_name = 'cmd.exe'
    )
    AND p.process_name = 'reg.exe'
