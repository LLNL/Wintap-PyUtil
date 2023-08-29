-- Services launching Cmd
SELECT child.pid_hash AS pid_hash
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.pid_hash
    AND parent.process_name = 'services.exe'
    AND child.process_name = 'cmd.exe'
    AND child.daypk = {{ search_day_pk|default(20230501, true) }}
    AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
