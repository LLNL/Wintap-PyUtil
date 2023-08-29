-- Get System Elevation

SELECT child.pid_hash AS pid_hash
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND
    (
        (
            child.process_name = 'cmd.exe'
            AND parent.process_name = 'services.exe'
            AND child.args LIKE '%echo%'
            AND child.args LIKE '%pipe%'
        )
        OR
        (
            parent.process_name = 'rundll32.exe'
            AND child.args LIKE '%,a /p:%'
        )
    )
    AND child.daypk = {{ search_day_pk|default(20230501, true) }}
    AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
