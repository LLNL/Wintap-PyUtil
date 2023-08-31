-- Remote PowerShell Sessions
SELECT
    child.pid_hash as pid_hash,
    COALESCE(child.first_seen, child.dayPK) as first_seen
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND parent.process_name = 'svchost.exe'
    AND child.process_name = 'wsmprovhost.exe'
    AND child.daypk = '{{ search_day_pk|default("20230501", true) }}'
    AND parent.daypk = '{{ search_day_pk|default("20230501", true) }}'
