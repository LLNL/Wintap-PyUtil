-- Generic Regsvr32
SELECT child.pid_hash AS pid_hash
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND parent.process_path LIKE '%regsvr32.exe'
    AND child.process_path NOT LIKE '%regsvr32.exe%'
    AND child.daypk = '{{ search_day_pk|default("20230501", true) }}'
    AND parent.daypk = '{{ search_day_pk|default("20230501", true) }}'
