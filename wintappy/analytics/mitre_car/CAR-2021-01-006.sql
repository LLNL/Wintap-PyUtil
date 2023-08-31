-- Unusual Child Process spawned using DDE exploit
SELECT
    child.pid_hash AS pid_hash,
    COALESCE(child.first_seen, child.dayPK) as first_seen
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND (
        child.process_name LIKE '%.exe'
        AND (
            parent.process_name LIKE '%excel.exe'
            OR parent.process_name LIKE '%word.exe'
            OR parent.process_name LIKE '%outlook.exe'
        )
    )
    AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
    AND child.daypk = {{ search_day_pk|default(20230501, true) }}
