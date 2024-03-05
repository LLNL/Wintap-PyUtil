-- Processes Started From Irregular Parent
-- Tactic: Defense Evasion; Technique: Process Injection
SELECT
    child.pid_hash AS pid_hash,
    child.first_seen AS first_seen
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND (
        (
            child.process_name = 'smss.exe'
            AND parent.process_name NOT IN ('smss.exe', 'System')
        )
        OR (
            child.process_name = 'csrss.exe'
            AND parent.process_name NOT IN ('smss.exe', 'svchost.exe')
        )
        OR (
            child.process_name = 'lsass.exe'
            AND parent.process_name NOT IN ('wininit.exe', 'winlogon.exe')
        )
        OR (
            child.process_name = 'wininit.exe'
            AND parent.process_name != 'smss.exe'
        )
        OR (
            child.process_name = 'winlogon.exe'
            AND parent.process_name != 'smss.exe'
        )
        OR (
            child.process_name = 'services.exe'
            AND parent.process_name != 'wininit.exe'
        )
        OR (
            child.process_name = 'spoolsv.exe'
            AND parent.process_name != 'services.exe'
        )
        OR (
            child.process_name IN ('taskhost.exe', 'taskhostw.exe')
            AND parent.process_name NOT IN ('services.exe', 'svchost.exe')
        )
        OR (
            child.process_name = 'userinit.exe'
            AND parent.process_name NOT IN ('dwm.exe', 'winlogon.exe')
        )
    )
{% if search_day_pk is defined and search_day_pk != None %}
    AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
    AND child.daypk = {{ search_day_pk|default(20230501, true) }}
{% endif %}
