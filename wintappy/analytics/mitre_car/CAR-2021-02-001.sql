-- Webshell-Indicative Process Tree
-- Tactic: Persistence; Technique: Server Software Component

SELECT
    child.pid_hash AS pid_hash,
    COALESCE(child.first_seen, child.daypk) AS first_seen
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND
    (
        parent.process_name IN ('w3wp.exe', 'httpd.exe', 'nginx.exe')
        OR parent.process_name LIKE 'tomcat%.exe'
    )
    AND child.process_name IN
    (
        'cmd.exe',
        'powershell.exe',
        'net.exe',
        'whoami.exe',
        'hostname.exe',
        'systeminfo.exe',
        'ipconfig.exe'
    )
    AND child.daypk = {{ search_day_pk|default(20230501, true) }}
    AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
