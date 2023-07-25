-- Webshell-Indicative Process Tree

SELECT
    child.pid_hash,
    parent.pid_hash
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND 
    (
        parent.process_name IN ('w3wp.exe', 'httpd.exe', 'nginx.exe') 
        OR parent.process_name LIKE 'tomcat%.exe'
    )
    AND child.process_name IN ('cmd.exe', 'powershell.exe', 'net.exe', 'whoami.exe', 'hostname.exe', 'systeminfo.exe', 'ipconfig.exe')
    AND child.daypk = {{ search_day_pk|default(20230501, true) }}
    AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
