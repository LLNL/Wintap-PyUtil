-- Services launching Cmd
SELECT
    child.pid_hash AS pid_hash,
    COALESCE(child.first_seen, child.daypk) AS first_seen
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND parent.process_name = 'services.exe'
    AND child.process_name = 'cmd.exe'
    {% if search_day_pk is defined and search_day_pk != None %}
    AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
    AND child.daypk = {{ search_day_pk|default(20230501, true) }}
    {% endif %}
