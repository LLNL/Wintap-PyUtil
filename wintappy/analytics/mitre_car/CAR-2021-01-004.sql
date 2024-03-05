-- Unusual Child Process for Spoolsv.Exe or Connhost.Exe
SELECT
    child.pid_hash AS pid_hash,
    child.first_seen AS first_seen
FROM process AS child,
    process AS parent
WHERE
    parent.pid_hash = child.parent_pid_hash
    AND child.process_name IN ('spoolsv.exe', 'conhost.exe')
    AND parent.process_name = 'cmd.exe'
{% if search_day_pk is defined and search_day_pk != None %}
    AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
    AND child.daypk = {{ search_day_pk|default(20230501, true) }}
{% endif %}
