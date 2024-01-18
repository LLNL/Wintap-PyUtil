{%- from 'macros.sql' import limit_search_days, select_fallback -%}

-- Reg.exe called from Command Shell
SELECT {{ select_fallback( columns ) }}
FROM process AS p
WHERE
    p.parent_pid_hash IN
    (
        SELECT child.pid_hash
        FROM process AS child,
            process AS parent
        WHERE
            parent.pid_hash = child.parent_pid_hash
            AND parent.process_name != 'explorer.exe'
            AND child.process_name = 'cmd.exe'
            {% if search_day_pk is defined and search_day_pk != None %}
            AND parent.daypk = {{ search_day_pk|default(20230501, true) }}
            AND child.daypk = {{ search_day_pk|default(20230501, true) }}
            {% endif %}
    )
    AND p.process_name = 'reg.exe'
    {{ limit_search_days( search_day_pk ) }}
