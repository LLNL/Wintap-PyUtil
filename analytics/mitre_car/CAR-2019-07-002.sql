-- Lsass Process Dump via Procdump
SELECT pid_hash
FROM process
WHERE
    process_name LIKE 'procdump%.exe'
    AND args LIKE '%lsass%'
