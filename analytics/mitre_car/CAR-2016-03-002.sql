-- Create Remote Process via WMIC
SELECT pid_hash
FROM process
WHERE
    process_name = 'wmic.exe'
    AND args LIKE '% process call create %'
