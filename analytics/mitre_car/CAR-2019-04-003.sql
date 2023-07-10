-- Squiblydoo
SELECT pid_hash
FROM process
WHERE
    process_path LIKE '%regsvr32.exe'
    AND args LIKE '%scrobj.dll'
