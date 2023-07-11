-- Component Object Model Hijacking
SELECT pid_hash
FROM process_registry
WHERE
    reg_path LIKE '%Software\Classes\CLSID%'
