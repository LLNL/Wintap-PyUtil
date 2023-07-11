-- AppInit DLLs
SELECT pid_hash
FROM process_registry
WHERE
    reg_path LIKE
    '%\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Windows\Appinit_Dlls\%'
    OR reg_path LIKE
    '%\SOFTWARE\Wow6432Node\Microsoft\Windows NT\CurrentVersion\Windows\Appinit_Dlls\%'
