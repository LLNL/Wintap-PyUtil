-- Credentials in Files & Registry
SELECT pid_hash
FROM process
WHERE
    args LIKE '%reg% query HKLM /f password /t REG_SZ /s%'
    OR args LIKE '%reg% query HKCU /f password /t REG_SZ /s%'
    OR args LIKE '%Get-UnattendedInstallFile%'
    OR args LIKE '%Get-Webconfig%'
    OR args LIKE '%Get-ApplicationHost%'
    OR args LIKE '%Get-SiteListPassword%'
    OR args LIKE '%Get-CachedGPPPassword%'
    OR args LIKE '%Get-RegistryAutoLogon%'
