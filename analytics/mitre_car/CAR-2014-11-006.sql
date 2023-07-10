-- Windows Remote Management (WinRM)
SELECT pid_hash
FROM process_net_conn
WHERE
    local_port = '5985'
    OR local_port = '5986'
    OR remote_port = '5985'
    OR remote_port = '5986'
