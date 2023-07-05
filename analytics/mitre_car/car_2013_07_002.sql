-- RDP Connection Detection
SELECT pid_hash
FROM process_net_conn
WHERE
    local_port = '3389'
    OR remote_port = '3389'
