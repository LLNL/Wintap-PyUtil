SELECT *
FROM raw_process_conn_incr
WHERE LocalPort = '3389'
  OR RemotePort = '3389'
