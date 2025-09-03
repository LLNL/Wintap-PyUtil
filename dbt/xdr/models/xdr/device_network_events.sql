{{ config(materialized='table',
    post_hook="COPY (FROM {{ this }}) TO '{{ export_path( this.name ) }}' (FORMAT PARQUET)"
) }}

SELECT
  -- core network columns
  pnc.first_seen                             AS Timestamp,
  coalesce(pnc.agent_id, pnc.hostname)       AS DeviceId,
  pnc.hostname                               AS DeviceName,
  --'NetworkConnection'::VARCHAR               AS ActionType,
  pnc.protocol                               AS Protocol,
  pnc.local_ip_addr                          AS SourceIpAddress,
  pnc.local_port                             AS SourcePort,
  pnc.remote_ip_addr                         AS DestinationIpAddress,
  pnc.remote_port                            AS DestinationPort,
  --'Outbound'::VARCHAR                        AS Direction,
  pnc.total_size                             AS BytesSentReceived,
  pnc.total_events                           AS EventCount,

  -- initiating process (the one opening the conn)
  -- Note: Unique ID seems to appear twice.
  pnc.pid_hash                               AS ProcessUniqueId,
  pnc.pid_hash                               AS InitiatingProcessUniqueId,
  p.os_pid                                   AS InitiatingProcessProcessId,
  COALESCE(p.process_name,p.filename)        AS InitiatingProcessFileName,
  p.process_path                             AS InitiatingProcessFolderPath,
  p.args                                     AS InitiatingProcessCommandLine,
  p.process_started                          AS InitiatingProcessCreationTime,
  p.user_name                                AS InitiatingProcessAccountName,
  p.token_elevation_type                     AS InitiatingProcessTokenElevation,
  p.file_sha2                                AS InitiatingProcessSHA256,
  p.file_md5                                 AS InitiatingProcessMD5,

  -- parent of initiating process
  pp.os_pid                                  AS InitiatingProcessParentProcessId,
  COALESCE(pp.process_name,pp.filename)      AS InitiatingProcessParentFileName,
  pp.process_started                         AS InitiatingProcessParentCreationTime,
  -- The rest of these aren't actually in the official schema, but I'm adding them
  p.parent_pid_hash                          AS InitiatingProcessParentUniqueId,
  pp.process_path                            AS InitiatingProcessParentFolderPath,
  pp.args                                    AS InitiatingProcessParentCommandLine,
  pp.user_name                               AS InitiatingProcessParentAccountName,
  pp.token_elevation_type                    AS InitiatingProcessParentTokenElevation,
  pp.file_sha2                               AS InitiatingProcessParentSHA256,
  pp.file_md5                                AS InitiatingProcessParentMD5
FROM {{ ref('process_net_conn') }} pnc
LEFT OUTER JOIN {{ ref('process') }} p ON pnc.pid_hash = p.pid_hash
LEFT OUTER JOIN {{ ref('process') }} pp ON p.parent_pid_hash = pp.pid_hash