{{ config(materialized='table',
    post_hook="COPY (FROM {{ this }}) TO '{{ export_path( this.name ) }}' (FORMAT PARQUET)"
) }}

SELECT
    coalesce(p.agent_id, p.hostname) AS DeviceId,
    p.hostname AS DeviceName,
    p.os_pid AS ProcessId,
    p.pid_hash AS ProcessUniqueId,
    p.process_started AS ProcessCreationTime,
    COALESCE(p.process_name, p.filename) AS FileName,
    p.process_path AS FolderPath,
    p.args AS ProcessCommandLine,
    p.user_name AS AccountName,
    -- Prefer process_started, fallback to first_seen, fallback to process_started_seconds (epoch seconds)
    COALESCE(
        p.process_started,
        p.first_seen
    ) AS Timestamp,
    p.exit_code AS ProcessExitCode,
    p.file_md5 AS MD5,
    p.file_sha2 AS SHA256,
    p.token_elevation_type AS ProcessTokenElevation,
    -- Parent Features
    -- Note: parent pid_hash is available on PROCESS, so use that, just in case there is no record for the parent process.
    p.parent_pid_hash AS InitiatingProcessUniqueId,
    pp.os_pid AS InitiatingProcessId,
    pp.args AS InitiatingProcessCommandLine,
    pp.process_started AS InitiatingProcessCreationTime,
    pp.process_path AS InitiatingProcessFolderPath,
    pp.user_name AS InitiatingProcessAccountName,
    pp.token_elevation_type AS InitiatingProcessProcessTokenElevation,
    pp.file_md5 AS InitiatingProcessMD5,
    pp.file_sha2 AS InitiatingProcessSHA256,
    COALESCE(pp.process_name, pp.filename) AS InitiatingProcessFileName,

    -- Grand Parent Features
    pp.parent_pid_hash AS InitiatingProcessParentUniqueId,
    gpp.os_pid AS InitiaInitiatingProcessParentId,
    gpp.args AS InitiatingProcessParentCommandLine,
    gpp.process_started AS InitiatingProcessParentCreationTime,
    gpp.process_path AS InitiatingProcessParentFolderPath,
    gpp.user_name AS InitiatingProcessParentAccountName,
    gpp.token_elevation_type AS InitiatingProcessProcessParentTokenElevation,
    gpp.file_md5 AS InitiatingProcessParentMD5,
    gpp.file_sha2 AS InitiatingProcessParentSHA256,
    COALESCE(gpp.process_name, gpp.filename) AS InitiatingProcessParentFileName,
FROM
    {{ ref('process') }} p
left outer join {{ ref('process') }} pp on p.parent_pid_hash=pp.pid_hash
left outer join {{ ref('process') }} gpp on pp.parent_pid_hash=gpp.pid_hash

