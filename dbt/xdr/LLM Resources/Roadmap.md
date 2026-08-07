Below is a **starter mapping** between Wintap’s native tables/columns and the Microsoft Defender for Endpoint (MDE) Threat-hunting (“advanced hunting”) schema.  You can use the _raw_ Wintap tables (process, process_file, process_net_conn, process_registry, process_image_load, host, host_ip) to populate MDE’s core event tables (DeviceProcessEvents, DeviceFileEvents, DeviceNetworkEvents, DeviceRegistryEvents, DeviceImageLoadEvents, DeviceInfo, DeviceNetworkInfo).  Any aggregated or enrichment‐only views (sigma_labels_summary, process_mitre_summary, process_lolbas_summary, process_uber_summary) you can land into custom Kusto tables or attach as extensions to your core events.

—  
## 1) Table‐level mapping

| Wintap source table              | MDE advanced-hunting table     | Comments                                 |
|----------------------------------|---------------------------------|------------------------------------------|
| process                          | DeviceProcessEvents             | one row per process start                |
| process_file                     | DeviceFileEvents                | one row per file open/close/read/write   |
| process_net_conn *(or process_conn_incr)* | DeviceNetworkEvents      | one row per TCP/UDP connection event     |
| process_registry                 | DeviceRegistryEvents            | one row per registry key/value op        |
| process_image_load               | DeviceImageLoadEvents           | one row per DLL/process image load       |
| host                             | DeviceInfo                       | host-level metadata                      |
| host_ip                          | DeviceNetworkInfo               | host network interfaces                  |
| sigma_labels_summary             | (custom ThreatIntelligenceSigma)| aggregate Sigma hits per PID             |
| process_mitre_summary            | (custom ThreatIntel_Mitre)      | aggregate MITRE labels per PID           |
| process_lolbas_summary           | (custom LOLBAS_Hits)            | aggregate LOLBAS hits per PID            |
| process_uber_summary             | (custom ProcessUberSummary)     | full “uber” summary, join of all above   |

—  
## 2) Field‐level examples

Below are three representative mappings.  You can follow the same pattern for registry, image-load, host and enrichment views.

### 2.1) DeviceProcessEvents ← Wintap.process

| Wintap column               | MDE column                    | Transformation / note                              |
|-----------------------------|--------------------------------|----------------------------------------------------|
| process_started_seconds     | Timestamp                      | `unixtime_seconds(process_started_seconds)`        |
| hostname                    | DeviceName                     | direct                                             |
| os_pid                      | ProcessId                      | direct                                             |
| process_name                | FileName                       | basename(process_path)                             |
| process_path                | FolderPath                     | dirname(process_path)                              |
| process_path                | FilePath                       | full path                                         |
| args                        | ProcessCommandLine             | direct                                             |
| parent_os_pid               | ParentProcessId                | direct                                             |
| (lookup via process_path)   | ParentProcessFileName          | self-join on parent_os_pid                         |
| user_name                   | AccountName                    | direct                                             |
| exit_code                   | ExitCode                       | direct                                             |
| duration_seconds            | (custom) DurationSeconds       | MDE doesn’t have built-in; store as custom column |

### 2.2) DeviceFileEvents ← Wintap.process_file

| Wintap column          | MDE column                  | Transformation / note                              |
|------------------------|-----------------------------|----------------------------------------------------|
| first_seen             | Timestamp                   | `unixtime_milliseconds_todatetime(first_seen*1e3)`|
| Hostname               | DeviceName                  | direct                                             |
| pid_hash → join→os_pid | ProcessId                   | join back to process table                         |
| process_name           | InitiatingProcessFileName   | direct                                             |
| activity_type          | ActionType                  | e.g. `READ`→`FileRead`, `WRITE`→`FileWrite`         |
| filename               | FileName                    | basename(full path)                                |
| filename               | FolderPath                  | dirname(full path)                                 |
| bytes_requested        | FileSize                    | map to MDE’s FileSize                              |
| file_hash (MD5)        | MD5Hash **(custom)**        | MDE supports SHA1/SHA256; store MD5 in MD5Hash      |

### 2.3) DeviceNetworkEvents ← Wintap.process_net_conn

| Wintap column        | MDE column          | Transformation / note                              |
|----------------------|---------------------|----------------------------------------------------|
| first_seen           | Timestamp           | `unixtime_milliseconds_todatetime(first_seen*1e3)`|
| Hostname             | DeviceName          | direct                                             |
| pid_hash → join→os_pid| ProcessId          | join back to process table                         |
| process_name         | InitiatingProcessFileName | direct                                      |
| remote_ip_addr       | RemoteIP            | direct                                             |
| remote_port          | RemotePort          | direct                                             |
| local_port           | LocalPort           | direct                                             |
| protocol             | Protocol            | direct (TCP/UDP)                                   |
| tcp_send_size        | BytesSent           | sum of tcp_send_size or udp_send_size               |
| tcp_recv_size        | BytesReceived       | sum of tcp_recv_size or udp_recv_size               |
| total_events         | (custom) EventCount | MDE doesn’t expose event count by default; custom  |

—  
## 3) DBT model snippet (Process → DeviceProcessEvents)

```sql
-- models/device_process_events.sql
with raw as (
  select
    hostname                                           as DeviceName,
    to_datetime(process_started_seconds)               as Timestamp,
    os_pid                                             as ProcessId,
    process_name                                       as FileName,
    dirname(process_path)                              as FolderPath,
    process_path                                       as FilePath,
    args                                               as ProcessCommandLine,
    parent_os_pid                                      as ParentProcessId,
    (
      select p2.process_name
      from {{ ref('process') }} p2
      where p2.os_pid = raw.parent_os_pid
        and p2.hostname = raw.hostname
      order by p2.process_started_seconds desc
      limit 1
    )                                                   as ParentProcessFileName,
    user_name                                          as AccountName,
    exit_code                                          as ExitCode,
    duration_seconds                                   as DurationSeconds
  from {{ ref('process') }}
)

select * from raw
```

—  
## 4) Next steps

1. **Fill gaps.**  MDE has some columns (e.g. `IntegrityLevel, ReportId, LogonId`) you may not have – either stub them or drop.
2. **Date conversions.**  Wintap timestamps are often seconds since epoch; MDE uses Kusto datetime.
3. **Joins.**  Wintap uses `pid_hash` as a surrogate; you’ll often join back to `process` to recover `os_pid` or parent/process paths.
4. **Custom tables.**  Any aggregated/enrichment views (Sigma, MITRE, LOLBAS) can land in _your own_ Kusto tables (e.g. `WIntapSigmaHits`) and then you can `join` them on `ProcessId` in your hunting queries.
5. **Validation.**  Once ingested, compare a few events side-by-side between MDE’s out-of-the-box logs and your Wintap-derived logs to ensure field‐level parity.

With those patterns you can extend to registry, image load, host, network adapters, etc., and rapidly get Wintap telemetry searchable in the exact same schema your hunters already use in Defender.