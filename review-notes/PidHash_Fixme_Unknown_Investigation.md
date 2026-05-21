# PidHash `fixme*` / `unknown*` Investigation Handoff

Date: 2026-05-21

## Original user prompt

> Now, another bug fix. Lets start with investigating where in the dotnet/ebpf code the pid_hash is being set to "unknown*" or "fixme*". These were "hacks" put in place to work around really noisy problems distracting from the main funcitonality. Identify the locations and analyze what you would do to resolve or test them. Prepare a plan and present to me.

## Human summary

The investigation found two related but distinct problem classes in the Wintap/Lintap sensor pipeline:

1. **Explicit fake PID hash values** such as `fixmepid` and `fixparentpidhash` are emitted in core serializer/resolver code as fallbacks when process attribution fails. These values directly pollute downstream raw/silver/gold data and are the source of DBT QA orphan rows.
2. **Linux/eBPF `unknown-*` placeholders** are mostly process name/path/user placeholder strings rather than PID hashes, but the same eBPF sensors also compute `PidHash` using event time rather than canonical process start time. That causes file/network/stop events to hash differently from the process start event and prevents joins by `pid_hash`.

Recommended initial fix:

- Stop producing `fixmepid` and `fixparentpidhash`.
- Log/drop unattributed file/network aggregates rather than creating fake process identities.
- Add QA checks for forbidden sentinel PID hash values.
- Then implement process attribution properly: maintain/consult an active process map and derive Linux PID hashes from canonical process start time, not arbitrary event time.

## Key files and locations

### Explicit `fixmepid` fallback

#### `wintap/wintap/core/etl/extract/TcpConnectionSerializer.cs`

Current pattern discovered:

```csharp
string tmpPidHash = sensorEvent["PidHash"]?.ToString() ??  "fixmepid";
ProcessConnIncrData pci = transform.Transformer.CreateProcessConn(sensorEvent, tmpPidHash, activeNics);
```

Likely downstream impact:

- Creates `raw_process_conn_incr.PidHash = 'fixmepid'`.
- This matched DBT QA failures where network orphan rows had `pid_hash = fixmepid` and `process_name = Unknown`.

Recommended first fix:

```csharp
string tmpPidHash = sensorEvent["PidHash"]?.ToString();
if (string.IsNullOrWhiteSpace(tmpPidHash))
{
    WintapLogger.Log.Append(
        $"Dropping TCP aggregate without PidHash: PID={sensorEvent["PID"]}, ProcessName={sensorEvent["ProcessName"]}",
        LogLevel.Warn);
    return;
}
```

Longer-term fix: upstream attribution should populate `PidHash` before this serializer runs.

#### `wintap/wintap/core/etl/extract/FileSerializer.cs`

Current pattern discovered:

```csharp
// GrantJ - force non-null
string pidHash = sensorEvent["PidHash"]?.ToString() ??  "fixmepid";
// string pidHash = sensorEvent["PidHash"].ToString();
```

Likely downstream impact:

- Creates `raw_process_file.PidHash = 'fixmepid'`.
- This matched DBT QA failures where file orphan rows had `pid_hash = fixmepid`.

Recommended first fix: same as TCP serializer: do not invent a PID hash; log/drop or route to an unattributed diagnostics stream.

### Explicit `fixparentpidhash` fallback

#### `wintap/wintap/core/infrastructure/ProcessResolver.cs`

Current pattern discovered around process record materialization:

```csharp
ParentPidHash = reader.IsDBNull(1) ? "fixparentpidhash" : reader.GetString(1),
```

Recommended first fix:

```csharp
ParentPidHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
```

Better long-term fix: make parent PID hash nullable/optional, then let DBT normalize empty string to null.

## Process attribution fallback problem

### `wintap/wintap/core/infrastructure/EventChannel.cs`

Current non-process event attribution flow:

```csharp
ProcessRecord ownerProcess = _processResolver.ResolveProcessAtTime(
    streamedEvent.PID,
    DateTime.FromFileTimeUtc(streamedEvent.EventTime));

if (ownerProcess != null)
{
    streamedEvent.PidHash = ownerProcess.PidHash;
    streamedEvent.ProcessName = ownerProcess.ProcessName;
}
else
{
    WintapLogger.Log.Append(
        $"Could not resolve owner process for PID {streamedEvent.PID} ({streamedEvent.MessageType})",
        LogLevel.Warn);

    streamedEvent.PidHash = _processResolver.GetPidHash(
        streamedEvent.PID,
        DateTime.FromFileTimeUtc(streamedEvent.EventTime));
    streamedEvent.ProcessName = "Unknown";
}
```

Problem:

- `GetPidHash` sounds like a generator, but it is actually a database lookup.
- If no process row exists, it returns null.
- That null can reach Esper and later serializers, which currently patch around it with `fixmepid`.

Recommended fixes:

- Rename/split behavior:
  - `TryResolveProcessAtTime(...)`
  - `TryGetPidHashForPidAtTime(...)`
  - `ComputePidHash(...)` only where canonical start time is known.
- Make `EventChannel` memory-first for non-process attribution using an active process table/cache.
- DB lookup should be fallback, not the primary hot path.
- Missing attribution should be explicit and observable, not silently converted to fake identities.

### `wintap/wintap/core/infrastructure/ProcessResolver.cs`

Current `GetPidHash` implementation:

```csharp
public string GetPidHash(int pid, DateTime createTime)
{
    SELECT pid_hash
    FROM process
    WHERE process_id = {pid}
      AND create_time <= TIMESTAMP ...
    ORDER BY create_time DESC
    LIMIT 1
}
```

Problem:

- Method name implies deterministic hash generation.
- Actual behavior is process table lookup and can return null.

Recommended fix: rename and adjust callers to reflect lookup semantics.

## Linux/eBPF-specific issue

These are not literal `fixmepid`, but they likely explain unresolved or mismatched `pid_hash` values in Linux data.

### eBPF process start sensors compute PID hash from event time

Files:

```text
wintap/wintap/platform/linux/sensor/ebpf/ExecveSensor.cs
wintap/wintap/platform/linux/sensor/ebpf/CloneSensor.cs
```

Current pattern:

```csharp
message.PidHash = _pidHashGenerator?.GenPidHash(message.PID, message.EventTime) ?? "";
```

Problem:

- `message.EventTime` is when userland handled the event, not canonical process start time.
- PID hash should be based on hostname/agent + PID + process start time.

Recommended fix:

- Add Linux process identity helper that derives process start time from `/proc/<pid>/stat` field 22 plus boot time and clock ticks.
- Convert to the time basis expected by `ProcessHash.GenPidHash`.
- Use that canonical process start time for process start events.

Possible helper:

```csharp
LinuxProcessIdentity.TryGetProcessStartFileTimeUtc(pid, out long startFileTimeUtc)
```

Then:

```csharp
message.PidHash = processHash.GenPidHash(pid, startFileTimeUtc);
```

### eBPF non-process sensors prepopulate PID hash from event time

Files:

```text
wintap/wintap/platform/linux/sensor/ebpf/FileOpsSensor.cs
wintap/wintap/platform/linux/sensor/ebpf/NetworkSensor.cs
wintap/wintap/platform/linux/sensor/ebpf/OpenAtSensor.cs
```

Current pattern:

```csharp
message.PidHash = _pidHashGenerator?.GenPidHash(pid, message.EventTime) ?? "";
message.ProcessName = evt.GetComm() ?? "unknown";
```

Problem:

- File/network event hashes are based on event time, so they do not join to process start rows.
- Prepopulating the PID hash can bypass/short-circuit central attribution.

Preferred fix:

- Non-process sensors should not compute `PidHash` from event time.
- Let `EventChannel` resolve from active process state.
- Sensors may provide PID and process name hint only.

Possible later shape:

```csharp
message.PidHash = "";
message.ProcessName = evt.GetComm() ?? "";
EventChannel.Send(message);
```

This requires `EventChannel` attribution handling to be reliable and non-noisy first.

### eBPF exit sensor computes PID hash from stop time

File:

```text
wintap/wintap/platform/linux/sensor/ebpf/ExitSensor.cs
```

Current pattern:

```csharp
message.PidHash = _pidHashGenerator?.GenPidHash(message.PID, message.EventTime) ?? "";
```

Problem:

- Stop event hash will differ from start event hash for the same process instance.

Recommended fix:

- Stop events should resolve `PidHash` from active process map by PID.
- If missing, try canonical process start time while `/proc/<pid>` still exists.
- If still missing, explicitly mark/drop as unattributed; do not fake-hash.

## `unknown-*` placeholder locations

Mostly metadata placeholders, not PID hashes.

### `wintap/wintap/platform/linux/sensor/ebpf/helpers/ProcessSensorHelper.cs`

Examples discovered:

```csharp
ExtractProcessName(string path, string fallback = "unknown-4")
return string.IsNullOrWhiteSpace(fallback) ? "unknown-fallback" : fallback.Trim();
Path = string.IsNullOrWhiteSpace(path) ? "unknown-5" : path,
UniqueProcessKey = "unknown-key"
ExtractProcessNameFromCmdline(... fallback = "unknown-6")
ExtractPathFromCmdline(... fallback = "unknown-7")
```

### `wintap/wintap/platform/linux/sensor/ebpf/ExecveSensor.cs`

Examples discovered:

```csharp
: "unknown-1";
processName = ProcessSensorHelper.ExtractProcessNameFromCmdline(rawCmdline, "unknown-2");
user: procData.Username ?? "unknown-3",
```

### Other eBPF sensors

Files:

```text
wintap/wintap/platform/linux/sensor/ebpf/CloneSensor.cs
wintap/wintap/platform/linux/sensor/ebpf/ExitSensor.cs
wintap/wintap/platform/linux/sensor/ebpf/FileOpsSensor.cs
wintap/wintap/platform/linux/sensor/ebpf/NetworkSensor.cs
wintap/wintap/platform/linux/sensor/ebpf/OpenAtSensor.cs
```

Examples:

```csharp
?? "unknown"
evt.GetComm() ?? "unknown"
```

Recommended treatment:

- Keep plain `unknown` only for display fields if needed.
- Remove numbered `unknown-*` values from normal analytical columns.
- Longer term, use explicit quality/source fields instead:
  - `ProcessIdentityStatus`
  - `ProcessNameSource`
  - `ProcessPathSource`
  - `AttributionStatus`

## Proposed implementation plan

### Phase 1 — Stop producing fake identity strings

Smallest safe fix.

1. Remove `fixmepid` fallback from:
   - `TcpConnectionSerializer.cs`
   - `FileSerializer.cs`
2. Remove `fixparentpidhash` fallback from:
   - `ProcessResolver.cs`
3. Add warning logs when serializers receive missing `PidHash`.
4. Decide immediate missing-attribution behavior:
   - Recommended now: drop aggregate and log.
   - Later: route to unattributed diagnostics stream/table.
5. Add DBT QA checks for forbidden sentinel values:
   - `fixmepid`
   - `fixparentpidhash`
   - maybe `unknown-%` in PID hash fields.

Tests/validation:

- Fresh capture should produce zero `PidHash = 'fixmepid'` rows.
- DBT QA should show no forbidden PID hash sentinels.
- Missing attribution should be visible in logs/counters rather than hidden in data.

### Phase 2 — Fix process attribution ownership

Medium change.

1. Make `EventChannel` authoritative for non-process attribution.
2. Maintain active process map:
   - `PID -> ProcessRecord`
   - updated on Process Start/Stop.
3. Non-process event attribution order:
   - active process map,
   - DB fallback,
   - explicit unattributed handling.
4. Avoid per-event DB queries in hot path.

Tests:

- Start(pid=123) then File(pid=123) gets same `PidHash`.
- Start(pid=123) then TCP(pid=123) gets same `PidHash`.
- Unknown PID never creates `fixmepid`.
- Serializer never receives null/empty `PidHash` for known active PID.

### Phase 3 — Fix Linux/eBPF PID hash time basis

Medium-to-large change.

1. Implement Linux canonical process start time reader:
   - parse `/proc/<pid>/stat` field 22,
   - convert boot-time + ticks to UTC/FileTime-style long.
2. Use canonical start time for:
   - `ExecveSensor`
   - `CloneSensor`
3. Stop non-process eBPF sensors from hashing by event time.
4. Stop `ExitSensor` from hashing by stop time; resolve from active process map.

Tests:

- Start and subsequent file/network events for same PID share `pid_hash`.
- Start and stop for same PID share `pid_hash`.
- PID reuse yields different `pid_hash`.
- Short-lived process either resolves correctly or is explicitly unattributed, never fake-hashed.

### Phase 4 — Clean up unknown metadata fallbacks

Lower-risk cleanup.

1. Replace numbered `unknown-*` values with plain `unknown` or null/empty where schema allows.
2. Add attribution/source/quality fields if feasible.
3. Add DBT monitoring tests for unknown rates:
   - count `process_name = 'unknown'`,
   - count empty path,
   - count unattributed events.

## Recommended first implementation slice for pickup

When returning to this task, start with:

1. Patch `TcpConnectionSerializer.cs` to remove `?? "fixmepid"` and log/drop null PID hash aggregates.
2. Patch `FileSerializer.cs` the same way.
3. Patch `ProcessResolver.cs` to remove `"fixparentpidhash"`.
4. Add/extend DBT QA SQL with forbidden-sentinel checks.
5. Run a short new capture and DBT build; confirm no new `fixmepid` rows.

This does not fully solve attribution, but it prevents bad fake identities from entering the dataset and makes remaining attribution misses visible.
