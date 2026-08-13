# Linux eBPF Network Sensor Notes

_Last updated: 2026-06-04_

This note captures what was learned while investigating Linux/Lintap network telemetry where the sensor-side/local IP address was being reported as `0.0.0.0`.

## Scope reviewed

Primary source files reviewed in `wintap`:

```text
wintap/platform/linux/sensor/ebpf/NetworkSensor.cs
wintap/platform/linux/sensor/ebpf/tracers/network_ops_tracer.bpf.c
wintap/platform/linux/sensor/shared/BaseEbpfSensor.cs
wintap/platform/linux/sensor/ebpf/helpers/LibBpf.cs
wintap/platform/linux/sensor/ebpf/helpers/StructHelper.cs
wintap/platform/linux/infrastructure/LinuxSubscriptionManager.cs
```

Related documentation reviewed:

```text
wintap/documentation/*.md
wintap/documentation/design/architecture-assessment.md
```

## Current Linux eBPF network architecture

At a high level:

1. `LinuxSubscriptionManager` constructs and starts `NetworkSensor` along with other Linux sensors.
2. `NetworkSensor` inherits `BaseEbpfSensor`.
3. `BaseEbpfSensor`:
   - finds the compiled `.bpf.o` file,
   - opens and loads it with libbpf,
   - attaches the configured primary BPF program,
   - opens the shared `events` ring buffer,
   - starts a polling thread.
4. `NetworkSensor` attaches additional BPF programs from the same object file and converts ring-buffer `network_event` structs into `WintapMessage` instances.
5. Messages are sent into the normal Wintap pipeline with `EventChannel.Send(message)`.

Important implementation details:

- eBPF object: `network_ops_tracer.bpf.o`
- ring-buffer map name: `events`
- primary program is selected by `NetworkSensor.BpfProgramName`
- additional programs are attached manually in `NetworkSensor.Start()`
- C# marshaling expects `NetworkEvent` to exactly match the C `struct network_event`

## Root cause: local TCP address was emitted as zero

The original network tracer emitted TCP connect events from:

```text
tracepoint/syscalls/sys_enter_connect
```

At syscall entry time the user has provided the remote sockaddr, but the kernel has not necessarily selected or exposed the local socket address yet. The tracer therefore emitted:

```c
emit_network_event(pid, 0, addr.sin_addr, 0, ...)
```

That means C# received `Saddr == 0`, which became `0.0.0.0` in the output. This is why the local/sensor-side IP was missing for outbound TCP connections.

The same general problem applied to:

- `sys_enter_accept`: user sockaddr is an output buffer that has not been filled yet.
- connected TCP `sendto` / `recvfrom` without sockaddr: syscall args do not contain peer/local socket addresses.

## Change made this round

The TCP connection source was moved to:

```text
tracepoint/sock/inet_sock_set_state
```

This tracepoint fires on TCP socket state transitions and includes both local and remote IPv4 addresses (`saddr`, `daddr`) plus local/remote ports. It is a better source for connection-level TCP records than syscall-entry tracepoints.

Updated files in `wintap`:

```text
wintap/platform/linux/sensor/ebpf/tracers/network_ops_tracer.bpf.c
wintap/platform/linux/sensor/ebpf/NetworkSensor.cs
```

Summary of changes:

- Added `trace_inet_sock_set_state` in `network_ops_tracer.bpf.c`.
- Emits TCP connect when state changes to `TCP_ESTABLISHED` from `TCP_SYN_SENT`.
- Emits TCP accept when state changes to `TCP_ESTABLISHED` from `TCP_SYN_RECV` or `TCP_LISTEN`.
- Emits TCP close when state changes to `TCP_CLOSE`.
- Uses tracepoint-provided local and remote IPv4 addresses instead of placeholder `0`.
- Changed `NetworkSensor.BpfProgramName` from `trace_connect` to `trace_inet_sock_set_state`.
- Stopped attaching `trace_accept`; it remains as a stub in the object file but is not used.
- Stopped emitting placeholder TCP send/recv events with `0.0.0.0` addresses from syscall paths.
- Simplified C# IPv4 conversion to preserve the raw byte order received from eBPF:

```csharp
return new IPAddress(BitConverter.GetBytes(ipNetworkOrder)).ToString();
```

## Validation performed

The eBPF object was rebuilt successfully:

```sh
cd /home/ubuntu/git/wintap/wintap/platform/linux/sensor/ebpf/tracers
make network_ops_tracer.bpf.o
```

Observed result:

```text
Compiled network_ops_tracer.bpf.c for architecture: arm64
```

A full C# build was attempted but was blocked by local dependency restore state:

```text
error NETSDK1064: Package AWSSDK.S3, version 3.7.400.1 was not found.
```

So eBPF compile validation passed; full .NET validation still needs a clean restore/build environment.

## Data contract implications

For TCP connection-level events, the Linux network sensor should populate:

| Wintap field | Expected meaning |
| --- | --- |
| `TcpConnection.SourceAddress` | Local/sensor-side IPv4 address |
| `TcpConnection.SourcePort` | Local/sensor-side port |
| `TcpConnection.DestinationAddress` | Remote peer IPv4 address |
| `TcpConnection.DestinationPort` | Remote peer port |
| `TcpConnection.PID` | Best-known owning process ID |

This matches the downstream idea that network records should have a usable `local_ip_addr` / sensor-side address and peer address. The post-processing layer should continue treating `0.0.0.0` as a warning/anomaly for established external TCP connections, not as normal data.

## Known limitations and risks

### 1. PID attribution for TCP accept/close may need more work

`inet_sock_set_state` provides socket addresses but not necessarily perfect process attribution for every state transition. For outbound `connect`, current process context is usually useful. For inbound connections or close transitions, the event may occur in kernel/softirq or another context, so `bpf_get_current_pid_tgid()` may not always identify the owning userspace process.

Potential follow-up:

- trace `inet_csk_accept` return path or `sys_exit_accept/accept4` to associate accepted sockets/fds with the accepting process,
- maintain a BPF map keyed by socket pointer or fd/process pair,
- combine socket-state events for addresses with syscall/kprobe events for ownership.

### 2. TCP send/recv byte counts are currently reduced

The prior implementation emitted TCP send/recv events without addresses when no sockaddr was available. Those events caused `0.0.0.0` output and were suppressed in this round. This improves address correctness but means TCP per-send/per-recv byte telemetry is not fixed yet.

Potential follow-up:

- trace TCP send/receive kernel functions that have access to `struct sock`,
- read addresses from `sock_common`,
- emit byte counts with local/remote addresses and stable socket ownership.

### 3. UDP local address is still incomplete

UDP send/recv currently relies on syscall sockaddr arguments. For UDP send with `dest_addr`, the remote address is available, but the local address is still emitted as `0.0.0.0`. UDP receive with `src_addr` has similar local-address limitations.

Potential follow-up:

- capture syscall fd and resolve the socket to a `struct sock`,
- use kprobes/tracepoints where socket context is available,
- maintain fd/socket maps for process ownership and local endpoint details.

### 4. IPv6 remains future work

The event struct contains IPv6 fields, but this round only addressed IPv4. `NetworkSensor.cs` still converts only `Saddr`/`Daddr` IPv4 values.

Potential follow-up:

- add IPv6 conversion in C#,
- populate `saddr_v6` / `daddr_v6` from `inet_sock_set_state`,
- confirm downstream schema support for IPv6.

### 5. Tracepoint struct compatibility should be hardened

The new tracer defines a local struct matching `sock/inet_sock_set_state`. This compiled locally, but longer-term code should prefer a more robust CO-RE pattern when practical.

Potential follow-up:

- include/use generated `vmlinux.h` consistently,
- use tracepoint format or CO-RE-friendly definitions,
- test on supported Ubuntu/kernel versions.

### 6. Dead/stub programs should be cleaned up later

`trace_connect` and `trace_accept` are now effectively stubs. They can remain briefly to minimize loader churn, but future cleanup should remove or repurpose them once TCP coverage is reworked.

## Suggested tests

Short-term manual smoke test:

1. Rebuild `network_ops_tracer.bpf.o`.
2. Deploy updated object and `NetworkSensor.cs` build.
3. Run Lintap/Wintap as root.
4. Generate outbound TCP traffic:

```sh
curl https://example.com
```

5. Inspect raw network parquet / logs for TCP connect records where:

- source/local address is not `0.0.0.0`,
- destination/remote address is the peer,
- ports are non-zero.

Recommended automated checks in post-processing/DBT:

- Warn when established TCP records have `local_ip_addr = '0.0.0.0'` and remote address is non-zero/non-loopback.
- Track counts of TCP records with missing local address by dataset/day/hour.
- Track counts of records with PID `0` or unknown process name for TCP accept/close events.

## Future design direction

The Linux network sensor likely needs a two-layer model:

1. **Connection lifecycle layer**: socket state transitions with correct local/remote endpoints.
2. **Ownership/byte-count layer**: syscall or kprobe data correlated to process/fd/socket state.

A BPF map keyed by socket pointer, fd, or connection tuple may be needed to join these layers before emitting Wintap events. This would also align with the broader architecture-assessment theme that attribution should be explicit, testable, and not depend on accidental context.
