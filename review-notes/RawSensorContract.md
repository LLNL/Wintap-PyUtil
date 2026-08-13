# Raw Sensor Contract

This is the formal input contract for the DBT-based Wintap/Lintap pipeline.

## Canonical input

`raw_sensor` is the only canonical input layer for post-processing.

The legacy flat `merged` directory should be treated as deprecated/local-debug only and should not appear in the normal pipeline.

## Root layout

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/<file>.parquet
```

Network events should eventually use an additional protocol partition:

```text
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp/<file>.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=udp/<file>.parquet
```

The DBT bronze network model now expects `protoPK`; new datasets should not use `proto=`.

## Partition names

Canonical partition keys:

| Key | Meaning | Example |
| --- | --- | --- |
| `dayPK` | data capture day in UTC | `dayPK=20240923` |
| `hourPK` | data capture hour in UTC | `hourPK=14` |
| `protoPK` | protocol for network increments | `protoPK=tcp` |

Deprecated/non-canonical variants:

- `uploadedDPK`
- `uploadedHPK`
- `proto`

S3 upload metadata can still use upload partitions internally, but local post-processing should operate on capture-time `dayPK/hourPK`.

## Event type naming

Recommended canonical raw event type names:

| Canonical name | Notes |
| --- | --- |
| `raw_host` | Host metadata. |
| `raw_macip` | Interface/IP metadata. The .NET source has been updated to emit this canonical name directly instead of `raw_macip_sensor`. |
| `raw_process` | Process start/refresh/stop records. |
| `raw_process_conn_incr` | Network event increments, with `protoPK`. |
| `raw_process_file` | File event increments. |
| `raw_process_registry` | Registry event increments. |
| `raw_imageload` | Current SQL expectation for image-load data. Naming cleanup is low priority. |

Known legacy/incoming aliases that should disappear from canonical output:

| Legacy/incoming | Canonical |
| --- | --- |
| `raw_host_sensor` | `raw_host` |
| `raw_macip_sensor` | `raw_macip` |
| `raw_file` | `raw_process_file` |
| `raw_processstop` | `raw_process` |
| `raw_registry` | `raw_process_registry` |
| TCP-specific event file names | `raw_process_conn_incr` + `protoPK=tcp` |
| UDP-specific event file names | `raw_process_conn_incr` + `protoPK=udp` |

## Filename guidance

The post-processing pipeline should not depend heavily on file names once files are in `raw_sensor`; partition path and parquet columns should carry semantics.

Current names often follow:

```text
<hostname>+<event_type>+<capture_filetime>.parquet
```

This can remain as an implementation detail, but outsiders should be taught the directory contract rather than the file-name parser.

## Network endpoint semantics

For canonical network increments (`raw_process_conn_incr`), downstream models should preserve a clear distinction between the sensor/local endpoint and remote peer endpoint.

Expected TCP connection lifecycle semantics for Linux after the eBPF update:

| Concept | Wintap source field | Meaning |
| --- | --- | --- |
| Local/sensor IP | `TcpConnection.SourceAddress` | IP address selected on the monitored host for the connection. |
| Local/sensor port | `TcpConnection.SourcePort` | Local port selected on the monitored host. |
| Remote peer IP | `TcpConnection.DestinationAddress` | Remote endpoint IP. |
| Remote peer port | `TcpConnection.DestinationPort` | Remote endpoint port. |

For established external TCP connection records, `0.0.0.0` in the local/sensor IP field should be treated as anomalous unless the remote endpoint is also a wildcard/local special case. The prior Linux TCP tracer emitted `0.0.0.0` because it collected connect events at syscall entry before the local address was assigned; this has been changed source-side to use `sock/inet_sock_set_state` for TCP connection lifecycle events.

Known caveats:

- UDP local endpoint capture is not fully fixed yet.
- TCP per-send/per-receive byte-count events with endpoint addresses are future work.
- IPv6 endpoint population is future work.
- TCP accept/close process attribution may require additional socket/fd correlation.

## Required vs optional sources

Minimum required for a meaningful build:

- `raw_process`

Strongly recommended:

- `raw_host`
- `raw_process_conn_incr`
- `raw_process_file`
- `raw_process_registry`
- `raw_imageload`

Optional:

- `raw_macip`
- enrichment labels/lookups
- future raw event types

The DBT pipeline provides typed empty relations for optional event types so the full graph can build even when a collection lacks registry or image-load data. LINTAP currently builds successfully with those sources absent.

## Timestamp policy

Timestamps are currently complex and vary by source/event.

For this planning pass:

- Preserve existing conversion logic.
- Centralize conversion macros in DBT.
- Document per-event timestamp fields.
- Add sanity tests such as `first_seen <= last_seen`.

## Dedupe policy

Current raw view creation deduplicates with `GROUP BY ALL` and can track `num_dups`.

For DBT v1:

- Preserve the current dedupe behavior.
- Surface duplicate counts where available.
- Add warning-level tests/metrics for unusually high duplication.
- Ask a human to confirm final policy before locking it as a data contract.
