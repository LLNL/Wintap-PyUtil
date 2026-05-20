# Open Questions and Follow-Up Tasks

This file tracks unresolved or deferred work after the DBT-first pipeline implementation.

## Resolved decisions

| Topic | Decision / status |
| --- | --- |
| Canonical input | `raw_sensor` is the only canonical post-processing input. |
| Flat `merged` stage | Deprecated and removed from normal pipeline. |
| `mergedtoraw.py` | Removed from `Lintap/teletap` and `Wintap-PyUtil`; new data should already be `raw_sensor`. |
| Primary ETL engine | DBT in `Wintap-PyUtil/wintap_dbt`. |
| Current official output | DuckDB database built by DBT. |
| Future output | Optional parquet export/wrapper, not implemented yet. |
| Partition naming | Canonical names are `dayPK`, `hourPK`, `protoPK`. |
| Host metadata naming | Source now emits `raw_host` and `raw_macip`; limited DBT aliases remain only where explicitly implemented. |
| Required empty optional inputs | DBT uses typed empty relations for current optional registry/image-load and enrichment stubs. |

## In progress / high priority

### 1. Add a polished single-command wrapper

DBT Makefile targets work, but there is not yet an outsider-friendly command such as:

```sh
wintap-etl build --dataset /path/to/dataset --start-day 20260520 --end-day 20260520 --database /tmp/wintap.duckdb
```

Desired wrapper behavior:

- validate dataset layout,
- run DBT build/test,
- print source row counts,
- print output row counts,
- print database/export locations,
- optionally run parquet export when implemented.

### 2. Implement DBT parquet export

Current official output is DuckDB. Some notebooks and published workflows still expect `stdview-*` parquet directories.

Options:

- use `dbt-duckdb` external materializations if they satisfy the directory contract,
- add a wrapper export step that writes selected DBT models to parquet,
- support both DuckDB and parquet outputs.

### 3. Wire real enrichment inputs into DBT

Current gold enrichment inputs are typed empty stubs:

- `labels_graph_process_summary`
- `process_lolbas_summary`
- `process_mitre_summary`
- `sigma_labels_summary`

Follow-up should load real inputs for:

- labels/networkx,
- LOLBAS/LOLC,
- MITRE,
- Sigma.

### 4. Revalidate DBT with newly collected canonical data

New .NET output writes:

```text
raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp|udp
```

DBT now expects `protoPK`; run a complete build/test against newly collected data.

## Medium priority

### 5. Confirm LINTAP timestamp semantics

TeleTap `summary_ddl.sql` mixes `win32_to_epoch(...)` and direct `to_timestamp(eventtime)`.

Confirm actual Linux raw parquet timestamp units for:

- process events,
- file events,
- network events.

### 6. Normalize image-load naming when safe

Naming is inconsistent across older docs/code:

- `raw_image_load`
- `raw_imageload`
- `process_image_load`

Current DBT uses `raw_imageload` as the raw event and `process_image_load` as the silver model. This should remain documented unless a source-side naming cleanup is made.

### 7. Inventory schema templates and optional relations

DBT has typed empty stubs for current optional inputs, but a more complete inventory would help future source types.

Useful output:

- raw event type,
- required vs optional,
- columns/types,
- empty-relation strategy,
- models depending on it.

### 8. Make deduplication strategy explicit

Legacy Python used `GROUP BY ALL` for raw views and tracked duplicate counts. DBT should document and test its intended dedupe behavior.

Questions to confirm later:

- Should all raw event types be deduped?
- At which layer should dedupe happen?
- Should duplicate metadata/counts be preserved for monitoring?

## Low priority

### 9. Analytics/notebook expectations vary

Some workshop docs use older pipenv/make flows and published `stdview` URLs. ACME4 explore uses `uv` and modern project structure.

Follow-up:

- identify current exemplar notebooks,
- document whether they consume DBT DuckDB output or exported parquet,
- update older workshop references if needed.

### 10. Backdated/reprocessing policy

S3 tooling historically noted backdated data where upload time differs from data-capture time. DBT currently builds a requested day range, but there is not yet a broader policy for automatic affected-partition reprocessing.

## Possible future docs

- `SchemaInventory.md` — raw/standard table schemas and column mappings.
- `Testing.md` — pytest/DBT/sample-data validation expectations.
- `MCP-AI.md` — MCP/chat/DuckDB real-time path, out of scope for the batch ETL review.
- `WebUI.md` — workbench UI and API endpoints, out of scope for this pass.
