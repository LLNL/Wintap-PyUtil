# Lintap/Wintap Data Pipeline Review Notes

This directory contains first-pass review notes and current implementation status for follow-on agents working across the four related repositories in this checkout. These notes are committed with `Wintap-PyUtil` because that repository now owns the canonical post-processing and DBT work.

- `wintap/` — .NET host telemetry sensor/runtime and shared UI/MCP code. Includes the Linux build target (`Lintap.csproj`) and current parquet writers.
- `Lintap/teletap/` — small Linux/TeleTap proof-of-concept tools for parquet normalization, DuckDB loading, and Streamlit visualization. The rest of `Lintap/` was intentionally not reviewed.
- `Wintap-PyUtil/` — Python/DuckDB ETL utilities that implement the canonical medallion-style post-processing flow.
- `wintap-analytics/` — notebooks and ad-hoc analysis projects consuming processed parquet views.

## Suggested reading order

1. [`ProjectSummary.md`](ProjectSummary.md) — durable summary of the goal, key decisions, implemented work, and current limitations.
2. [`ImplementationStatus.md`](ImplementationStatus.md) — what has already been implemented since the initial plan.
3. [`PipelineRunbook.md`](PipelineRunbook.md) — exact commands for running and inspecting the current DBT pipeline.
4. [`DataFlow.md`](DataFlow.md) — DBT-first end-to-end flow from raw_sensor parquet through DuckDB outputs.
5. [`Architecture.md`](Architecture.md) — repository roles and component boundaries.
6. [`DBT_Pipeline_Plan.md`](DBT_Pipeline_Plan.md) — DBT migration/orchestration plan.
7. [`DBT_Model_Map.md`](DBT_Model_Map.md) — current SQL objects mapped to DBT models.
8. [`RawSensorContract.md`](RawSensorContract.md) — canonical raw input layout and naming contract.
9. [`DataModel.md`](DataModel.md) — DBT-era dataset/model conventions and historical layer mapping.
10. [`Dependencies.md`](Dependencies.md) — runtime/tool dependencies and relevant entry points.
11. [`Teletap.md`](Teletap.md) — focused notes on the current Linux/TeleTap scaffolding.
12. [`LinuxEbpfNetworkSensor.md`](LinuxEbpfNetworkSensor.md) — current Linux eBPF network sensor findings, local-IP fix, risks, and follow-up work.
13. [`OpenQuestions.md`](OpenQuestions.md) — remaining follow-up work and deferred questions.

## Scope of this pass

The original emphasis was the data path from initial parquet files into analysis notebooks. A later focused pass reviewed the Linux eBPF network sensor because upstream local endpoint quality directly affects raw network parquet and downstream DBT models. This pass still does **not** fully audit all sensor collection internals, the web UI, AI/chat features, or every notebook.

## Current code status summary

- `Wintap-PyUtil/wintap_dbt/` exists and can build ACME4 and LINTAP sample raw_sensor data into DuckDB.
- `Wintap-PyUtil` now uses `uv` project configuration and includes `dbt-duckdb`.
- The .NET sensor has been adjusted to emit canonical metadata names (`raw_host`, `raw_macip`) at the source.
- Legacy `merged` tooling remains in the tree but is no longer the desired normal path.
- A focused Linux eBPF network sensor review identified why TCP local/source IPs were emitted as `0.0.0.0`; the tracer was changed to use `sock/inet_sock_set_state` for TCP connection lifecycle records.
