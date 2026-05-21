# Dependencies and Entry Points

## System/runtime dependencies

### Sensor/build side

- .NET 8 SDK/runtime.
- Linux build for `Lintap.csproj` targets `linux-x64` and `linux-arm64`.
- eBPF tooling and compiled tracer objects for Linux collection.
- DuckDB.NET is used by .NET code that materializes/queries parquet.
- Systemd hosting package is referenced for Linux builds.

### Python ETL side

`Wintap-PyUtil` now uses `uv` and `pyproject.toml` for project configuration. Core runtime dependencies include:

- `duckdb`
- `dbt-duckdb`
- `pyarrow`
- `pandas`
- `boto3`, `s3fs`, `fsspec`
- `dynaconf`
- `jinja2`, `jinjasql`
- `networkx`
- `mitreattack-python`
- `altair`, `matplotlib`
- `ipyfilechooser`, `humanfriendly`, `python-dotenv`, `pyyaml`, `tqdm`

Development dependencies are declared in the `dev` dependency group and include `black`, `isort`, `mypy`, `pytest`, `sqlfluff`, `ipykernel`, and `nbstripout`.

The active Python requirement is `>=3.10,<3.13`.

### Analytics side

`wintap-analytics` top-level uses pipenv/make in older workshop docs.

`wintap-analytics/2025-acme4-explore/pyproject.toml` uses Python `>=3.11` with `uv`. Key dependencies:

- `duckdb`
- `pandas`, `numpy`
- `jupyterlab`, `ipykernel`, `ipywidgets`, `jupysql`
- `python-dotenv`
- `scikit-learn`, `umap-learn`, `fast-hdbscan`, `vectorizers`
- `bs4`, `requests` indirectly used in code for HTTP directory listing

### DBT side

The DBT project lives in:

```text
Wintap-PyUtil/wintap_dbt/
```

The DuckDB profile writes to the required environment variable:

```text
WINTAP_DBT_DATABASE
```

Normal runs should define this through `wintap-run.env`.

Useful commands:

```sh
cd Wintap-PyUtil
make dbt-build
make dbt-test

source wintap-run.env
make dbt-build
```

### TeleTap visualization side

`Lintap/teletap/grokdata.py` uses:

- `streamlit`
- `duckdb`
- `pandas`
- `plotly`

`process-data.sh` assumes:

- `uv`
- `duckdb` CLI on PATH
- dataset hard-coded at `~/data/lintap/lintap-dev` unless scripts are edited.

## Python console scripts from `Wintap-PyUtil`

Active script declarations live in `pyproject.toml`.

DBT is the primary ETL path for new work. The Python ETL scripts below remain available as legacy/reference tools unless they are later converted into DBT wrappers.

| Command | Module | Status / purpose |
| --- | --- | --- |
| `downloadfroms3` | `wintappy.etlutils.downloadfroms3:main` | Compatibility utility for pulling raw sensor parquet from S3 into local capture-time partitions. |
| `rawtorolling` | `wintappy.etlutils.rawtorolling:main` | Legacy ETL: build daily `rolling` parquet from `raw_sensor`. |
| `rawtostdview` | `wintappy.etlutils.rawtostdview:main` | Legacy ETL: build analysis-ready `stdview-*` parquet from `rolling`. |
| `ubersummary` | `wintappy.etlutils.ubersummary:main` | Legacy ETL: build label/threat-intel summaries and `process_uber_summary`. |
| `collectbinaries` | `wintappy.etlutils.collectbinaries:main` | Binary collection utility, not reviewed in depth. |
| `dbhelpers` | `wintappy.etlutils.dbhelpers:main` | Helper utilities, not reviewed in depth. |
| `run_enrichment` | `wintappy.etlutils.run_enrichment:main` | Enrichment runner, not reviewed in depth. |

`setup.py`, `setup.cfg`, and `Pipfile` may still exist for historical compatibility, but `pyproject.toml` is the active configuration source.

## Configuration

`Wintap-PyUtil/wintappy/config.py` uses Dynaconf with:

- config file: `wintappy_settings.toml`
- env var prefix: `WINTAPPY_`

Common options:

- `DATASET`
- `START`, `END`
- `AGGLEVEL`
- `LOG_LEVEL`
- AWS options: `AWS_PROFILE`, `AWS_REGION`, `AWS_S3_BUCKET`, `AWS_S3_PREFIX`

Example config file in repo:

- `Wintap-PyUtil/example_wintappy_settings.toml`

## Data access dependencies

The pipeline relies heavily on DuckDB capabilities:

- `parquet_scan` / `read_parquet`
- Hive partition discovery via `hive_partitioning=1`
- `union_by_name=true` for evolving raw schemas
- `COPY <table> TO '<path>' (FORMAT 'parquet')`
- HTTPFS extension in ACME4 notebooks for remote parquet

## Key SQL files

### Canonical DBT ETL

- `Wintap-PyUtil/wintap_dbt/dbt_project.yml`
- `Wintap-PyUtil/wintap_dbt/profiles.yml.example`
- `Wintap-PyUtil/wintap_dbt/macros/*.sql`
- `Wintap-PyUtil/wintap_dbt/models/bronze/*.sql`
- `Wintap-PyUtil/wintap_dbt/models/silver/*.sql`
- `Wintap-PyUtil/wintap_dbt/models/gold/*.sql`
- `Wintap-PyUtil/wintap_dbt/models/monitoring/build_summary.sql`

### Legacy/reference Python SQL

- `Wintap-PyUtil/wintappy/datautils/initdb.sql`
- `Wintap-PyUtil/wintappy/datautils/rawtostdview.sql`
- `Wintap-PyUtil/wintappy/datautils/process_summary.sql`
- `Wintap-PyUtil/wintappy/datautils/process_path.sql`
- `Wintap-PyUtil/wintappy/datautils/label_summary.sql`
- `Wintap-PyUtil/wintappy/datautils/lolbas_summary.sql`
- `Wintap-PyUtil/wintappy/datautils/mitre_summary.sql`
- `Wintap-PyUtil/wintappy/datautils/sigma_summary.sql`
- `Wintap-PyUtil/wintappy/datautils/uber_summary.sql`

### TeleTap scaffold

- `Lintap/teletap/initdb.sql`
- `Lintap/teletap/load-data.sql`
- `Lintap/teletap/load-pidstat.sql`
- `Lintap/teletap/summary_ddl.sql`
- `Lintap/teletap/summary.sql`

## Installation snippets

Development install for `Wintap-PyUtil`:

```sh
cd Wintap-PyUtil
uv sync --dev
```

ACME4 analytics project:

```sh
cd wintap-analytics/2025-acme4-explore
uv run python -m ipykernel install --user --name acme4-explore --display-name "ACME4 Explore"
uv run jupyter lab
```
