# Ilum smoke test for Wintap dbt/Spark

This runbook validates the minimum Ilum path for the `wintap_dbt` proof of concept.

Current live/validated connector:

```text
Spark Connect/gRPC service name: spark
Host: spark.acme.dev
Port: 15002
SPARK_REMOTE=sc://spark.acme.dev:15002
```

Current validated S3 input:

```text
s3a://ilum-data/lintap/raw_sensor
```

The older Thrift/Kyuubi flow is still documented below because some Ilum deployments expose that shape, but for the current running connector use the `spark` dbt target, not `ilum`.

This runbook validates:

1. a long-running Spark connector service exposing Spark SQL access;
2. `dbt debug` from an Ilum/local job against that endpoint;
3. the POC dbt model proving Spark can read Wintap Parquet from S3 and write a managed table.

The instructions below are intentionally explicit but still contain a few deployment-specific placeholders. Ilum REST payloads and UI labels can vary by version; when in doubt, create the object in the Ilum UI first, inspect/export the generated JSON/YAML, then map the same fields into the example payloads.

## What this repository provides

Relevant project files:

```text
wintap_dbt/profiles.yml.example
wintap_dbt/profiles.yml
wintap_dbt/models/bronze/poc_s3_raw_process.sql
wintap_dbt/scripts/submit_ilum_dbt_job.py
```

The dbt Spark profiles are controlled by environment variables. Use `spark` for Spark Connect/gRPC and `ilum` only for a Thrift/Kyuubi endpoint:

```yaml
spark:
  type: spark
  method: session
  host: "{{ env_var('SPARK_CONNECT_HOST', 'spark.acme.dev') }}"
  port: "{{ env_var('SPARK_CONNECT_PORT', '15002') | int }}"
  schema: "{{ env_var('WINTAP_DBT_SCHEMA', 'wintap_smoke') }}"
  server_side_parameters:
    spark.remote: "{{ env_var('SPARK_REMOTE', 'sc://spark.acme.dev:15002') }}"

ilum:
  type: spark
  method: "{{ env_var('ILUM_KYUUBI_METHOD', 'thrift') }}"
  host: "{{ env_var('ILUM_KYUUBI_HOST') }}"
  port: "{{ env_var('ILUM_KYUUBI_PORT', '10009') | int }}"
  schema: "{{ env_var('WINTAP_DBT_SCHEMA', 'wintap') }}"
```

The POC model reads:

```text
$WINTAP_DBT_DATASET/raw_sensor/raw_process
```

using Spark SQL Parquet path syntax:

```sql
parquet.`s3a://.../raw_sensor/raw_process`
```

## Prerequisites

You need:

- an Ilum cluster with Spark application support;
- an Ilum Spark connector / Kyuubi / Thrift service capability;
- S3/object-storage credentials configured in Ilum, not in dbt;
- a Wintap dataset in S3 with at least:

```text
s3a://<bucket>/<prefix>/parquet/raw_sensor/raw_process/dayPK=<YYYYMMDD>/hourPK=<HH>/*.parquet
```

- an image available to Ilum that can run this repository's dbt workflow. The image must include, or be able to install:
  - Python compatible with the project;
  - `git`;
  - `uv` or another Python dependency installer;
  - `dbt-spark[PyHive]`;
  - Java/Spark/Hadoop client libraries needed by the Ilum environment.

The repository dependency list includes:

```text
dbt-duckdb
dbt-spark[PyHive]
```

so a job can normally run:

```sh
uv run --isolated --dev --project . dbt ...
```

if `uv` is present in the runtime image.

## Variables used in this runbook

Set these values before creating the Ilum resources:

```sh
# Ilum API/UI
export ILUM_API_URL="https://<ilum-api-host>"
export ILUM_API_TOKEN="<token-if-required>"

# Git source for the dbt project
export ILUM_GIT_REPO="https://github.com/LLNL/Wintap-PyUtil.git"
export ILUM_GIT_BRANCH="<this-branch>"

# Wintap data in S3. This path should contain raw_sensor/ beneath it.
export WINTAP_DBT_DATASET="s3a://ilum-data/lintap"
export WINTAP_DBT_START_DAY="20260530"
export WINTAP_DBT_END_DAY="20260530"
export WINTAP_DBT_INCLUDE_MONITORING=False
export WINTAP_DBT_AVAILABLE_RAW_EVENTS=raw_host,raw_process,raw_macip,raw_process_conn_incr,raw_process_file

# dbt/Spark namespace
export WINTAP_DBT_SCHEMA="wintap_smoke"

# Current Spark Connect/gRPC endpoint.
export SPARK_CONNECT_HOST="spark.acme.dev"
export SPARK_CONNECT_PORT="15002"
export SPARK_REMOTE="sc://spark.acme.dev:15002"

# For Thrift/Kyuubi only, use target=ilum and set:
# export ILUM_KYUUBI_HOST="<kyuubi-service-host>"
# export ILUM_KYUUBI_PORT="10009"
# export ILUM_KYUUBI_METHOD="thrift"
```

Use a small day range for the first smoke test. The POC only needs a day/hour containing a few `raw_process` rows.

## Step 1: Create and run the Spark connector service

The first resource should be a long-running Spark connector service that creates a Kyuubi/Thrift endpoint. dbt connects to this endpoint using the `dbt-spark` Thrift adapter.

In Ilum this may appear as one of the following depending on version/configuration:

- **Spark Connector**;
- **Kyuubi connector**;
- **Interactive Spark Service**;
- **Spark Thrift Server**;
- a service/job template that exposes a Thrift endpoint for Spark SQL.

### Required connector settings

Configure the connector/service with:

| Setting | Value / guidance |
| --- | --- |
| Runtime | Spark runtime compatible with the cluster |
| SQL gateway | Kyuubi or Spark Thrift-compatible endpoint |
| Endpoint protocol | Thrift preferred for current `profiles.yml` |
| Port | `10009` unless your Ilum template uses another port |
| Catalog | Hive metastore by default, or the cluster's configured Iceberg/Delta catalog |
| Object storage | Ilum-managed object-storage credentials, e.g. `ilum-objectstorage` |
| S3 scheme | `s3a://` |
| Hadoop S3A implementation | `org.apache.hadoop.fs.s3a.S3AFileSystem` |
| Lifetime | Long enough for all smoke-test jobs to run |

Do **not** put AWS access keys in `profiles.yml`. The connector's Spark driver/executors should receive object-storage credentials through Ilum/Kubernetes configuration.

### Spark configuration checklist

The exact syntax is Ilum-specific, but the service should include the equivalent of:

```properties
spark.sql.catalogImplementation=hive
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

If your cluster uses Iceberg or Delta, add the catalog extensions/warehouse settings required by that Ilum environment. Keep those settings on the Spark/Ilum service, not in the dbt profile unless your local standards require otherwise.

### Start the connector

Using the Ilum UI:

1. Open the Ilum application/services/jobs page.
2. Create a new Spark connector / interactive Spark SQL service.
3. Attach the object-storage alias/configuration, for example `ilum-objectstorage`.
4. Start the service.
5. Wait until the service is `RUNNING`/`READY`.
6. Record the service DNS name and Thrift port.

Expected endpoint examples:

```text
<connector-name>.<namespace>.svc.cluster.local:10009
<connector-name>-kyuubi.<namespace>.svc:10009
<external-ilum-gateway-host>:10009
```

Export the endpoint for later jobs:

```sh
export ILUM_KYUUBI_HOST="<actual-host-from-ilum>"
export ILUM_KYUUBI_PORT="10009"
```

### Optional REST/API creation pattern

If your Ilum deployment supports creating connector services through the API, create the connector with the API shape documented by your cluster. The following is a semantic template, not a guaranteed schema:

```json
{
  "name": "wintap-dbt-kyuubi-smoke",
  "type": "spark-connector",
  "mode": "interactive",
  "connector": "kyuubi",
  "sparkConf": {
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
  },
  "objectStorage": "ilum-objectstorage",
  "ports": [
    {
      "name": "thrift",
      "port": 10009
    }
  ]
}
```

Submit with the endpoint and method used by your Ilum version, then poll until ready. If your UI can export the connector JSON, prefer the exported payload over this template.

## Step 2: Create and run a `dbt debug` job

This job confirms that an Ilum workload can:

- clone or otherwise access this repository;
- install/resolve the dbt runtime;
- render `wintap_dbt/profiles.yml`;
- connect to the Spark/Kyuubi Thrift endpoint.

Important: `dbt debug` proves the Spark/Kyuubi connection. It does **not** by itself prove that Spark can read the Wintap S3 path. Step 3 runs the POC model for the S3 read/write test.

### Command to run in the Ilum job

Use this as the job command:

```sh
set -euxo pipefail
rm -rf /tmp/Wintap-PyUtil
git clone --branch "$ILUM_GIT_BRANCH" --depth 1 "$ILUM_GIT_REPO" /tmp/Wintap-PyUtil
cd /tmp/Wintap-PyUtil
uv run --isolated --dev --project . dbt debug \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target ilum
```

If your image does not include `uv`, replace the last command with your image's dbt executable, for example:

```sh
dbt debug --project-dir wintap_dbt --profiles-dir wintap_dbt --target ilum
```

### Required job environment

The `dbt debug` job needs these environment variables:

```sh
WINTAP_DBT_TARGET=ilum
WINTAP_DBT_DATASET=s3a://<bucket>/<prefix>/parquet
WINTAP_DBT_START_DAY=<YYYYMMDD>
WINTAP_DBT_END_DAY=<YYYYMMDD>
WINTAP_DBT_SCHEMA=wintap_smoke
WINTAP_DBT_INCLUDE_MONITORING=False

ILUM_KYUUBI_HOST=<connector-thrift-host>
ILUM_KYUUBI_PORT=10009
ILUM_KYUUBI_METHOD=thrift
```

`WINTAP_DBT_DATASET`, `WINTAP_DBT_START_DAY`, and `WINTAP_DBT_END_DAY` are included even though `dbt debug` mainly validates profile connectivity. They keep project rendering consistent with the POC build job.

### Semantic Ilum job payload

Use your Ilum UI/template to create a job with the command above, or adapt this semantic API payload to your Ilum version:

```json
{
  "name": "wintap-dbt-debug-smoke",
  "type": "job",
  "image": "<image-with-git-python-uv-dbt-spark>",
  "env": {
    "ILUM_GIT_REPO": "https://github.com/LLNL/Wintap-PyUtil.git",
    "ILUM_GIT_BRANCH": "<this-branch>",
    "WINTAP_DBT_TARGET": "ilum",
    "WINTAP_DBT_DATASET": "s3a://<bucket>/<prefix>/parquet",
    "WINTAP_DBT_START_DAY": "20250101",
    "WINTAP_DBT_END_DAY": "20250101",
    "WINTAP_DBT_SCHEMA": "wintap_smoke",
    "WINTAP_DBT_INCLUDE_MONITORING": "False",
    "ILUM_KYUUBI_HOST": "<connector-thrift-host>",
    "ILUM_KYUUBI_PORT": "10009",
    "ILUM_KYUUBI_METHOD": "thrift"
  },
  "command": [
    "/bin/bash",
    "-lc",
    "set -euxo pipefail && rm -rf /tmp/Wintap-PyUtil && git clone --branch \"$ILUM_GIT_BRANCH\" --depth 1 \"$ILUM_GIT_REPO\" /tmp/Wintap-PyUtil && cd /tmp/Wintap-PyUtil && uv run --isolated --dev --project . dbt debug --project-dir wintap_dbt --profiles-dir wintap_dbt --target ilum"
  ]
}
```

### Expected `dbt debug` success criteria

The logs should show:

```text
Connection test: OK connection ok
All checks passed!
```

Also confirm the resolved profile in the logs references:

```text
type: spark
method: thrift
host: <connector-thrift-host>
port: 10009
schema: wintap_smoke
```

### Common `dbt debug` failures

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `Could not connect to any of ...:10009` | Connector service not running, wrong host, wrong port, network policy | Verify connector is ready and reachable from the job namespace |
| `Runtime Error: database must be omitted or have the same value as schema` | Spark profile contains `database` that differs from `schema` | Use the repository's current `profiles.yml` without `database` for the Spark target |
| `No module named pyhive` / adapter load failure | Runtime image lacks `dbt-spark[PyHive]` | Use `uv run --isolated --dev --project .` or bake dependencies into the image |
| Authentication/SASL errors | Kyuubi auth mode mismatch | Align `dbt-spark` profile auth settings with the Ilum/Kyuubi service |
| Schema creation denied | Spark/Kyuubi principal lacks catalog permissions | Grant create/use permissions for `WINTAP_DBT_SCHEMA` |

## Step 3: Run the POC S3 read/write dbt job

After `dbt debug` passes, run the POC model. This is the actual S3 smoke test.

### Command

```sh
set -euxo pipefail
rm -rf /tmp/Wintap-PyUtil
git clone --branch "$ILUM_GIT_BRANCH" --depth 1 "$ILUM_GIT_REPO" /tmp/Wintap-PyUtil
cd /tmp/Wintap-PyUtil
uv run --isolated --dev --project . dbt build \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target ilum \
  --select poc_s3_raw_process
```

Use the same environment variables as the `dbt debug` job.

### Expected build success criteria

The logs should show:

```text
1 of 1 OK created sql table model ...poc_s3_raw_process
Completed successfully
```

The compiled SQL should contain a Spark Parquet path similar to:

```sql
from parquet.`s3a://<bucket>/<prefix>/parquet/raw_sensor/raw_process`
```

This confirms:

- dbt connected to Spark through the connector's Thrift endpoint;
- Spark could read the raw Wintap Parquet path in S3;
- dbt/Spark could create a managed table in `WINTAP_DBT_SCHEMA`.

## Step 4: Verify the created table

Use one of these options.

### Option A: dbt run-operation or ad hoc Spark SQL through the same endpoint

If you have a SQL client configured for the connector, run:

```sql
show tables in wintap_smoke;
select count(*) from wintap_smoke.poc_s3_raw_process;
select * from wintap_smoke.poc_s3_raw_process limit 10;
```

### Option B: create a second lightweight dbt job

Run:

```sh
uv run --isolated --dev --project . dbt show \
  --project-dir wintap_dbt \
  --profiles-dir wintap_dbt \
  --target ilum \
  --inline "select count(*) as rows from {{ target.schema }}.poc_s3_raw_process"
```

If `dbt show --inline` is not available in the installed dbt version, use a SQL client or a tiny Python/PyHive script in the same runtime image.

## Optional: submit with the repository helper script

This repository includes:

```text
wintap_dbt/scripts/submit_ilum_dbt_job.py
```

The helper posts to:

```text
POST $ILUM_API_URL/api/v1/jobs
```

Because Ilum job schemas vary, the safest use is to provide an exact payload exported from your Ilum UI:

```sh
export ILUM_API_URL="https://<ilum-api-host>"
export ILUM_API_TOKEN="<token-if-required>"
export ILUM_JOB_PAYLOAD_FILE="/path/to/dbt-debug-job-payload.json"
python3 wintap_dbt/scripts/submit_ilum_dbt_job.py
```

For the POC build job:

```sh
export ILUM_JOB_PAYLOAD_FILE="/path/to/dbt-build-poc-job-payload.json"
python3 wintap_dbt/scripts/submit_ilum_dbt_job.py
```

If your Ilum API accepts the helper's default payload shape, you can instead set:

```sh
export ILUM_API_URL="https://<ilum-api-host>"
export ILUM_API_TOKEN="<token-if-required>"
export ILUM_GIT_REPO="https://github.com/LLNL/Wintap-PyUtil.git"
export ILUM_GIT_BRANCH="<this-branch>"
export WINTAP_DBT_DATASET="s3a://<bucket>/<prefix>/parquet"
export WINTAP_DBT_START_DAY="20250101"
export WINTAP_DBT_END_DAY="20250101"
export WINTAP_DBT_SCHEMA="wintap_smoke"
export ILUM_KYUUBI_HOST="<connector-thrift-host>"
export ILUM_KYUUBI_PORT="10009"

export ILUM_DBT_COMMAND="uv run --isolated --dev --project . dbt debug --project-dir wintap_dbt --profiles-dir wintap_dbt --target ilum"
python3 wintap_dbt/scripts/submit_ilum_dbt_job.py
```

Then repeat with:

```sh
export ILUM_DBT_COMMAND="uv run --isolated --dev --project . dbt build --project-dir wintap_dbt --profiles-dir wintap_dbt --target ilum --select poc_s3_raw_process"
python3 wintap_dbt/scripts/submit_ilum_dbt_job.py
```

## Cleanup

After the smoke test:

1. Drop the test schema/table if desired:

```sql
drop table if exists wintap_smoke.poc_s3_raw_process;
drop database if exists wintap_smoke cascade;
```

2. Stop/delete the dbt smoke-test jobs.
3. Stop/delete the Spark connector service if it was created only for this test.
4. Remove any temporary service accounts, tokens, or test object-storage grants.

## Minimal success checklist

The smoke test is successful when all of the following are true:

- [ ] Spark connector service is `RUNNING`/`READY`.
- [ ] A Thrift endpoint host and port are available to Ilum jobs.
- [ ] `dbt debug --target ilum` exits successfully from an Ilum job.
- [ ] `dbt build --select poc_s3_raw_process --target ilum` exits successfully from an Ilum job.
- [ ] `wintap_smoke.poc_s3_raw_process` or your chosen schema/table exists in the Spark catalog.
- [ ] `select count(*)` from the POC table returns rows or a valid zero-row result for the selected day range.

## Notes and assumptions

- `dbt debug` validates the dbt profile and Spark/Kyuubi connectivity. It does not fully validate S3 access; the POC build does that.
- The repository profile intentionally does not hardcode S3 credentials.
- `WINTAP_DBT_INCLUDE_MONITORING=False` keeps the smoke test focused on the Spark connector and the POC model.
- Use `s3a://`, not `s3://`, for Spark/Hadoop reads unless your Ilum cluster explicitly maps `s3://`.
- Start with a narrow day range to keep the initial Spark job fast and cheap.
