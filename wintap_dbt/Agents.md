This summary is formatted as a comprehensive technical instruction document (Agent context) designed for use with an AI coding assistant like **pi.dev**. It synthesizes our conversation and the technical requirements for migrating your pipeline to **Ilum**.

***

# Agent Context: dbt-to-Ilum Migration

## Mission
Migrate the existing dbt pipeline from the **Wintap-PyUtil** project (branch: `grants-add-dbt`, directory: `wintap_dbt`) from a local DuckDB/Parquet environment to a distributed **Spark** architecture managed by **Ilum**. The final implementation must support reading from **S3**, writing to a persistent Spark catalog (Hive/Iceberg/Delta), and execution via a custom Python application using the **Ilum REST API**.

## Technical Architecture
*   **Orchestration & Execution:** Ilum (Kubernetes-native Spark management).
*   **SQL Gateway:** Apache Kyuubi (fronting the Spark engine).
*   **Storage:** AWS S3 (configured via the `ilum-objectstorage` alias to decouple credentials from dbt profiles).
*   **Metadata/Catalog:** Hive Metastore (default) or Iceberg/Delta Lake.

## Task 1: dbt Profile Configuration (`profiles.yml`)
Configure a Spark profile optimized for Ilum's Kyuubi gateway.
*   **Adapter:** `dbt-spark`.
*   **Method:** `thrift` or `http` (to connect to long-running Interactive Services).
*   **Host/Port:** Point to the Ilum cluster endpoint.
*   **Schema:** Target database in the Hive Metastore.
*   **Note:** Do **not** hardcode S3 credentials; Ilum injects these automatically into the Spark driver.

## Task 2: Model Adaptation for Spark/S3
Refactor dbt models in the `wintap_dbt` directory to align with Spark SQL and S3-backed storage.
*   **Source Definitions:** Update `sources.yml` to utilize S3 URIs (e.g., `s3a://...`).
*   **Materializations:** Implement `table` or `incremental` materializations. 
*   **Nested Data:** Use dot notation for accessing nested fields in Spark-processed Parquet files.
*   **Transpilation:** If existing DuckDB SQL syntax is incompatible with Spark SQL, utilize Ilum's built-in dialect transpiler.

## Task 3: Python Orchestration Layer
Create or modify a Python script within the application to trigger dbt jobs programmatically via the **Ilum REST API (OpenAPI 3.0)**.
*   **Endpoint:** Utilize `POST /api/v1/jobs` for batch executions or submit code snippets to an active **Interactive Service** for reduced startup time.
*   **Payload:** Specify the Git repository (`Wintap-PyUtil`), branch (`grants-add-dbt`), and the specific dbt command (e.g., `dbt build --select poc_table`).
*   **Monitoring:** Use the returned `Group ID` to poll the API for job status, performance metrics, and logs.

## Implementation Steps for Agent
1.  **Analyze** the `wintap_dbt` directory structure on the `grants-add-dbt` branch of the provided repository.
2.  **Generate** a `profiles.yml` specifically for a Spark/Kyuubi connection within the Ilum environment.
3.  **Draft** a proof-of-concept dbt model that reads a single table from S3 and materializes it in the Hive catalog.
4.  **Create** a Python trigger script that demonstrates a `POST` request to the Ilum API to launch this dbt task.
5.  **Ensure** the solution follows dbt "Collective Wisdom" best practices, such as modularity and incremental loading logic.