# Section 1: Developing Code — Calibrated Exam Question Bank

> **Domain:** Section 1: Developing Code for Data Processing using Python and SQL (23%)  
> **Source:** Post-October 2026 Databricks Certified Data Engineer Professional Syllabus  
> **Purpose:** Permanent retention, review, and exam revision.

---

## Question Bank Index

- [Subpoint 1.1: DABs Project Structures & Environments](#subpoint-11-dabs-project-structures--environments)
- [Subpoint 1.2: Dependency Conflicts & Resolution](#subpoint-12-dependency-conflicts--resolution)
- [Subpoint 1.3: User-Defined Functions (UDFs) & Unity Catalog](#subpoint-13-user-defined-functions-udfs--unity-catalog)
- [Subpoint 1.4: Lakeflow Pipelines & Auto Loader](#subpoint-14-lakeflow-pipelines--auto-loader)
- [Subpoint 1.5: Lakeflow Jobs Automation](#subpoint-15-lakeflow-jobs-automation)
- [Subpoint 1.6: Streaming Tables vs. Materialized Views](#subpoint-16-streaming-tables-vs-materialized-views)
- [Subpoint 1.7: AUTO CDC & SCD Type 1/2](#subpoint-17-auto-cdc--scd-type-12)
- [Subpoint 1.8: Structured Streaming vs. Lakeflow Declarative Pipelines](#subpoint-18-structured-streaming-vs-lakeflow-declarative-pipelines)
- [Subpoint 1.9: Lakeflow Jobs Control-Flow Operators](#subpoint-19-lakeflow-jobs-control-flow-operators)
- [Subpoint 1.10: Compute Selection & Serverless Constraints](#subpoint-110-compute-selection--serverless-constraints)
- [Subpoint 1.11: Unit & Integration Testing](#subpoint-111-unit--integration-testing)
- [Subpoint 1.12: Structured Streaming Stateful Processing & Idempotency](#subpoint-112-structured-streaming-stateful-processing--idempotency)

---

### Subpoint 1.1: DABs Project Structures & Environments

#### Question 1.1.1: Development vs. Production Mode Semantics
A data engineering team is deploying a Lakeflow Pipeline and a scheduled Lakeflow Job using Declarative Automation Bundles (DABs). A developer deploys their bundle using a custom target configured with `mode: development`.

Which set of behaviors will the Databricks CLI automatically enforce for this deployment?

- A) The pipeline is deployed without prefixes, schedules remain active, and code is synced to `/Workspace/.bundle/`.
- B) Deployed resource names are prefixed with `[dev <username>]`, scheduled job triggers are automatically paused, and files are deployed under the user's home workspace directory.
- C) The deployment fails unless a Service Principal token is provided in the `run_as` block.
- D) The bundle forces the execution of all unit tests in `tests/` before provisioning any workspace compute.

**Correct Answer:** **B**

> **Explanation:**  
> In Databricks Asset Bundles, setting `mode: development` provides three automatic safety guardrails:
> 1. It prefixes resource names with `[dev <username>]` so developers don't clash with shared or production assets.
> 2. It automatically pauses triggers and schedules on jobs so unintended cron runs don't consume compute during active development.
> 3. It targets the developer's personal workspace directory (`/Workspace/Users/<username>/.bundle/...`).
> Production mode (`mode: production`), conversely, keeps exact names, enables schedules, and typically runs under a service principal.

> **🎯 10-Second Elimination Rule:**  
> Look for "prefixed with `[dev <username>]`" and "schedules are automatically paused". Any option claiming schedules remain active in `development` mode is an immediate elimination.

---

#### Question 1.1.2: Modular Project Layout and Artifact Resolution
A data engineer is structuring a large-scale project containing multiple Lakeflow pipelines, daily batch jobs, and a shared Python utility package in `src/`. The team wants to ensure clean CI/CD integration, modular code review, and prevent `databricks.yml` from becoming a monolithic configuration file.

Which project structure represents the Databricks recommended best practice for Declarative Automation Bundles?

- A) Store all job definitions, pipeline definitions, and Python code inside a single monolithic `databricks.yml` file in the root directory.
- B) Separate pipeline and job definitions into individual YAML files inside a `resources/` directory, reference them in `databricks.yml` using `include: - resources/*.yml`, and configure the `artifacts` section to package `src/` into a wheel.
- C) Keep pipeline definitions in `dbfs:/FileStore/bundles/`, compile Python scripts into `.jar` files, and reference them via DBFS mounts.
- D) Store all pipeline configurations as JSON inside Unity Catalog Volume metadata tags and invoke them dynamically using `spark.sql()`.

**Correct Answer:** **B**

> **Explanation:**  
> The enterprise standard for DABs separates infrastructure resources into modular YAML files within a `resources/` directory (`resources/pipelines.yml`, `resources/jobs.yml`), which are imported into the root `databricks.yml` via `include: - resources/*.yml`. Shared Python logic in `src/` is packaged as an installable wheel using the `artifacts` block (`type: whl`), allowing automated build and deployment across `dev` and `prod` targets. DBFS mounts (Option C) and monolithic configurations (Option A) violate 2026 enterprise best practices.

> **🎯 10-Second Elimination Rule:**  
> Eliminate any option mentioning `dbfs:/` or mounts (violates Law 1). Eliminate monolithic configurations. Look for the `include:` directive with `resources/*.yml` and wheel packaging in `artifacts`.

---

### Subpoint 1.2: Dependency Conflicts & Resolution

#### Question 1.2.1: Managing External Python Dependencies in Lakeflow Pipelines
A data engineer is building a streaming Lakeflow Pipeline using Python notebooks. One of the transformation notebooks requires an external geospatial parsing package (`geopandas==0.14.2`). The engineer places `%pip install geopandas==0.14.2` in the first cell of the notebook. When the pipeline update is triggered, the execution fails with an initialization error.

What is the root cause of this failure, and how should the engineer resolve it?

- A) Serverless Lakeflow Pipelines require libraries to be installed via `dbfs:/init.sh` cluster init scripts.
- B) Lakeflow Pipelines do not support `%pip` inside pipeline notebooks because they initialize cluster workers in parallel DAG tasks; dependencies must be specified declaratively in the pipeline configuration via `pipelines.pip.requirements` or a referenced `requirements.txt`.
- C) Lakeflow Pipelines only support pure SQL transformations; external Python libraries cannot be imported into streaming tables.
- D) The engineer forgot to run `dbutils.fs.refresh()` after executing the `%pip install` command.

**Correct Answer:** **B**

> **Explanation:**  
> `%pip` and `%conda` magic commands are designed for interactive, single-user notebook execution. In Lakeflow Pipelines, tables are compiled into an execution DAG distributed across worker nodes. Running `%pip` dynamically inside a pipeline notebook breaks cluster node synchronization and fails initialization. Dependencies in Lakeflow Pipelines must be declared in the pipeline configuration (e.g., using `pipelines.pip.requirements` in the pipeline settings or specifying a wheel/package in the pipeline definition).

> **🎯 10-Second Elimination Rule:**  
> Eliminate any option proposing init scripts (Option A) or `dbutils.fs` (Option D). Eliminate Option C (Python is fully supported in Lakeflow). Recognize that `%pip` inside Lakeflow notebooks is an explicit anti-pattern.

---

#### Question 1.2.2: Deploying Custom and Proprietary Libraries in Regulated / Egress-Restricted Environments
A security policy in a financial institution prohibits Databricks Serverless compute from accessing the public internet, preventing downloads from `pypi.org`. A data engineering team needs to deploy an in-house proprietary encryption package (`fin_crypto-1.2.0-py3-none-any.whl`) to both a Lakeflow Pipeline and a scheduled Lakeflow Job.

Which deployment strategy satisfies both security policy and Databricks best practices?

- A) Upload the wheel to a Unity Catalog Volume (e.g. `/Volumes/prod_catalog/secure_schema/wheels/`), grant `READ VOLUME` permissions to the execution identity, and reference the volume path in the job and pipeline library configurations.
- B) Base64-encode the wheel file and pass it as a dynamic Spark SQL session parameter (`spark.conf.set("spark.wheel.payload", ...)`).
- C) Create a cluster init script that downloads the wheel from an unauthenticated public S3 bucket using `curl`.
- D) Commit the binary `.whl` directly into the root of the Git repository and import it using Python's `sys.path.append()`.

**Correct Answer:** **A**

> **Explanation:**  
> In modern Unity Catalog architectures, Unity Catalog Volumes provide governed, auditable, and secure storage for non-tabular data, including `.whl`, `.jar`, and `.zip` files. In air-gapped or egress-restricted environments where public PyPI is blocked, organization-approved libraries are uploaded to a UC Volume. Access is governed via UC ACLs (`READ VOLUME`), and jobs or pipelines reference the volume path directly (`/Volumes/...`).

> **🎯 10-Second Elimination Rule:**  
> Eliminate Init scripts (Option C - forbidden on Serverless). Eliminate SQL session parameters for binary files (Option B). Eliminate storing large binaries in Git (Option D). UC Volumes is the standard for governed non-tabular assets.

---

### Subpoint 1.3: User-Defined Functions (UDFs) & Unity Catalog

#### Question 1.3.1: Performance Hierarchy & Photon Execution
A data engineer needs to apply a complex financial discount calculation across a 100-million row Delta table on a Databricks Serverless compute environment. The calculation relies solely on basic arithmetic, date checks, and string manipulation. The company wants to minimize compute runtime and take full advantage of the native Photon acceleration engine.

Which implementation strategy should the engineer choose?

- A) Implement the logic as a standard Python UDF using the `@udf` decorator with `returnType=DoubleType()`.
- B) Define a SQL UDF directly in Unity Catalog (`CREATE FUNCTION ... RETURN ...`), allowing the Catalyst optimizer and Photon engine to inline and accelerate the execution natively.
- C) Implement a Python UDF and set `spark.conf.set("spark.sql.execution.pythonUDF.photon.enabled", "true")`.
- D) Write the logic as an RDD map transformation using `df.rdd.map()` to bypass the DataFrame API overhead.

**Correct Answer:** **B**

> **Explanation:**  
> SQL UDFs registered in Unity Catalog are natively inlined and compiled by the Catalyst optimizer and Photon C++ execution engine. They incur zero serialization overhead. Standard Python UDFs (Option A) suffer from row-by-row pickle serialization, Option C refers to a non-existent Spark configuration, and RDD operations (Option D) are strictly forbidden on Serverless compute / Spark Connect.

> **🎯 10-Second Elimination Rule:**  
> Look for Photon acceleration $\rightarrow$ must be native Spark SQL or Unity Catalog SQL UDF. Eliminate any option mentioning RDDs (Serverless Law 3) or fake Spark confs.

---

#### Question 1.3.2: Cross-Language Governance with Unity Catalog Functions
A data platform team wants to standardize customer churn scoring. The scoring algorithm was developed by a data scientist in Python using custom business rules. The team needs this exact logic to be:
1. Centrally version-controlled and governed with access controls (`GRANT EXECUTE`).
2. Directly callable by data analysts writing pure SQL queries in Databricks SQL Warehouses.
3. Usable by data engineers inside PySpark streaming pipelines without maintaining duplicate code.

Which solution satisfies all requirements with the lowest maintenance overhead?

- A) Create a Python UDF registered in Unity Catalog (`CREATE OR REPLACE FUNCTION ... LANGUAGE PYTHON AS $$ ... $$`), and grant `EXECUTE` privileges to the relevant groups.
- B) Publish a Python wheel to a shared Git repository and instruct all analysts and engineers to install it in their local environments.
- C) Export the logic as an ONNX model and query it using the legacy `mlflow.pyfunc.spark_udf()` API on all clusters.
- D) Copy-paste the Python function into every user's personal notebook root directory.

**Correct Answer:** **A**

> **Explanation:**  
> Unity Catalog supports functions authored in Python (`LANGUAGE PYTHON`). These functions are registered directly in the catalog hierarchy (`catalog.schema.function_name`) and can be called seamlessly by SQL users in SQL Warehouses as well as PySpark users in pipelines. Access control is managed centrally via `GRANT EXECUTE ON FUNCTION`. Publishing wheels (Option B) doesn't allow SQL Warehouse analysts to call them, and manual copying (Option D) creates maintenance chaos.

> **🎯 10-Second Elimination Rule:**  
> Analysts in Databricks SQL Warehouses cannot run Python scripts or install wheels. The only way to provide them custom Python logic is via Unity Catalog Functions (`LANGUAGE PYTHON`).

---

### Subpoint 1.4: Lakeflow Pipelines & Auto Loader

#### Question 1.4.1: Auto Loader Schema Evolution & Rescued Data
A data engineer is ingesting semi-structured JSON transaction files from a cloud storage volume into a Bronze Delta table using Auto Loader (`cloudFiles`). The downstream analytics application requires that the Bronze table schema remains strictly fixed and does not automatically add new columns when upstream developers introduce new JSON attributes. However, regulatory requirements dictate that no data is permanently lost or dropped if unexpected columns or type mismatches appear.

Which Auto Loader configuration should the engineer implement?

- A) Set `.option("cloudFiles.schemaEvolutionMode", "addNewColumns")` and use `DROP UNKNOWN COLUMNS` in the query.
- B) Set `.option("cloudFiles.schemaEvolutionMode", "rescue")`, allowing unexpected fields and type-mismatched data to be parsed into the `_rescued_data` column while keeping the table schema static.
- C) Set `.option("cloudFiles.schemaEvolutionMode", "failOnNewSchema")` and configure an automated email alert to notify engineers of new columns.
- D) Set `.option("cloudFiles.schemaEvolutionMode", "none")` and disable schema inference.

**Correct Answer:** **B**

> **Explanation:**  
> Setting `cloudFiles.schemaEvolutionMode = "rescue"` prevents the schema from evolving (adding new columns) while ensuring zero data loss. Any unexpected columns or records with mismatched data types (such as non-numeric strings in numeric columns) are captured as JSON in the automatically appended `_rescued_data` column. `addNewColumns` (Option A) modifies the schema, `failOnNewSchema` (Option C) halts pipeline execution, and `none` (Option D) drops unknown fields.

> **🎯 10-Second Elimination Rule:**  
> "Schema remains strictly fixed" + "No data permanently lost or dropped" $\rightarrow$ `schemaEvolutionMode = "rescue"` and `_rescued_data`.

---

#### Question 1.4.2: Lakeflow Pipelines State & Checkpoint Architecture
A senior data architect is reviewing a junior engineer's pull request that introduces a new Bronze-to-Silver streaming table within a Lakeflow Declarative Pipeline (`@dlt.table`). The junior engineer has included the following line in the streaming table definition:

```python
.option("checkpointLocation", "/Volumes/prod_catalog/checkpoints/bronze_to_silver")
```

What feedback should the senior architect provide according to Databricks best practices and engine architecture?

- A) The syntax is valid, but the checkpoint path must use a legacy DBFS mount (`dbfs:/mnt/checkpoints/`) rather than a Unity Catalog Volume.
- B) The `checkpointLocation` option should be removed because Lakeflow Pipelines automatically manage checkpointing, state recovery, and metadata internally without manual path specifications.
- C) The checkpoint location must point to an Amazon S3 or Azure Blob container directly using cloud credentials rather than a Unity Catalog namespace.
- D) In Lakeflow Pipelines, `checkpointLocation` is only valid when the pipeline runs in continuous mode (`continuous: true`), not in triggered mode.

**Correct Answer:** **B**

> **Explanation:**  
> In Lakeflow Pipelines (formerly Delta Live Tables), the runtime execution engine fully manages the storage location for checkpointing, transaction logs, and internal streaming state. Manually configuring `checkpointLocation` or `cloudFiles.schemaLocation` inside a Lakeflow pipeline is an exam anti-pattern and triggers an analysis/validation error.

> **🎯 10-Second Elimination Rule:**  
> Eliminate any option containing DBFS mounts (Option A). Eliminate cloud storage direct credential paths (Option C). Remember Law 4: Lakeflow Pipelines manage checkpoints automatically $\rightarrow$ remove `checkpointLocation`.

---

### Subpoint 1.5: Lakeflow Jobs Automation & DAG Orchestration

#### Question 1.5.1: Conditional Task Execution (`run_if`)
A data platform engineer is designing an enterprise Lakeflow Job DAG in Databricks consisting of 4 distinct tasks:
1. `validate_inputs`: Checks raw file schemas and data volumes.
2. `process_silver`: Transforms Bronze data into cleansed Silver tables (depends on `validate_inputs`).
3. `trigger_pagerduty_alert`: Sends high-priority notifications to the on-call engineer (depends on `process_silver`).
4. `archive_cluster_logs`: Cleans up temporary audit files and logs (depends on `process_silver` and `trigger_pagerduty_alert`).

The engineer requires `archive_cluster_logs` to execute **regardless of whether upstream tasks succeeded, failed, or were skipped**, to ensure compliance logs are never lost. Furthermore, `trigger_pagerduty_alert` must trigger **only if** `process_silver` fails.

Which task condition (`run_if`) policy must the engineer configure for `trigger_pagerduty_alert` and `archive_cluster_logs` respectively?

- A) `trigger_pagerduty_alert`: `AT_LEAST_ONE_FAILED` | `archive_cluster_logs`: `ALL_DONE`
- B) `trigger_pagerduty_alert`: `ALL_FAILED` | `archive_cluster_logs`: `ALL_SUCCESS`
- C) `trigger_pagerduty_alert`: `NONE_FAILED` | `archive_cluster_logs`: `AT_LEAST_ONE_SUCCESS`
- D) `trigger_pagerduty_alert`: `ON_FAILURE` | `archive_cluster_logs`: `ALWAYS`

**Correct Answer:** **A**

> **Explanation:**  
> In Databricks Lakeflow Jobs (Workflows), task conditional execution is controlled via the `run_if` attribute:
> - `ALL_SUCCESS` (default): Runs only when all upstream dependencies succeeded.
> - `AT_LEAST_ONE_FAILED`: Runs if at least one dependency failed (ideal for alert/fallback tasks).
> - `ALL_FAILED`: Runs only if all dependencies failed.
> - `ALL_DONE`: Runs once all dependencies finish, regardless of their outcome (`SUCCESS`, `FAILED`, `UPSTREAM_FAILED`, or `SKIPPED`).
> Options with `ON_FAILURE` or `ALWAYS` (Option D) are distractor syntax from airflow/generic CI/CD, not valid Databricks Lakeflow Job syntax.

> **🎯 10-Second Elimination Rule:**  
> Identify Databricks `run_if` enums: `ALL_DONE` is the only policy that runs regardless of outcome. `AT_LEAST_ONE_FAILED` is the standard trigger for alerting tasks. Eliminate fake values like `ALWAYS` or `ON_FAILURE`.

---

#### Question 1.5.2: Inter-Task Dynamic State Passing (`taskValues`)
In a multi-task Lakeflow Job, an initial task (`model_evaluation`) computes dynamic performance metrics (e.g., `f1_score = 0.94` and `dataset_drift = False`). A downstream deployment task (`deploy_endpoint`) needs to read these runtime metrics to determine whether to promote the model or halt deployment.

Which Databricks mechanism enables sharing runtime values between independent tasks within the same job run?

- A) Write the values to an OS environment variable using `os.environ["F1_SCORE"] = "0.94"`.
- B) Use `dbutils.jobs.taskValues.set(key=..., value=...)` in the producer task and `dbutils.jobs.taskValues.get(taskKey=..., key=...)` in the consumer task.
- C) Pass the values via a shared Spark broadcast variable (`sc.broadcast(metrics)`).
- D) Save the values in a temporary in-memory Pandas dataframe `metrics_df` accessible across notebook workers.

**Correct Answer:** **B**

> **Explanation:**  
> Databricks provides `taskValues` (`dbutils.jobs.taskValues`) specifically for passing small dynamic runtime states (up to 48KB total per task) between tasks in a single Lakeflow Job run. OS environment variables (Option A), Spark broadcast variables (Option C), and in-memory DataFrames (Option D) are scoped strictly to individual cluster/driver instances and cannot cross task boundaries in a distributed multi-task job.

---

### Subpoint 1.6: Streaming Tables vs. Materialized Views

#### Question 1.6.1: Latency, Cost, and Aggregation Architecture
A data engineer is designing a Lakeflow Pipeline to support executive FinOps reporting. 
The business requirements specify:
1. Upstream raw billing events arrive continuously into cloud storage (approx. 5,000 JSON files per day).
2. The executive dashboard displays total revenue per customer, average transaction size, and customer tier ranking across all historical data.
3. Executives review this dashboard **once every morning at 8:00 AM UTC**.
4. The cloud architecture team mandates minimizing compute costs while meeting the 8:00 AM freshness SLA.

Which architecture should the engineer deploy?

- A) Create a continuous `STREAMING TABLE` with an Auto Loader stream that continuously computes `GROUP BY customer_id` with `SUM(revenue)` and `ROW_NUMBER()`.
- B) Ingest raw data into a Bronze/Silver `STREAMING TABLE` using Auto Loader, and build a Gold `MATERIALIZED VIEW` that computes the aggregations and window rankings, scheduled to refresh once daily prior to 8:00 AM.
- C) Create a standard Delta table and configure a cron Lakeflow Job running every 5 minutes to execute `VACUUM` and `OPTIMIZE` before running a Python script that calculates rankings using `df.rdd.groupByKey()`.
- D) Ingest raw files directly into a Gold `MATERIALIZED VIEW` using `spark.readStream.format("cloudFiles")` running with continuous execution mode.

**Correct Answer:** **B**

> **Explanation:**  
> A Materialized View is designed for query-driven, pre-computed analytical aggregations and window functions that refresh on a scheduled trigger (such as daily before 8:00 AM). Ingesting raw files into a Streaming Table via Auto Loader ensures efficient incremental ingestion, while refreshing the Materialized View once per day eliminates the massive unnecessary compute costs of running continuous streaming aggregations 24/7 for a dashboard checked only once daily.

> **🎯 10-Second Elimination Rule:**  
> Identify the reporting frequency: "Checked once a day" $\rightarrow$ eliminate continuous streaming options (Option A & D). Eliminate Option C because `.rdd` violates Serverless compute constraints (Law 3).

---

#### Question 1.6.2: Handling Upstream Modifications (Updates & Deletes)
A data engineering team maintains an upstream Silver Delta table that captures corrected billing records. Due to billing disputes and refund processing, accounting analysts frequently execute `UPDATE` and `DELETE` operations on this Silver table.

A junior engineer attempts to ingest this Silver table into a downstream analytical aggregation table using:
```sql
CREATE OR REFRESH STREAMING TABLE gold_refund_totals AS
SELECT customer_id, sum(refund_amount) AS total_refunds
FROM STREAM(portfolio_catalog.silver.corrected_billing)
GROUP BY customer_id;
```

During testing, the pipeline fails with an error indicating that updates and deletes cannot be processed by the stream.

How should the team resolve this failure while ensuring downstream reports accurately reflect all refunds, updates, and deletes?

- A) Recreate the downstream table as a `MATERIALIZED VIEW` without the `STREAM()` keyword:
  ```sql
  CREATE OR REFRESH MATERIALIZED VIEW gold_refund_totals AS
  SELECT customer_id, sum(refund_amount) AS total_refunds
  FROM portfolio_catalog.silver.corrected_billing
  GROUP BY customer_id;
  ```
- B) Add `IGNORE DELETES` to the streaming query: `FROM STREAM(portfolio_catalog.silver.corrected_billing) WITH (IGNORE DELETES)`.
- C) Enable Deletion Vectors on the streaming table and run `OPTIMIZE` on the Silver table every 10 minutes.
- D) Recreate the downstream table using a legacy Hive Metastore table with `PARTITION BY (customer_id)` and `ZORDER BY (refund_amount)`.

**Correct Answer:** **A**

> **Explanation:**  
> Streaming Tables (`STREAM()`) require an append-only source log. If the source table undergoes in-place `UPDATE` or `DELETE` operations, standard streaming ingestion fails because the streaming engine cannot automatically propagate row retractions in append mode. Recreating the downstream table as a Materialized View queries the snapshot state of the table at refresh time, correctly capturing all current updates and deletions.

---

### Subpoint 1.7: AUTO CDC APIs & SCD Types

#### Question 1.7.1: Handling Out-of-Order Delivery in SCD Type 2 Pipelines
A data engineer is designing a Lakeflow Pipeline to maintain a historical customer dimension (`dim_customers`) using CDC feeds from an enterprise MySQL database. Due to network partitioning across regional message brokers, change events often arrive minutes out of chronological order.

The business requires an unbroken timeline of historical customer records, with each record tracking its valid time window (`__start_at` and `__end_at`).

Which Lakeflow SQL statement correctly implements this requirement?

- A)
  ```sql
  CREATE OR REFRESH STREAMING TABLE dim_customers
  AS SELECT * FROM STREAM(portfolio_catalog.bronze.customers_cdc)
  WHERE operation != 'DELETE'
  CLUSTER BY (customer_id);
  ```
- B)
  ```sql
  CREATE FLOW customer_scd2_flow
  AS AUTO CDC INTO portfolio_catalog.silver.dim_customers
  FROM STREAM(portfolio_catalog.bronze.customers_cdc)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = 'DELETE'
  SEQUENCE BY change_timestamp
  STORED AS SCD TYPE 2;
  ```
- C)
  ```sql
  APPLY CHANGES INTO portfolio_catalog.silver.dim_customers
  FROM STREAM(portfolio_catalog.bronze.customers_cdc)
  KEYS (customer_id)
  SEQUENCE BY change_timestamp
  COLUMNS * EXCEPT (operation);
  ```
- D)
  ```sql
  CREATE OR REFRESH MATERIALIZED VIEW dim_customers AS
  SELECT customer_id, change_timestamp AS __start_at, 
         LEAD(change_timestamp) OVER (PARTITION BY customer_id ORDER BY change_timestamp) AS __end_at
  FROM portfolio_catalog.bronze.customers_cdc;
  ```

**Correct Answer:** **B**

> **Explanation:**  
> In Databricks Lakeflow Pipelines (post-Oct 2026), the `CREATE FLOW ... AS AUTO CDC INTO` API replaces the legacy `APPLY CHANGES INTO` syntax. To track historical slowly changing dimensions without corrupting timelines when messages arrive out-of-order, specifying `SEQUENCE BY <timestamp_col>` and `STORED AS SCD TYPE 2` instructs the engine to automatically manage the `__start_at` and `__end_at` timeline windows.

> **🎯 10-Second Elimination Rule:**  
> Look for the modern `AUTO CDC` syntax. Eliminate Option C (uses deprecated `APPLY CHANGES INTO`). Eliminate Option A (regular streaming table cannot handle CDC updates/deletes).

---

#### Question 1.7.2: Identifying Active Records in Lakeflow SCD Type 2 Dimensions
A data analyst is querying a dimension table `portfolio_catalog.silver.dim_customers` that was populated using a Lakeflow `AUTO CDC` pipeline configured with `STORED AS SCD TYPE 2`.

The analyst needs to write a query that returns **only the currently active record** for each customer to join with a daily sales fact table.

Which filter predicate must the analyst include in their `WHERE` clause?

- A) `WHERE is_current = TRUE`
- B) `WHERE __end_at = '9999-12-31 23:59:59'`
- C) `WHERE __end_at IS NULL`
- D) `WHERE operation != 'DELETE' AND row_status = 'ACTIVE'`

**Correct Answer:** **C**

> **Explanation:**  
> In Databricks Lakeflow SCD Type 2 tables, the engine automatically adds two system columns: `__start_at` and `__end_at`. The currently active row always has `__end_at IS NULL`. Sentinel date values like `'9999-12-31'` (Option B) are legacy data warehousing patterns not used by Lakeflow.

---

### Subpoint 1.8: Structured Streaming vs. Lakeflow Declarative Pipelines

#### Question 1.8.1: Architectural Framework Selection Under Operational Constraints
A financial data platform team is building a streaming fraud detection workload. 
The requirements are:
1. Ingest real-time transactions from an existing Kafka topic.
2. For every suspicious transaction detected, immediately trigger an authenticated HTTP POST request to an external banking risk REST API and publish an alert payload to a downstream Kafka notifications topic.
3. The pipeline must support arbitrary Python connection pooling and custom authorization token refresh mechanisms inside each micro-batch.

Which framework should the data engineering team choose to implement this workload?

- A) Lakeflow Declarative Pipelines configured with `@dlt.table` and `pipelines.channel = "PREVIEW"`
- B) Spark Structured Streaming using `writeStream.foreachBatch(...)`
- C) A Lakeflow Materialized View scheduled with a 10-second refresh trigger
- D) Lakeflow Connect configured with an outbound JDBC connector

**Correct Answer:** **B**

> **Explanation:**  
> When a streaming workload requires writing to arbitrary external systems (such as publishing to an external Kafka topic, writing to a Redis cache, or dispatching HTTP requests to a REST API), Spark Structured Streaming with `foreachBatch(...)` provides the necessary granular control to instantiate custom connection pools and execute arbitrary Python logic per micro-batch. Lakeflow Declarative Pipelines are designed for governed Delta/Iceberg tables in Unity Catalog and do not support arbitrary third-party outbound sink connections.

> **🎯 10-Second Elimination Rule:**  
> Look for "arbitrary REST API call" or "write to external Kafka topic" $\rightarrow$ must be Spark Structured Streaming with `foreachBatch`.

---

#### Question 1.8.2: Minimizing Operational Maintenance for Medallion Lakehouses
A data architecture team wants to standardize its core customer data platform across 15 business units.
The platform requirements are:
1. Ingest raw semi-structured JSON events into Bronze tables.
2. Cleanse and validate Silver tables using built-in data quality metrics (such as dropping records that fail schema validation and tracking violation counts in pipeline event logs).
3. Compute daily Gold business summary tables with complex aggregations and joins within the same pipeline dependency graph.
4. Minimize operational overhead by eliminating manual checkpoint management, custom cluster resizing logic, and manual query recovery scripts.

Which solution satisfies all requirements with the lowest operational maintenance overhead?

- A) Lakeflow Declarative Pipelines defining Streaming Tables for Bronze/Silver and Materialized Views for Gold
- B) Apache Spark Structured Streaming with manual `checkpointLocation` options set on Unity Catalog Volumes and custom `assert` statements
- C) A scheduled Databricks Notebook running standard batch `DataFrame.write.mode("append")` inside a Lakeflow Job with retry policies
- D) Spark Structured Streaming writing to Delta tables with `Trigger.ProcessingTime("10 seconds")` running on a single-node cluster

**Correct Answer:** **A**

> **Explanation:**  
> Lakeflow Declarative Pipelines (formerly DLT) provide a fully managed declarative framework designed specifically for medallion architectures. Key benefits include: built-in data quality expectations (`expect`, `expect_or_drop`), automatic checkpoint management without manual path configurations, unified execution of Streaming Tables and Materialized Views within a single dependency DAG, and automatic compute scaling/retry handling.

---

### Subpoint 1.9: Lakeflow Jobs Control-Flow Operators (`If/Else` & `For Each`)

#### Question 1.9.1: Dynamic Iteration and Granular Failure Recovery with `For Each` Tasks
A data engineering team runs a daily settlement notebook across 35 country codes. Currently, a single driver notebook uses a Python `for` loop calling `dbutils.notebook.run("settle_country", 300, {"country": c})` sequentially.

The team complains about two operational problems:
1. Running 35 countries sequentially takes over 3 hours.
2. Whenever a transient network error causes 1 country to fail, re-running the job re-executes all 35 countries from scratch.

Which Lakeflow Jobs architecture resolves both issues while adhering to Databricks best practices?

- A) Replace the notebook with a `condition_task` that checks if `country_count == 35` and runs `dbutils.notebook.run()` inside a Python `multiprocessing.Pool(35)`.
- B) Configure a `for_each_task` inside the Lakeflow Job with `inputs` set to the JSON array of 35 country codes, set `concurrency` (e.g., `concurrency: 10`), pass `{"country": "{{input}}"}` in `base_parameters`, and use Lakeflow **Repair Run** to re-run only failed country iterations.
- C) Create 35 separate Lakeflow Pipelines in `databricks.yml` and trigger all 35 pipelines simultaneously using a cron schedule.
- D) Configure a `for_each_task` that nests a second `for_each_task` inside it, passing `{"country": "{{inputs}}"}` to the child notebook.

**Correct Answer:** **B**

> **Explanation:**  
> In Lakeflow Jobs, the `for_each_task` operator replaces sequential `dbutils.notebook.run()` loops. Setting `concurrency` enables parallel execution of iterations, passing `"{{input}}"` (singular) binds each array item to the child task's parameters, and Lakeflow's native **Repair Run** capability allows re-running only the specific failed country iterations without re-executing successful ones.

> **🎯 10-Second Elimination Rule:**  
> Eliminate `dbutils.notebook.run()` loops (Option A). Eliminate nested `for_each_task` and plural `"{{inputs}}"` (Option D — both are invalid syntax).

---

#### Question 1.9.2: Branching on Runtime Data Metrics vs. Task Execution Status
In a multi-task Lakeflow Job, Task 1 (`validate_silver_batch`) calculates the percentage of anomalous records and publishes it using:
```python
dbutils.jobs.taskValues.set(key="anomaly_pct", value=14.5)
```
Task 1 always completes with status `SUCCESS`.

The architecture requires that:
- If `anomaly_pct` is **greater than `10.0`**, Task 2A (`quarantine_audit_workflow`) must run.
- If `anomaly_pct` is **less than or equal to `10.0`**, Task 2B (`publish_gold_dashboard`) must run instead.

How should the data engineer configure this branching logic in the Lakeflow Job?

- A) Configure Task 2A with `run_if: AT_LEAST_ONE_FAILED` and Task 2B with `run_if: ALL_SUCCESS` directly depending on Task 1.
- B) Add a `condition_task` (`If/Else`) that compares `{{tasks.validate_silver_batch.values.anomaly_pct}}` `GREATER_THAN` `10.0`, and configure Task 2A to depend on the condition task with `outcome: "true"` and Task 2B with `outcome: "false"`.
- C) Use a `for_each_task` with `inputs: "{{tasks.validate_silver_batch.values.anomaly_pct}}"` and `concurrency: 1`.
- D) Set `spark.conf.set("spark.databricks.job.branch", "true")` inside Task 1 so Task 2A automatically detects the Spark configuration on its own cluster.

**Correct Answer:** **B**

> **Explanation:**  
> While `run_if` branches on task execution status (`SUCCESS` vs `FAILED`), a `condition_task` (`If/Else`) evaluates dynamic runtime values such as `taskValues` or job parameters on the Serverless Control Plane. Downstream tasks connect to the condition task by specifying `outcome: "true"` or `outcome: "false"` inside their `depends_on` block.

---

### Subpoint 1.10: Compute Selection & Serverless Configuration

#### Question 1.10.1: Performance-Optimized vs. Standard Mode for Scheduled Workloads
A Lakeflow Job runs every night at 01:00 and loads a Delta table. Business users first open the dashboard at 09:00. The team wants the lowest compute cost, and a few extra minutes of startup time is acceptable.

Which configuration should the engineer choose?

- A) Standard performance mode (`performance_target: STANDARD`) on the serverless job.
- B) Performance-optimized mode, because the workload uses serverless compute.
- C) A classic all-purpose cluster with an init script that keeps it warm all night.
- D) Standard performance mode on an interactive serverless notebook, scheduled from the notebook UI.

**Correct Answer:** **A**

> **Explanation:**  
> Serverless compute for Lakeflow Jobs and Pipelines offers two performance modes: `PERFORMANCE_OPTIMIZED` (default, warm pool for low startup latency) and `STANDARD` (up to ~70% cost reduction with a 4–6 minute cold start). For scheduled overnight jobs without tight startup SLAs, `STANDARD` mode minimizes DBU expenditure. Standard mode is not available for interactive notebooks (Option D).

> **🎯 10-Second Elimination Rule:**  
> "Scheduled batch + minimize cost + startup delay acceptable" $\rightarrow$ `STANDARD` mode on the job. Eliminate interactive notebooks with Standard mode (Option D) and all-purpose clusters (Option C).

---

#### Question 1.10.2: Disabling Auto-Optimized Retries for Non-Idempotent Tasks
A serverless job task calls an external API to create an invoice for each row. The task is not idempotent. During a transient failure, an invoice was created twice.

What should the engineer do?

- A) Disable auto-optimization (`disable_auto_optimization: true`) for that task so serverless does not retry it automatically.
- B) Switch the job to performance-optimized mode.
- C) Add a cluster init script that checks for duplicate invoices.
- D) Increase the notebook's memory setting to the high-memory option.

**Correct Answer:** **A**

> **Explanation:**  
> Serverless Lakeflow Jobs automatically retry failed tasks as part of auto-optimization. When a task is non-idempotent (e.g., triggering payments, invoices, or external webhooks that should run at most once), engineers must disable auto-optimized retries (`disable_auto_optimization: true`) at the task level to prevent duplicate side effects.

> **🎯 10-Second Elimination Rule:**  
> "Non-idempotent task + duplicate side effects on failure" $\rightarrow$ disable auto-optimized retries (`disable_auto_optimization: true`).

---

#### Question 1.10.3: Resolving Out-of-Memory (OOM) Errors in Serverless Notebooks
A serverless notebook task fails with an out-of-memory error. The code collects a large result set to the driver and the team has confirmed it needs that data in memory.

Which change fixes this on serverless compute?

- A) Select the high-memory option in the notebook's Environment side panel.
- B) Add `spark.conf.set("spark.driver.memory", "64g")` at the top of the notebook.
- C) Call `.cache()` on the DataFrame before collecting it.
- D) Attach an init script that raises the JVM heap size.

**Correct Answer:** **A**

> **Explanation:**  
> On Databricks Serverless compute, driver memory size is managed via the notebook's Environment settings (enabling the High-Memory REPL option). Setting `spark.driver.memory` dynamically via `spark.conf.set()` (Option B), calling `.cache()` (Option C — unsupported on Spark Connect), and attaching cluster init scripts (Option D — unsupported on Serverless) are all invalid.

---

### Subpoint 1.11: Unit & Integration Testing in PySpark

#### Question 1.11.1: Comparing Distributed PySpark DataFrames with Floating-Point & Row-Order Variations
A data engineer is writing a `pytest` suite to verify a PySpark transformation function `calculate_regional_tax(df)` that computes a `DOUBLE` column (`tax_amount`) using distributed joins and aggregations.

Because the transformation performs a shuffle without an `ORDER BY`, the output rows may appear in any order across runs, and floating-point arithmetic introduces minor rounding differences at the 5th decimal place (`0.00001`).

Which assertion statement is the Databricks-recommended way to verify the output DataFrame against an expected DataFrame?

- A) `assert actual_df.collect() == expected_df.collect()`
- B) `assert actual_df.rdd.subtract(expected_df.rdd).isEmpty()`
- C) `assertDataFrameEqual(actual_df, expected_df, checkRowOrder=False, rtol=1e-4)`
- D) `assertSchemaEqual(actual_df.schema, expected_df.schema, checkRowOrder=False)`

**Correct Answer:** **C**

> **Explanation:**  
> `assertDataFrameEqual` from `pyspark.testing` compares both schema and data across two DataFrames. Setting `checkRowOrder=False` (the default) ensures distributed shuffle order differences do not cause false failures, while `rtol` / `atol` handle floating-point rounding tolerances.

> **🎯 10-Second Elimination Rule:**  
> Eliminate `.collect() == .collect()` (fails on row order and float precision), eliminate `.rdd` (Law 3 violation), and eliminate `assertSchemaEqual` when row data must be verified.

---

#### Question 1.11.2: Structuring PySpark Code for Modular Unit Testing
A team has a 400-line Databricks notebook that reads directly from `spark.read.table("prod_catalog.bronze.orders")`, applies 6 business transformations inline, and writes to `prod_catalog.silver.clean_orders`. Whenever engineers want to test a change to one of the 6 rules, they have to clone the entire production table.

How should the team refactor this code to support fast, isolated unit tests in CI/CD?

- A) Keep the logic inside the notebook and call `dbutils.notebook.run()` from `pytest` with `"env": "prod"`.
- B) Extract each business rule into a pure Python function (`DataFrame -> DataFrame`) inside a module, chain them in production with `df.transform(rule_fn)`, and test each function in `pytest` using small synthetic DataFrames and `assertDataFrameEqual`.
- C) Convert all DataFrames to `.rdd` objects and test them using Python's `unittest.mock.patch`.
- D) Add `%pip install pytest` to the top of the production notebook so tests run automatically during every production Lakeflow Pipeline update.

**Correct Answer:** **B**

> **Explanation:**  
> Decoupling I/O (`spark.read.table` / `df.write.saveAsTable`) from pure `DataFrame -> DataFrame` transformation functions allows production pipelines to chain transformations via `df.transform(fn)` while unit tests pass small synthetic DataFrames into the exact same functions and assert results with `assertDataFrameEqual`.

> **🎯 10-Second Elimination Rule:**  
> Look for `DataFrame.transform()` + pure functions + `pytest` + `assertDataFrameEqual`. Eliminate `.rdd` (Option C) and `%pip` in pipelines (Option D).








