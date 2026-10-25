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

> **🎯 10-Second Elimination Rule:**  
> Inter-task communication in Lakeflow Jobs = `dbutils.jobs.taskValues.set()` and `dbutils.jobs.taskValues.get()`. Eliminate `sc.broadcast` (Law 3: no `sc` on Serverless) and local OS/memory approaches.


