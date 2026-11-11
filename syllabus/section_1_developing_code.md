# Section 1: Developing Code for Data Processing using Python and SQL (23%)

> **Exam Domain Weight:** 23% of total exam score (Largest single domain on the exam — ~14 out of 60 questions)  
> **Official Exam Syllabus:** Post-October 9, 2026 Outline  
> **Cross-Referenced With:** Official Databricks Documentation (`docs.databricks.com`) & CertSafari Post-Oct 9 Study Guide  
> **Key Architecture:** Declarative Automation Bundles (DABs), Lakeflow Declarative Pipelines, AUTO CDC, Lakeflow Jobs Control-Flow, Serverless Compute & Environments, PySpark Testing (`pyspark.testing`), Structured Streaming State & Idempotency

---

## Complete Domain Breakdown: 12 Official Objectives & CertSafari Sub-Lessons

| # | Official Exam Objective | CertSafari Sub-Lessons | Repo Implementation |
|---|---|---|---|
| **1.1** | Scalable Python project structures for Declarative Automation Bundles (`databricks.yml`) | `1.1.1` Bundle project structure: `databricks.yml`, `include`, and targets; `1.1.2` Python code in bundles: wheel artifacts, library dependencies, and Python-defined resources | `databricks.yml`, `resources/pipelines.yml`, `resources/jobs.yml`, `pyproject.toml` |
| **1.2** | Troubleshooting library & dependency conflicts across serverless, pipeline, and bundle environments | `1.2.1` Troubleshooting serverless library installs: paths, architecture, private repos, and `%pip`; `1.2.2` Fixing library dependencies in Lakeflow pipelines and bundle deployments | `resources/pipelines.yml` (`pipelines.pip.requirements`), `pyproject.toml` |
| **1.3** | Develop User-Defined Functions (UDFs) using Pandas, Python, and SQL, including Unity Catalog functions | `1.3.1` Session-scoped Python and pandas UDFs in PySpark; `1.3.2` Unity Catalog functions: SQL, Python, and Batch Python UDFs | `src/pipelines/03_udf_enrichment.py`, `tests/integration/section_1/test_subpoint_1_3_udf_sabotage.py` |
| **1.4** | Create production-ready streaming pipelines using Lakeflow Pipelines and Auto Loader | `1.4.1` Auto Loader ingestion into Lakeflow Streaming Tables; `1.4.2` Running Auto Loader pipelines in production | `src/pipelines/01_bronze_variant.py`, `src/pipelines/02_silver_cdc_quarantine.py` |
| **1.5** | Create and automate ETL workloads using Lakeflow Jobs via UI, API, and CLI | `1.5.1` Building, triggering, and parameterizing ETL jobs; `1.5.2` Automating jobs via CLI, SDK, REST API, triggers, and `taskValues` | `resources/jobs.yml`, `src/notebooks/task_producer.py`, `task_consumer_report.py`, `task_cleanup.py` |
| **1.6** | Choose between a Streaming Table and a Materialized View for latency, cost, and refresh needs | `1.6.1` Streaming Tables vs. Materialized Views semantics & correctness; `1.6.2` Latency, refresh costs, incremental refresh, and pipeline modes | `src/pipelines/04_gold_materialized_views.py`, `tests/integration/section_1/test_subpoint_1_6_gold_mv_sabotage.py` |
| **1.7** | Implement CDC pipelines using `AUTO CDC` APIs in Lakeflow Pipelines (`stored_as_scd_type`) | `1.7.1` AUTO CDC flows and SCD Type 1 in Lakeflow Pipelines; `1.7.2` SCD Type 2 history, `SEQUENCE BY`, deletes, and truncates | `src/pipelines/05_auto_cdc_pipeline.py`, `tests/unit/section_1/test_subpoint_1_7_cdc_sabotage.py` |
| **1.8** | Choose between Spark Structured Streaming and Apache Spark™ Declarative Pipelines | `1.8.1` Control vs. Automation trade-offs; `1.8.2` Streaming trigger modes (`availableNow`, `processingTime`, `continuous`) | `src/pipelines/06_streaming_vs_declarative.py`, `tests/unit/section_1/test_subpoint_1_8_streaming_choice_sabotage.py` |
| **1.9** | Create Lakeflow Jobs using control-flow operators (`If/Else` conditions and `For Each` loops) | `1.9.1` If/else `condition_task` in Lakeflow Jobs; `1.9.2` `for_each_task` loops, `concurrency`, and Repair Runs | `resources/jobs.yml`, `src/notebooks/task_regional_processor.py`, `tests/unit/section_1/test_subpoint_1_9_control_flow_sabotage.py` |
| **1.10** | Choose appropriate compute & configuration for environments, dependencies, memory, and retries | `1.10.1` Serverless environments and dependency management; `1.10.2` Performance mode (`STANDARD` vs `PERFORMANCE_OPTIMIZED`), high-memory REPL, `disable_auto_optimization` | `resources/jobs.yml`, `tests/unit/section_1/test_subpoint_1_10_serverless_config_sabotage.py` |
| **1.11** | Develop unit and integration tests using `assertDataFrameEqual`, `assertSchemaEqual`, `DataFrame.transform`, and `pytest` | `1.11.1` Testing PySpark transformations with `assertDataFrameEqual` and `pytest`; `1.11.2` Running Databricks tests: unit tests, notebook tests, and pipeline integration tests | `tests/conftest.py`, `tests/integration/section_1/test_subpoint_1_11_pyspark_testing_sabotage.py` |
| **1.12** | Apply Structured Streaming stateful-processing semantics (watermarks, output modes, `foreachBatch`, checkpoints) | `1.12.1` Watermarks, state store cleanup, output modes, idempotent `foreachBatch` (`txnAppId`, `txnVersion`), and checkpoints | `tests/integration/section_1/test_subpoint_1_12_stateful_streaming_sabotage.py` |

---

## Subpoint 1.1: Scalable Python Project Structures for Bundles (DABs)

> **Official Docs:** [What are Databricks Asset Bundles?](https://docs.databricks.com/en/dev-tools/bundles/index.html)

### 1.1.1 Bundle Project Structure: `databricks.yml`, `include`, and Targets

Declarative Automation Bundles (formerly Databricks Asset Bundles / DABs) treat Databricks workflows, pipelines, schemas, and code as Infrastructure-as-Code (IaC).

```text
project_root/
├── databricks.yml           # Root bundle manifest (bundle name, include, variables, artifacts, targets)
├── resources/               # Modular resource definitions (split by domain/asset type)
│   ├── pipelines.yml        # Lakeflow Declarative Pipeline definitions
│   └── jobs.yml             # Lakeflow Job workflow definitions
├── src/                     # Modular Python package source code
│   ├── transformations/     # Pure DataFrame -> DataFrame functions (testable via .transform())
│   ├── pipelines/           # Pipeline definitions (@dlt.table / AUTO CDC)
│   └── notebooks/           # Lightweight orchestration wrappers / job entrypoints
├── tests/                   # Pytest suites separated by execution tier
│   ├── unit/                # Fast local logic/YAML/AST tests (@pytest.mark.unit)
│   └── integration/         # Serverless Spark Connect tests (@pytest.mark.integration)
└── pyproject.toml           # Python wheel build metadata ([project.scripts] entrypoints, dependencies)
```

#### Core YAML Sections in `databricks.yml`:
```yaml
bundle:
  name: databricks-serverless-finops-lakehouse

include:
  - resources/*.yml          # Glob pattern importing modular resource files

variables:
  catalog:
    description: "Target Unity Catalog name"
    default: "portfolio_catalog"

artifacts:
  finops_wheel:
    type: whl
    build: uv build          # Or 'hatchling' / 'poetry build' / default pip wheel build
    path: .

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: "portfolio_catalog"
  prod:
    mode: production
    workspace:
      host: https://dbc-xxxx.cloud.databricks.com
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: "00000000-0000-0000-0000-000000000000"
    variables:
      catalog: "finops_prod_catalog"
```

#### Target Modes (`mode: development` vs. `mode: production`):
| Feature | `mode: development` | `mode: production` |
|---|---|---|
| **Resource Name Prefix** | Automatically prefixes with `[dev <username>]` | No prefix (exact production names) |
| **Schedules & Triggers** | Automatically **paused** (prevents accidental cron runs) | **Active** (schedules & file arrival triggers run live) |
| **Lakeflow Pipeline Mode** | Runs in `development: true` (faster restarts, easier debugging) | Runs in `development: false` (strict production retries) |
| **Default Workspace Path** | `/Workspace/Users/<user>/.bundle/<bundle>/dev` | `/Workspace/Users/<sp_or_user>/.bundle/<bundle>/prod` (or custom `root_path`) |
| **Concurrent Runs** | Allows concurrent testing by multiple developers | Enforces production identity (`run_as: service_principal_name`) |
| **Custom Override** | Can be customized via `presets:` (e.g., `name_prefix`, `trigger_pause_status`) | Locked down via `permissions:` (`CAN_VIEW`, `CAN_MANAGE_RUN`) |

### 1.1.2 Python Code in Bundles: Wheel Artifacts, Library Dependencies, and Python-Defined Resources

1. **Packaging Shared Code as a `.whl` (`python_wheel_task`):**
   - Shared Python business logic in `src/` is compiled into a `.whl` file via the `artifacts:` block during `databricks bundle deploy`.
   - In `pyproject.toml`, console entrypoints are declared under `[project.scripts]`:
     ```toml
     [project.scripts]
     run_ingestion = "src.pipelines.bronze:main"
     ```
   - In `resources/jobs.yml`, a `python_wheel_task` calls that entrypoint directly:
     ```yaml
     tasks:
       - task_key: wheel_ingest
         python_wheel_task:
           package_name: databricks_serverless_finops_lakehouse
           entry_point: run_ingestion
           named_parameters:
             catalog: ${var.catalog}
     ```
2. **Additional High-Yield Exam Nuance — Python-Defined Resources (`databricks-bundles` Python SDK):**
   - In addition to static YAML in `resources/*.yml`, DABs supports defining resources **programmatically in Python**!
   - By adding a `python:` block in `databricks.yml`:
     ```yaml
     python:
       venv_path: .venv
       resources:
         - "resources:load_resources"
     ```
   - Engineers can write Python functions/loops that dynamically generate hundreds of standardized `Job` or `Pipeline` definitions at `bundle validate` / `bundle deploy` time.
3. **Essential CLI Commands Lifecycle:**
   - `databricks bundle validate` — Validates YAML schema, variable references, and resource configuration locally.
   - `databricks bundle deploy -t dev` — Builds wheel artifacts, uploads files to Workspace, and updates Jobs/Pipelines via State IDs.
   - `databricks bundle run <resource_key> -t dev` — Triggers an immediate execution of a deployed Job or Pipeline.
   - `databricks bundle destroy -t dev` — Cleanly deletes all workspace resources tracked by that bundle target.

---

## Subpoint 1.2: Troubleshooting Library & Dependency Conflicts

> **Official Docs:** [Serverless compute environment & dependencies](https://docs.databricks.com/en/compute/serverless/dependencies.html)

### 1.2.1 Troubleshooting Serverless Library Installs: Paths, Architecture, Private Repos, and `%pip`

When a library installation fails on Serverless Notebooks, Jobs, or Lakeflow Pipelines, the exam tests **5 root causes**:

| Failure Scenario | Root Cause | Official Fix |
|---|---|---|
| **1. `OS / CPU Architecture Mismatch` (`not a supported wheel on this platform`)** | A `.whl` containing compiled C/C++/Rust extensions (e.g., custom `confluent-kafka` or `polars` build) was compiled on **macOS (`macosx_arm64`)** or **Windows (`win_amd64`)**, whereas Databricks Serverless runs on **Linux (`manylinux` / `linux_x86_64` or `aarch64`)**. | Build either a pure-Python wheel (`py3-none-any.whl`) or cross-compile inside a Linux container / CI runner (`pip wheel --platform manylinux2014_x86_64`). |
| **2. `Legacy DBFS or Local Driver Paths`** | Specifying `dbfs:/FileStore/jars/my_pkg.whl` or `/dbfs/mnt/...` on Serverless compute. Serverless blocks legacy DBFS root and mounts (Law 1). | Store `.whl`, `.tar.gz`, or `requirements.txt` in **Unity Catalog Volumes** (`/Volumes/<catalog>/<schema>/<volume>/my_pkg.whl`) or **Workspace Files / Git Folders** and ensure the `run_as` identity has `READ VOLUME` privilege. |
| **3. `Public Internet Blocked (Air-Gapped / Egress-Restricted)`** | Serverless environment has outbound internet blocked by Serverless Egress Control / Network Policies, causing `pip install` from `pypi.org` to time out. | Upload approved `.whl` files to a **Unity Catalog Volume** (`/Volumes/...`) and install from the Volume path, or whitelist the private artifact repository in the **Serverless Network Policy**. |
| **4. `Private PyPI Authentication Failure`** | Attempting to use a cluster init script (`dbfs:/init.sh`) to configure `/etc/pip.conf` on Serverless (init scripts do not exist on Serverless!). | Pass credentials stored in **Databricks Secrets** (`dbutils.secrets.get()`) or configure `--index-url https://token@pypi.company.com/simple` inside the environment configuration. |
| **5. `Version Conflict with Base Serverless Environment`** | A required package version conflicts with a pre-installed package in the Serverless container. | Pin the **Serverless Environment Client Version** (`client: "1"`, `"2"`, `"3"`) and declare explicit version constraints (`package==X.Y.Z`) in the task's isolated `environments:` block or `requirements.txt`. |

### 1.2.2 Fixing Library Dependencies in Lakeflow Pipelines and Bundle Deployments

- **Anti-Pattern Trap (`%pip` in Lakeflow Pipelines):**
  - While `%pip install` works at the top of interactive/job notebooks (where it automatically restarts the Python interpreter), **Lakeflow Declarative Pipelines do NOT support dynamic `%pip` magic commands inside pipeline source files** when orchestrating multi-table DAGs.
  - Instead, declare dependencies in `resources/pipelines.yml` via:
    1. `configuration: pipelines.pip.requirements: "pydantic>=2.0.0"` or a path to `requirements.txt`, OR
    2. Environment / library blocks inside the pipeline definition referencing PyPI packages or `/Volumes/.../*.whl`.
- **Serverless Jobs `environments:` Block in `databricks.yml` / `resources/jobs.yml`:**
  - Classic clusters attach `libraries:` directly to a task alongside `job_cluster_key`.
  - **Serverless Jobs** declare an **`environments:`** block at the job level and reference `environment_key:` on the task:
    ```yaml
    resources:
      jobs:
        my_serverless_job:
          name: "serverless-job-with-deps"
          tasks:
            - task_key: transform_task
              environment_key: finops_env
              notebook_task:
                notebook_path: ../src/notebooks/task_producer.py
          environments:
            - environment_key: finops_env
              spec:
                client: "2"
                dependencies:
                  - "pydantic>=2.5.0"
                  - "/Volumes/portfolio_catalog/landing/wheels/custom_utils-0.1.0-py3-none-any.whl"
    ```

---

## Subpoint 1.3: User-Defined Functions (UDFs) using Pandas, Python, and SQL

> **Official Docs:** [User-defined functions (UDFs) in Unity Catalog](https://docs.databricks.com/en/udf/unity-catalog.html) | [Pandas UDFs (Vectorized UDFs)](https://docs.databricks.com/en/udf/pandas.html)

### 1.3.1 Session-Scoped Python and Pandas UDFs in PySpark

#### The 4-Tier Performance Hierarchy (Fastest to Slowest):
1. **Tier 1: Built-in Spark SQL Functions (`pyspark.sql.functions`) & Unity Catalog SQL UDFs**
   - Evaluated directly inside the **Catalyst Optimizer** and **Photon (C++ vectorized engine)**.
   - **Zero** JVM-to-Python serialization overhead.
2. **Tier 2: Vectorized Pandas UDFs (`@pandas_udf`) & Unity Catalog Batch Python UDFs**
   - Uses **Apache Arrow** to transfer batches of rows (commonly 10,000 rows per batch) between the engine and Python workers without row-by-row pickling.
   - Vectorized C/NumPy operations inside Pandas run up to **10x–100x faster** than standard Python UDFs.
3. **Tier 3: Standard Scalar Python UDFs (`@udf` / UC Scalar `LANGUAGE PYTHON`)**
   - Serializes rows **one by one** (via Pickle), ships them to an isolated Python process, executes Python bytecode per row, and serializes results back.
   - Prevents Catalyst from pushing predicates through the UDF (black-box barrier).

#### High-Yield Exam Pattern: `@pandas_udf` Series-to-Series vs. `Iterator[pd.Series]`
When your Pandas UDF only performs fast math, use **Series-to-Series**:
```python
import pandas as pd
from pyspark.sql.functions import pandas_udf


@pandas_udf("double")
def vectorized_discount(amounts: pd.Series) -> pd.Series:
    return amounts * 0.85
```
**Exam Trap:** What if your Pandas UDF needs to load a **500MB Machine Learning model** or initialize an expensive encryption state from disk?
If you use `pd.Series -> pd.Series`, Spark reloads the 500MB model **on every single Arrow batch**!
Instead, use **`Iterator[pd.Series] -> Iterator[pd.Series]`** (or `Iterator[Tuple[pd.Series, ...]] -> Iterator[pd.Series]` for multiple input columns):
```python
from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf


@pandas_udf("double")
def score_with_heavy_model(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # 1. Executed ONCE per Python worker task BEFORE processing batches!
    model = load_heavy_ml_model_from_uc_volume("/Volumes/main/models/churn_v1.pkl")

    # 2. Iterate over Arrow batches using the already-loaded model
    for series_batch in batch_iter:
        yield model.predict(series_batch)
```

#### Grouped Map (`applyInPandas`) & Partition Map (`mapInPandas`):
- `df.mapInPandas(fn, schema)`: Transforms an `Iterator[pd.DataFrame] -> Iterator[pd.DataFrame]` within each existing partition (no shuffle).
- `df.groupBy("region").applyInPandas(fn, schema)`: Groups all rows for each `region` into a single `pd.DataFrame` and passes it to `fn`.
  - **Exam Warning:** `applyInPandas` forces a **full shuffle** and loads the **entire group into memory** on one worker. If one `region` has 500 million rows (data skew), the worker will fail with an **`OutOfMemoryError` (`OOM`)**!

### 1.3.2 Unity Catalog Functions: SQL, Python, and Batch Python UDFs

1. **Unity Catalog SQL Scalar & Table Functions:**
   ```sql
   -- Scalar SQL Function (Inlined by Catalyst & accelerated by Photon)
   CREATE OR REPLACE FUNCTION portfolio_catalog.silver.calculate_finops_tax(amount DOUBLE, tax_rate DOUBLE DEFAULT 0.23)
   RETURNS DOUBLE
   LANGUAGE SQL
   DETERMINISTIC
   COMMENT 'Computes rounded tax'
   RETURN ROUND(amount * tax_rate, 2);

   -- SQL Table-Valued Function (Returns a full relation)
   CREATE OR REPLACE FUNCTION portfolio_catalog.silver.high_value_events(min_amt DOUBLE)
   RETURNS TABLE(event_id STRING, amount DOUBLE)
   RETURN SELECT event_id, amount FROM portfolio_catalog.silver.clean_events WHERE amount >= min_amt;
   ```
2. **Unity Catalog Python UDFs & Custom Dependencies (`ENVIRONMENT` Clause):**
   - Registered permanently in Unity Catalog (`catalog.schema.func_name`) and governed via `GRANT EXECUTE ON FUNCTION ... TO ...`.
   - Can be called from **both SQL Warehouses (by BI analysts)** and **PySpark Pipelines (by data engineers)**.
   - Can specify external PyPI packages or UC Volume wheels directly in SQL DDL using the `ENVIRONMENT` clause:
     ```sql
     CREATE OR REPLACE FUNCTION portfolio_catalog.silver.redact_iban(text STRING)
     RETURNS STRING
     LANGUAGE PYTHON
     ENVIRONMENT (
       dependencies = '["phonenumbers==8.13.0"]',
       environment_version = 'None'
     )
     AS $$
       import re
       return re.sub(r'[A-Z]{2}\d{2}[A-Z0-9]{10,30}', '[REDACTED_IBAN]', text or '')
     $$;
     ```
3. **Unity Catalog Batch Python UDFs:**
   - Standard UC Python UDFs execute row-by-row. **UC Batch Python UDFs** execute vectorized batches of rows in Unity Catalog, bridging the governance of Unity Catalog with the Arrow vectorization speed of Pandas UDFs.

---

## Subpoint 1.4: Production Streaming Pipelines with Lakeflow & Auto Loader

> **Official Docs:** [What is Auto Loader?](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html) | [Lakeflow Declarative Pipelines](https://docs.databricks.com/en/ldp/index.html)

### 1.4.1 Auto Loader Ingestion into Lakeflow Streaming Tables

Auto Loader incrementally and idempotently processes new files arriving in Unity Catalog Volumes (`/Volumes/...`) or Cloud External Locations (`s3://`, `abfss://`, `gs://`).

#### Python (`cloudFiles`) vs. SQL (`read_files`) in Lakeflow Pipelines:
```python
# Python Lakeflow Streaming Table
import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="raw_events_bronze",
    cluster_by=["event_date"],
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
def raw_events_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "amount DOUBLE, event_id STRING")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/portfolio_catalog/landing/raw_events")
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
```
```sql
-- Equivalent SQL Lakeflow Streaming Table using read_files()
CREATE OR REFRESH STREAMING TABLE raw_events_bronze
CLUSTER BY (event_date)
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/portfolio_catalog/landing/raw_events',
  format => 'json',
  schemaHints => 'amount DOUBLE, event_id STRING',
  schemaEvolutionMode => 'addNewColumns'
);
```

#### The 4 `cloudFiles.schemaEvolutionMode` Options (Memorize for Exam!):
| Mode | Behavior on New Column | Behavior on Type Mismatch | Restart Required? |
|---|---|---|---|
| **`addNewColumns`** *(Default when schema is inferred)* | **Fails** current micro-batch, updates schema location, and evolves target table on retry/restart (or automatically inside Lakeflow Pipelines). | Captured in `_rescued_data` | Automatic in Lakeflow Pipelines; retry in standalone Structured Streaming |
| **`rescue`** | **Does NOT modify table schema.** Places new columns into `_rescued_data` JSON column. | Captured in `_rescued_data` | Never fails or restarts |
| **`failOnNewColumns`** | **Fails** the stream and **does NOT** update schema location (requires manual intervention). | Captured in `_rescued_data` | Manual intervention required |
| **`none`** | **Silently ignores** new columns (neither added to schema nor placed in `_rescued_data`). | Captured in `_rescued_data` | Never fails or restarts |

### 1.4.2 Running Auto Loader Pipelines in Production

1. **File Detection Modes:**
   - **Directory Listing Mode (Default):** Lists the storage directory incrementally using lexical file-path ordering. Fast and zero extra permissions required for smaller file counts.
   - **Managed File Events / File Notification Mode:** Uses cloud event notifications (AWS SQS/SNS, Azure Event Grid, GCP Pub/Sub) or **Unity Catalog Managed File Events** (`useManagedFileEvents = "true"`). Best when ingesting **millions of files** across deep nested directories where `LIST` API calls become slow and expensive.
2. **Production Rate Limiting & Backpressure:**
   - `.option("cloudFiles.maxFilesPerTrigger", 1000)` or `.option("cloudFiles.maxBytesPerTrigger", "10g")`: Prevents Driver/Executor OOMs when starting a stream against a historical backlog of terabytes of files.
3. **File Lineage Metadata (`_metadata`):**
   - Never use deprecated `input_file_name()`. Always select `_metadata.file_path`, `_metadata.file_name`, `_metadata.file_size`, and `_metadata.file_modification_time`.

---

## Subpoint 1.5: Create and Automate ETL Workloads using Lakeflow Jobs

> **Official Docs:** [Lakeflow Jobs](https://docs.databricks.com/en/jobs/index.html)

### 1.5.1 Building, Triggering, and Parameterizing ETL Jobs

1. **4 Job Trigger Types in Lakeflow Jobs:**
   - **Scheduled (Cron):** Runs on a fixed time schedule (e.g., `quartz_cron_expression: "0 0 1 * * ?"`).
   - **File Arrival (`file_arrival`):** Monitors a Unity Catalog Volume or External Location and starts a run as soon as new files arrive!
     - Supports `min_time_between_triggers_seconds` and `wait_after_last_change_seconds` to avoid triggering 100 times when an upstream system uploads 100 files over 2 minutes.
   - **Table Update (`table_update`):** Monitors one or more Unity Catalog Delta tables (`table_names: ["portfolio_catalog.bronze.raw_events"]`) and triggers the job whenever the upstream table(s) are updated!
   - **Continuous (`continuous`):** Automatically starts a new job run as soon as the previous run completes (ensuring exactly 1 active run at all times).
2. **Job-Level Parameters vs. Task-Level Parameters:**
   - **Job Parameters** defined at the root of the job automatically push down to every task in the DAG and can be overridden at runtime via UI, CLI (`--params env=prod`), SDK, or REST API.

### 1.5.2 Task Dependencies (`run_if`), `taskValues`, CLI, SDK, and REST API

#### All 6 `run_if` Dependency Policies:
| `run_if` Enum | When the Downstream Task Runs | Typical Production Use Case |
|---|---|---|
| **`ALL_SUCCESS`** *(Default)* | All direct parent tasks succeeded. | Standard Bronze → Silver → Gold pipeline progression. |
| **`AT_LEAST_ONE_SUCCESS`** | At least one parent task succeeded. | Merging outputs after an `If/Else` branch where one branch succeeded and the other was skipped. |
| **`NONE_FAILED`** | All parent tasks either succeeded or were skipped (zero failures). | Downstream step after optional conditional branches. |
| **`ALL_DONE`** | All parent tasks finished in **any** state (`SUCCESS`, `FAILED`, `SKIPPED`, `UPSTREAM_FAILED`). | **Cleanup, audit logging, and resource teardown tasks.** |
| **`AT_LEAST_ONE_FAILED`** | At least one parent task failed. | **PagerDuty / Slack alerting or rollback tasks.** |
| **`ALL_FAILED`** | Every parent task failed. | Fallback recovery when both primary and secondary sources fail. |

#### Passing Runtime State Between Tasks (`dbutils.jobs.taskValues`):
- **Producer Task:** `dbutils.jobs.taskValues.set(key="quarantined_records", value=58)` (supports JSON-serializable values up to **48 KB** per task).
- **Consumer Task (in Python):** `dbutils.jobs.taskValues.get(taskKey="ingest_and_score", key="quarantined_records", default=0, debugValue=10)`
  - *Exam Tip:* `debugValue=10` allows running the consumer notebook interactively in the workspace UI without failing when no job run context exists!
- **Consumer Task (in YAML Dynamic Value Reference):** `"{{tasks.ingest_and_score.values.quarantined_records}}"`

#### Triggering & Monitoring Jobs Programmatically:
- **Databricks CLI:** `databricks bundle run finops_e2e_workflow` or `databricks jobs run-now --job-id 12345`
- **Python SDK (`databricks-sdk`):**
  ```python
  from databricks.sdk import WorkspaceClient

  w = WorkspaceClient()
  run_wait = w.jobs.run_now(job_id=12345, job_parameters={"env": "prod"})
  result = run_wait.result()  # Waits for completion and returns Run object
  ```
- **REST API:** `POST /api/2.2/jobs/run-now` (and `POST /api/2.2/jobs/runs/repair` to repair failed tasks).

---

## Subpoint 1.6: Streaming Tables vs. Materialized Views

> **Official Docs:** [Streaming Tables vs. Materialized Views](https://docs.databricks.com/en/ldp/concepts.html)

### 1.6.1 Semantics, Correctness, and Architectural Comparison

| Dimension | Temporary View (`@dlt.view` / `TEMPORARY VIEW`) | Streaming Table (`@dlt.table` + `readStream` / `STREAMING TABLE`) | Materialized View (`@dlt.table` batch / `MATERIALIZED VIEW`) |
|---|---|---|---|
| **Physical Storage** | **None** (Virtual logical view; zero storage cost) | **Physical Unity Catalog Delta Table** | **Physical Unity Catalog Delta Table** |
| **Input Source** | Batch or Stream | **Append-only streaming source** (`STREAM(...)` or `read_files()`) | Batch tables, views, or tables with **`UPDATE` / `DELETE` / `MERGE`** |
| **Processing Model** | Evaluated on-the-fly when queried by downstream pipeline tables | Processes each incoming record **exactly once** via managed streaming checkpoints | Pre-computes query results; **incrementally refreshes** when possible, or recomputes full snapshot |
| **Complex SQL (`GROUP BY` without watermark, `ROW_NUMBER()`, multi-way joins)** | Supported (depends on consumer) | ❌ **Fails** (`UNSUPPORTED_OPERATION`) without watermarks due to unbounded state | ✅ **Fully supported** |
| **Primary Medallion Layer** | Intermediate pipeline staging / reusable logic | **Bronze & Silver** (low-latency append/cleanse) | **Gold** (aggregations, dimensional models, BI dashboards) |

### 1.6.2 Latency, Refresh Costs, Incremental Refresh, and Upstream Deletes

1. **How Materialized Views Refresh Incrementally on Serverless:**
   - When a Materialized View is defined on Serverless Lakeflow Pipelines or Databricks SQL Warehouse, the query optimizer checks if the upstream Delta tables have **Row Tracking** and **Change Data Feed (`delta.enableChangeDataFeed = true`)** enabled.
   - If the MV query uses incrementalizable operators (`SELECT`, `WHERE`, `INNER JOIN`, `UNION ALL`, `GROUP BY` with `SUM`/`COUNT`/`MIN`/`MAX`), the engine **only recomputes the changed partitions/rows** rather than scanning the entire table!
2. **Handling Upstream `UPDATE` and `DELETE` on Streaming Tables:**
   - By default, if someone runs `DELETE` or `UPDATE` on `bronze.events`, a downstream `STREAMING TABLE` reading `FROM STREAM(bronze.events)` **fails immediately** because append-only streams cannot retract already-emitted downstream rows.
   - **3 Exam Solutions depending on business requirements:**
     1. *Need downstream aggregations to reflect the updates/deletes accurately?* → Change the downstream table to a **`MATERIALIZED VIEW`**.
     2. *Need row-level upserts/deletes mirrored into a Silver target table?* → Use **`AUTO CDC INTO`** (`Subpoint 1.7`).
     3. *Want the downstream append-only Streaming Table to simply ignore the upstream delete/update commits and keep running?* → Pass **`skipChangeCommits => true`** (SQL) or **`.option("skipChangeCommits", "true")`** (PySpark).

---

## Subpoint 1.7: Implement CDC Pipelines using AUTO CDC APIs

> **Official Docs:** [Change data capture with AUTO CDC in Lakeflow Pipelines](https://docs.databricks.com/en/ldp/cdc.html)

### 1.7.1 `AUTO CDC` Syntax (Replacing Legacy `APPLY CHANGES INTO`)

#### Complete SQL Syntax (Including High-Yield Exam Clauses):
```sql
-- Step 1: Declare the target streaming table first
CREATE OR REFRESH STREAMING TABLE portfolio_catalog.silver.dim_customers
CLUSTER BY (customer_id);

-- Step 2: Define the AUTO CDC flow feeding into the target table
CREATE FLOW customer_cdc_flow
AS AUTO CDC INTO portfolio_catalog.silver.dim_customers
FROM STREAM(portfolio_catalog.bronze.customers_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'   -- Supported in SCD Type 1 only!
IGNORE NULL UPDATES                             -- Prevents NULLs in partial updates from wiping existing values!
SEQUENCE BY struct(commit_timestamp, seq_no)    -- Can be a single column or a STRUCT for tie-breaking!
COLUMNS * EXCEPT (operation, commit_timestamp, seq_no, _rescued_data)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (last_login_ts);      -- Optional: changes to last_login_ts update in-place without creating a new SCD2 version!
```

#### Complete Python Syntax (`dlt.create_auto_cdc_flow`):
```python
import dlt
from pyspark.sql import functions as F

dlt.create_streaming_table(
    name="dim_customers",
    cluster_by=["customer_id"],
)

dlt.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_cdc_bronze",
    keys=["customer_id"],
    sequence_by=F.col("commit_timestamp"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    ignore_null_updates=True,
    except_column_list=["operation", "commit_timestamp", "_rescued_data"],
    stored_as_scd_type=2,
    track_history_except_column_list=["last_login_ts"],
)
```

### 1.7.2 SCD Type 1 vs. SCD Type 2 Mechanics & Out-of-Order Events

| Feature | `STORED AS SCD TYPE 1` *(Default)* | `STORED AS SCD TYPE 2` |
|---|---|---|
| **History Retention** | Overwrites existing record in-place; **no history** kept. | Preserves full history by closing old row and inserting new row. |
| **System Timeline Columns** | None added. | Automatically adds **`__start_at`** and **`__end_at`**. |
| **How to Query Active Rows** | Every row in the table is active. | Filter with **`WHERE __end_at IS NULL`**. |
| **Out-of-Order Event (`SEQUENCE BY`)** | If an older event (`ts = 10:00`) arrives *after* a newer event (`ts = 10:05`), it is **ignored** so it never overwrites the newer state. | Retroactively splices the late event into the timeline between `10:00` and `10:05` without corrupting current state! |
| **`APPLY AS TRUNCATE`** | ✅ Supported | ❌ **Not supported** |

---

## Subpoint 1.8: Structured Streaming vs. Lakeflow Declarative Pipelines

> **Official Docs:** [Structured Streaming vs. Lakeflow Declarative Pipelines](https://docs.databricks.com/en/structured-streaming/index.html)

### 1.8.1 Control vs. Automation Decision Matrix

| Operational Requirement | Choose Lakeflow Declarative Pipelines | Choose Spark Structured Streaming |
|---|---|---|
| Medallion tables (Bronze/Silver/Gold) in Unity Catalog | ✅ **Best Choice** (Managed DAG, auto checkpoints, built-in expectations, auto-scaling) | ❌ High boilerplate |
| Declarative Data Quality Expectations (`expect_or_drop`, `expect_or_fail`) & SCD1/SCD2 `AUTO CDC` | ✅ **Best Choice** | ❌ Must hand-code filter splits & `MERGE INTO` |
| Writing to **non-Delta external sinks** (e.g., Kafka topic, REST API webhook, Redis, Cassandra) | ❌ Unsupported (or limited to Delta/Iceberg sinks) | ✅ **Best Choice** (via `.writeStream.foreachBatch()` or `.format("kafka")`) |
| Multi-target complex procedural logic per micro-batch (e.g., calling external Python SDKs per batch) | ❌ Declarative SQL/DataFrame only | ✅ **Best Choice** (`foreachBatch`) |
| Fine-grained custom Stateful Processing (`flatMapGroupsWithState` / `applyInPandasWithState`) | ❌ Limited | ✅ **Best Choice** |

### 1.8.2 All 4 Structured Streaming Trigger Modes Compared

| Trigger Mode | Syntax | How It Works | Serverless Compatibility |
|---|---|---|---|
| **Available-Now (Incremental Batch)** | `.trigger(availableNow=True)` | Processes all currently available data across **multiple rate-limited micro-batches**, then stops the query and releases compute. *(Replaces deprecated `Trigger.Once()`, which loaded all backlog data into a single huge batch and caused OOMs).* | ✅ **Mandatory standard on Serverless** (Law 3) |
| **Fixed-Interval Micro-Batch** | `.trigger(processingTime="30 seconds")` | Runs micro-batches continuously at fixed time intervals (`24/7` compute). | ❌ Blocked on Free Edition Serverless; high 24/7 cost |
| **Default (Unbounded Micro-Batch)** | *(No `.trigger()` specified)* | Starts the next micro-batch immediately after the previous one completes. | ❌ Blocked on Free Edition Serverless |
| **Continuous Processing** | `.trigger(continuous="1 second")` | Experimental ultra-low-latency (~1ms) epoch-based streaming; only supports stateless map/filter projections. | ❌ Not supported on Serverless |

---

## Subpoint 1.9: Lakeflow Jobs Control-Flow Operators (`If/Else` & `For Each`)

> **Official Docs:** [Add conditional logic with If/else tasks](https://docs.databricks.com/en/jobs/conditional-tasks.html) | [Use a For Each task to run another task in a loop](https://docs.databricks.com/en/jobs/for-each.html)

### 1.9.1 `If/Else` Condition Tasks (`condition_task`)

- Evaluates a boolean comparison (`EQUAL_TO`, `NOT_EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`) on the **Serverless Control Plane** (zero cluster startup time, zero DBU cost).
- Compares dynamic references such as `{{tasks.<producer>.values.<key>}}`, `{{job.parameters.<param>}}`, or `{{job.trigger.type}}`.
- **Downstream Branching Rule:** Any task that lists a `condition_task` in its `depends_on:` block **must** specify `outcome: "true"` or `outcome: "false"`.
- **Joining Branches Downstream:** If a final summary task depends on both the `"true"` branch task and the `"false"` branch task, configure the summary task with **`run_if: AT_LEAST_ONE_SUCCESS`** or **`run_if: NONE_FAILED`** (because the untaken branch is marked `SKIPPED`, which would cause the default `run_if: ALL_SUCCESS` to skip the final task too!).

### 1.9.2 `For Each` Loop Tasks (`for_each_task`)

- Replaces sequential `for x in items: dbutils.notebook.run(...)` anti-patterns.
- Consists of **two layers** in YAML:
  1. **Outer `for_each_task`:** Defines `inputs` (either a JSON array like `'["US", "DE", "UK"]'` or a dynamic `taskValues` array reference `"{{tasks.get_regions.values.region_list}}"`) and `concurrency` (how many iterations run in parallel, e.g., `concurrency: 5`).
  2. **Inner `task`:** The single task template executed per element, receiving the current item via singular **`"{{input}}"`** in `base_parameters`:
     ```yaml
     - task_key: process_each_region
       depends_on:
         - task_key: check_quarantine_threshold
           outcome: "true"
       for_each_task:
         inputs: '["US", "DE", "UK"]'
         concurrency: 3
         task:
           task_key: regional_settlement_iteration
           notebook_task:
             notebook_path: ../src/notebooks/task_regional_processor.py
             base_parameters:
               region_code: "{{input}}"
     ```
- **Inside the Child Notebook (`task_regional_processor.py`):**
  ```python
  dbutils.widgets.text("region_code", "US")
  region = dbutils.widgets.get("region_code")
  ```
- **Constraints & Repair Run Advantage:**
  - Cannot nest another `for_each_task` or `condition_task` directly inside `for_each_task`.
  - If 2 out of 50 iterations fail, **Lakeflow Repair Run** (`Repair and Rerun`) re-executes **only the 2 failed iterations**, preserving the 48 successful ones!

---

## Subpoint 1.10: Compute Selection & Serverless Configuration

> **Official Docs:** [Serverless compute for jobs](https://docs.databricks.com/en/jobs/run-serverless-jobs.html) | [Configure high-memory for serverless notebooks](https://docs.databricks.com/en/compute/serverless/notebooks.html)

### 1.10.1 Serverless Environments & Dependency Management
- **Client / Environment Version Pinning (`client: "1"`, `"2"`, `"3"`):** Determines the pre-installed Python, PySpark, and library versions on Serverless compute. Pinning the environment version guarantees that automatic Databricks serverless upgrades do not unexpectedly break production jobs.
- **Shared Base Environments:** Teams can define a shared `environment.yml` stored in Workspace files or a Unity Catalog Volume and reference it across notebooks and jobs.

### 1.10.2 Performance Mode, High-Memory REPL, and Auto-Optimization Retries

| Setting / Feature | Where It Is Configured | Options & Behavior | Exam Scenario |
|---|---|---|---|
| **Performance Mode (`performance_target`)** | **Serverless Lakeflow Jobs & Pipelines ONLY** *(Not available on interactive notebooks!)* | • **`PERFORMANCE_OPTIMIZED` (Default):** Sub-second / warm-pool startup for time-sensitive SLAs.; **`STANDARD`:** Delays startup by up to **4–6 minutes** in exchange for **significantly lower DBU cost**. | *"Nightly batch job runs at 01:00 AM, dashboard read at 09:00 AM, minimize cost"* → **`performance_target: STANDARD`**. |
| **High-Memory Notebook Option** | Notebook **Environment side panel** (or task environment spec) | Increases the Python driver / REPL memory allocation for notebooks that must bring a large dataset into local driver memory (e.g., Pandas/matplotlib or `.collect()`). *(Note: `spark.conf.set("spark.driver.memory", ...)` fails on Serverless!).* | *"Serverless notebook task fails with Driver OOM when collecting a required summary dataset"* → **Enable High-Memory option in the Environment panel**. |
| **Auto-Optimization Retries (`disable_auto_optimization`)** | Task level in **Serverless Lakeflow Jobs** (`disable_auto_optimization: true`) | By default, Serverless auto-optimization automatically retries tasks on transient failures. For **non-idempotent** tasks (calling an external payment API, sending emails, inserting into a non-transactional table), automatic retries cause **duplicate side effects**. | *"Serverless job task calls an external billing API and generated duplicate invoices after a transient error"* → **`disable_auto_optimization: true`**. |

---

## Subpoint 1.11: Unit & Integration Testing in PySpark

> **Official Docs:** [Unit testing for PySpark on Databricks](https://docs.databricks.com/en/dev-tools/testing.html)

### 1.11.1 Testing PySpark Transformations with `assertDataFrameEqual`, `assertSchemaEqual`, and `DataFrame.transform`

1. **Decouple Transformations from I/O using `DataFrame.transform()`:**
   - Never hardcode `spark.read.table(...)` and `df.write.saveAsTable(...)` inside business logic functions.
   - Write pure functions (`DataFrame -> DataFrame`) in `src/transformations/` and chain them via `df.transform(fn)`:
     ```python
     def filter_valid_payments(df: DataFrame) -> DataFrame:
         return df.filter(F.col("amount") > 0)
     ```
2. **`assertDataFrameEqual` & `assertSchemaEqual` (`from pyspark.testing import ...`):**
   ```python
   from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

   # Schema comparison (use ignoreNullable=True when SQL expressions like ROUND() alter nullability flags)
   assertSchemaEqual(actual_df.schema, expected_df.schema, ignoreNullable=True)

   # Data + Schema comparison
   assertDataFrameEqual(
       actual_df,
       expected_df,
       checkRowOrder=False,  # Default False: ignores distributed shuffle row ordering differences!
       rtol=1e-4,  # Relative tolerance for floating-point (DOUBLE/FLOAT) rounding
       atol=1e-4,  # Absolute tolerance for floating-point rounding
       includeDiffRows=True,  # Attaches exact mismatching rows to PySparkAssertionError.data
   )
   ```

### 1.11.2 Running Unit vs. Integration Tests in CI/CD

| Test Tier | Pytest Marker | Dataset Size | What It Tests | Where It Runs |
|---|---|---|---|---|
| **Unit Tests** (`tests/unit/`) | `@pytest.mark.unit` | **3–10 synthetic in-memory rows** targeting edge cases (`None`, `0`, `-5`, duplicates, boundary dates) | Pure transformation functions (`DataFrame.transform`), YAML bundle rules, AST checks | Every Git commit / PR in seconds |
| **Integration Tests** (`tests/integration/`) | `@pytest.mark.integration` | **500–1,000 rows** in isolated dev/staging Unity Catalog schemas (`portfolio_catalog`) | End-to-end Lakeflow Pipelines, UC UDFs, Auto Loader, Delta table properties, permissions | On Databricks Serverless via `databricks-connect` or a Lakeflow Job task |

---

## Subpoint 1.12: Structured Streaming Stateful Processing Semantics

> **Official Docs:** [Stateful streaming & watermarks](https://docs.databricks.com/en/structured-streaming/watermarks.html) | [Idempotent table writes in `foreachBatch`](https://docs.databricks.com/en/structured-streaming/delta-lake.html#idempotent-table-writes-in-foreachbatch)

### 1.12.1 Watermarks, Output Modes, Checkpoints, State Stores, and Idempotent `foreachBatch`

#### 1. Stateless vs. Stateful Streaming Operations:
- **Stateless Operations (`select`, `filter`, `withColumn`, `parse_json`):** Each row is processed independently. Spark does not need to remember past micro-batches in RAM.
- **Stateful Operations (`dropDuplicates`, windowed `groupBy`, stream-stream `join`, `flatMapGroupsWithState`):** Spark must remember records from previous micro-batches in a **State Store** across batches.

#### 2. Checkpoint vs. State Store vs. Watermark:
| Component | Physical Location | Plain-English Role | What Happens If Missing / Misconfigured |
|---|---|---|---|
| **Checkpoint (`checkpointLocation`)** | **Cloud / UC Volume Disk** (`/Volumes/.../_checkpoints/`) | **The Bookmark on Disk:** Records which source files/offsets have been processed (`offsets/`), committed (`commits/`), and snapshots of state (`state/`). | If lost or deleted, the stream forgets its progress and re-processes all data from scratch! |
| **State Store** | **Compute Memory (JVM Heap or RocksDB SSD)** | **The Working Notepad in RAM:** Holds active keys/windows waiting for duplicates, late matches, or window completion. | If never cleaned up, grows forever ("unbounded state") until the cluster crashes with `OutOfMemoryError` (`OOM`). |
| **Watermark (`.withWatermark("event_ts", "2 hours")`)** | **Logical Event-Time Cutoff Timer** | **The RAM Eraser Rule:** Tracks `max(event_ts) - 2 hours`. Tells Spark: *"Drop any late events older than this cutoff, finalize results, and purge old keys from the State Store."* | Without a watermark on stateful operations, old keys stay in the State Store forever → `OOM`. |

> **High-Yield Exam Nuance — RocksDB State Store Provider:**
> When a stateful stream tracks **tens of millions of keys**, the default JVM heap state store suffers from high Garbage Collection (GC) overhead and `OOM`. Enable the **RocksDB State Store** (`spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")`) to manage state in native off-heap memory and local SSD disk!

#### 3. Output Modes (`append`, `update`, `complete`) & When Results Emit:
| Output Mode | How It Works | Supported Queries & Watermark Interaction |
|---|---|---|
| **`append`** *(Default)* | Writes **only newly finalized rows** that will never change again. | • Stateless queries: emits immediately every batch.; Windowed `groupBy`: **requires a watermark** and waits until the watermark passes the end of the window before emitting! |
| **`update`** | Writes **only rows that were updated** in the current micro-batch. | Useful when writing stateful aggregations to a downstream upsert sink via `foreachBatch` + `MERGE INTO`. |
| **`complete`** | Rewrites the **entire aggregation result table** on every single micro-batch. | Only supported on aggregation queries (`groupBy`). Does **not** purge state with watermarks because it must remember all historical totals. |

#### 4. Stateful Deduplication Rule (Law 6):
```python
deduped_df = (
    raw_stream.withWatermark("event_timestamp", "2 hours").dropDuplicates(
        ["event_id"]
    )  # ONLY the business key — NEVER include the varying retry timestamp!
)
```
- **Why?** If a webhook retries 30 seconds later with the same `event_id = "EVT-1"` but a new retry timestamp, putting `"event_timestamp"` inside `dropDuplicates(["event_id", "event_timestamp"])` makes Spark see them as two different composite keys and lets the duplicate slip through!

#### 5. Stream-Stream Joins: Inner vs. Outer Joins (Law 6):
- **Stream-Stream Inner Join:** Watermarks and event-time range constraints (`LEFT.ts BETWEEN RIGHT.ts AND RIGHT.ts + INTERVAL 1 HOUR`) are *optional* for syntax compilation, but **mandatory in production** so Spark can evict old unmatched rows from the State Store.
- **Stream-Stream Outer Join (`LEFT OUTER`, `RIGHT OUTER`, `FULL OUTER`):** Watermarks on **both** streams AND an event-time range condition (`BETWEEN ... AND ...`) are **strictly mandatory** (Spark throws an `AnalysisException` without them because it cannot know when an unmatched row has waited long enough to emit with `NULL`s!).

#### 6. Idempotent Multi-Table Writes in `foreachBatch` (`txnAppId` & `txnVersion`):
By default, `foreachBatch(func)` only guarantees **at-least-once** execution: if `func` writes to **Table A** and then fails while writing to **Table B**, Spark retries the exact same `batch_id`. Without idempotency options, **Table A** gets duplicate rows!
```python
def write_to_two_tables_idempotently(batch_df: DataFrame, batch_id: int) -> None:
    # Write 1 -> Table A (Clean Silver Table)
    (
        batch_df.filter("is_valid = true")
        .write.format("delta")
        .mode("append")
        .option("txnAppId", "finops_clean_writer")
        .option("txnVersion", str(batch_id))
        .saveAsTable("portfolio_catalog.silver.clean_events")
    )

    # Write 2 -> Table B (Quarantine Silver Table)
    (
        batch_df.filter("is_valid = false")
        .write.format("delta")
        .mode("append")
        .option("txnAppId", "finops_quarantine_writer")
        .option("txnVersion", str(batch_id))
        .saveAsTable("portfolio_catalog.silver.quarantine_events")
    )
```
- **How Delta Enforces Idempotency:** Each target Delta table stores `(txnAppId, txnVersion)` inside its `_delta_log` transaction log. If `batch_id = 7` is retried after a failure on Table B, Table A checks its `_delta_log`, sees `("finops_clean_writer", 7)` was already committed, and **silently skips** the duplicate write while Table B completes its write!

---

## Master Q&A Vault: Every Question You Asked in Section 1

This section captures **all 19 engineering questions you asked during Section 1** along with their clear, production-grounded answers for rapid pre-exam review.

### Q1: Why do we use the `bundle` keyword in `databricks.yml` (`databricks bundle deploy`), and what happens if a job with that name already exists in the workspace?
- **Answer:** Declarative Automation Bundles (DABs) track deployed resources using an internal **Terraform-style state file** stored inside your workspace directory (`.bundle/<bundle_name>/<target>/state/`).
- When you run `databricks bundle deploy`, it does **not** match jobs by name—it matches them by the resource ID tracked in its bundle state.
- If a job with the same name was created manually in the UI outside the bundle, DABs creates a new bundle-managed job (and in `mode: development`, prefixes it with `[dev <username>]` so it never collides). If you want a bundle to take over an existing UI-created resource, you use `databricks bundle deployment bind <resource_key> <existing_id>`.

### Q2: What is the difference between a Lakeflow Job (Workflow) and an ETL / Lakeflow Declarative Pipeline? If both use `.py` files, why is one called a Pipeline and one a Job?
- **Answer:**
  - **Lakeflow Declarative Pipeline (`resources/pipelines.yml`, `@dlt.table`):** A **dataflow engine** specialized for building and maintaining **Delta tables (Streaming Tables & Materialized Views)**. Your `.py` file only *declares* functions that return DataFrames (`@dlt.table`), and Databricks automatically figures out the table dependency order, checkpoints, expectations, and incremental refreshes.
  - **Lakeflow Job (`resources/jobs.yml`):** A **general-purpose workflow orchestrator (DAG)** that runs any sequence of tasks (`notebook_task`, `pipeline_task`, `python_wheel_task`, `sql_task`, `condition_task`, `for_each_task`). In fact, a Lakeflow Job often has a `pipeline_task` as Task 1 (to refresh the Bronze/Silver/Gold tables) followed by a `notebook_task` as Task 2 (to send an alert or call an external API)!

### Q3: Why did the IDE Testing tab show no tests at first, and how should unit vs. integration tests be separated?
- **Answer:**
  - In VS Code / Antigravity, the Testing sidebar requires `python.testing.pytestEnabled: true` and `python.testing.pytestArgs: ["tests"]` in `.vscode/settings.json` plus `testpaths = ["tests"]` in `pyproject.toml`.
  - Best practice is to separate tests into two directories with explicit Pytest markers:
    1. `tests/unit/` (`@pytest.mark.unit`): Fast local checks (<1 second) that do not require cloud network calls or cluster spin-up.
    2. `tests/integration/` (`@pytest.mark.integration`): End-to-end tests that connect to Databricks Serverless via `databricks-connect`.

### Q4: What do "declarative" vs. "imperative" mean in plain English?
- **Answer:**
  - **Imperative ("HOW to do it step-by-step"):** Like giving a taxi driver turn-by-turn directions (*"Turn left, drive 2 miles, open checkpoint folder X, write batch 1, if error retry 3 times"*). Standard PySpark scripts and `writeStream.foreachBatch()` are **imperative**.
  - **Declarative ("WHAT final result you want"):** Like typing your destination address into Uber (*"I want a Materialized View `daily_billing_summary` grouped by `event_date` clustered by `event_date`"*). The engine decides the optimal route, checkpoint paths, and incremental refresh strategy automatically. **Lakeflow Declarative Pipelines** and **SQL** are **declarative**.

### Q5: Why do we still see `import dlt` and `@dlt.table` in Python code if Delta Live Tables was renamed to Lakeflow Declarative Pipelines?
- **Answer:**
  - In the post-October 2026 exam and product UI, **Delta Live Tables (DLT)** was officially renamed to **Lakeflow Declarative Pipelines** (or **Apache Spark™ Declarative Pipelines**).
  - However, in Python code, Databricks kept the `import dlt` module name (`@dlt.table`, `@dlt.view`, `dlt.create_auto_cdc_flow`) so thousands of existing customer repositories wouldn't break!
  - **Exam Rule:** On exam questions, choose **"Lakeflow Declarative Pipelines"** (not "Delta Live Tables") for architectural product names, and choose **`AUTO CDC` / `dlt.create_auto_cdc_flow`** (not `APPLY CHANGES INTO` / `dlt.apply_changes`) for CDC syntax.

### Q6: Why do we use `CLUSTER BY` (Liquid Clustering) on a Materialized View?
- **Answer:**
  - Unlike a standard `CREATE VIEW` (which is just a saved SQL query with zero data on disk), a **Materialized View (`CREATE MATERIALIZED VIEW`)** is stored physically on disk as a **real Delta table** containing the pre-computed rows!
  - Because it is a physical Delta table that BI dashboards query all day (`WHERE event_date = '2026-10-25'`), adding `CLUSTER BY (event_date)` organizes the underlying Parquet files on disk so dashboard queries skip 99% of irrelevant files (**Data Skipping**).

### Q7: What is a "non-watermarked `GROUP BY`", what are "arbitrary window functions", and why does a Streaming Table fail when you run them?
- **Answer:**
  - A **non-watermarked `GROUP BY`** is an aggregation over an entire infinite stream without a time window and watermark (e.g., `SELECT user_id, SUM(amount) FROM STREAM(clean_events) GROUP BY user_id`).
  - **Arbitrary window functions** are ranking/analytic functions over unbounded partitions like `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC)` or `LEAD()` / `LAG()`.
  - **Why Streaming Tables fail (`[STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION]`):** Because a stream never ends, Spark can never know if a new row for `user_id = 'U1'` will arrive tomorrow that changes their `SUM(amount)` or their `#1` rank! In `append` mode (used by Streaming Tables), Spark is forbidden from rewriting already-emitted rows, and keeping every row in RAM forever would crash the cluster with `OOM`.

### Q8: What is the difference between `AUTO CDC` and reading `STREAM()` into a normal Streaming Table?
- **Answer:**
  - **`FROM STREAM(table)` into a Streaming Table:** Strictly **append-only**. Every incoming row is added as a brand-new row at the bottom of the target table. It cannot update or delete existing rows in the target table.
  - **`AUTO CDC INTO target FROM STREAM(cdc_source)`:** Reads a stream of **change events** (`INSERT`, `UPDATE`, `DELETE`) and **merges** them by primary key (`KEYS (customer_id)`) into the target table—either overwriting rows in place (`STORED AS SCD TYPE 1`) or closing old history rows and opening new ones (`STORED AS SCD TYPE 2`), ordered safely by `SEQUENCE BY`.

### Q9: When Spark throws `[STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION]` on a streaming aggregation in `append` mode, what are the 2 real code fixes?
- **Answer:**
  - **Fix 1 (Best for Gold tables & historical totals):** Use a **Materialized View** (batch snapshot / incremental refresh) instead of a Streaming Table:
    ```sql
    CREATE OR REFRESH MATERIALIZED VIEW gold_totals AS
    SELECT event_type, SUM(amount) AS total_amount FROM clean_events GROUP BY event_type;
    ```
  - **Fix 2 (Best for real-time time-bucketed metrics in a stream):** Add a **Watermark** AND a **Time Window** (`F.window`) to the `groupBy`:
    ```python
    windowed_df = (
        stream_df.withWatermark("event_timestamp", "2 hours")
        .groupBy(F.window("event_timestamp", "1 hour"), "event_type")
        .agg(F.sum("amount").alias("hourly_total"))
    )
    ```

### Q10: What is a "distractor" on the certification exam?
- **Answer:**
  - In multiple-choice exam design, a **distractor** is one of the **wrong answer choices** specifically written to look plausible to someone who hasn't studied the latest 2026 rules.
  - On the Databricks DE Pro exam, the most common distractors are **deprecated legacy syntax** (like `APPLY CHANGES INTO`, `dbfs:/mnt/`, `ZORDER BY`, `.cache()`) or **invented parameter names** (like `readChangeData` instead of `readChangeFeed`, or `"{{inputs}}"` instead of `"{{input}}"`).

### Q11: What are "CDC integrations"?
- **Answer:**
  - **CDC (Change Data Capture) integrations** are tools that read the internal transaction log (binlog / WAL) of operational databases (like MySQL, PostgreSQL, SQL Server, Oracle, Salesforce) and turn every `INSERT`, `UPDATE`, and `DELETE` into a stream of change records with metadata (`operation`, `commit_timestamp`).
  - On Databricks, the native managed service for this is **Lakeflow Connect** (formerly custom Debezium/Kafka pipelines), which lands those CDC records into Bronze so **`AUTO CDC`** can merge them into Silver.

### Q12: Why is `APPLY AS DELETE WHEN operation = 'DELETE'` needed in `AUTO CDC`?
- **Answer:**
  - When a user is deleted in an upstream PostgreSQL database, the CDC feed does **not** reach into Databricks and delete a file—it actually **appends a new message** to the CDC stream that says: `{"customer_id": "C-101", "operation": "DELETE", "commit_ts": 200}` (called a **tombstone record**).
  - Without `APPLY AS DELETE WHEN operation = 'DELETE'`, Databricks would treat that message as a normal update/insert and keep `"C-101"` in the table! With `APPLY AS DELETE WHEN`, `AUTO CDC` knows to delete `"C-101"` (in SCD Type 1) or set its `__end_at = 200` (in SCD Type 2).

### Q13: In a Lakeflow Job `condition_task` (`If/Else`), when is `outcome: "true"` / `"false"` required in `depends_on:`, and why wasn't it on normal tasks?
- **Answer:**
  - A normal task (like a notebook or pipeline) only **succeeds or fails**; downstream tasks use `run_if: ALL_SUCCESS` or `AT_LEAST_ONE_FAILED` to react to its execution status.
  - A **`condition_task` (`If/Else`)** always succeeds as long as it can evaluate the comparison (`58 > 50`), and its result is a boolean **outcome** (`"true"` or `"false"`).
  - Therefore, **only when a task lists a `condition_task` inside its `depends_on:` block**, you must add `outcome: "true"` (to run on the True branch) or `outcome: "false"` (to run on the False branch).

### Q14: In `for_each_task`, why are there 2 `task_key` entries inside the YAML block, and what is the difference between `region_code` and `region` in the child notebook?
- **Answer:**
  - **Why 2 `task_key` entries in `for_each_task`:**
    1. Outer `task_key: process_each_region`: The **Loop Manager** on the Job DAG canvas that holds the array `inputs: '["US", "DE", "UK"]'` and `concurrency: 3`.
    2. Inner `task_key: regional_settlement_iteration`: The **Worker Template** inside `for_each_task.task` that actually runs 3 times in parallel (once for `"US"`, once for `"DE"`, once for `"UK"`).
  - **`region_code` vs. `region` in `task_regional_processor.py`:**
    - `"region_code"` is the **external parameter key** passed by `jobs.yml` (`base_parameters: { region_code: "{{input}}" }`) and read by `dbutils.widgets.get("region_code")`.
    - `region` is simply the **local Python variable name** inside your script (`region = dbutils.widgets.get("region_code")`) that stores the value (`"US"`, `"DE"`, or `"UK"`).

### Q15: What does "idempotent" mean in plain English?
- **Answer:**
  - An operation is **idempotent** if **running it 1 time or running it 5 times with the same input produces the exact same final state** without creating duplicate rows or duplicate side effects.
  - *Idempotent Example:* `MERGE INTO` by primary key, or `foreachBatch` with `.option("txnAppId", app).option("txnVersion", batch_id)`.
  - *Non-Idempotent Example:* Blindly running `INSERT INTO audit_log` or calling an external Stripe API to charge a customer's credit card without an idempotency key (retrying after a timeout charges the customer twice!).

### Q16: In real production, do unit tests really only use 3–10 synthetic rows, or do they run on full production tables?
- **Answer:**
  - **Real production unit tests (`@pytest.mark.unit`) ALWAYS use 3–10 carefully crafted synthetic rows!**
  - Why? Because a transformation rule like `amount > 0` works identically whether there are 4 rows or 4 billion rows. Testing 4 hand-crafted rows (`amount = 100.0`, `amount = -5.0`, `amount = None`, `amount = 0.0`) proves 100% of the logic branches in **0.5 seconds** on every Git commit, whereas scanning a 4-billion-row production table belongs in **Data Quality Expectations (`@dlt.expect`)** and **staging Integration Tests (`@pytest.mark.integration`)**.

### Q17: How is the stateful streaming in Subpoint 1.12 different from the streaming we did in Subpoints 1.4, 1.6, and 1.8?
- **Answer:**
  - In **Subpoints 1.4, 1.6, and 1.8**, our streams were **Stateless Row-by-Row Streams** (`parse_json`, `filter(amount > 0)`, `withColumn`): Spark read a micro-batch, transformed each row on its own, wrote it to Delta, and immediately forgot about those rows.
  - In **Subpoint 1.12**, our streams are **Stateful Streams** (`withWatermark` + `dropDuplicates(["event_id"])` or windowed `groupBy`): Spark must **remember past rows across micro-batches in RAM (the State Store)** so that if a duplicate `event_id` arrives 15 minutes later in Batch #2, Spark recognizes it saw that ID in Batch #1 and drops it!

### Q18: What are "state", "state forever", "watermark", "checkpoint", and "emit"?
- **Answer:**
  - **State:** The summary notepad in **RAM** where Spark remembers keys/windows from previous micro-batches (e.g., *"I have already seen `event_id = EVT-1` at `12:00`"*).
  - **State Forever (Unbounded State):** What happens if you run `dropDuplicates` or `groupBy` on a stream *without* a watermark—Spark keeps every key from Day 1 in RAM forever until the cluster crashes with `OutOfMemoryError` (`OOM`).
  - **Watermark (`.withWatermark("event_timestamp", "2 hours")`):** The **expiration timer for RAM**. Spark looks at the newest event time seen so far (say `15:00`), subtracts `2 hours` (`13:00`), and erases any state keys older than `13:00` from RAM.
  - **Checkpoint (`checkpointLocation`):** The **permanent folder on disk** (`/Volumes/.../_checkpoints/`) where Spark saves which files/batches are finished and backs up the state notepad so the stream can recover after a crash or restart.
  - **Emit:** The moment Spark **outputs/writes** a finished row from its internal processing into the target table.

### Q19: What do `txnVersion`, `txnAppId`, `"finops_silver_clean_writer"`, and `CLEAN_TABLE` ("Table A" vs "Table B") mean in `foreachBatch`, and why did passing `str(batch_id + 1)` on a retry cause duplicate rows?
- **Answer:**
  - Suppose inside `foreachBatch(batch_df, batch_id)` your function writes clean rows to **Table A (`CLEAN_TABLE`)** and bad rows to **Table B (`QUARANTINE_TABLE`)**.
  - If Table A succeeds on `batch_id = 0`, and 1 second later Table B fails due to a network blip, Spark retries the entire `foreachBatch` function for `batch_id = 0`.
  - To stop Table A from inserting `batch_id = 0`'s rows a second time, we tag the write with:
    - **`txnAppId = "finops_silver_clean_writer"`:** The name tag of our writer pipeline.
    - **`txnVersion = str(batch_id)`:** The exact micro-batch receipt number (`"0"`).
  - When `batch_id = 0` retries, Delta checks Table A's `_delta_log`, sees receipt `("finops_silver_clean_writer", 0)` is already recorded, and **skips the duplicate write**!
  - In our sabotage drill, the buggy code passed `str(batch_id + 1)` on the retry, which tricked Delta into thinking Retry #1 was a brand-new batch (`txnVersion = 1`) instead of a retry of `batch_id = 0`, causing duplicate rows!

---

## Rapid-Fire 10-Second Exam Elimination Rules (Section 1 Master Checklist)

1. **See `dbfs:/mnt/`, `hive_metastore`, `input_file_name()`** → ❌ Eliminate immediately! Choose **Unity Catalog 3-level namespaces**, **UC Volumes (`/Volumes/...`)**, and **`_metadata.file_path`**.
2. **See `APPLY CHANGES INTO` or `dlt.apply_changes`** → ❌ Eliminate immediately! Choose **`CREATE FLOW ... AS AUTO CDC INTO`** or **`dlt.create_auto_cdc_flow`**.
3. **See `checkpointLocation` or `cloudFiles.schemaLocation` inside a `@dlt.table` / Lakeflow Pipeline** → ❌ Eliminate immediately! Lakeflow Pipelines manage checkpoints and schema locations automatically.
4. **See `%pip install` inside a multi-table Lakeflow Pipeline notebook** → ❌ Eliminate immediately! Declare dependencies in the pipeline configuration (`pipelines.pip.requirements` / `dependencies`).
5. **See `.cache()`, `.persist()`, `CACHE TABLE`, `.rdd`, or `spark.sparkContext` on Serverless** → ❌ Eliminate immediately! Unsupported on Spark Connect / Serverless.
6. **See `spark.conf.set("spark.driver.memory", ...)` on Serverless** → ❌ Eliminate immediately! Use the **High-Memory option** in the Notebook Environment side panel.
7. **See a scheduled overnight Serverless job where startup latency doesn't matter and cost must be minimized** → ✅ Choose **`performance_target: STANDARD`**.
8. **See a non-idempotent Serverless job task (external API call / invoice creation) duplicating actions on failure** → ✅ Choose **`disable_auto_optimization: true`**.
9. **See `for_each_task` with plural `"{{inputs}}"` or nested `for_each_task`** → ❌ Eliminate immediately! Use singular **`"{{input}}"`** and `concurrency` + **Repair Run**.
10. **See `foreachBatch` writing to multiple Delta tables needing exactly-once idempotency** → ✅ Choose **`.option("txnAppId", app_id).option("txnVersion", batch_id)`**.
