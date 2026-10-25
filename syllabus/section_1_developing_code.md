# Section 1: Developing Code for Data Processing using Python and SQL (23%)

> **Exam Domain Weight:** 23% of total exam score (Largest single domain on the exam)  
> **Official Exam Syllabus:** Post-October 2026 Outline  
> **Key Architecture:** Declarative Automation Bundles (DABs), Lakeflow Pipelines, AUTO CDC, Streaming State, Serverless Constraints, Testing Frameworks

---

## Complete Domain Breakdown: 12 Core Objectives

| # | Objective | Exam Focus |
|---|---|---|
| **1.1** | Scalable Python Project Structures for Bundles (DABs) | Modular layout (`src/`, `resources/`), CI/CD integration, target environments (`dev` vs `prod`). |
| **1.2** | Troubleshooting Library & Dependency Conflicts | PyPI, local wheels, source archives across serverless, pipelines, and bundle deployments. |
| **1.3** | User-Defined Functions (UDFs) across Python, Pandas, & SQL | Unity Catalog governed functions, vectorized Pandas UDFs, performance & execution constraints. |
| **1.4** | Production Streaming Pipelines with Lakeflow & Auto Loader | CloudFiles format, schema evolution, rescue data column, incremental ingestion. |
| **1.5** | ETL Workloads Automation via Lakeflow Jobs | Declarative job YAML, Tasks orchestration, CLI/API/UI trigger modes. |
| **1.6** | Streaming Tables vs. Materialized Views | Latency vs. compute cost tradeoffs, incremental recompute vs. full refresh. |
| **1.7** | AUTO CDC APIs & SCD Types | `STORED AS SCD TYPE 1` vs `TYPE 2`, `KEYS`, `SEQUENCE BY`, handling out-of-order changes. |
| **1.8** | Structured Streaming vs. Lakeflow Declarative Pipelines | Operational constraints, control overhead, cluster management tradeoffs. |
| **1.9** | Lakeflow Jobs Control-Flow Operators | `If/Else` conditional branching, `For Each` iterative task execution, task parameter passing. |
| **1.10** | Compute Selection & Serverless Configuration | Serverless environments, dependency isolation, retry policies, high-memory node tasks. |
| **1.11** | Unit & Integration Testing in PySpark | `assertDataFrameEqual`, `assertSchemaEqual`, `DataFrame.transform()`, pytest fixtures. |
| **1.12** | Structured Streaming Stateful Processing | Watermarks, output modes (`append`, `update`, `complete`), `foreachBatch`, idempotent commits (`txnAppId`, `txnVersion`). |

---

## Subpoint 1.1: Scalable Python Project Structures for Bundles (DABs)

### Architecture & Mechanics
Declarative Automation Bundles (formerly Databricks Asset Bundles) provide software-engineering rigor for the lakehouse by defining infrastructure, pipelines, jobs, and code as version-controlled YAML and Python files.

```
project_root/
├── databricks.yml           # Root bundle definition (variables, targets, environments)
├── resources/               # Modular resource definitions (separated by asset type)
│   ├── pipelines.yml        # Lakeflow Pipeline definitions
│   └── jobs.yml             # Lakeflow Job workflow definitions
├── src/                     # Pure Python source code / transformations / pipelines
│   ├── transformations/     # Modular business logic (pure DataFrame.transform functions)
│   └── pipelines/           # Pipeline entrypoints
├── tests/                   # Pytest unit and integration test suites
├── pyproject.toml           # Python packaging / dependencies
└── fixtures/                # Mock datasets for local unit tests
```

### Key Bundle Concepts
1. **Target Environments (`mode` setting):**
   - `mode: development`:
     - Prefixes all deployed resource names with `[dev <username>]`.
     - Deploys code to `/Workspace/Users/<user>/.bundle/<bundle_name>/dev`.
     - Automatically pauses scheduled jobs and pipelines to prevent accidental runs during development.
     - Sets run-as user to current developer.
   - `mode: production`:
     - Strict production deployment (no username prefixes).
     - Deploys to `/Workspace/.bundle/<bundle_name>/prod`.
     - Scheduled jobs and pipelines run active.
     - Requires explicit service principal or admin permissions (`run_as`).
2. **Variable Substitution:**
   - `${var.<variable_name>}` allows parameterizing schemas, catalogs, and paths between environments (`dev` vs `prod`).

---

## Subpoint 1.2: Troubleshooting Library & Dependency Conflicts

### Dependency Hierarchy & Precedence
1. **Cluster / Serverless Environment Level:** Pre-installed runtime libraries (Databricks Runtime / Serverless base container).
2. **Bundle / Pipeline Library Level:** Libraries defined inside `databricks.yml` or Lakeflow pipeline YAML under `libraries:`.
3. **Notebook / Task Level:** `%pip install` or task-scoped environment definitions.

### Common Failure Modes & Resolution
- **Serverless Compute:** Does not allow custom cluster init scripts. Dependencies must be specified via `requirements.txt`, PyPI packages in Lakeflow YAML, or compiled `.whl` files stored in a Unity Catalog Volume.
- **Dependency Conflicts:** When a required library conflicts with pre-installed Databricks Runtime packages:
  - Resolution: Use task-isolated virtual environments or specify explicit wheel pins.
  - In Serverless Lakeflow Pipelines: Specify library dependencies directly in the pipeline definition under `libraries: - pypi: {package: "package==1.2.3"}`.

---

## Subpoint 1.3: User-Defined Functions (UDFs) & Unity Catalog Functions

### UDF Comparison Matrix

| UDF Type | Execution Engine | Performance | Unity Catalog Governance | Best Used For |
|---|---|---|---|---|
| **SQL UDF** | Catalyst / Native C++ (Photon) | ⚡⚡⚡ Maximum (inlined by optimizer) | Full (registered in UC schema) | Standard business formulas, string parsing, conditional logic. |
| **Vectorized Pandas UDF (`@pandas_udf`)** | Python worker via Apache Arrow | ⚡⚡ High (batches rows into PyArrow) | Registered via Python in UC | Statistical scoring, ML inference, vectorized math. |
| **Standard Python UDF (`@udf`)** | Python worker (row-by-row serialization) | 🐢 Slow (Pickle overhead per row) | Session-only or UC registered | Complex external library calls that cannot be vectorized. |

### Unity Catalog Governed Functions
```sql
CREATE OR REPLACE FUNCTION portfolio_catalog.silver.calculate_tax(amount DOUBLE, tax_rate DOUBLE)
RETURNS DOUBLE
LANGUAGE SQL
DETERMINISTIC
CONTAINS SQL
COMMENT 'Calculates sales tax'
RETURN amount * tax_rate;
```
- **Exam Rule:** SQL UDFs registered in Unity Catalog run directly inside the execution engine and do not incur Python serialization overhead. Always prefer SQL UDFs or native functions before writing Python UDFs.

---

## Subpoint 1.4: Production Streaming Pipelines with Lakeflow & Auto Loader

### Auto Loader (`cloudFiles`)
Auto Loader incrementally ingests new files landing in cloud storage or UC Volumes without scanning directories repeatedly.

```python
bronze_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/catalog/schema/checkpoints/bronze_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/Volumes/catalog/landing/raw_events/")
)
```

### Schema Evolution Modes:
- `addNewColumns`: Default. Appends newly discovered columns.
- `rescue`: Places unexpected or malformed columns into a dedicated `_rescued_data` column.
- `failOnNewSchema`: Aborts the stream immediately if schema drifts.
- `none`: Ignores new columns.

---

## Subpoint 1.5: ETL Workloads Automation via Lakeflow Jobs

### Lakeflow Jobs Architecture
- Multi-task orchestration defined declaratively via YAML or UI.
- Supports heterogeneous task types: Notebooks, SQL queries, Python scripts, Lakeflow Pipelines, dbt models, and conditional control tasks.
- Task dependencies are declared using `depends_on: [{task_key: "task_a"}]`.

---

## Subpoint 1.6: Streaming Tables vs. Materialized Views

### Architectural Mechanics & Fundamental Distinctions
- **Streaming Tables (ST):**
  - Powered by Apache Spark Structured Streaming under the hood.
  - Ingests and processes records incrementally from an unbounded streaming source (`FROM STREAM(catalog.schema.table)` or Auto Loader `read_files()`).
  - Processes each record **exactly once**, maintaining internal streaming checkpoints managed by Lakeflow.
  - **Memory & State Store Constraint:** Cannot execute non-watermarked full-table aggregations (`GROUP BY customer_id` without an event-time watermark) or arbitrary window functions (`ROW_NUMBER() OVER (...)`) because an infinite stream with no time cutoff causes the memory state store to grow unbounded until the cluster encounters Out Of Memory (`OOM`).
  - Best used for Bronze ingestion and Silver row-level cleansing/filtering.

- **Materialized Views (MV):**
  - Powered by Databricks SQL / Catalyst cost-based optimizer.
  - A Materialized View is a **physical Delta table on disk** containing pre-computed query results (not a virtual view).
  - Can query any batch table, view, or streaming table snapshot.
  - **Transformations Supported:** Full arbitrary SQL including `GROUP BY`, `COUNT(DISTINCT)`, complex multi-way joins, and window functions (`RANK()`, `LEAD()`, `LAG()`).
  - **Liquid Clustering:** Configured with `CLUSTER BY (column_name)` so that analytical queries and BI dashboards benefit from file-level Data Skipping.
  - Refreshed incrementally when possible, or via automatic full recomputation on schedule.
  - Best used for Gold business metrics, analytical summaries, and BI dashboards.

### Core Terminology Guide:
1. **Declarative vs. Imperative:**
   - *Imperative ("How"):* Step-by-step code execution (e.g. standard PySpark scripts with manual `checkpointLocation` and loops).
   - *Declarative ("What"):* Declaring the desired final state (e.g. Lakeflow Pipelines / `CREATE MATERIALIZED VIEW`), allowing the engine to optimize execution, manage checkpoints, and coordinate failure recovery automatically.
2. **`@dlt` Decorator:**
   - Delta Live Tables (DLT) was rebranded to **Lakeflow Pipelines** in the post-Oct 2026 syllabus.
   - For backwards compatibility, the Python library retains `@dlt.table` and `@dlt.view` syntax.
3. **`STREAM()` Keyword:**
   - In Lakeflow SQL, `FROM STREAM(table)` tells the engine to read the Delta transaction log incrementally rather than scanning the table as a static batch snapshot.
4. **AUTO CDC:**
   - Replaces legacy `APPLY CHANGES INTO`. Declarative API to ingest database change feeds (`INSERT`, `UPDATE`, `DELETE`) and automatically merge them as SCD Type 1 or SCD Type 2.

### Exam Decision Matrix

| Requirement / Scenario | Choose Streaming Table | Choose Materialized View |
|---|---|---|
| Ingesting cloud files via Auto Loader (`cloudFiles`) | ✅ **Yes** | ❌ No (use ST for ingestion) |
| Low-latency, append-only micro-batch processing | ✅ **Yes** | ❌ No (higher refresh latency) |
| Arbitrary `GROUP BY` without watermarks / `COUNT(DISTINCT)` | ❌ No (Throws state store error) | ✅ **Yes** |
| Window functions (`ROW_NUMBER()`, `LEAD()`, `LAG()`) | ❌ No (Requires bounded watermarks) | ✅ **Yes** |
| Business report queried once per day by stakeholders | ❌ No (Wasteful continuous compute) | ✅ **Yes** (Scheduled daily refresh) |
| Source table undergoes in-place `UPDATE` and `DELETE` | ❌ No (Requires append-only or AUTO CDC) | ✅ **Yes** (Computes over table state) |

---

## Subpoint 1.7: AUTO CDC APIs & SCD Types in Lakeflow

Replaces legacy `APPLY CHANGES INTO`:

```sql
CREATE FLOW customer_cdc_flow
AS AUTO CDC INTO portfolio_catalog.silver.dim_customers
FROM STREAM(portfolio_catalog.bronze.customer_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY commit_timestamp
STORED AS SCD TYPE 2;
```

- **SCD Type 1:** In-place overwrite. Retains current state only.
- **SCD Type 2:** Historical versioning. Automatically creates `__start_at` and `__end_at` columns.
- **`SEQUENCE BY`:** Ensures out-of-order CDC events arriving late do not overwrite newer versions.

---

## Subpoint 1.8: Structured Streaming vs. Lakeflow Declarative Pipelines

- **Spark Structured Streaming (Imperative):**
  - Requires manual checkpoint management (`checkpointLocation`).
  - Requires manual schema evolution management.
  - Requires custom orchestration and error retry logic.
  - Full granular control over trigger rates, sinks, and custom `foreachBatch` logic.
- **Lakeflow Declarative Pipelines (Declarative):**
  - Automatically manages checkpoint locations, compute elasticity, and dependency DAGs.
  - Built-in data quality expectations (`expect`, `expect_or_drop`, `expect_or_fail`).
  - Automated maintenance and transaction optimizations.

---

## Subpoint 1.9: Lakeflow Jobs Control-Flow Operators

Lakeflow Jobs support dynamic control-flow operators:
1. **If/Else Condition Task:** Evaluates boolean expressions (e.g. `{{tasks.check_data.values.row_count}} > 0`). Routes execution to true or false branches.
2. **For Each Loop Task:** Iterates over an array of items (e.g. processing a list of customer partitions or region codes concurrently or sequentially).
3. **Run Job Task:** Triggers nested jobs.

---

## Subpoint 1.10: Compute Selection & Serverless Configuration

### Compute Tradeoffs

| Compute Type | Startup Time | Cost Profile | Best Use Case |
|---|---|---|---|
| **Serverless Compute** | ~Instant (seconds) | Pay-per-second, automatic scale-down | Ad-hoc notebooks, scheduled jobs, Lakeflow pipelines. |
| **Classic All-Purpose** | 3–7 minutes | Hourly DBU + VM costs | Long-running interactive multi-user development. |
| **Classic Job Compute** | 3–5 minutes | Lower DBU rate, ephemeral | High-customization workloads requiring custom OS libraries. |

### Serverless Rules:
- No manual RDD operations or driver-side `spark.sparkContext`.
- No `.cache()` or `.persist()` (managed disk caching replaces manual memory pinning).

---

## Subpoint 1.11: Unit & Integration Testing in PySpark

### Testing Mandates for Databricks DE Pro:
1. **Pure Transformation Functions:** Decouple DataFrame transformations from I/O using `.transform()`:
   ```python
   def clean_events(df: DataFrame) -> DataFrame:
       return df.filter(F.col("status") == "ACTIVE")
   ```
2. **`pyspark.testing` Built-ins:**
   - `assertDataFrameEqual(actual_df, expected_df)`: Compares row values, handling floating-point tolerances and schema matching.
   - `assertSchemaEqual(actual_schema, expected_schema)`: Verifies column data types and nullability.

---

## Subpoint 1.12: Structured Streaming Stateful Processing Semantics

### Stateful Components:
1. **Watermarks:** Define how late data can arrive before being discarded (`.withWatermark("event_time", "2 hours")`).
2. **Output Modes:**
   - `append`: Only newly finalized rows are output (standard for storage writes).
   - `update`: Rows that changed since last micro-batch are output.
   - `complete`: Entire result table is output every batch (requires aggregations).
3. **Idempotent Micro-Batch Writes (`foreachBatch`):**
   ```python
   def write_micro_batch(batch_df, batch_id):
       (
           batch_df.write.format("delta")
           .mode("append")
           .option("txnAppId", "unique_app_id")
           .option("txnVersion", batch_id)
           .saveAsTable("target_table")
       )
   ```
