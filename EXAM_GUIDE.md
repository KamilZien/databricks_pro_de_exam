# Official Databricks Certified Data Engineer Professional Exam Guide (Oct 2026+)

## 1. PRE-COMPUTED "RENAMED PRODUCT & DEPRECATED SYNTAX" TRAP TABLE

Never use legacy names on the left unless pointing out an exam distractor. Always use the current post-Oct 2026 names on the right:

| Legacy / Deprecated Name (Exam Distractor) | Current Official Name (Oct 2026 Exam) | What It Does & Exam Rule |
|---|---|---|
| **Delta Live Tables (DLT)** | **Lakeflow Pipelines** / **Apache Spark™ Declarative Pipelines** | Declarative framework for batch/streaming (`STREAMING TABLE` & `MATERIALIZED VIEW`) with managed checkpoints and expectations. (Cite: https://docs.databricks.com/en/lakeflow/index.html) |
| **`APPLY CHANGES INTO` / `apply_changes`** | **`AUTO CDC` APIs** (`CREATE FLOW ... AS AUTO CDC INTO` / `create_auto_cdc_flow`) | Processes CDC feeds into SCD Type 1 or Type 2 tables via `stored_as_scd_type` and `SEQUENCE BY`. (Cite: https://docs.databricks.com/en/lakeflow/auto-cdc.html) |
| **Databricks Asset Bundles (DABs) / `dbx`** | **Declarative Automation Bundles** (`databricks.yml`) | Infrastructure-as-code CLI deployment for jobs, pipelines, and resources across `dev`/`staging`/`prod` targets. (Cite: https://docs.databricks.com/en/dev-tools/bundles/index.html) |
| **Databricks Workflows** | **Lakeflow Jobs** | Orchestrates multi-task DAGs with `If/Else` conditions, `For Each` loops, repair runs, and triggers. (Cite: https://docs.databricks.com/en/workflows/index.html) |
| **Databricks Repos** | **Databricks Git Folders** | Git integration in workspaces for version control and CI/CD integration. (Cite: https://docs.databricks.com/en/repos/index.html) |
| **Delta Sharing** | **OpenSharing** (D2D and Databricks-to-Open) | Open protocol for secure data sharing across Databricks workspaces (D2D) or external clients (D2O). (Cite: https://docs.databricks.com/en/open-sharing/index.html) |
| **Custom JDBC / Debezium Ingestion** | **Lakeflow Connect** | Managed zero-code CDC connectors for SQL Server, MySQL, PostgreSQL, and enterprise SaaS apps. (Cite: https://docs.databricks.com/en/connectors/lakeflow-connect.html) |
| **Databricks SQL Alerts (Legacy)** | **Databricks Lakehouse Alerts** | Monitors business metrics, data quality, cost, warehouse health, security events, and triggers Lakeflow Job branching. (Cite: https://docs.databricks.com/en/sql/user/alerts/index.html) |
| **Hardcoded Dashboard SQL Metrics** | **Unity Catalog Metric Views** | Centralizes governed, reusable business metric definitions (e.g., net revenue) across BI dashboards and queries. (Cite: https://docs.databricks.com/en/metric-views/index.html) |
| **`PARTITION BY` + `ZORDER BY`** | **Liquid Clustering** (`CLUSTER BY` / `CLUSTER BY AUTO`) + **Predictive Optimization** | Replaces Hive-style partitioning and `ZORDER` to eliminate write amplification and manual `OPTIMIZE`/`VACUUM` tuning. (Cite: https://docs.databricks.com/en/delta/clustering.html) |
| **JSON Strings / Rigid `from_json` Structs** | **`VARIANT` Data Type** (`parse_json`, `variant_get`, `:` syntax) | Stores and queries schema-drifting semi-structured data without pre-declaring rigid schemas. (Cite: https://docs.databricks.com/en/delta/variant.html) |
| **Per-Table `ALTER COLUMN SET MASK` at Scale** | **Unity Catalog ABAC Policies with Governed Tags** | Centrally enforces row filters and column masks across all current and future tables carrying a governed tag (e.g., `'pii'`). (Cite: https://docs.databricks.com/en/data-governance/unity-catalog/tags.html) |
| **Mount Points (`dbfs:/mnt/`) & `hive_metastore`** | **Unity Catalog 3-Level Namespaces & UC Volumes** | Uses `<catalog>.<schema>.<table>` and `/Volumes/<catalog>/<schema>/<volume>/`. (Cite: https://docs.databricks.com/en/data-governance/unity-catalog/index.html) |

---

## 2. OFFICIAL 9-SECTION EXAM OUTLINE & WEIGHTS

### Section 1: Developing Code for Data Processing using Python and SQL (23%)
- **1.1** Implement scalable Python project structures for Declarative Automation Bundles (`databricks.yml`) to support modular development and CI/CD integration.
- **1.2** Apply troubleshooting techniques to dependency conflicts and installation failures for external libraries (PyPI, local wheels, source archives) across serverless, pipeline, and bundle-deployed environments.
- **1.3** Develop User-Defined Functions (UDFs) using Pandas, Python, and SQL, including Unity Catalog functions for the given constraints.
- **1.4** Create production-ready data pipelines for streaming data using Lakeflow Pipelines and Auto Loader.
- **1.5** Create and automate ETL workloads using Lakeflow Jobs via the UI, API, and CLI.
- **1.6** Choose between a streaming table and a materialized view for a given latency, cost, and refresh requirement.
- **1.7** Implement CDC pipelines using AUTO CDC APIs in Lakeflow Pipelines, including SCD Type 1 and Type 2 via `stored_as_scd_type`.
- **1.8** Choose between Spark Structured Streaming and Apache Spark™ Declarative Pipelines for scalable ETL given operational constraints.
- **1.9** Create Lakeflow Jobs that use control-flow operators such as `If/Else` conditions and `For Each` loops.
- **1.10** Choose appropriate compute and configuration for environments and dependencies — including serverless compute (serverless environments, dependency management, performance mode), high-memory notebook tasks, and auto-optimization settings (e.g., disallowing retries).
- **1.11** Develop unit and integration tests using `assertDataFrameEqual`, `assertSchemaEqual`, `DataFrame.transform`, and testing frameworks (`pytest`) to ensure code correctness.
- **1.12** Apply Structured Streaming stateful-processing semantics, including watermarks, output modes, `foreachBatch`, and checkpoints for fault-tolerant exactly-once state recovery.

### Section 2: Data Ingestion & Acquisition (12%)
- **2.1** Develop data ingestion pipelines to ingest Delta Lake, Parquet, JSON, Iceberg, CSV, and Binary from message buses (Kafka, Kinesis, Pub/Sub) and cloud storage.
- **2.2** Configure incremental CDC pipelines using Lakeflow Pipelines with Delta or Iceberg as the target table format.
- **2.3** Configure CDC ingestion pipelines from relational database sources (SQL Server, MySQL, PostgreSQL) using Lakeflow Connect.
- **2.4** Implement OpenSharing (D2D and Databricks-to-Open) and Clean Rooms for privacy-preserving collaboration.
- **2.5** Configure Lakehouse Federation with governance across supported source systems, applying UC permissions and connection-level credentials.

### Section 3: Data Manipulation (12%)
- **3.1** Apply advanced data transformations, including window functions, joins, and aggregations, using Spark SQL and PySpark to process large datasets.
- **3.2** Develop a model and query semi-structured data using the `VARIANT` data type and related functions (`parse_json`, `variant_get`).
- **3.3** Apply AI functions, including `ai_query`, to perform model inference within data pipelines for enrichment and classification tasks.
- **3.4** Implement data quality expectations in Lakeflow Pipelines to quarantine, drop, or fail on bad records.

### Section 4: Monitoring and Alerting (10%)
- **4.1** Use Databricks system tables (`billing`, `compute`, `access`, `lakeflow`) for cost analysis, auditing, and workload monitoring.
- **4.2** Use Databricks REST APIs / CLI and SDK for monitoring and analyzing jobs and pipelines.
- **4.3** Use Pipeline event logs to monitor pipeline health and data quality.
- **4.4** Use Databricks Lakehouse alerts to monitor governed business metrics, data quality and anomaly signals, usage and cost, SQL warehouse/query health, audit and security events, AI agent quality, and Lakeflow Job branching.
- **4.5** Use Lakeflow Jobs UI and Jobs API to monitor job status and performance metrics.

### Section 5: Cost & Performance Optimization (15%)
- **5.1** Explain how Unity Catalog managed tables, Predictive Optimization, and Liquid Clustering reduce operational and maintenance overhead for a given workload.
- **5.2** Choose the appropriate Delta optimization technique (deletion vectors, Liquid Clustering, `CLUSTER BY AUTO`) for a given table access pattern.
- **5.3** Understand how caching (Delta cache / disk cache) improves query performance for repeated reads of the same data (and why `.cache()`/`persist()` fails on Serverless).
- **5.4** Apply Change Data Feed (CDF) to expose row-level changes (updates/deletes) for efficient incremental downstream processing.
- **5.5** Use the query profile to identify performance bottlenecks, such as data-skipping inefficiencies, join strategies, and shuffle operations.
- **5.6** Compare Liquid Clustering vs partitioning/ZORDER for a given table size and query pattern.

### Section 6: Ensuring Data Security and Compliance (8%)
- **6.1** Apply least-privilege access control lists (ACLs) to secure Unity Catalog securable objects and workspace resources.
- **6.2** Apply attribute-based access control (ABAC) policies with governed tags to enforce row filters and column masks at scale.
- **6.3** Apply anonymization and pseudonymization methods — hashing, tokenization, suppression, and generalization — to confidential data.
- **6.4** Implement a compliant data pipeline that enforces PII detection and masking controls across batch and streaming workloads using Unity Catalog features.
- **6.5** Implement a data purging strategy that satisfies data retention and deletion policies (GDPR right-to-be-forgotten: `DELETE` + `REORG TABLE ... APPLY (PURGE)` + `VACUUM`) using Delta Lake and Unity Catalog features.

### Section 7: Data Governance (5%)
- **7.1** Demonstrate understanding of Unity Catalog tags and comments as mechanisms for adding metadata to securable objects to improve data discoverability.
- **7.2** Demonstrate understanding of the Unity Catalog permission inheritance model across catalogs, schemas, and objects (including future tables created under a granted catalog/schema).

### Section 8: Debugging and Deploying (10%)
- **8.1** Identify pertinent diagnostic information using Spark UI, cluster logs, system tables, and query profiles to troubleshoot errors.
- **8.2** Analyze the errors and remediate failed job runs with job repairs (`Repair and Rerun`) and parameter overrides.
- **8.3** Deploy Databricks resources using Declarative Automation Bundles (`databricks.yml`).
- **8.4** Configure Git-based CI/CD workflows using Databricks Git folders to deploy notebooks and code.

### Section 9: Data Modeling (5%)
- **9.1** Design scalable Delta/Iceberg table layouts for large data assets by mapping partitioning to data grain, aligning clustering to relationship access patterns, and maintaining balanced file sizes through compaction.
- **9.2** Design dimensional models for analytical workloads, leveraging Materialized Views for pre-computed aggregation and **Unity Catalog Metric Views** for governed, reusable metric definitions.

---

## 3. CALIBRATION BENCHMARK: 10 SAMPLE QUESTIONS & KEY

### Question 1 (Developing Code - AUTO CDC / SCD Type 2)
A data engineer is implementing a CDC pipeline in a Lakeflow Pipeline to capture customer address history. When customer address updates arrive out of order, the pipeline must maintain an unbroken timeline of historical records without data corruption.
Which API configuration should the data engineer use?
- **A.** `dlt.apply_changes(target="dim_customer", source="cdc_stream", keys=["customer_id"], sequence_by="update_ts", stored_as_scd_type="2")`
- **B.** `CREATE FLOW customer_scd2 AS AUTO CDC INTO dim_customer FROM STREAM(cdc_stream) KEYS (customer_id) SEQUENCE BY update_ts STORED AS SCD TYPE 2;`
- **C.** `CREATE OR REFRESH STREAMING TABLE dim_customer AS SELECT * FROM STREAM(cdc_stream) WHERE customer_id IS NOT NULL`
- **D.** Write a `foreachBatch` function with `MERGE INTO` specifying `WHEN MATCHED AND target.update_ts < source.update_ts THEN UPDATE SET *`
- **Correct Answer:** **B**
- **Citation:** https://docs.databricks.com/en/lakeflow/auto-cdc.html
- **Rule:** Declarative AUTO CDC syntax in Lakeflow Pipelines is `CREATE FLOW ... AS AUTO CDC INTO ... KEYS (...) SEQUENCE BY ... STORED AS SCD TYPE 1/2`. Option A uses the legacy DLT syntax.

### Question 2 (Cost & Performance - Serverless Caching)
A developer running a notebook via Spark Connect against Databricks Serverless Compute calls `df.cache()` on a DataFrame read from Delta Lake to speed up multiple subsequent transformations. The execution fails with an exception.
What is the root cause and the official recommendation?
- **A.** Serverless Compute requires `df.persist(StorageLevel.MEMORY_ONLY_SER)` instead of `.cache()`.
- **B.** Spark Connect and Serverless compute disallow manual in-memory RDD/DataFrame caching; queries automatically leverage the managed disk/Delta cache.
- **C.** The developer forgot to run `SET spark.databricks.cache.enabled = true` in the session configuration.
- **D.** Serverless compute only supports table caching via `CACHE TABLE <table>`.
- **Correct Answer:** **B**
- **Citation:** https://docs.databricks.com/en/compute/serverless/index.html
- **Rule:** On Serverless Spark Connect, manual caching APIs (`.cache()`, `.persist()`, `CACHE TABLE`) throw exceptions; automatic caching handles repeated scans.

### Question 3 (Security & Compliance - GDPR Physical Purge)
A customer requests data erasure under GDPR Right-to-be-Forgotten. The engineer executes `DELETE FROM customers WHERE customer_id = 'C123'`. Deletion Vectors are enabled on the target table.
What additional sequence of operations MUST be executed to physically purge PII from cloud storage?
- **A.** `VACUUM customers RETAIN 0 HOURS`
- **B.** `OPTIMIZE customers ZORDER BY (customer_id)` followed by `VACUUM customers`
- **C.** `REORG TABLE customers APPLY (PURGE)` followed by `VACUUM customers RETAIN <retention> HOURS`
- **D.** `ALTER TABLE customers SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)` followed by `VACUUM customers`
- **Correct Answer:** **C**
- **Citation:** https://docs.databricks.com/en/delta/vacuum.html
- **Rule:** When Deletion Vectors are enabled, `DELETE` only writes a soft bitmap. `REORG TABLE ... APPLY (PURGE)` physically rewrites Parquet files without deleted rows; `VACUUM` cleans old unreferenced files.

### Question 4 (Developing Code - Streaming Joins & Watermarks)
A data engineer creates a stream-stream left outer join between `clicks` and `impressions`. Both streams have watermarks defined on their respective timestamp columns. The query fails during analysis.
What is missing?
- **A.** Left outer joins between two streams are unsupported in Structured Streaming.
- **B.** A state-store checkpoint directory must be explicitly configured with `maxVersionsToKeep = 1`.
- **C.** An event-time range condition (e.g., `clicks.ts BETWEEN impressions.ts AND impressions.ts + INTERVAL 1 HOUR`) is mandatory on outer stream-stream joins.
- **D.** The left side (`clicks`) must use `outputMode("complete")`.
- **Correct Answer:** **C**
- **Citation:** https://docs.databricks.com/en/structured-streaming/stream-stream-joins.html
- **Rule:** Stream-stream outer joins require watermarks on BOTH sides AND an event-time constraint (`BETWEEN ... AND ...`) to limit state retention and guarantee output.

### Question 5 (Data Ingestion - Auto Loader with Semi-Structured Data)
A payment provider sends JSON payloads where new nested fields appear unpredictably, and existing numeric fields occasionally arrive formatted as strings.
Which Auto Loader configuration ingests this data efficiently into Bronze without pipeline failure?
- **A.** Ingest as plain text with Auto Loader, then parse with a strict `from_json(body, schema)` StructType schema.
- **B.** Configure Auto Loader with `cloudFiles.inferColumnTypes = false`, `schemaEvolutionMode = "addNewColumns"`, and parse raw payloads using the `VARIANT` type (`parse_json`).
- **C.** Set `cloudFiles.schemaEvolutionMode = "rescue"` and query corrupted rows from `_rescued_data` exclusively.
- **D.** Set `cloudFiles.partitionColumns = "event_date"` and run `MSCK REPAIR TABLE`.
- **Correct Answer:** **B**
- **Citation:** https://docs.databricks.com/en/delta/variant.html
- **Rule:** Combining Auto Loader with `VARIANT` handles unpredictable schema drift, nested polymorphism, and type drift without pipeline schema compilation crashes.

### Question 6 (Data Modeling - Streaming Table vs. Materialized View)
A marketing analytics dashboard requires aggregated user engagement metrics refreshed every 4 hours with minimum compute cost. The underlying data is stored in an append-only transaction log table.
Which table type and framework should be selected in Lakeflow Pipelines?
- **A.** Streaming Table, because the underlying source is an append-only log.
- **B.** Materialized View with a scheduled refresh trigger, because aggregation over historical intervals is pre-computed and incrementally updated at scheduled intervals.
- **C.** A standard Delta view (`CREATE VIEW`) without materialized caching.
- **D.** A streaming table configured with continuous trigger `Trigger.ProcessingTime('4 hours')`.
- **Correct Answer:** **B**
- **Citation:** https://docs.databricks.com/en/lakeflow/materialized-views.html
- **Rule:** Use Materialized Views when computing aggregations/joins over time windows that benefit from periodic refresh. Streaming Tables are for continuous/append-only incremental ingestion.

### Question 7 (Ensuring Security - Attribute-Based Access Control)
A security administrator must mask `email` and `ssn` columns across 200 tables in Unity Catalog. New tables are created weekly by analytics teams.
What is the most maintainable, scalable way to enforce masking policies?
- **A.** Write an automated Python script that loops over `information_schema.columns` and runs `ALTER TABLE <table> ALTER COLUMN <col> SET MASK <udf>`.
- **B.** Define a Unity Catalog SQL UDF mask, and apply an Attribute-Based Access Control (ABAC) policy linked to a governed tag (e.g., `tag:pii`).
- **C.** Create separate dynamic views for each of the 200 tables with `CASE WHEN is_account_group_member('hr') THEN ssn ELSE '***' END`.
- **D.** Grant `SELECT` permissions only on column subsets via catalog-level ACLs.
- **Correct Answer:** **B**
- **Citation:** https://docs.databricks.com/en/data-governance/unity-catalog/tags.html
- **Rule:** ABAC with governed tags applies row filters and column masks dynamically across existing and future tables tagged with the specified metadata.

### Question 8 (Cost & Performance - Liquid Clustering)
A high-throughput IoT telemetry table receives 5 billion events per month. Queries frequently filter by `device_id`, `event_date`, or both. Previously, the table was partitioned by `event_date` and Z-ordered by `device_id`, leading to severe write latency and partition skew.
How should the table be restructured using 2026 standards?
- **A.** Partition by `device_id` and Z-order by `event_date`.
- **B.** Define `CLUSTER BY (device_id, event_date)` without `PARTITION BY`, and enable Predictive Optimization.
- **C.** Partition by `year(event_date)`, `month(event_date)` and run daily `OPTIMIZE ... ZORDER BY (device_id)`.
- **D.** Enable Delta cache and set file size target to 8MB.
- **Correct Answer:** **B**
- **Citation:** https://docs.databricks.com/en/delta/clustering.html
- **Rule:** Liquid Clustering (`CLUSTER BY`) replaces `PARTITION BY` + `ZORDER BY`, avoids file over-fragmentation/skew, and is maintained automatically by Predictive Optimization.

### Question 9 (Debugging & Deploying - Declarative Automation Bundles)
A data team is promoting a bundle `finops-lakehouse` from `dev` to `prod`. The pipeline definition contains parameters that must resolve differently between environments without hardcoding credentials or paths.
Where should the target-specific parameters be specified in `databricks.yml`?
- **A.** In the root `variables` block with hardcoded values for each target.
- **B.** Under `targets.prod.variables` and `targets.dev.variables`, mapped to top-level `variables` declarations.
- **C.** In separate `databricks_dev.yml` and `databricks_prod.yml` files.
- **D.** Using local shell environment variables exported via `.bashrc` on the Databricks cluster.
- **Correct Answer:** **B**
- **Citation:** https://docs.databricks.com/en/dev-tools/bundles/index.html
- **Rule:** Declarative Automation Bundles declare top-level `variables`, then override target-specific values under `targets.<target_name>.variables`.

### Question 10 (Monitoring & Alerting - System Tables)
A FinOps engineer needs to calculate total DBU consumption and cost attribution by project tag across all serverless pipelines for the current billing cycle.
Which system table contains the definitive raw consumption metrics?
- **A.** `system.billing.usage`
- **B.** `system.lakeflow.events`
- **C.** `system.compute.clusters`
- **D.** `system.access.audit`
- **Correct Answer:** **A**
- **Citation:** https://docs.databricks.com/en/administration-guide/system-tables/billing.html
- **Rule:** `system.billing.usage` records billable DBU consumption per workspace, SKU, identity, and usage metadata tags.
