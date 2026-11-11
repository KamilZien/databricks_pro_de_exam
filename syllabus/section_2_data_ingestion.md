# Section 2: Data Ingestion & Acquisition (12%)

> **Exam Domain Weight:** 12% of total exam score (~7 to 8 out of 60 questions)  
> **Official Exam Syllabus:** Post-October 9, 2026 Outline  
> **Cross-Referenced With:** Official Databricks Documentation (`docs.databricks.com`) & CertSafari Post-Oct 9 Study Guide  
> **Key Architecture:** Multi-Format Cloud & Message Bus Ingestion (Kafka, Kinesis, Pub/Sub, `binaryFile`, Iceberg UniForm), Incremental CDC (`AUTO CDC` & `AUTO CDC FROM SNAPSHOT`), Lakeflow Connect (Gateway + Ingestion Pipeline), OpenSharing (D2D vs. D2O & Clean Rooms), Lakehouse Federation (Connections, Foreign Catalogs, Query Pushdown vs. Client-Side Filtering)

---

## Complete Domain Breakdown: 5 Official Objectives & CertSafari Sub-Lessons

| # | Official Exam Objective | CertSafari Sub-Lessons | Repo Implementation |
|---|---|---|---|
| **2.1** | Develop data ingestion pipelines to ingest Delta Lake, Parquet, JSON, Iceberg, CSV, and Binary from message buses (Kafka, Kinesis, Pub/Sub) and cloud storage | • `2.1.1` Auto Loader & Cloud Storage File Ingestion (JSON, CSV, Parquet, Binary, Delta, Iceberg)<br>• `2.1.2` Streaming Ingestion from Kafka, Kinesis, and Pub/Sub (connectors, offsets, payload decoding) | `src/ingestion/kafka_and_binary_ingest.py`, `tests/integration/section_2/test_subpoint_2_1_multi_format_ingest_sabotage.py` |
| **2.2** | Configure incremental CDC pipelines using Lakeflow Pipelines with Delta or Iceberg as the target table format | • `2.2.1` Incremental `AUTO CDC` in Lakeflow Pipelines (keys, sequencing, SCD types, Deletion Vectors)<br>• `2.2.2` CDC Target Tables (Delta vs. Iceberg UniForm) & **`AUTO CDC FROM SNAPSHOT`** | `src/ingestion/cdc_uniform_pipeline.py`, `tests/integration/section_2/test_subpoint_2_2_cdc_uniform_sabotage.py` |
| **2.3** | Configure CDC ingestion pipelines from relational database sources (SQL Server, MySQL, PostgreSQL) using Lakeflow Connect | • `2.3.1` Lakeflow Connect Database Connectors (Ingestion Gateway + Ingestion Pipeline architecture, WAL/binlog setup, UC Connections) | `src/ingestion/lakeflow_connect_ingest.py`, `tests/integration/section_2/test_subpoint_2_3_lakeflow_connect_sabotage.py` |
| **2.4** | Implement OpenSharing (D2D and Databricks-to-Open) and Clean Rooms for privacy-preserving collaboration | • `2.4.1` OpenSharing: Databricks-to-Databricks (D2D) vs. Databricks-to-Open (D2O)<br>• `2.4.2` Databricks Clean Rooms: Privacy-preserving multi-party collaboration | `src/ingestion/opensharing_and_clean_rooms.py`, `tests/unit/section_2/test_subpoint_2_4_opensharing_sabotage.py` |
| **2.5** | Configure Lakehouse Federation with governance across supported source systems, applying UC permissions and connection-level credentials | • `2.5.1` Lakehouse Federation: Connections, Foreign Catalogs, Query Federation vs. Catalog Federation, Query Pushdown (`PUSHDOWN` vs. `FULL_SCAN_CLIENT_FILTER`), and UC Permissions | `src/ingestion/lakehouse_federation.py`, `tests/unit/section_2/test_subpoint_2_5_federation_sabotage.py` |

---

## Subpoint 2.1: Multi-Format Ingestion from Cloud Storage & Message Buses

> **Official Docs:** [Auto Loader file formats](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html) | [Structured Streaming with Kafka](https://docs.databricks.com/en/structured-streaming/kafka.html) | [Read binary files](https://docs.databricks.com/en/query/formats/binary-file.html)

### 2.1.1 Cloud Storage Ingestion Across All 6 Exam Formats (JSON, CSV, Parquet, Binary, Delta, Iceberg)

#### High-Yield Exam Rule — Which Formats Use Auto Loader (`cloudFiles`) vs. Direct Table/Catalog Readers?

| Format | How to Ingest Incrementally | Key Options & Exam Traps |
|---|---|---|
| **JSON** | Auto Loader (`format("cloudFiles")` + `cloudFiles.format = "json"`) | Combine with `parse_json()` $\rightarrow$ **`VARIANT`** (Law 5) or `cloudFiles.inferColumnTypes = "true"`. Captures malformed fields in `_rescued_data`. |
| **CSV** | Auto Loader (`cloudFiles.format = "csv"`) | Use `header = "true"`, `cloudFiles.inferColumnTypes = "true"`, `cloudFiles.schemaHints = "zip_code STRING"`, and `multiLine = "true"` if fields contain embedded newlines. |
| **Parquet** | Auto Loader (`cloudFiles.format = "parquet"`) | Embedded schema in file footers; `cloudFiles.schemaEvolutionMode = "addNewColumns"` evolves target Delta schema automatically when new Parquet columns appear. |
| **Binary (`binaryFile`)** | Auto Loader (`cloudFiles.format = "binaryFile"`) or batch `spark.read.format("binaryFile")` | Used for unstructured files (PDFs, images, audio, ZIPs) **up to 2 GB per file**. Always outputs **4 fixed columns**: `path` (`STRING`), `modificationTime` (`TIMESTAMP`), `length` (`LONG`), `content` (`BINARY`). Use `.option("pathGlobFilter", "*.pdf")` and `.option("recursiveFileLookup", "true")`. |
| **Delta Lake** | `spark.readStream.format("delta").table("cat.sch.tbl")` (or `FROM STREAM(cat.sch.tbl)`) | ❌ **Exam Trap:** Do **NOT** use Auto Loader (`cloudFiles`) to read a Delta table's underlying `.parquet` files directly (reading raw Parquet bypasses `_delta_log` and corrupts results when Deletion Vectors or compactions occur!). Always stream via the Delta transaction log (`format("delta")`). |
| **Apache Iceberg** | **Catalog Federation** (Unity Catalog Foreign Iceberg Catalog via AWS Glue / Iceberg REST) or `spark.read.format("iceberg")` | ❌ **Exam Trap:** Do **NOT** use Auto Loader (`cloudFiles`) on an Iceberg table directory (which would ingest both active and orphan/uncommitted Parquet snapshots!). Read via Iceberg metadata or Unity Catalog Federation. |

### 2.1.2 Streaming Ingestion from Message Buses: Kafka, AWS Kinesis, and Google Pub/Sub

#### Comparison of the 3 Message Bus Connectors on the Exam:

| Feature | Apache Kafka (`format("kafka")`) | Amazon Kinesis (`format("kinesis")`) | Google Cloud Pub/Sub (`format("pubsub")`) |
|---|---|---|---|
| **Connection Options** | `kafka.bootstrap.servers`, `subscribe` (or `subscribePattern`) | `streamName`, `region` (plus IAM role / Unity Catalog Service Credential) | `subscriptionId`, `projectId` (plus GCP Service Credential) |
| **Starting Offset Option** | `startingOffsets`: **`"earliest"`**, **`"latest"`**, or JSON partition map | `initialPosition`: **`"trim_horizon"`** *(oldest)*, **`"latest"`**, or **`"at_timestamp"`** | Managed by Pub/Sub subscription backlog (unacknowledged messages) |
| **Rate-Limiting Option** | `maxOffsetsPerTrigger` | `maxFetchDuration` / `shardsPerTask` | `messagesPerSecond` |
| **Expired Offset Recovery** | **`failOnDataLoss = "false"`** (skips expired offsets instead of crashing) | Handled via shard iterator / `failOnDataLoss` | Managed by subscription retention |
| **Raw Output Schema** | 7 columns: **`key` (`BINARY`)**, **`value` (`BINARY`)**, `topic` (`STRING`), `partition` (`INT`), `offset` (`LONG`), `timestamp` (`TIMESTAMP`), `timestampType` (`INT`) | **`data` (`BINARY`)**, `partitionKey` (`STRING`), `sequenceNumber` (`STRING`), `approximateArrivalTimestamp` (`TIMESTAMP`), `stream` (`STRING`), `shardId` (`STRING`) | **`payload` / `data` (`BINARY`)**, `attributes` (`MAP<STRING, STRING>`), `messageId` (`STRING`), `publishTimestamp` (`TIMESTAMP`) |

> **Exam Memory Hook:**
> - In **Kafka**, the oldest position is **`"earliest"`** and the payload column is **`value` (`BINARY`)**.
> - In **Kinesis**, the oldest position is **`"trim_horizon"`** and the payload column is **`data` (`BINARY`)**.
> - In all three message buses, the message payload arrives as **`BINARY` bytes**—you must cast it to `STRING` (`CAST(value AS STRING)`) before parsing JSON (`parse_json(CAST(value AS STRING))`)!

---

## Subpoint 2.2: Incremental CDC Pipelines into Delta or Iceberg Targets

> **Official Docs:** [Change data capture with AUTO CDC](https://docs.databricks.com/en/ldp/cdc.html) | [Read Iceberg with UniForm](https://docs.databricks.com/en/delta/uniform.html)

### 2.2.1 `AUTO CDC` from a Change Feed (`CREATE FLOW ... AS AUTO CDC INTO`)

1. **Table vs. Flow Separation in Lakeflow Declarative Pipelines:**
   - **Streaming Table (`CREATE OR REFRESH STREAMING TABLE`):** The physical destination table in Unity Catalog.
   - **Flow (`CREATE FLOW <flow_name> AS AUTO CDC INTO <target>`):** The internal pipeline ingestion process that reads a change stream and merges records into the target table. Multiple flows can write into a single target table (e.g., merging regional CDC streams into one global customer table).
2. **Core `AUTO CDC` Clauses:**
   - `KEYS (id)`: Primary key(s) used for matching rows.
   - `SEQUENCE BY commit_ts`: Orders events by source commit timestamp/LSN so late-arriving older messages never overwrite newer updates.
   - `APPLY AS DELETE WHEN op = 'DELETE'`: Processes tombstone delete events by deleting the row (SCD1) or closing `__end_at` (SCD2).
   - `APPLY AS TRUNCATE WHEN op = 'TRUNCATE'`: Truncates the target table (**supported in SCD Type 1 only**—forbidden in SCD Type 2).
   - `IGNORE NULL UPDATES`: Prevents `NULL` values in partial update messages from overwriting existing non-null columns.
   - `COLUMNS * EXCEPT (op, commit_ts)`: Excludes internal CDC metadata columns from the target table.
   - `STORED AS SCD TYPE 1` (in-place overwrite) vs. `STORED AS SCD TYPE 2` (historical versioning with `__start_at` and `__end_at = NULL` for active rows).

### 2.2.2 High-Yield Exam Nuance — `AUTO CDC FROM SNAPSHOT` (`create_auto_cdc_from_snapshot_flow`)

What if an upstream vendor or legacy mainframe does **not** provide a row-level CDC transaction log (`INSERT`/`UPDATE`/`DELETE`), and instead drops **periodic full table snapshots** (e.g. a daily full Parquet dump or snapshot table)?

- **Do NOT write manual full-outer-join diffing scripts!**
- Use **`AUTO CDC FROM SNAPSHOT`** (`dlt.create_auto_cdc_from_snapshot_flow` in Python):
  ```python
  import dlt


  dlt.create_streaming_table("dim_vendors_scd2")

  dlt.create_auto_cdc_from_snapshot_flow(
      target="dim_vendors_scd2",
      source="daily_vendor_snapshot",
      keys=["vendor_id"],
      stored_as_scd_type=2,
  )
```
- **How `AUTO CDC FROM SNAPSHOT` Works:**
  - Compares the incoming full snapshot against the target table state.
  - New keys $\rightarrow$ Inserted.
  - Changed attributes for existing keys $\rightarrow$ Updated (or new SCD2 version opened).
  - **Keys missing from the new snapshot $\rightarrow$ Automatically treated as DELETED!**

### 2.2.3 Delta vs. Apache Iceberg (UniForm) as CDC Target Table Formats

- **Deletion Vectors (`delta.enableDeletionVectors = true`):**
  - Crucial for CDC target tables: instead of rewriting an entire 100 MB Parquet file when 1 row is updated or deleted, Delta writes a lightweight bitmap tracking deleted row offsets, eliminating write amplification.
- **UniForm (Universal Format) for Apache Iceberg Targets:**
  - Lakeflow Pipelines write to Unity Catalog tables backed by Delta Lake and expose them as native **Apache Iceberg** tables via **UniForm**:
    ```sql
    CREATE OR REFRESH STREAMING TABLE portfolio_catalog.silver.dim_inventory_iceberg
    TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'true',
      'delta.columnMapping.mode' = 'name',
      'delta.universalFormat.enabledFormats' = 'iceberg'
    )
    CLUSTER BY (item_id);
    ```
  - **UniForm Prerequisites & Mechanics:**
    1. Table must be registered in **Unity Catalog**.
    2. **Column Mapping (`'delta.columnMapping.mode' = 'name'`)** is required (automatically enabled when creating a new table with UniForm).
    3. After each Delta transaction commits, Databricks **asynchronously** generates Iceberg metadata snapshots (`metadata/*.metadata.json`) pointing to the exact same Parquet files (**zero data duplication**).
    4. External engines (Snowflake, Amazon Athena, Trino, BigQuery) read the table via the **Unity Catalog Iceberg REST Catalog API**.

---

## Subpoint 2.3: Relational CDC Ingestion with Lakeflow Connect

> **Official Docs:** [What is Lakeflow Connect?](https://docs.databricks.com/en/ingestion/lakeflow-connect/index.html)

### 2.3.1 Lakeflow Connect Database Connectors Architecture (SQL Server, MySQL, PostgreSQL, Oracle)

**Lakeflow Connect** is Databricks' managed, zero-code ingestion suite that replaces self-managed Debezium/Kafka clusters and brittle JDBC polling scripts.

#### High-Yield Exam Architecture — The 2-Part Relational Connector Pipeline:
For relational databases (**PostgreSQL, MySQL, SQL Server, Oracle**), Lakeflow Connect uses a **two-component architecture** together with a **Unity Catalog Connection** and a **Unity Catalog Staging Volume**:

```text
[Source DB: SQL Server / MySQL / PostgreSQL]
        │  (Reads database WAL / binlog / CDC tables via UC Connection)
        ▼
┌──────────────────────────────────────────────────────────┐
│ 1. INGESTION GATEWAY (Continuous VM / Classic Compute)   │
│    • Continuously extracts changes from DB transaction   │
│      logs before the DB purges its binlog/WAL            │
│    • Writes raw change files into a UC Staging Volume    │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼  (/Volumes/<catalog>/<schema>/<staging_volume>)
┌──────────────────────────────────────────────────────────┐
│ 2. INGESTION PIPELINE (Serverless Lakeflow Pipeline)     │
│    • Reads staged CDC files from the UC Staging Volume   │
│    • Performs initial baseline snapshot + incremental    │
│      AUTO CDC (SCD Type 1 or SCD Type 2) into UC Tables  │
│    • Automatically evolves schema on upstream DDL changes│
└──────────────────────────────────────────────────────────┘
```

#### Why Does Lakeflow Connect Separate the Gateway from the Ingestion Pipeline?
- Operational databases only retain their transaction logs (binlog / WAL) for a short window (e.g., a few hours).
- The lightweight **Ingestion Gateway** runs continuously to drain the database log into durable **Unity Catalog Volume** storage so zero changes are ever lost, while the **Serverless Ingestion Pipeline** can run on a cost-efficient schedule (e.g., every 15 minutes or hourly) to merge those staged changes into Bronze/Silver Delta tables!

#### Source Database Prerequisites (Least-Privilege Setup):
| Source Database | Native CDC Mechanism Used by Lakeflow Connect |
|---|---|
| **PostgreSQL** | Logical Replication (`wal_level = logical`), user with `REPLICATION` privilege, and a `PUBLICATION` on target tables. |
| **MySQL** | Binary Log replication (`binlog_format = ROW`, `binlog_row_image = FULL`, `binlog_expire_logs_seconds`). |
| **Microsoft SQL Server** | SQL Server **Change Tracking (CT)** (for tables with primary keys) and/or **Change Data Capture (CDC)** (`sys.sp_cdc_enable_table`, required for tables without primary keys). |

---

## Subpoint 2.4: OpenSharing (D2D and Databricks-to-Open) & Clean Rooms

> **Official Docs:** [What is Delta Sharing / OpenSharing?](https://docs.databricks.com/en/delta-sharing/index.html) | [What are Databricks Clean Rooms?](https://docs.databricks.com/en/clean-rooms/index.html)

### 2.4.1 OpenSharing: Databricks-to-Databricks (D2D) vs. Databricks-to-Open (D2O)

OpenSharing shares live data (tables, views, volumes, models) across organizations **without copying data**.

| Dimension | Databricks-to-Databricks (D2D) | Databricks-to-Open (D2O) |
|---|---|---|
| **Recipient Platform** | Another organization (or another account) that **uses Databricks with Unity Catalog**. | Any external non-Databricks tool (**Pandas, Power BI, Tableau, Apache Spark on-prem, Excel**). |
| **How Recipient is Created** | Using the recipient's **Sharing Identifier** (`<cloud>:<region>:<metastore_uuid>`):<br>`CREATE RECIPIENT partner_a USING ID 'aws:us-west-2:1111...';` | Without a Sharing ID:<br>`CREATE RECIPIENT partner_b;` |
| **Authentication Mechanism** | **Direct Unity Catalog Metastore-to-Metastore cryptography.** Zero credential files, zero activation links, zero bearer tokens! | Provider sends a one-time **activation link**; recipient downloads a **credential profile JSON file** (`shareCredentialsVersion`, HTTPS `endpoint`, `bearerToken`, `expirationTime`). |
| **Token Rotation** | Managed automatically by Databricks platform. | Provider rotates expiring tokens via:<br>`ALTER RECIPIENT partner_b ROTATE TOKEN WITH (expire_in_seconds = 86400);` |
| **What Assets Can Be Shared?** | Tables, Dynamic Views, **Unity Catalog Volumes**, AI Models, Notebooks. | Tabular data (Delta tables & views). |

#### Provider & Recipient SQL Workflow (Memorize for Exam!):
```sql
-- ==========================================
-- 1. ON THE PROVIDER WORKSPACE:
-- ==========================================
CREATE SHARE finops_partner_share;

-- Share a table including Change Data Feed and historical versions (for time-travel / streaming reads)
ALTER SHARE finops_partner_share
ADD TABLE portfolio_catalog.silver.clean_events
WITH CHANGE DATA FEED
WITH HISTORY;

-- Grant access to the recipient
GRANT SELECT ON SHARE finops_partner_share TO RECIPIENT partner_a;

-- ==========================================
-- 2. ON THE RECIPIENT WORKSPACE (D2D):
-- ==========================================
SHOW SHARES IN PROVIDER pharma_corp_provider;

-- Mount the shared data as a read-only Catalog in the Recipient's Unity Catalog:
CREATE CATALOG partner_shared_catalog
USING SHARE pharma_corp_provider.finops_partner_share;
```

#### Dynamic Row-Level Filtering per Recipient (`current_recipient()`):
- Need to share **one single table** with 10 different partner companies so each partner only sees their own country's rows?
- Create a **Dynamic View** using `current_recipient('country')`, set recipient properties (`ALTER RECIPIENT partner_de SET PROPERTIES ('country' = 'DE')`), and add the view to the share!

### 2.4.2 Databricks Clean Rooms: Privacy-Preserving Multi-Party Collaboration

- **When OpenSharing is NOT Enough:**
  - In OpenSharing, the recipient can run `SELECT *` on the shared table or view and see individual rows.
  - When two organizations (e.g., a Hospital and an Insurance Provider, or a Bank and a Retailer) want to join overlapping customer keys **without either party ever seeing the other party's raw rows or PII**, use **Databricks Clean Rooms**.
- **How Clean Rooms Work:**
  1. Each collaborator links tables/views/volumes from their own Unity Catalog into the Clean Room (no raw data is copied out to the other party).
  2. Computations run inside an **isolated, serverless Clean Room compute environment**.
  3. Collaborators can **only run mutually approved notebooks/queries** that output **aggregated metrics** (e.g., `COUNT(*)`, `AVG(spend)` with minimum group-size thresholds and differential privacy noise) so individual customers cannot be re-identified.

---

## Subpoint 2.5: Lakehouse Federation with Unity Catalog Governance

> **Official Docs:** [What is Lakehouse Federation?](https://docs.databricks.com/en/query-federation/index.html)

### 2.5.1 Connections, Foreign Catalogs, Query Federation vs. Catalog Federation, and Pushdown

**Lakehouse Federation** allows querying external data sources (Snowflake, PostgreSQL, MySQL, SQL Server, Amazon Redshift, Google BigQuery, AWS Glue) in-place using Unity Catalog's 3-level namespace (`<foreign_catalog>.<schema>.<table>`) **without building ETL pipelines**.

#### 1. The 2 Unity Catalog Objects & Required Privileges:

| Step | SQL Command | Unity Catalog Privileges Required |
|---|---|---|
| **Step 1: Create Connection** | ```sql
CREATE CONNECTION snowflake_conn
TYPE SNOWFLAKE
OPTIONS (
  host 'xy123.snowflakecomputing.com',
  port '443',
  user secret('finops_scope', 'sf_user'),
  password secret('finops_scope', 'sf_pass'),
  sfWarehouse 'COMPUTE_WH'
);
``` | **`CREATE CONNECTION`** on the Unity Catalog Metastore. |
| **Step 2: Create Foreign Catalog** | ```sql
CREATE FOREIGN CATALOG snowflake_dw
USING CONNECTION snowflake_conn
OPTIONS (database 'ANALYTICS_PROD');
``` | **`CREATE FOREIGN CATALOG`** on the Metastore **AND** either ownership of `snowflake_conn` or **`CREATE FOREIGN SECURABLE`** on `snowflake_conn`. |
| **Step 3: Grant Read Access to Analysts** | ```sql
GRANT USE CATALOG ON CATALOG snowflake_dw TO analysts;
GRANT USE SCHEMA ON SCHEMA snowflake_dw.sales TO analysts;
GRANT SELECT ON TABLE snowflake_dw.sales.orders TO analysts;
``` | Standard Unity Catalog ACL inheritance (plus row filters / column masks!). |

#### 2. Query Federation vs. Catalog Federation (High-Yield Exam Nuance!):
- **Query Federation:** Used for external relational databases and cloud data warehouses (PostgreSQL, MySQL, SQL Server, Snowflake, BigQuery, Redshift). Databricks sends SQL queries to the **external system's compute engine** and streams back results.
- **Catalog Federation:** Used when federating an external metadata catalog (e.g., **AWS Glue Catalog**, external **Apache Iceberg / Delta Lake** tables stored in S3/ADLS/GCS). Instead of routing queries through an external database engine, Unity Catalog reads the metadata from the external catalog and **executes the query directly on cloud object storage using Databricks Serverless/Photon compute**!

#### 3. `PUSHDOWN` vs. `FULL_SCAN_CLIENT_FILTER` (Why Some Federated Queries Are Fast and Others Are Slow):

| Execution Mode | Query Pattern Example | What Happens Under the Hood | Network & Performance Impact |
|---|---|---|---|
| **Query Pushdown (`PUSHDOWN`)** | ```sql
SELECT customer_id, SUM(amount)
FROM snowflake_dw.sales.orders
WHERE order_date >= '2026-01-01'
GROUP BY customer_id;
``` | Databricks Catalyst translates the `WHERE` filter, column projection, and `GROUP BY` aggregation into native Snowflake SQL and pushes them to Snowflake. Snowflake filters 100M rows down to 1,000 summary rows. | ⚡ **Fast & Cheap:** Only 1,000 summary rows cross the network. |
| **Client-Side Filter (`FULL_SCAN_CLIENT_FILTER`)** | ```python
@udf("boolean")
def custom_py_filter(amt):
    return amt > 500


df = spark.table(
    "snowflake_dw.sales.orders"
).filter(custom_py_filter(col("amount")))
``` | Because the external Snowflake/PostgreSQL database cannot execute custom local Python bytecode (`@udf`), Catalyst **cannot push down** the filter! Databricks is forced to fetch **all 100,000,000 raw rows** over the network and run the Python filter on the Databricks side. | 🐢 **Slow & Expensive:** Full table scan transferred over the network! *(Fix: Rewrite the filter using native SQL expressions so Catalyst can push it down).* |

---

## Master Q&A Vault: Every Question You Asked in Section 2

### Q1: What is the difference between CDC (Change Data Capture) and `enableChangeDataFeed` (CDF)?
- **Answer:**
  - **CDC (Change Data Capture):** The general pattern (and tools like Lakeflow Connect / Debezium) for capturing `INSERT`, `UPDATE`, and `DELETE` events from an **external operational database** (like PostgreSQL or MySQL) and bringing them into Databricks using **`AUTO CDC`**.
  - **Change Data Feed (`delta.enableChangeDataFeed = true`):** Delta Lake's **built-in feature** that records row-level changes (`insert`, `update_preimage`, `update_postimage`, `delete`) inside a **Delta table's own transaction log** so downstream tables/consumers can read only what changed (`readChangeFeed` / `table_changes()`) instead of re-scanning the entire Delta table.

### Q2: How does `enableDeletionVectors` work, and why does it matter so much for CDC targets?
- **Answer:**
  - Parquet files are immutable. Without Deletion Vectors, deleting or updating 1 single row inside a 100 MB Parquet file forces Spark to read the entire 100 MB file and rewrite a brand-new 99.9 MB Parquet file (**Write Amplification**).
  - With **Deletion Vectors (`delta.enableDeletionVectors = true`)**, Delta leaves the 100 MB Parquet file untouched on disk and writes a tiny **bitmap file** marking row #42 as deleted ("skip this row when reading"). Physical file rewriting is deferred until background compaction (`OPTIMIZE` / `REORG TABLE ... APPLY (PURGE)`), making CDC updates/deletes up to 10x faster.

### Q3: In `CREATE FLOW product_cdc_flow AS AUTO CDC INTO ...`, what kind of object is a `FLOW`? Is it a table, view, pipeline, or job?
- **Answer:**
  - A **`FLOW`** is the **internal streaming worker/process inside a Lakeflow Declarative Pipeline** that moves data from a source into a target Streaming Table.
  - The **Streaming Table (`CREATE STREAMING TABLE`)** is the physical storage container in Unity Catalog; the **`FLOW` (`CREATE FLOW`)** is the pipe that pours/merges data into that container. Separating the Table from the Flow allows multiple flows (e.g., `eu_cdc_flow` and `us_cdc_flow`) to write into the exact same target table!

### Q4: Does `AUTO CDC` only work if the input is JSON and the state is update/delete?
- **Answer:**
  - No! The input source to `AUTO CDC` can be **any streaming source** (a Bronze Delta table, Auto Loader reading JSON/CSV/Parquet, or Kafka) and it handles **all three operations**: `INSERT` (new keys), `UPDATE` (existing keys), and `DELETE` (via `APPLY AS DELETE WHEN ...`).
  - Furthermore, if you only receive full periodic table dumps without operation flags, **`AUTO CDC FROM SNAPSHOT`** (`dlt.create_auto_cdc_from_snapshot_flow`) automatically diffs consecutive snapshots to apply inserts, updates, and deletes!

### Q5: In our Subpoint 2.1 Kafka test, why was `message_key` cast to a `STRING` (`'KEY-101'`) instead of a SHA-256 hash?
- **Answer:**
  - When Kafka sends messages, `key` and `value` arrive as raw `BINARY` bytes (`bytearray(b"KEY-101")`). Casting `key` to `STRING` (`col("key").cast("string")`) simply decodes those UTF-8 bytes back into the human-readable business routing key (`"KEY-101"`).
  - A **SHA-256 hash** (`F.sha2(col("content"), 256)`) is used when ingesting unstructured files with `format("binaryFile")` (like PDFs or images) to create a unique 64-character fingerprint of the file's byte content for deduplication and integrity verification.

### Q6: In Question 2.4.1, how can Partner A use Databricks-to-Databricks (D2D) if there is only one provider company?
- **Answer:**
  - In B2B data sharing, **Partner A is a separate company that also happens to be a Databricks customer** (they have their own Databricks account and Unity Catalog metastore). Because both our company (the Provider) and Partner A (the Recipient) use Databricks Unity Catalog, Partner A gives us their **Sharing Identifier** (`aws:us-west-2:...`) and we connect via **D2D** with zero credential files.
  - **Partner B**, on the other hand, does **not** use Databricks at all (they run local Python/Pandas or Power BI), so we provision Partner B using **Databricks-to-Open (D2O)** with a bearer token profile JSON file.

### Q7: What is the difference between `PUSHDOWN` and `FULL_SCAN_CLIENT_FILTER` in Lakehouse Federation?
- **Answer:**
  - **`PUSHDOWN` (Query Pushdown):** When you write standard SQL filters/aggregations (`WHERE order_date >= '2026-01-01'`), Databricks translates the filter into the remote database's native SQL (e.g., Snowflake or PostgreSQL). The remote database filters its 100M rows down to 500 matching rows, and **only 500 rows travel over the network** to Databricks.
  - **`FULL_SCAN_CLIENT_FILTER` (Client-Side Filter):** When your `WHERE` clause calls a custom local Python UDF (`@udf`), the remote Snowflake/PostgreSQL database has no way to execute your Python function. Databricks is forced to **download all 100M rows across the network** (`FULL_SCAN`) and run the Python filter on the Databricks cluster (`CLIENT_FILTER`).

---

## Rapid-Fire 10-Second Exam Elimination Rules (Section 2 Master Checklist)

1. **See `failOnDataLoss = "false"` on a Kafka stream** $\rightarrow$ ✅ Correct fix when Kafka retention purges unread offsets while a stream was paused.
2. **See Auto Loader (`cloudFiles`) reading a Delta or Iceberg table directory directly** $\rightarrow$ ❌ Eliminate immediately! Use `spark.readStream.format("delta")` for Delta and Catalog Federation / `format("iceberg")` for Iceberg.
3. **See external engines (Athena, Trino, Snowflake) needing to read Delta tables as Iceberg without copying data** $\rightarrow$ ✅ Choose **UniForm** (`'delta.universalFormat.enabledFormats' = 'iceberg'`).
4. **See periodic full table dumps needing SCD Type 1 / Type 2 tracking without change logs** $\rightarrow$ ✅ Choose **`AUTO CDC FROM SNAPSHOT`** (`dlt.create_auto_cdc_from_snapshot_flow`).
5. **See relational database CDC (PostgreSQL, MySQL, SQL Server) needing zero-code managed ingestion** $\rightarrow$ ✅ Choose **Lakeflow Connect** (Unity Catalog Connection + Ingestion Gateway + Ingestion Pipeline).
6. **See a Databricks-to-Databricks (D2D) recipient being emailed a `.share` bearer token file** $\rightarrow$ ❌ Eliminate immediately! **D2D uses the recipient's Sharing Identifier (`USING ID`) and never uses credential files**; bearer token files are strictly for **Databricks-to-Open (D2O)**.
7. **See two companies collaborating on joint data where neither party may view the other's raw rows or PII** $\rightarrow$ ✅ Choose **Databricks Clean Rooms**.
8. **See `CREATE EXTERNAL CATALOG` or `CREATE FEDERATED CATALOG`** $\rightarrow$ ❌ Eliminate immediately! The official Lakehouse Federation syntax is **`CREATE FOREIGN CATALOG <name> USING CONNECTION <conn> OPTIONS (...)`**.
