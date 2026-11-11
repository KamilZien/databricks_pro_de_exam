# Section 2: Data Ingestion & Acquisition (12%)

> **Exam Domain Weight:** 12% of total exam score (~7 to 8 questions)  
> **Official Exam Syllabus:** Post-October 9, 2026 Outline  
> **Key Architecture:** Multi-format ingestion (Kafka, binaryFile, UniForm Iceberg), Lakeflow incremental CDC, Lakeflow Connect, OpenSharing (D2D/D2O), Lakehouse Federation

---

## Domain Breakdown: 5 Core Objectives

| # | Official Objective | Core Architecture & Exam Traps |
|---|---|---|
| **2.1** | **Multi-Format Ingestion from Message Buses & Cloud Storage** | Raw 7-column Kafka schema (`key`, `value` in BINARY), `failOnDataLoss = false`, `binaryFile` format (`path`, `modificationTime`, `length`, `content`), UniForm Iceberg table properties (`delta.universalFormat.enabledFormats = 'iceberg'`). |
| **2.2** | **Incremental CDC Pipelines into Delta or Iceberg Targets** | Lakeflow CDC ingestion targeting Delta or UniForm/Iceberg; schema compatibility, Deletion Vectors, and metadata. |
| **2.3** | **Relational Ingestion with Lakeflow Connect** | Zero-code managed gateway ingestion for SQL Server, MySQL, and PostgreSQL (replacing legacy custom Debezium/JDBC pipelines). |
| **2.4** | **OpenSharing (Delta Sharing) & Clean Rooms** | Databricks-to-Databricks (D2D) vs. Databricks-to-Open (D2O), recipient profiles, credential management, and privacy-preserving Clean Rooms. |
| **2.5** | **Lakehouse Federation with Unity Catalog Governance** | Foreign catalogs, connection objects (`CREATE CONNECTION`), pushdown query optimization, and UC permission mapping across external databases. |

---

## Subpoint 2.1: Multi-Format Ingestion from Message Buses & Cloud Storage

### 1. Message Bus Ingestion (Kafka / Kinesis / Pub/Sub)
- **Raw 7-Column Kafka Schema:**
  - `key` (BINARY), `value` (BINARY), `topic` (STRING), `partition` (INT), `offset` (LONG), `timestamp` (TIMESTAMP), `timestampType` (INT).
- **Decoding Binary Payloads:**
  - Cast `key` to `STRING`: `col("key").cast("string")`.
  - Parse `value` into `VARIANT`: `expr("parse_json(cast(value AS STRING))")`.
- **`failOnDataLoss` Option:**
  - By default (`true`), Spark fails the streaming query if Kafka retention deletes unread offsets recorded in the checkpoint.
  - Setting `.option("failOnDataLoss", "false")` allows the stream to gracefully resume from the earliest available offset.

### 2. Binary File Ingestion (`format("binaryFile")`)
- Used for unstructured documents (PDFs, images, audio, ZIPs) up to 2GB per row.
- **4 Native Schema Columns:**
  1. `path` (STRING): Full cloud URI.
  2. `modificationTime` (TIMESTAMP): Last-modified file timestamp.
  3. `length` (LONG): File size in bytes.
  4. `content` (BINARY): Raw byte array.
- Extracting leaf file names: `element_at(split(col("path"), "/"), -1)`.
- Data integrity / deduplication: `sha2(col("content"), 256)` generates a 64-hexadecimal character SHA-256 fingerprint.

### 3. UniForm (Universal Format) for Apache Iceberg
- Generates Iceberg metadata on top of Delta Parquet data files asynchronously.
- Enables non-Databricks engines (Amazon Athena, Trino, Snowflake) to read the same physical storage with **zero data duplication**.
- **Table Properties Required:**
  ```sql
  TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
  );
  ```

---

## Subpoint 2.2: Incremental CDC Pipelines Targeting Delta & Iceberg (UniForm)

### 1. The Role of `FLOW` in Lakeflow Declarative Pipelines
- **Table vs. Flow Separation:**
  - A **Streaming Table** (`CREATE STREAMING TABLE ...`) is the physical destination table in Unity Catalog.
  - A **`FLOW`** (`CREATE FLOW ... AS AUTO CDC INTO ...`) is the managed ingestion process that extracts from the source stream and merges into the target table.
  - Multiple flows can target the same destination table.

### 2. Modern `AUTO CDC` DDL Syntax
```sql
CREATE FLOW inventory_cdc_flow
AS AUTO CDC INTO portfolio_catalog.silver.dim_inventory
FROM STREAM(portfolio_catalog.bronze.inventory_cdc_raw)
KEYS (item_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY commit_timestamp
STORED AS SCD TYPE 1;
```

### 3. Deletion Vectors & CDC Performance
- **Write Amplification Problem:** Traditional Delta had to rewrite entire 100MB Parquet files just to update/delete 1 row.
- **Deletion Vectors Solution (`delta.enableDeletionVectors = true`):** Leaves the original Parquet files intact and writes lightweight bitmap vectors indicating which row indexes are deleted, reducing CDC write amplification by up to 90%.

### 4. SCD Type 1 vs. SCD Type 2 CDC Constraints
- `APPLY AS TRUNCATE` is supported **only on SCD Type 1** (cannot truncate historical audit chains in SCD Type 2).
- `SEQUENCE BY <col>` ensures out-of-order events arriving over the network do not overwrite newer database commits.

---

## Subpoint 2.3: Relational Ingestion with Lakeflow Connect

### 1. What Lakeflow Connect Replaces
- **Legacy Approach:** Custom Kafka clusters + Debezium CDC containers + brittle JDBC polling scripts (`WHERE updated_at > ...`).
  - *Problems:* Overloads production OLTP databases, misses hard deletes, and requires heavy infrastructure maintenance.
- **Lakeflow Connect Approach:** Databricks' **native, zero-code, managed ingestion service** that reads database write-ahead logs (binlog / WAL) directly from **PostgreSQL, MySQL, SQL Server, and Oracle**, as well as enterprise SaaS connectors.

### 2. Two-Phase Snapshot-to-Stream Ingestion
1. **Phase 1 (Automatic Initial Snapshot):** Lakeflow Connect automatically performs a full baseline table snapshot upon initial launch without requiring manual database dumps.
2. **Phase 2 (Continuous WAL Streaming):** Seamlessly transitions into streaming incremental binlog/WAL events (`INSERT`, `UPDATE`, `DELETE`) into Bronze/Silver tables.

### 3. Credential Security & Governance
- Relational sources connect via **Unity Catalog Connection Objects** (`CREATE CONNECTION ...`).
- Passwords and sensitive connection tokens **must** be stored in **Databricks Secret Scopes** and referenced via `secret('scope', 'key')`—never hardcoded in YAML or notebooks.
- Access to connections is governed using standard UC permissions (`GRANT USE CONNECTION ON CONNECTION <name> TO <identity>`).

### 4. Automated Schema Evolution
- When upstream operational databases add, rename, or drop columns, Lakeflow Connect automatically detects the DDL changes and evolves the destination tables without crashing the pipeline.

---

## Subpoint 2.4: OpenSharing (Delta Sharing) & Databricks Clean Rooms

### 1. OpenSharing Protocols: D2D vs. D2O
| Sharing Mode | Recipient Environment | How Access Is Established | Credential Management |
|---|---|---|---|
| **Databricks-to-Databricks (D2D)** | Recipient has Databricks with Unity Catalog | `CREATE RECIPIENT <name> USING ID '<sharing_identifier>';` | **Zero credential files.** Direct Unity Catalog metastore-to-metastore trust. |
| **Databricks-to-Open (D2O)** | External clients (Pandas, Apache Spark on-prem, PowerBI, Tableau, Excel) | `CREATE RECIPIENT <name>;` (generates activation link) | Recipient downloads a secure **bearer token profile JSON file** (HTTPS endpoint, bearer token, expiration timestamp). |

### 2. Sharing Change Data Feed (CDF)
- To allow recipients to read incremental row changes via `table_changes(...)` or `readChangeFeed`, the provider must explicitly include CDF in the share:
  ```sql
  ALTER SHARE finops_partner_share ADD TABLE portfolio_catalog.silver.clean_events WITH CHANGE DATA FEED;
  ```

### 3. Token Rotation for D2O
- When an external recipient's bearer token is nearing expiration, the provider rotates the token:
  ```sql
  ALTER RECIPIENT external_partner ROTATE TOKEN;
  ```
- Allows a configurable grace period where both old and new tokens remain valid during credential migration.

### 4. Databricks Clean Rooms
- **The Problem:** Two or more parties want to run analytical joins on shared data, but regulations or competitive concerns forbid either party from seeing the other's raw rows or PII.
- **The Solution:** A secure, isolated multi-party compute environment where:
  - Both parties contribute data without copying raw files to third parties.
  - Queries are restricted to pre-approved aggregation templates (e.g. `COUNT(*)`, `AVG(spend)`).
  - Differential privacy prevents reverse-engineering individual records.



