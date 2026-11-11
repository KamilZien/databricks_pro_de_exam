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

