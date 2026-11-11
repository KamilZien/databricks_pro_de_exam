# Section 2: Data Ingestion & Acquisition — Calibrated Question Bank

> **Domain:** Section 2: Data Ingestion & Acquisition (12%)  
> **Source:** Post-October 2026 Databricks Certified Data Engineer Professional Syllabus  
> **Purpose:** Permanent retention, review, and exam revision.

---

### Subpoint 2.1: Multi-Format Ingestion from Message Buses & Cloud Storage

#### Question 2.1.1: Kafka Ingestion & Stream Stability
A data engineer creates a Spark Structured Streaming pipeline to ingest transactional payment events from an enterprise Apache Kafka cluster. The Kafka topic has a retention policy of 24 hours. Over a holiday weekend, the streaming cluster was paused for 48 hours for an infrastructure maintenance window. 

When the engineer restarts the stream from its existing `checkpointLocation`, the streaming query immediately fails with an exception stating that the starting offset recorded in the checkpoint no longer exists on the Kafka brokers.

Which configuration option should the engineer add to the `spark.readStream.format("kafka")` configuration to allow the stream to automatically recover by skipping the expired offsets and processing the earliest available messages?

- A) `.option("startingOffsets", "latest")`
- B) `.option("failOnDataLoss", "false")`
- C) `.option("kafka.auto.offset.reset", "earliest")`
- D) Delete the `checkpointLocation` directory and set `.trigger(availableNow=True)`

**Correct Answer:** **B**

> **Explanation:**  
> When a Structured Streaming query reads from Kafka, it records the next expected offset per partition in its `checkpointLocation`. If Kafka's topic retention policy deletes messages before the stream reads them, Spark detects data loss and raises an exception by default (`failOnDataLoss: "true"`). Adding `.option("failOnDataLoss", "false")` instructs Spark to log a warning and proceed reading from the oldest available offset instead of crashing. `startingOffsets` (Option A) is only evaluated on initial stream startup without a checkpoint, and deleting the checkpoint (Option D) re-reads all available data from scratch, violating exactly-once stream semantics.

> **🎯 10-Second Elimination Rule:**  
> "Kafka offset out of range / expired retention error on stream restart" $\rightarrow$ `.option("failOnDataLoss", "false")`.

---

#### Question 2.1.2: Cross-Platform Sharing & UniForm Iceberg
A multinational enterprise uses Databricks for ETL and data engineering, while an external analytics team queries datasets using **Amazon Athena** and **Apache Trino**, which natively read Apache Iceberg. The data architecture team requires that a large 50-terabyte Delta table (`portfolio_catalog.bronze.transactions`) be queryable by the Athena/Trino engines with **zero data duplication** and without having to build and maintain a duplicate export pipeline.

Which Delta Lake feature and configuration should the engineer implement?

- A) Convert the Delta table to Parquet using `CONVERT TO DELTA ... NO REWRITE` and point Trino directly to the raw Parquet directory.
- B) Enable Universal Format (UniForm) on the Delta table by setting the table property `'delta.universalFormat.enabledFormats' = 'iceberg'`.
- C) Create a Lakeflow Job that executes a daily `INSERT INTO iceberg_table SELECT * FROM delta_table`.
- D) Export the Delta transaction log using `DESCRIBE HISTORY` and use an AWS Lambda function to generate Iceberg snapshots.

**Correct Answer:** **B**

> **Explanation:**  
> Delta Universal Format (UniForm) automatically generates Apache Iceberg (and Hudi) metadata files alongside Delta Lake transaction logs without rewriting or copying the underlying Parquet data files. External query engines like Amazon Athena, Snowflake, and Apache Trino can read the Iceberg metadata directly while Databricks workloads continue querying Delta Lake, achieving 100% shared storage with zero data duplication.

> **🎯 10-Second Elimination Rule:**  
> "Read Delta tables from Athena / Trino as Iceberg without duplicating data or pipelines" $\rightarrow$ Universal Format (UniForm) via `'delta.universalFormat.enabledFormats' = 'iceberg'`.

---

### Subpoint 2.2: Incremental CDC Pipelines into Delta or Iceberg Targets

#### Question 2.2.1: CDC Pipeline Out-of-Order Delivery & Tombstone Deletes
A data engineer is designing a Lakeflow Declarative Pipeline in Databricks to synchronize an operational inventory database with a Unity Catalog Silver table (`silver.dim_inventory`). 

Due to network latency between regions, CDC events frequently arrive out of chronological order. In addition, when an item is permanently discontinued in the operational database, a tombstone record is emitted with `operation = 'DELETE'`.

Which Lakeflow SQL statement correctly updates `silver.dim_inventory` as an **SCD Type 1** table, ensuring late-arriving older events do not overwrite newer inventory balances and deleted items are removed?

- A)
  ```sql
  CREATE FLOW inventory_cdc_flow
  AS AUTO CDC INTO portfolio_catalog.silver.dim_inventory
  FROM STREAM(portfolio_catalog.bronze.inventory_cdc_raw)
  KEYS (item_id)
  APPLY AS DELETE WHEN operation = 'DELETE'
  SEQUENCE BY event_timestamp
  STORED AS SCD TYPE 1;
  ```
- B)
  ```sql
  APPLY CHANGES INTO portfolio_catalog.silver.dim_inventory
  FROM STREAM(portfolio_catalog.bronze.inventory_cdc_raw)
  KEYS (item_id)
  IGNORE DELETES
  SEQUENCE BY event_timestamp;
  ```
- C)
  ```sql
  CREATE OR REFRESH STREAMING TABLE dim_inventory AS
  SELECT * FROM STREAM(portfolio_catalog.bronze.inventory_cdc_raw)
  WHERE operation != 'DELETE'
  CLUSTER BY (item_id);
  ```
- D)
  ```sql
  CREATE FLOW inventory_cdc_flow
  AS AUTO CDC INTO portfolio_catalog.silver.dim_inventory
  FROM STREAM(portfolio_catalog.bronze.inventory_cdc_raw)
  KEYS (item_id)
  APPLY AS TRUNCATE WHEN operation = 'DELETE'
  STORED AS SCD TYPE 2;
  ```

**Correct Answer:** **A**

> **Explanation:**  
> In Lakeflow Declarative Pipelines, the `CREATE FLOW ... AS AUTO CDC INTO` API merges CDC change events into a target table. Specifying `KEYS (item_id)` defines the primary identity, `APPLY AS DELETE WHEN operation = 'DELETE'` removes records carrying delete tombstones, and `SEQUENCE BY event_timestamp` prevents late-arriving older events from overwriting newer commits. Option B uses deprecated `APPLY CHANGES INTO` syntax, Option C uses a regular streaming table which cannot apply row-level updates/deletes, and Option D specifies `APPLY AS TRUNCATE` with SCD Type 2 (which is invalid).

> **🎯 10-Second Elimination Rule:**  
> Modern CDC syntax requires `CREATE FLOW ... AS AUTO CDC INTO`. Eliminate `APPLY CHANGES INTO` (Option B) and eliminate `APPLY AS TRUNCATE` on SCD Type 2 (Option D).

---

#### Question 2.2.2: CDC Target Optimization & Write Amplification
A high-volume CDC pipeline ingests 20 million updates and deletes per day into a 10-terabyte Delta table in Unity Catalog. The operations team notices severe write amplification and high cloud storage I/O costs because even single-row updates are causing entire 100MB Parquet data files to be rewritten to disk.

Which Delta Lake table feature should the engineer verify is enabled on the target table to eliminate the need to rewrite entire Parquet files on row-level updates and deletes?

- A) Predictive Optimization with `OPTIMIZE ... ZORDER BY`
- B) Deletion Vectors (`delta.enableDeletionVectors = true`)
- C) Auto-compaction with `delta.autoOptimize.optimizeWrite = true`
- D) Change Data Feed (`delta.enableChangeDataFeed = true`)

**Correct Answer:** **B**

> **Explanation:**  
> Deletion Vectors (`delta.enableDeletionVectors = true`) decouple row deletion and updates from file rewriting. When rows are modified or deleted, Delta writes a small bitmap file tracking the deleted row offsets instead of rewriting the entire Parquet file, drastically reducing write amplification, latency, and cloud storage costs. Option A mentions `ZORDER BY` (deprecated in favor of Liquid Clustering), Option C deals with file sizes on initial write, and Option D exposes row-level changes for downstream consumers.

> **🎯 10-Second Elimination Rule:**  
> "Cure write amplification on updates and deletes" $\rightarrow$ Deletion Vectors (`delta.enableDeletionVectors = true`).

---

### Subpoint 2.3: Relational Ingestion with Lakeflow Connect

#### Question 2.3.1: Relational CDC Ingestion Architecture
A data engineering team needs to ingest ongoing transactional changes from an operational **PostgreSQL** database containing 50 million historical rows into Unity Catalog. The solution must:
1. Capture historical data on initial launch without running a manual database export.
2. Ingest ongoing `INSERT`, `UPDATE`, and `DELETE` transactions in near real-time by reading the database Write-Ahead Log (WAL).
3. Automatically adapt to upstream schema changes (such as added or altered columns) without failing the ingestion pipeline.
4. Minimize infrastructure maintenance by avoiding custom Debezium containers, Kafka clusters, or JDBC polling scripts.

Which Databricks capability should the data engineer implement?

- A) A scheduled Databricks Job running PySpark with `format("jdbc")` and `dbtable = "(SELECT * FROM orders WHERE updated_at > :last_sync)"`.
- B) A self-hosted Debezium connector on an EC2 instance publishing WAL events to Amazon Kinesis, ingested via Auto Loader.
- C) **Lakeflow Connect** configured with a Unity Catalog PostgreSQL connection object, an ingestion gateway, and a Lakeflow Declarative Pipeline.
- D) Direct external table creation using `CREATE EXTERNAL TABLE ... OPTIONS (dbtable "orders")`.

**Correct Answer:** **C**

> **Explanation:**  
> Lakeflow Connect is Databricks' native managed CDC ingestion service. It connects directly to relational databases (PostgreSQL, MySQL, SQL Server, Oracle), takes an initial baseline snapshot, continuously streams WAL/binlog mutations, and handles schema evolution automatically without requiring external Kafka or Debezium infrastructure. JDBC polling (Option A) misses hard deletes, and self-hosted Debezium (Option B) incurs unnecessary management overhead.

> **🎯 10-Second Elimination Rule:**  
> "Native zero-code relational database CDC ingestion with initial snapshot + WAL stream" $\rightarrow$ **Lakeflow Connect**.

---

#### Question 2.3.2: Lakeflow Connect Credential Governance
A cloud security architect is establishing policies for connecting Lakeflow Connect to an on-premises **Microsoft SQL Server** instance for CDC replication. Company compliance strictly forbids embedding database credentials in YAML pipeline files, Git repositories, or hardcoded notebook cells.

What is the recommended Databricks architecture to securely manage authentication for Lakeflow Connect?

- A) Store database username and password as base64-encoded strings inside the Declarative Automation Bundle's `databricks.yml` target variables.
- B) Create a Unity Catalog **Connection** object (`CREATE CONNECTION ...`) that references database credentials stored securely in a **Databricks Secret Scope**, and grant `USE CONNECTION` to the pipeline identity.
- C) Pass the SQL Server administrator credentials as dynamic Spark session parameters (`spark.conf.set("spark.sql.connection.password", ...)`) at the beginning of each pipeline update.
- D) Embed the password in a cluster init script that writes credentials directly to `/etc/environment` on the driver node.

**Correct Answer:** **B**

> **Explanation:**  
> In Unity Catalog, external database authentication is governed via securable **Connection** objects (`CREATE CONNECTION ...`) that retrieve secrets from Databricks Secret Scopes (`secret('scope', 'key')`). This decouples credentials from code, ensures encryption at rest and in transit, and allows administrators to control access using Unity Catalog ACLs (`GRANT USE CONNECTION`). Storing credentials in YAML (Option A), Spark session configs (Option C), or cluster init scripts (Option D) violate enterprise security policies.

> **🎯 10-Second Elimination Rule:**  
> "Secure external database credentials in Unity Catalog" $\rightarrow$ Unity Catalog **Connection** (`CREATE CONNECTION`) + **Databricks Secret Scope**.

---

### Subpoint 2.4: OpenSharing & Clean Rooms

#### Question 2.4.1: OpenSharing D2D vs. D2O Credential Governance
A data sharing administrator at a pharmaceutical company needs to share a governed Delta table `portfolio_catalog.clinical.trial_results` with two external partners:
1. **Partner A:** Uses Databricks with Unity Catalog enabled.
2. **Partner B:** Uses an external Python data science environment running on-premises that queries data using the open-source Delta Sharing client.

Which procedure correctly provisions access for each partner while adhering to Databricks security best practices?

- A) Send an encrypted `.share` credential file containing an activation URL via corporate email to both Partner A and Partner B.
- B) Provision Partner A using Databricks-to-Databricks (D2D) by creating a recipient with their Unity Catalog **Metastore / Sharing ID** (no credential file needed); provision Partner B using Databricks-to-Open (D2O) by generating an activation URL to download a **bearer token credential profile JSON file**.
- C) Grant Partner A and Partner B direct IAM `s3:GetObject` permissions to the underlying Delta table S3 bucket.
- D) Configure a Lakehouse Federation foreign catalog in Partner A and Partner B's environments pointing to the pharma company's JDBC endpoint.

**Correct Answer:** **B**

> **Explanation:**  
> In OpenSharing:
> - Databricks-to-Databricks (D2D) connects two Unity Catalog metastores directly via the recipient's Sharing Identifier (`USING ID '<metastore_id>'`). No credential files, bearer tokens, or activation URLs exist for D2D.
> - Databricks-to-Open (D2O) provisions external, non-Databricks clients via an activation URL from which the recipient downloads a credential profile JSON file containing a temporary bearer token and HTTPS endpoint.

> **🎯 10-Second Elimination Rule:**  
> "Partner has Databricks UC" $\rightarrow$ D2D with Sharing ID (no credential files). "Partner has non-Databricks client" $\rightarrow$ D2O with bearer token file.

---

#### Question 2.4.2: Multi-Party Collaboration & Privacy Preservation
A retail bank and a streaming media provider want to collaborate to identify consumer overlap and evaluate joint co-branded subscription promotions. 

Compliance and data privacy regulations strictly mandate:
1. Neither company is allowed to see the other company's individual customer identities, email addresses, or raw transaction rows.
2. The collaboration must only produce aggregated analytical results (e.g., total count of overlapping subscribers, average subscription tenure).
3. The underlying data must remain in each company's respective cloud storage account without copying raw data to an external third-party server.

Which Databricks feature should the data architect implement to fulfill this requirement?

- A) OpenSharing (D2O) with a 24-hour bearer token expiration window.
- B) **Databricks Clean Rooms** configured with approved aggregation query templates and differential privacy controls.
- C) A shared Unity Catalog Dynamic View configured with `MASK` functions on the email column.
- D) Lakeflow Connect with a unidirectional snapshot sync to a shared AWS S3 bucket.

**Correct Answer:** **B**

> **Explanation:**  
> Databricks Clean Rooms provides a privacy-preserving environment where multiple participants can analyze joint data assets without sharing raw data with one another. Clean Rooms enforces strict governance: raw rows cannot be queried directly or exported, participants can only run approved aggregation queries, and differential privacy guarantees prevent re-identification of individuals. Standard OpenSharing (Option A) and Dynamic Views (Option C) provide access to table rows, violating the strict privacy mandate.

> **🎯 10-Second Elimination Rule:**  
> "Collaborate on joint data without either party seeing raw rows/PII" $\rightarrow$ **Databricks Clean Rooms**.



