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
