# Section 1: Developing Code for Data Processing (23%)

> **Official Weight:** 23% of total exam score (Largest single domain)  
> **Key Exam Standard:** Post-October 2026 syllabus (Serverless compute, Lakeflow Pipelines, AUTO CDC, Spark Connect)

---

## 1. Declarative Pipelines (Lakeflow Pipelines / DLT)

### Core Architectural Principle
Lakeflow Pipelines allow engineers to declare **what** datasets exist (`@dlt.table`, `CREATE STREAMING TABLE`) instead of writing imperative streaming loops.

```
Raw Cloud Files / Kafka 
      ↓ (Auto Loader)
Bronze Streaming Table (`parse_json` → VARIANT)
      ↓ (Expectations / AUTO CDC)
Silver Clean / Quarantine Tables
      ↓ (Periodic Aggregate Refresh)
Gold Materialized Views
```

### Essential Exam Rules
1. **Never manual checkpoints:** Never write `checkpointLocation` or `cloudFiles.schemaLocation` inside Lakeflow Pipelines. The runtime manages checkpoint state internally.
2. **Streaming Table vs. Materialized View:**
   - **Streaming Table:** For append-only data ingestion where records arrive continuously (low-latency, stateful ingestion).
   - **Materialized View:** For queries with aggregations, window functions, and joins that benefit from scheduled periodic recomputation of affected data windows.

---

## 2. Change Data Capture (AUTO CDC & SCD Types)

### What is AUTO CDC?
Replaces legacy `dlt.apply_changes()` with clean SQL/Python syntax:

```sql
CREATE FLOW customer_scd2_flow
AS AUTO CDC INTO silver_customers
FROM STREAM(bronze_cdc_stream)
KEYS (customer_id)
APPLY AS DELETE WHEN op_code = 'D'
SEQUENCE BY event_timestamp
STORED AS SCD TYPE 2;
```

### SCD Type 1 vs SCD Type 2
- **SCD Type 1:** Overwrites existing row with latest values. No historical version tracking.
- **SCD Type 2:** Preserves historical audit trail. Lakeflow automatically appends `__start_at` and `__end_at` timestamp columns and manages active record flags.
- **Out-of-Order Handling:** `SEQUENCE BY <timestamp_col>` guarantees that older updates arriving late are never allowed to overwrite newer commits.

---

## 3. Streaming Joins & Watermarks

### Stream-Stream Inner Join
- Watermarks and time constraints are **optional for correctness**, but **mandatory for memory cleanup** (preventing cluster OOM).

### Stream-Stream Outer Join (Left / Right / Full Outer)
- **Rule 1:** Watermarks **MUST** be defined on **BOTH** streams:
  ```python
  streamA.withWatermark("timestampA", "1 hour")
  streamB.withWatermark("timestampB", "1 hour")
  ```
- **Rule 2:** An **event-time range condition** (`BETWEEN` or `<=` / `>=`) is **mandatory** in the join expression:
  ```python
  join_condition = """
      streamA.key = streamB.key AND
      streamA.timestampA BETWEEN streamB.timestampB AND streamB.timestampB + INTERVAL 2 HOURS
  """
  ```

---

## 4. Idempotency in `foreachBatch`

When writing streaming micro-batches to Delta Lake using `.foreachBatch(process_batch)`:
- Network retries or driver failures can cause a batch to be executed twice.
- **To guarantee exactly-once idempotent commits:**
  ```python
  def process_batch(microBatchDF, batchId):
      (
          microBatchDF.write
          .format("delta")
          .mode("append")
          .option("txnAppId", "unique_pipeline_id")
          .option("txnVersion", batchId)
          .saveAsTable("portfolio_catalog.silver.clean_events")
      )
  ```
  Delta's ACID transaction log stores `(txnAppId, txnVersion)`. If that batch ID arrives again on a retry, Delta skips writing duplicate rows.

---

## 5. Serverless Compute Caching Rules

- On Databricks Serverless Compute / Spark Connect:
  - Calling `df.cache()`, `df.persist()`, or `CACHE TABLE` throws an `AnalysisException` / `UnsupportedOperationException`.
  - Serverless compute automatically leverages managed local high-speed NVMe SSD disk caching (Delta Cache).
