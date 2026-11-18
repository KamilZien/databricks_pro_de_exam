# Section 5: Cost & Performance Optimization — Exam Question Bank (15%)

> **Progress:** Subpoints 5.1–5.6 Complete (10/10 — 100%) ✅

---

## Subpoint 5.1: Liquid Clustering, Predictive Optimization & Delta File Layout

### Question 5.1.1 — Liquid Clustering Evolution & Predictive Optimization Scope
A FinOps team manages a **Unity Catalog External** table (`portfolio_catalog.silver.legacy_billing`) that was originally created with `PARTITION BY (event_date)` and `LOCATION 's3://corp-lake/legacy_billing'`. They want to:
1. Switch to **Liquid Clustering** on `(tenant_id, event_date)` and let Databricks automatically adapt clustering keys in the future (`CLUSTER BY AUTO`).
2. Enable **Predictive Optimization** at the catalog level so Databricks automatically runs `OPTIMIZE`, `VACUUM`, and `ANALYZE`.

Which statement is true about migrating this table?

- **A.** Running `ALTER TABLE portfolio_catalog.silver.legacy_billing CLUSTER BY (tenant_id, event_date)` immediately converts the partitioned External table into a Liquid Clustered table, and Predictive Optimization will automatically maintain it.
- **B.** Predictive Optimization automatically runs `OPTIMIZE` and `VACUUM` on both External and Managed UC tables once enabled at the catalog level, even if `PARTITION BY` is kept.
- **C.** `PARTITION BY` and `CLUSTER BY` are mutually exclusive (so the table must be recreated without `PARTITION BY`, e.g. `CREATE OR REPLACE TABLE ... CLUSTER BY (tenant_id, event_date)` or `CLUSTER BY AUTO`), **AND** Predictive Optimization only runs on **Unity Catalog Managed** tables (not External tables).
- **D.** Liquid Clustering requires `ZORDER BY (tenant_id, event_date)` to be run manually after every `ALTER TABLE ... CLUSTER BY` change.

- **User Answer:** **C** ✅
- **Official Rationale:** `PARTITION BY` and `CLUSTER BY` cannot coexist on the same Delta table, and Predictive Optimization (`OPTIMIZE`, `VACUUM`, `ANALYZE`) only supports Unity Catalog Managed tables.

---

### Question 5.1.2 — GDPR Physical Purge with Deletion Vectors Enabled
A European customer submits a GDPR erasure request. The target table `portfolio_catalog.silver.customers` is a Unity Catalog Managed table with **Deletion Vectors** enabled by default. The engineer executes:
```sql
DELETE FROM portfolio_catalog.silver.customers WHERE customer_id = 'CUST-99';
```
What additional steps are required to **physically remove** the customer's PII bytes from cloud object storage?

- **A.** Run `VACUUM portfolio_catalog.silver.customers RETAIN 0 HOURS` immediately after `DELETE`.
- **B.** Disable Deletion Vectors (`ALTER TABLE ... SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)`) and run `ANALYZE TABLE`.
- **C.** Run `OPTIMIZE ... ZORDER BY (customer_id)` followed by `REFRESH TABLE`.
- **D.** Run **`REORG TABLE portfolio_catalog.silver.customers APPLY (PURGE)`** to physically rewrite Parquet files without the soft-deleted rows, followed by **`VACUUM portfolio_catalog.silver.customers RETAIN <hours> HOURS`** to remove unreferenced old files.

- **User Answer:** **D** ✅
- **Official Rationale:** With Deletion Vectors enabled, `DELETE` writes a soft bitmap without rewriting Parquet files. `REORG TABLE ... APPLY (PURGE)` rewrites the files without deleted rows, and `VACUUM` removes the old unreferenced files from cloud storage.

---

## Subpoint 5.3: Caching (`Disk Cache` vs. Spark `.cache()` vs. `Query Result Cache`)

### Question 5.3.1 — Serverless Caching Failure & Remediation
A data engineer migrates a PySpark ETL notebook to **Databricks Serverless Compute**. Inside the notebook, an expensive multi-table join DataFrame `enriched_df` is reused to write two separate Gold tables (`gold_daily` and `gold_monthly`). When the engineer runs `enriched_df.cache()`, the notebook throws an unsupported-operation exception.

Why did `.cache()` fail, and what is the recommended Serverless alternative?

- **A.** Serverless runs on Spark Connect with shared/managed compute where manual `.cache()`, `.persist()`, and `CACHE TABLE` are unsupported; rely on the automatic **disk cache** for repeated Delta table scans or **write `enriched_df` to a temporary Delta table / Materialized View** if the expensive join must not be recomputed.
- **B.** Serverless only supports `df.persist(StorageLevel.DISK_ONLY)` instead of `df.cache()`.
- **C.** The engineer forgot to run `SET spark.databricks.io.cache.enabled = true` before calling `df.cache()`.
- **D.** Serverless compute only allows caching via SQL `CACHE TABLE enriched_view`.

- **User Answer:** **A** ✅
- **Official Rationale:** Serverless runs on Spark Connect with shared/managed compute, so manual `.cache()`, `.persist()`, and `CACHE TABLE` are unsupported. Use automatic disk cache for repeated file scans, or materialize expensive intermediate joins to a Delta table / Materialized View.

---

### Question 5.3.2 — Disk Cache vs. Spark `.cache()` Staleness
A Delta table `portfolio_catalog.silver.orders` is read repeatedly by analytics queries on a warehouse/compute cluster with **Disk Cache** enabled. While analysts are querying the table, an upstream streaming job executes a `MERGE INTO` that updates 500 existing rows.

How does the **Disk Cache** behave on the very next query after the `MERGE INTO` commits?

- **A.** It serves stale pre-merge rows until an administrator runs `UNCACHE TABLE portfolio_catalog.silver.orders`.
- **B.** It automatically re-parses the entire `orders` table from S3/ADLS because Disk Cache disables itself permanently after any `MERGE`.
- **C.** It caches the final SQL `SELECT` output rows, so any query with a different `WHERE` filter bypasses Disk Cache.
- **D.** It **never serves stale data**: Delta writes new immutable Parquet files for the updated rows and updates `_delta_log`, so the next query reads the newly committed files (caching them on local SSD) while still hitting local SSD for unchanged Parquet files.

- **User Answer:** **D** ✅
- **Official Rationale:** Disk Cache caches immutable Parquet files by filename. When `MERGE INTO` commits a new Delta version, `_delta_log` points to newly written files for changed rows, so Disk Cache never returns stale rows.

---

## Subpoint 5.4: Change Data Feed (CDF) for Incremental Downstream Processing

### Question 5.4.1 — CDF Table Property vs. PySpark Read Option
A data engineer enables Change Data Feed on `portfolio_catalog.silver.accounts` and wants to stream row-level inserts, updates, and deletes in PySpark starting from version `10`.

Which combination of table property and PySpark stream reader option is syntactically valid?

- **A.** `TBLPROPERTIES (delta.readChangeFeed = true)` and `.option("enableChangeDataFeed", "true")`
- **B.** `TBLPROPERTIES (delta.changeDataFeed = true)` and `.option("readChangeData", "true")`
- **C.** `TBLPROPERTIES (delta.enableChangeDataFeed = true)` and `.option("readChangeFeed", "true")`
- **D.** `TBLPROPERTIES (delta.enableChangeFeed = true)` and `.option("cdcMode", "true")`

- **User Answer:** **C** ✅
- **Official Rationale:** `delta.enableChangeDataFeed = true` enables CDF on the Delta table, while `.option("readChangeFeed", "true")` is the exact PySpark DataFrameReader/DataStreamReader option to read CDF.

---

### Question 5.4.2 — Deduplicating CDF Changes Before `MERGE INTO`
Between Delta versions `10` and `15` of `portfolio_catalog.silver.accounts`, account `A-42` was updated three times. A downstream Gold job reads `table_changes('portfolio_catalog.silver.accounts', 10, 15)` and runs `MERGE INTO portfolio_catalog.gold.accounts`. The job fails with `[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE]`.

How should the engineer transform the `table_changes` output before executing `MERGE INTO`?

- **A.** Filter `WHERE _change_type != 'update_preimage'` and deduplicate per `account_id` keeping the latest commit (`QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _commit_version DESC) = 1`).
- **B.** Filter `WHERE _change_type = 'update_preimage'` and run `VACUUM` before `MERGE INTO`.
- **C.** Enable `delta.enableDeletionVectors = false` so CDF only emits one row per account.
- **D.** Use `GROUP BY account_id` with `FIRST(tier)` without filtering `_change_type`.

- **User Answer:** **A** ✅
- **Official Rationale:** `MERGE INTO` fails if multiple source rows match a single target row. Filtering out `update_preimage` and deduplicating to the latest `_commit_version` per key ensures at most one change row per key enters `MERGE INTO`.

---

## Subpoint 5.5: Query Profile Bottlenecks (Data Skipping, Join Strategies, Shuffle)

### Question 5.5.1 — Data-Skipping Inefficiency in Query Profile
A FinOps table `portfolio_catalog.silver.usage_events` is Liquid Clustered on `event_ts` (`CLUSTER BY (event_ts)`) and freshly `OPTIMIZE`d. An analyst runs:
```sql
SELECT SUM(cost_usd)
FROM portfolio_catalog.silver.usage_events
WHERE DATE(event_ts) = '2026-11-15';
```
In the Query Profile `Scan` node, the analyst sees **`Files pruned: 0`** and all 8,000 Parquet files scanned. Why did Data Skipping fail, and how should the query be fixed?

- **A.** Delta Data Skipping does not support Liquid Clustered tables; the table must be recreated with `PARTITION BY (event_ts)`.
- **B.** `DATE(event_ts)` requires `df.cache()` to be called before Data Skipping activates.
- **C.** `event_ts` must be moved to the `GROUP BY` clause for Data Skipping to prune files.
- **D.** Wrapping `event_ts` in `DATE(event_ts)` prevents Delta min/max file statistics pruning; rewrite the filter as a direct range (`WHERE event_ts >= '2026-11-15 00:00:00' AND event_ts < '2026-11-16 00:00:00'`).

- **User Answer:** **D** ✅
- **Official Rationale:** Applying a function like `DATE(event_ts)` or `YEAR(event_ts)` makes the predicate non-sargable, preventing Delta min/max file statistics from pruning files.

---

### Question 5.5.2 — Identifying Join & Shuffle Bottlenecks in Query Profile
While inspecting a slow SQL query in the Databricks Query Profile, a data engineer sees two `Exchange` operators feeding a `SortMergeJoin` between a `2 TB` fact table and a `15 MB` dimension table (after filtering), and in another branch sees a `BroadcastNestedLoopJoin` on `ON f.event_ts BETWEEN d.valid_from AND d.valid_to`.

Which statement accurately explains both Query Profile observations?

- **A.** `Exchange` means data is being read from Disk Cache, and `BroadcastNestedLoopJoin` is the fastest $O(N)$ join strategy.
- **B.** `SortMergeJoin` never uses `Exchange` operators; only `BroadcastHashJoin` triggers `Exchange` shuffles on both tables.
- **C.** `Exchange` indicates a network shuffle across executors (avoidable on the 2 TB + 15 MB join via `BroadcastHashJoin`), and `BroadcastNestedLoopJoin` indicates an $O(N \times M)$ non-equi/Cartesian join caused by missing an equality key (`=`) in the join condition.
- **D.** `BroadcastNestedLoopJoin` occurs whenever `CLUSTER BY AUTO` is enabled on a Unity Catalog Managed table.

- **User Answer:** **C** ✅
- **Official Rationale:** `Exchange` represents a network shuffle across executors, and `BroadcastNestedLoopJoin` occurs on non-equi or `OR` join conditions without an equality key.

---

## Subpoint 5.6: Liquid Clustering vs. Partitioning / `ZORDER` by Table Size & Query Pattern

### Question 5.6.1 — Choosing Layout for a 400 GB Skewed Table
A 400 GB Delta table `portfolio_catalog.silver.orders` is currently defined with `PARTITION BY (country_code)` and `OPTIMIZE ... ZORDER BY (customer_id, order_date)`. `US` accounts for 85% of rows while 40 smaller countries have < 5 MB each, causing severe small-file overhead and slow writes.

How should the engineer redesign the table layout?

- **A.** Add `CLUSTER BY (customer_id, order_date)` while keeping `PARTITION BY (country_code)`.
- **B.** Recreate the table using **Liquid Clustering** (`CLUSTER BY (country_code, customer_id, order_date)` without `PARTITION BY`) and enable Predictive Optimization.
- **C.** Partition by `(country_code, customer_id)` instead so each customer gets a dedicated directory.
- **D.** Run `ZORDER BY (country_code, customer_id, order_date, amount, currency)` daily across 5 columns.

- **User Answer:** **B** ✅
- **Official Rationale:** `PARTITION BY` on skewed distributions creates small files in low-volume partitions (`< 5 MB`). Liquid Clustering eliminates directory skew and supports multi-column skipping across up to 4 keys.

---

### Question 5.6.2 — Evolving Access Patterns Across a 3 TB Table
A 3 TB Unity Catalog Managed table was originally clustered by `(region, event_date)`. Six months later, a new ML team begins querying the table primarily by `(account_id, service_tier)`. The team wants to adapt clustering with minimal immediate compute cost and zero downtime.

What happens when the engineer runs `ALTER TABLE portfolio_catalog.silver.billing CLUSTER BY (account_id, service_tier)`?

- **A.** Databricks immediately rewrites all 3 TB of historical Parquet files in a single blocking transaction.
- **B.** The command fails because clustering keys on a Delta table cannot be changed without `CREATE OR REPLACE TABLE`.
- **C.** It updates table metadata immediately without rewriting existing data; newly ingested data and future incremental `OPTIMIZE` / Predictive Optimization runs cluster using `(account_id, service_tier)`.
- **D.** It automatically converts the table into a `PARTITION BY (account_id, service_tier)` layout.

- **Correct Answer:** **C** ✅ (Verified via Repair Question `5.6.R2` = **C** ✅)
- **Official Rationale:** `ALTER TABLE ... CLUSTER BY (...)` is metadata-only and does not rewrite existing files. Historical files remain readable; new writes and incremental `OPTIMIZE` runs use the new keys (or `OPTIMIZE ... FULL` can re-cluster historical files if desired).
