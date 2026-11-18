# Section 5: Cost & Performance Optimization (15%)

> **Official Weight:** 15% of total exam score  
> **Official Subpoints Covered:** 5.1, 5.2, 5.3, 5.4, 5.5, 5.6  
> **Official Docs:**  
> - [Use Liquid Clustering for Delta Tables](https://docs.databricks.com/en/delta/clustering.html)  
> - [Predictive Optimization for UC Managed Tables](https://docs.databricks.com/en/optimizations/predictive-optimization.html)  
> - [Deletion Vectors](https://docs.databricks.com/en/delta/deletion-vectors.html)  
> - [Disk Cache & Serverless Limitations](https://docs.databricks.com/en/optimizations/disk-cache.html)  
> - [Delta Lake Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)  
> - [Query Profile](https://docs.databricks.com/en/sql/user/queries/query-profile.html)

---

## 1. Subpoints 5.1 & 5.2: Liquid Clustering, `CLUSTER BY AUTO`, Predictive Optimization & Deletion Vectors

| Technique | Problem Solved | Optimizes | Key Exam Rule |
| :--- | :--- | :--- | :--- |
| **Deletion Vectors** | Full Parquet file rewrites on small `UPDATE`/`DELETE`/`MERGE` | **Writes** | Merge-on-read `.bin` bitmaps; physically purged by `REORG TABLE ... APPLY (PURGE)` (or `OPTIMIZE`) + `VACUUM`. |
| **Liquid Clustering (`CLUSTER BY`)** | Slow reads on known filter keys, partition skew | **Reads** | Up to 4 keys; mutually exclusive with `PARTITION BY`; keys must be within `delta.dataSkippingNumIndexedCols` (default 32). |
| **`CLUSTER BY AUTO`** | Unknown or evolving query filter patterns | **Reads** | Requires Unity Catalog **Managed** table + **Predictive Optimization**. |
| **Predictive Optimization** | Manual `OPTIMIZE`, `VACUUM`, `ANALYZE` cron jobs | **Ops & Reads** | Runs **only** on UC **Managed** tables (never External tables or `hive_metastore`). |

---

## 2. Subpoint 5.3: Caching (`Disk Cache` vs. Spark `.cache()` vs. `Query Result Cache`)

| Cache Layer | What It Stores | Stale After `UPDATE`? | Supported on Serverless? |
| :--- | :--- | :--- | :--- |
| **Disk Cache** (formerly Delta Cache) | Immutable remote Parquet files on local NVMe SSD | **No** (new versions read new files) | **Yes** (managed automatically) |
| **Spark Cache** (`.cache()` / `.persist()` / `CACHE TABLE`) | Computed DataFrame in executor RAM/disk | **Yes** unless `.unpersist()` called | **No** (throws exception on Spark Connect) |
| **Query Result Cache** (SQL Warehouse) | Final SQL query output | **No** (invalidated on Delta version change) | **Yes** (0-DBU instant return on identical queries) |

---

## 3. Subpoint 5.4: Change Data Feed (CDF)
- Enable: `ALTER TABLE tbl SET TBLPROPERTIES (delta.enableChangeDataFeed = true);`
- Read in PySpark: `.option("readChangeFeed", "true")` *(never `readChangeData` or `enableChangeFeed`)*.
- Read in SQL: `SELECT * FROM table_changes('tbl', start_ver, end_ver)`.
- **Deduplicating before `MERGE INTO`:** Filter `WHERE _change_type != 'update_preimage'` and keep only the latest commit per key (`QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _commit_version DESC) = 1`) to avoid `[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE]`.
- **`VACUUM` interaction:** `VACUUM` purges old `_change_data` files past the retention window.

---

## 4. Subpoint 5.5: Query Profile Bottlenecks
- **Data Skipping Inefficiencies (`Scan`):** `Files pruned: 0` when wrapping a clustered column in a function (`YEAR(event_ts) = 2026` or `DATE(event_ts)`) or when column index >= 32. Rewrite as sargable range (`event_ts >= '...' AND event_ts < '...'`).
- **Join Strategies (`Join`):** `BroadcastHashJoin` (fastest, 0 shuffle on large side) vs. `SortMergeJoin` (2 `Exchange` shuffles) vs. **`BroadcastNestedLoopJoin`** ($O(N \times M)$ caused by non-equi joins without an `=` key or `OR` conditions).
- **Shuffle (`Exchange`):** High shuffle bytes / `Spill (Disk)` mitigated by broadcasting small tables, pre-filtering/projecting before joins, and AQE Skew Join.

---

## 5. Subpoint 5.6: Liquid Clustering vs. `PARTITION BY` / `ZORDER`
- Use **Liquid Clustering** for almost all tables (< 1 TB, skewed data, high-cardinality keys, multi-column queries, evolving filters).
- `ALTER TABLE tbl CLUSTER BY (new_cols)` updates metadata **without** rewriting historical files; new writes and future incremental `OPTIMIZE` runs use the new keys (use `OPTIMIZE tbl FULL` only if historical files must be re-clustered immediately).
- `PARTITION BY` is reserved only for rare massive multi-TB tables where every partition is >= 1 TB and queries/deletes align strictly to one low-cardinality partition.
