# Section 3: Data Manipulation & Quality (12%)

> **Official Weight:** 12% of total exam score  
> **Key Exam Standard:** Semi-structured VARIANT data, Lakeflow Expectations, Quarantine patterns, AI Functions

---

## 1. Handling Bad Data: The 3 Lakeflow Expectation Actions vs. Quarantine

In production lakehouses, upstream files arrive with missing IDs, negative prices, null dates, or malformed JSON.
Databricks Lakeflow provides built-in **Expectations** to enforce data quality at the streaming level.

### The 3 Native Actions (Single-Dataset Constraints)

| Action | Lakeflow Syntax | What Happens to Bad Rows | When to Use | Exam Tradeoff |
|---|---|---|---|---|
| **Warn / Retain** | `@dlt.expect("name", "expr")` | Kept in the target table. Failure is logged to pipeline event log. | Low-severity anomalies (e.g. outlier prices) | Table contains dirty data, but data flows uninterrupted. |
| **Drop Row** | `@dlt.expect_or_drop("name", "expr")` | Silently dropped from target table. | Telemetry logs, ad clicks where losing noisy data is fine | Bad records are permanently lost; no audit record exists. |
| **Fail Update** | `@dlt.expect_or_fail("name", "expr")` | Entire pipeline stops immediately with error. | Fatal regulatory or schema errors | High operational overhead; requires engineer to fix manually. |

---

### The Quarantine Pattern (The Production Standard)

In critical business domains (banking, e-commerce, healthcare), **you cannot drop data** (audit violation) and **you cannot crash the pipeline** (halts business operations).
Instead, engineers use the **Quarantine Pattern**:

```
                  ┌── [Valid rows] ─────────► Silver Clean Table
Bronze Events ───┤
                  └── [Invalid rows + tag] ──► Silver Quarantine Table
```

#### How It Works:
1. **Validation Tagging:** Each row is evaluated against quality rules and tagged with a boolean `_is_valid` flag and a reason string `_quarantine_reason` (e.g. `NULL_EVENT_TYPE`, `MISSING_PAYMENT_AMOUNT`).
2. **Stream Splitting:**
   - Rows where `_is_valid = True` are streamed into `portfolio_catalog.silver.clean_events`.
   - Rows where `_is_valid = False` are streamed into `portfolio_catalog.silver.quarantine_events`.
3. **Remediation Loop:** Data stewards query the quarantine table, fix upstream webhook bugs, and replay the quarantined records.

---

## 2. Change Data Feed (CDF): What It Is & How It Works

> **Official Docs:** [Use Delta Lake Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)

### What Problem Does CDF Solve?
By default, when you read a Delta table, you read the **current state** of the rows.
If an upstream process runs an `UPDATE` or `DELETE`, a downstream batch job cannot easily tell *which* specific rows changed or what the old values were without doing an expensive full-table diff.

**Change Data Feed (CDF)** records row-level changes (inserts, updates, deletes) alongside their metadata directly in the Delta transaction log!

### Enabling CDF on a Table
```sql
-- During table creation:
CREATE TABLE portfolio_catalog.silver.clean_events (...)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Or on an existing table:
ALTER TABLE portfolio_catalog.silver.clean_events
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### The 4 Change Types Produced by CDF
When you query CDF, Delta automatically outputs 4 special metadata columns:
1. `_change_type`:
   - `'insert'`: Newly inserted row.
   - `'update_preimage'`: The row's values **before** the update.
   - `'update_postimage'`: The row's values **after** the update.
   - `'delete'`: The row that was deleted.
2. `_commit_version`: The Delta commit version that made the change.
3. `_commit_timestamp`: Exact timestamp of the commit.

### How to Read CDF (The Critical Exam Syntax)

#### In SQL:
```sql
-- Read changes between commit version 0 and 5:
SELECT * FROM table_changes('portfolio_catalog.silver.clean_events', 0, 5);

-- Read changes starting from a specific timestamp:
SELECT * FROM table_changes('portfolio_catalog.silver.clean_events', '2026-10-18 00:00:00');
```

#### In Python (Batch or Streaming):
```python
# Streaming or Batch read of Delta Change Data Feed
changes_df = (
    spark.readStream.format("delta")  # or spark.read
    .option("readChangeFeed", "true")  # <--- THE OFFICIAL EXAM KEYWORD
    .option("startingVersion", 0)  # or startingTimestamp
    .table("portfolio_catalog.silver.clean_events")
)
```

> **🎯 10-Second Elimination Rule for CDF:**  
> The official PySpark option to read Change Data Feed is **`readChangeFeed`** (case-sensitive).  
> Any option like `readChangeData`, `enableChangeFeed`, or `cdcMode` is an exam distractor!
