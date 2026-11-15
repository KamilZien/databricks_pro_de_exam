# Section 3: Data Manipulation — Exam Question Bank (12%)

> **Progress:** Subpoints 3.1–3.4 Complete (8/8 — 100%) ✅

---

## Subpoint 3.1: Advanced Data Transformations (Window Functions, Joins, Aggregations)

### Question 3.1.1 — Window Framing (`ROWS BETWEEN` vs. Default `RANGE BETWEEN`) & `QUALIFY`
A financial data engineer writes the following Spark SQL query on a transaction table `portfolio_catalog.silver.payments` to compute a sequential row-by-row running total for each account and keep only the top 2 highest-value transactions per account:

```sql
SELECT
  account_id,
  txn_id,
  txn_timestamp,
  amount,
  SUM(amount) OVER (
    PARTITION BY account_id
    ORDER BY txn_timestamp
  ) AS running_total
FROM portfolio_catalog.silver.payments
WHERE DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) <= 2;
```

When tested in production, the query fails with a syntax/analysis error on the `WHERE` clause, and during unit testing of the `SUM(amount) OVER (...)` expression alone, transactions with identical `txn_timestamp` values show identical lumped `running_total` values instead of sequential row-by-row increments. Which TWO changes must the engineer make to fix both issues?

- **A.** Replace the `WHERE` clause with `HAVING DENSE_RANK() OVER (...) <= 2`, and add `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` to the `SUM(amount)` window specification.
- **B.** Replace the `WHERE` clause with `QUALIFY DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) <= 2`, and add `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (with `txn_id` as a deterministic tie-breaker in `ORDER BY txn_timestamp, txn_id`) to the `SUM(amount)` window specification.
- **C.** Keep the `WHERE` clause by calling `.cache()` on the table first, and replace `SUM(amount)` with `LAG(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_timestamp)`.
- **D.** Replace `DENSE_RANK()` in the `WHERE` clause with `GROUPING(account_id) <= 2`, and add `CLUSTER BY (txn_timestamp)` to the `SUM(amount)` window specification.

- **User Answer:** **B** ✅
- **Official Rationale:**
  1. Window functions cannot appear in `WHERE` or `HAVING`; Databricks SQL provides `QUALIFY` to filter on window function results after window evaluation.
  2. When `ORDER BY` is specified inside `OVER (...)` without an explicit frame, Spark defaults to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which sums all peer rows sharing the same `ORDER BY` value together. Specifying `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (plus a unique tie-breaker column in `ORDER BY`) forces physical row-by-row running totals.

---

### Question 3.1.2 — Broadcast Hash Join Rules on Outer Joins & Stream-Stream Joins
A data engineer is optimizing two pipelines in Databricks:
1. **Batch Pipeline:** Performs a `LEFT OUTER JOIN` where the left table `orders` is **2 TB** and the right lookup table `currencies` is **15 MB** (`orders LEFT OUTER JOIN currencies ON ...`). However, when they also run a second query performing a `LEFT OUTER JOIN` where the left table `small_vip_customers` is **5 MB** and the right table `all_clicks` is **5 TB**, they try to force a Broadcast Hash Join by adding `/*+ BROADCAST(small_vip_customers) */` on the left table.
2. **Streaming Pipeline:** Performs a Stream-Stream `LEFT OUTER JOIN` between `orders_stream` and `shipments_stream`.

Which statement accurately describes how Spark Catalyst handles these two scenarios?

- **A.** In a `LEFT OUTER JOIN`, Spark can broadcast EITHER the left table (`small_vip_customers`) or the right table (`currencies`), and in a Stream-Stream `LEFT OUTER JOIN`, watermarks are completely optional.
- **B.** In a `LEFT OUTER JOIN`, Spark can ONLY broadcast the **right** table (`currencies`), because each executor must stream its partition of the outer-preserved left side while probing the full broadcasted right lookup map (so `/*+ BROADCAST(small_vip_customers) */` on the left side of a `LEFT OUTER JOIN` cannot be used for a Broadcast Hash Join); and in the Stream-Stream `LEFT OUTER JOIN`, watermarks on **both** streams plus an **event-time range condition** (`BETWEEN ... AND ... + INTERVAL`) are mandatory.
- **C.** Broadcast Hash Joins are completely prohibited on all `LEFT OUTER` and `RIGHT OUTER` joins in Spark SQL; only `INNER` joins support `/*+ BROADCAST(...) */`.
- **D.** In the Stream-Stream `LEFT OUTER JOIN`, adding `/*+ BROADCAST(shipments_stream) */` eliminates the need for watermarks and event-time range conditions.

- **User Answer:** **B** ✅
- **Official Rationale:**
  1. In a Broadcast Hash Join (`BHJ`), one side is copied in full to every executor (`build side`), while the other side stays split across executors (`streamed side`). For a `LEFT OUTER JOIN`, the **left** side is the outer-preserved side (must emit `(left_row, NULL)` if never matched across the *entire* right table). If Spark broadcasted the left table (`small_vip_customers`) to 100 executors and kept the 5 TB `all_clicks` partitioned across those 100 executors, Executor 1 would only see 1% of `all_clicks` and would falsely emit `(vip_customer, NULL)` just because that customer had no clicks in Partition 1! Therefore:
     - `LEFT OUTER` / `LEFT SEMI` / `LEFT ANTI` can **only broadcast the RIGHT table**.
     - `RIGHT OUTER` can **only broadcast the LEFT table**.
     - `FULL OUTER` **cannot use Broadcast Hash Join at all** (must use Sort-Merge Join or Shuffle Hash Join).
  2. For any Stream-Stream Outer Join (`leftOuter`, `rightOuter`, `fullOuter`), watermarks on **both** streams AND an **event-time range condition** (`BETWEEN ... AND ... + INTERVAL`) are mandatory so RocksDB knows when the match window expires and can emit `(row, NULL)` and evict state.

---

## Subpoint 3.2: Using the `VARIANT` Data Type for Semi-Structured Data

### Question 3.2.1 — `TRY_PARSE_JSON`, `TRY_VARIANT_GET`, and Bracket Key Navigation
An e-commerce ingestion pipeline reads raw webhook strings from `portfolio_catalog.bronze.raw_webhooks` (column `raw_body STRING`). Upstream producers occasionally emit truncated/malformed JSON strings, and valid JSON records contain a nested field with a dot in its key name (`{"http.status": "200"}` in most rows, but occasionally `{"http.status": "OK"}`).

The data engineer wants to create a Silver query that:
1. Parses `raw_body` into a `VARIANT` column without failing when `raw_body` contains malformed JSON.
2. Extracts the `"http.status"` key as an `INT` without failing when the value is `"OK"`.

Which SQL snippet meets both requirements?

- **A.** `SELECT PARSE_JSON(raw_body) AS v, VARIANT_GET(PARSE_JSON(raw_body), '$.http.status', 'INT') AS status_code`
- **B.** `SELECT FROM_JSON(raw_body, 'VARIANT') AS v, TRY_PARSE_JSON(raw_body):http.status::INT AS status_code`
- **C.** `SELECT TRY_VARIANT_GET(raw_body, '$', 'VARIANT') AS v, PARSE_JSON(raw_body):['http.status']::INT AS status_code`
- **D.** `SELECT TRY_PARSE_JSON(raw_body) AS v, TRY_VARIANT_GET(TRY_PARSE_JSON(raw_body), '$.["http.status"]', 'INT') AS status_code` (or `TRY_CAST(TRY_PARSE_JSON(raw_body):['http.status'] AS INT)`)

- **Correct Answer:** **D** ✅ (Verified via Repair Question `3.2.R1` = **A** ✅)
- **Official Rationale:**
  1. `TRY_PARSE_JSON(raw_body)` converts a JSON `STRING` into `VARIANT`, returning `NULL` instead of failing on malformed JSON strings.
  2. `TRY_VARIANT_GET(v, path, type)` (or `TRY_CAST(v:['key'] AS type)`) safely extracts and casts the field from `VARIANT`, returning `NULL` if a row contains an incompatible type like `"OK"` for an `INT`.
  3. Keys containing dots (`"http.status"`) must be enclosed in brackets (`['http.status']` or `'$.["http.status"]'`) so they are not interpreted as nested object paths.

---

### Question 3.2.2 — `LATERAL VARIANT_EXPLODE_OUTER`, `IS_VARIANT_NULL`, and `SCHEMA_OF_VARIANT_AGG`
A table `portfolio_catalog.bronze.iot_events` contains a column `payload VARIANT`.
- Row 1 contains `{"device_id": "D1", "reading": null, "alerts": ["HIGH_TEMP", "LOW_BATTERY"]}`
- Row 2 contains `{"device_id": "D2", "reading": 98.6, "alerts": []}`
- Row 3 contains `{"device_id": "D3", "reading": "OFFLINE", "alerts": ["CONN_LOST"]}`

A data engineer needs to:
1. Identify rows where the `"reading"` key is explicitly present as a JSON `null` (without casting `payload:reading`).
2. Flatten the `payload:alerts` array into individual rows while **preserving** Row 2 (which has an empty `alerts` array `[]`) in the result set.
3. Compute a single merged schema string across all three rows in the table to inspect schema drift on the `"reading"` field.

Which combination of Databricks SQL functions accomplishes all three tasks?

- **A.** Use `IS_VARIANT_NULL(payload:reading)` to detect explicit JSON `null`, `LATERAL VARIANT_EXPLODE_OUTER(payload:alerts)` to flatten `alerts` while preserving empty/null arrays, and `SCHEMA_OF_VARIANT_AGG(payload)` (which resolves the drifted `reading` field to `VARIANT` in `OBJECT<alerts: ARRAY<STRING>, device_id: STRING, reading: VARIANT>`).
- **B.** Use `payload:reading IS NULL` to detect explicit JSON `null`, `LATERAL VIEW OUTER EXPLODE(payload:alerts)` to flatten `alerts`, and `SCHEMA_OF_VARIANT(payload)` to merge schemas across all rows.
- **C.** Use `IS_NULL_VALUE(payload:reading)` to detect explicit JSON `null`, `LATERAL VARIANT_EXPLODE(payload:alerts)` to preserve empty arrays, and `SCHEMA_OF_JSON_AGG(payload)` to merge schemas across all rows.
- **D.** Use `TRY_VARIANT_GET(payload, '$.reading', 'NULL')` to detect explicit JSON `null`, `UNNEST(payload:alerts)` to flatten `alerts`, and `DESCRIBE VARIANT payload` to merge schemas across all rows.

- **Correct Answer:** **A** ✅ (Verified via Repair Question `3.2.R2` = **C** ✅)
- **Official Rationale:**
  1. `IS_VARIANT_NULL(expr)` checks if an uncast `VARIANT` path holds an explicit JSON `null` (`payload:reading IS NULL` only checks if the path is missing).
  2. `LATERAL VARIANT_EXPLODE_OUTER(payload:alerts)` flattens a `VARIANT` array/object while preserving parent rows whose array is empty (`[]`) or `NULL`.
  3. `SCHEMA_OF_VARIANT_AGG(payload)` aggregates the `VARIANT` schema across all rows in the group/table, widening conflicting field types (`DOUBLE` vs `STRING`) to `VARIANT`.

---

## Subpoint 3.3: Leveraging Built-In AI Functions (`ai_query` & Task-Specific AI Functions)

### Question 3.3.1 — `ai_query(..., failOnError => false)` & Quarantine Routing
A data engineer builds a Lakeflow streaming pipeline that calls a Mosaic AI Model Serving endpoint `'databricks-meta-llama-3-3-70b-instruct'` on 500,000 incoming customer reviews using `ai_query(...)`. Occasionally, transient HTTP 429 rate-limit errors occur on individual rows, and the engineer wants the pipeline to continue running without failing while routing any failed inference rows to a retry/quarantine table.

```sql
SELECT
  review_id,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    review_text,
    returnType => 'STRING',
    failOnError => false
  ) AS ai_out
FROM STREAM(portfolio_catalog.bronze.reviews)
```

How must the engineer reference the model output and detect failed rows from `ai_out`?

- **A.** Read `ai_out` directly as a `STRING`, and route rows to quarantine where `ai_out IS NULL`.
- **B.** Read `ai_out.output` for the model text, and route rows to quarantine where `ai_out.status_code != 200`.
- **C.** Read `ai_out.result` for the model output, and route rows to quarantine where `ai_out.errorMessage IS NOT NULL`.
- **D.** Wrap `ai_query` inside `TRY_VARIANT_GET(ai_query(...), '$.result', 'STRING')` because `failOnError => false` returns a `VARIANT` column.

- **User Answer:** **C** ✅
- **Official Rationale:** Setting `failOnError => false` changes the return type of `ai_query` to `STRUCT<result: <returnType>, errorMessage: STRING>`. Successful rows populate `ai_out.result` with `ai_out.errorMessage IS NULL`, whereas failed rows set `ai_out.result` to `NULL` and populate `ai_out.errorMessage`.

---

### Question 3.3.2 — Task-Specific AI Functions vs. Custom ML Endpoints
A data engineer needs to perform two operations in Databricks SQL on `portfolio_catalog.silver.support_tickets`:
1. Redact customer names and email addresses from `ticket_body` without managing or deploying any custom Model Serving endpoint.
2. Score each ticket's churn risk (`DOUBLE`) using a custom Scikit-Learn/XGBoost model deployed at Mosaic AI Model Serving endpoint `'finops-churn-xgb'` that expects three named features: `monthly_spend`, `active_days`, and `support_calls`.

Which pair of SQL expressions correctly implements both operations?

- **A.** `ai_mask(ticket_body, 'person, email')` and `ai_classify('finops-churn-xgb', ARRAY(monthly_spend, active_days, support_calls))`
- **B.** `ai_extract(ticket_body, ARRAY('person', 'email'))` and `ai_query('finops-churn-xgb', ARRAY(monthly_spend, active_days, support_calls), returnType => 'DOUBLE')`
- **C.** `ai_gen(CONCAT('Mask PII: ', ticket_body))` and `ai_similarity('finops-churn-xgb', named_struct('monthly_spend', monthly_spend, 'active_days', active_days, 'support_calls', support_calls))`
- **D.** `ai_mask(ticket_body, ARRAY('person', 'email'))` and `ai_query('finops-churn-xgb', named_struct('monthly_spend', monthly_spend, 'active_days', active_days, 'support_calls', support_calls), returnType => 'DOUBLE')`

- **User Answer:** **D** ✅
- **Official Rationale:** `ai_mask(text, ARRAY('person', 'email'))` is a zero-config task-specific AI function that masks entities specified as an `ARRAY<STRING>`. Calling a multi-feature custom MLflow/XGBoost endpoint via `ai_query` requires packaging the feature columns into a `named_struct(...)`.

---

## Subpoint 3.4: Data Quality Expectations, Quarantine & Event Log Auditing

### Question 3.4.1 — Quarantine Pattern vs. Unity Catalog Table `CHECK` Constraints
A FinOps data engineering team must ingest streaming billing events into `portfolio_catalog.silver.clean_events`. Regulatory compliance requires that **zero invalid records** (e.g., negative `cost_usd` or `NULL` `event_id`) enter `clean_events`, **AND** every invalid record must be preserved in `portfolio_catalog.silver.quarantine_events` for auditing and replay without halting the pipeline.

Which Lakeflow Declarative Pipeline design meets all requirements?

- **A.** Define an intermediate `@dp.temporary_view` with **`@dp.expect_all(RULES)`** (retain mode) and a computed boolean `is_quarantined` column; then feed `silver.clean_events` (`WHERE NOT is_quarantined`) and `silver.quarantine_events` (`WHERE is_quarantined`) from that view.
- **B.** Add `ALTER TABLE portfolio_catalog.silver.clean_events ADD CONSTRAINT valid_cost CHECK (cost_usd >= 0)` directly on the target Delta table and expect Lakeflow to automatically route failing rows to `quarantine_events`.
- **C.** Attach **`@dp.expect_all_or_drop(RULES)`** on the intermediate `@dp.temporary_view`, and read from the same view in `quarantine_events` using `WHERE is_quarantined = true`.
- **D.** Attach **`@dp.expect_all_or_fail(RULES)`** on `silver.clean_events` and rely on `failOnError => false` to divert failing records into `quarantine_events`.

- **User Answer:** **A** ✅
- **Official Rationale:**
  - `@dp.expect_all(RULES)` (retain mode) logs pass/fail expectation metrics in the Lakeflow Event Log while keeping invalid rows in the temporary view so `quarantine_events` can capture them.
  - Using `@dp.expect_all_or_drop` on the staging view drops bad rows *before* `quarantine_events` can read them, and Unity Catalog `CHECK` constraints (`ALTER TABLE ADD CONSTRAINT`) always fail the entire write transaction.

---

### Question 3.4.2 — Querying Expectation Metrics in the Lakeflow Event Log
A data engineer wants to build an automated SQL audit query that tracks daily `passed_records` and `failed_records` counts for every data quality rule across a Lakeflow Declarative Pipeline (`pipeline_id = 'c5c9197b-6b9f-40ac-b10a-c0ef31b28abe'`).

Which query correctly extracts the per-expectation metrics from the pipeline's Event Log?

- **A.** `SELECT * FROM system.billing.usage WHERE pipeline_id = 'c5c9197b-...'`
- **B.** `SELECT details:dataset_progress.expectations FROM event_log('c5c9197b-...') WHERE event_type = 'dataset_progress'`
- **C.** Query `event_log('c5c9197b-...')` filtered by **`WHERE event_type = 'flow_progress'`**, and explode **`details:flow_progress.data_quality.expectations`** into individual `(name, dataset, passed_records, failed_records)` rows.
- **D.** Run `DESCRIBE HISTORY portfolio_catalog.silver.clean_events` and inspect the `operationMetrics.expectationFailures` key.

- **User Answer:** **C** ✅
- **Official Rationale:** Lakeflow Declarative Pipelines emit data quality expectation telemetry inside `event_log('<pipeline-id>')` rows with `event_type = 'flow_progress'` under `details:flow_progress.data_quality.expectations`.
