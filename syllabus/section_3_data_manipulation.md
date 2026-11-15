# Section 3: Data Manipulation (12%)

> **Official Weight:** 12% of total exam score  
> **Key Exam Standard:** Advanced SQL Transformations, `VARIANT` Semi-Structured Data, AI Functions (`ai_query`), Lakeflow Expectations & Quarantine, Change Data Feed (CDF)  
> **Official Docs:**  
> - [Window Functions (`ROWS` vs `RANGE`, `QUALIFY`)](https://docs.databricks.com/en/sql/language-manual/sql-ref-window-functions.html)  
> - [VARIANT Type & Functions](https://docs.databricks.com/en/sql/language-manual/data-types/variant-type.html)  
> - [AI Functions on Databricks](https://docs.databricks.com/en/large-language-models/ai-functions.html)  
> - [Lakeflow Pipeline Expectations](https://docs.databricks.com/en/ldp/expectations.html)

---

## 1. Subpoint 3.1: Advanced Data Transformations (Windows, Joins, Aggregations)

### A. Window Functions & `QUALIFY`
- **`ROW_NUMBER()` vs. `RANK()` vs. `DENSE_RANK()`** on tied values `[100, 100, 80]`:
  - `ROW_NUMBER()`: `1, 2, 3`
  - `RANK()`: `1, 1, 3` *(leaves gap)*
  - `DENSE_RANK()`: `1, 1, 2` *(no gap)*
- **`QUALIFY` Clause:** Filters rows *after* window evaluation without needing a CTE/subquery. Window functions are **forbidden** in `WHERE` and `HAVING`.
- **Window Frame Trap (`ROWS` vs. Default `RANGE`):**
  - When `ORDER BY` is present inside `OVER (...)` with **no explicit frame**, Spark defaults to **`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`**, which lumps all rows sharing the same `ORDER BY` value together.
  - For sequential row-by-row running totals, explicitly specify **`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`** (plus a unique tie-breaker column in `ORDER BY`).

### B. Join Strategies & Broadcast Side Rules

| Join Type | Can Broadcast **LEFT** Table? | Can Broadcast **RIGHT** Table? | Why? |
| :--- | :--- | :--- | :--- |
| **`INNER JOIN`** | Yes | Yes | Only matched rows are emitted; no `NULL` emission decision needed. |
| **`LEFT OUTER` / `LEFT SEMI` / `LEFT ANTI`** | **NEVER** | **ONLY RIGHT** | Each executor needs the entire Right table locally to know if a Left row has zero matches across the whole dataset. |
| **`RIGHT OUTER`** | **ONLY LEFT** | **NEVER** | Each executor needs the entire Left table locally to know if a Right row has zero matches. |
| **`FULL OUTER`** | **NEVER** | **NEVER** | Both sides emit unmatched `NULL`s; Spark falls back to Sort-Merge Join or Shuffle Hash Join. |

### C. Stream-Stream Joins (Law 6)
- **Inner Join:** Watermarks and event-time range bounds are optional for compilation, recommended for state cleanup.
- **Outer Join (`leftOuter`, `rightOuter`, `fullOuter`):** Watermarks on **BOTH** streams **AND** an **event-time range condition** (`payment_ts BETWEEN order_ts AND order_ts + INTERVAL 1 HOUR`) are **mandatory** so Spark can emit unmatched `(row, NULL)` records and evict RocksDB state.

### D. `ROLLUP`, `CUBE`, and `GROUPING()`
- `ROLLUP(a, b)` → Hierarchical subtotals (`(a, b)`, `(a)`, `()`).
- `CUBE(a, b)` → All $2^N$ combinations (`(a, b)`, `(a)`, `(b)`, `()`).
- **`GROUPING(col)`:** Returns `1` for a generated subtotal `NULL` and `0` for a genuine source `NULL` (`CASE WHEN GROUPING(Region) = 1 THEN 'All Regions' ELSE Region END`). Never use `COALESCE(Region, 'All Regions')` if `Region` can have real `NULL`s.

---

## 2. Subpoint 3.2: Using the `VARIANT` Data Type

- **Ingesting JSON → `VARIANT`:** `PARSE_JSON(str)` throws on malformed JSON; **`TRY_PARSE_JSON(str)`** returns SQL `NULL` safely.
- **Extracting & Casting:**
  - `payload:customer.tier` (no cast) → returns **`VARIANT`**, not `STRING`.
  - Keys with dots/spaces require brackets: `payload:['http.status']`.
  - `VARIANT_GET(v, path, type)` / `::type` throws on bad casts (e.g., `"free"` → `DOUBLE`); **`TRY_VARIANT_GET(v, path, type)`** / `TRY_CAST` returns `NULL`.
- **JSON `null` vs. Missing Key Trap:**
  - `payload:discount IS NULL` → returns **`FALSE`** when `"discount": null` exists in JSON!
  - **`IS_VARIANT_NULL(payload:discount)`** → returns **`TRUE`** for explicit JSON `null`.
- **Flattening `VARIANT` Arrays/Objects:** Standard `EXPLODE()` throws `DATATYPE_MISMATCH`. Must use **`LATERAL VARIANT_EXPLODE(...)`** (or **`LATERAL VARIANT_EXPLODE_OUTER(...)`** to preserve rows with empty `[]` / `NULL` arrays).
- **Schema Drift Inspection:** `SCHEMA_OF_VARIANT(v)` (per row) vs. **`SCHEMA_OF_VARIANT_AGG(v)`** (aggregates across rows, widening conflicting types to `VARIANT`).

---

## 3. Subpoint 3.3: Built-In AI Functions & `ai_query`

### A. Task-Specific AI Functions (Zero Endpoint Setup)
- `ai_analyze_sentiment(text)` → `STRING` (`positive`, `negative`, `neutral`, `mixed`)
- `ai_classify(text, ARRAY('billing', 'bug'))` → `STRING`
- `ai_extract(text, ARRAY('person', 'invoice'))` → `STRUCT<person: STRING, invoice: STRING>` *(2nd arg MUST be `ARRAY<STRING>`, never a comma-separated string)*
- `ai_mask(text, ARRAY('person', 'email'))` → `STRING`
- `ai_summarize(text [, max_words])`, `ai_translate(text, lang)`, `ai_similarity(s1, s2)` (`FLOAT`), `ai_gen(prompt)`

### B. General-Purpose `ai_query()` (Mosaic AI Model Serving)
- Calls Foundation Models, External Models, or Custom MLflow endpoints (requires `CAN QUERY` on the endpoint).
- **Multi-Feature ML Models:** Package input columns in **`named_struct('col1', col1, 'col2', col2)`**.
- **Resilient Error Handling (`failOnError => false`):**
  - Default `failOnError => true` aborts the entire query on a single HTTP 429/500 error.
  - Setting **`failOnError => false`** wraps every row in **`STRUCT<result: <returnType>, errorMessage: STRING>`** so valid rows use `ai_out.result` and failed rows route to quarantine via `ai_out.errorMessage IS NOT NULL`.

---

## 4. Subpoint 3.4: Lakeflow Expectations, Quarantine & Event Log

### A. The 3 Expectation Modes vs. Delta `CHECK` Constraints

| Action | Python (`pyspark.pipelines` / `dlt`) | SQL (Lakeflow Pipeline) | Behavior |
| :--- | :--- | :--- | :--- |
| **Retain / Warn** | `@dp.expect` / `@dp.expect_all(RULES)` | `CONSTRAINT name EXPECT (expr)` | Keeps invalid rows and logs pass/fail metrics. |
| **Drop Row** | `@dp.expect_or_drop` / `@dp.expect_all_or_drop(RULES)` | `CONSTRAINT name EXPECT (expr) ON VIOLATION DROP ROW` | Silently drops invalid rows and logs metrics. |
| **Fail Update** | `@dp.expect_or_fail` / `@dp.expect_all_or_fail(RULES)` | `CONSTRAINT name EXPECT (expr) ON VIOLATION FAIL UPDATE` | Halts pipeline immediately on first violation. |

- **Expectations vs. UC Table `CHECK` Constraints:** `ALTER TABLE tbl ADD CONSTRAINT chk CHECK (amount > 0)` applies to Unity Catalog Delta tables and **always fails** writes on violation (no retain or drop mode).

### B. The Quarantine Pattern (Zero Data Loss)
1. Attach **`@dp.expect_all(RULES)`** (retain mode—never `drop`!) on an intermediate `@dp.temporary_view` with a computed `is_quarantined` boolean column.
2. Split into `clean_events` (`WHERE NOT is_quarantined`) and `quarantine_events` (`WHERE is_quarantined`).

### C. Auditing Expectations in the Event Log
Query `event_log('<pipeline-id>')` filtered by **`WHERE event_type = 'flow_progress'`** and explode **`details:flow_progress.data_quality.expectations`** to read `(name, dataset, passed_records, failed_records)`.

---

## 5. Change Data Feed (CDF) Quick Reference
- Enable: `TBLPROPERTIES (delta.enableChangeDataFeed = true)`
- SQL read: `SELECT * FROM table_changes('catalog.schema.table', 0, 5)`
- PySpark read: `.option("readChangeFeed", "true")` *(10-second elimination: `readChangeFeed`, never `readChangeData` or `enableChangeFeed`)*
- `_change_type` values: `insert`, `update_preimage`, `update_postimage`, `delete`.
