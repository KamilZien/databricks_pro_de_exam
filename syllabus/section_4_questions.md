# Section 4: Monitoring and Alerting — Exam Question Bank (10%)

> **Progress:** Subpoints 4.1–4.2 Complete (4/4 — 100%) ✅

---

## Subpoint 4.1: Monitoring Lakeflow Jobs, Spark Execution & System Tables

### Question 4.1.1 — Soft SLA Duration Alerts vs. Hard Timeouts in Lakeflow Jobs
A production FinOps Lakeflow Job normally completes in 20 minutes. The platform team wants an automated Slack/email warning whenever a run takes longer than **30 minutes (1,800 seconds)** so they can investigate, **without canceling or failing the active run** (the job should still be allowed to finish up to 2 hours).

Which Lakeflow Job configuration meets this requirement?

- **A.** Set `timeout_seconds: 1800` at the job level and configure `email_notifications.on_failure`.
- **B.** Set `max_retries: 0` and configure `email_notifications.on_start`.
- **C.** Configure `health.rules` with `metric: RUN_DURATION_SECONDS`, `op: GREATER_THAN`, `value: 1800`, add `on_duration_warning_threshold_exceeded` notifications, and keep `timeout_seconds: 7200`.
- **D.** Query `system.billing.usage` inside a `for_each_task` loop every 5 minutes and raise an exception if `usage_quantity > 10`.

- **User Answer:** **C** ✅
- **Official Rationale:** `health.rules` with `RUN_DURATION_SECONDS` and `on_duration_warning_threshold_exceeded` fires a non-fatal warning when a run exceeds its SLA threshold, whereas `timeout_seconds` kills the active run (`TIMEDOUT`).

---

### Question 4.1.2 — System Tables & Query Profile Diagnosis
A data engineer wants to audit all Lakeflow Job task failures and duration spikes across the entire workspace over the past 7 days, and simultaneously notices in the Query Profile of a failing job that **1 task out of 200 took 48 minutes** while the other 199 tasks finished in 4 seconds (with `Spill (Disk) = 0 GB` and `Files read = 150`).

Which system table should the engineer query for the task-level timeline, and what is the Query Profile bottleneck?

- **A.** Query `system.lakeflow.job_task_run_timeline` for task-level durations/states; the Query Profile shows **Data Skew** (mitigated by AQE Skew Join / salting or filtering skewed `NULL` keys).
- **B.** Query `system.lakeflow.jobs` for task-level run durations; the Query Profile shows the **Small File Problem**.
- **C.** Query `system.access.audit` for task-level durations; the Query Profile shows **Disk Spill**.
- **D.** Query `information_schema.tables` for task-level durations; the Query Profile shows **Broadcast Timeout**.

- **User Answer:** **A** ✅
- **Official Rationale:** `system.lakeflow.job_task_run_timeline` stores per-task execution slices (`task_key`, `result_state`, `period_start_time`, `period_end_time`). A single straggler task with zero disk spill and normal file sizes indicates Data Skew.

---

## Subpoint 4.2: Databricks Lakehouse Alerts, Streaming Metrics & FinOps Billing Alerts

### Question 4.2.1 — Databricks Lakehouse Alerts & FinOps Billing Monitoring
A FinOps engineer wants to receive a Slack notification **once per incident** (whenever a pipeline's daily estimated DBU spend crosses `\$100`, without spamming Slack every 15 minutes while it remains above `\$100`). They write a query joining `system.billing.usage` and `system.billing.list_prices` grouped by `usage_metadata.dlt_pipeline_id`, returning one row per pipeline.

Which combination ensures the Databricks Lakehouse Alert evaluates **all** pipelines accurately and avoids repeated Slack spam?

- **A.** Keep `GROUP BY dlt_pipeline_id` ordered alphabetically and set notification frequency to `Each time alert is evaluated`.
- **B.** Run the alert on an All-Purpose cluster with `df.cache()` and set notification frequency to `UNKNOWN`.
- **C.** Query `system.lakeflow.jobs` instead of `system.billing.usage` and set `timeout_seconds: 100`.
- **D.** Aggregate or order the query so Row 0 always reflects the highest-spend breach (e.g., `SELECT MAX(estimated_cost_usd) AS max_cost ...` or `ORDER BY estimated_cost_usd DESC LIMIT 1`) on a SQL Warehouse, and set notification frequency to **`Just once`**.

- **User Answer:** **D** ✅
- **Official Rationale:** Databricks Lakehouse Alerts run on a SQL Warehouse and evaluate **only the first row** of the query result. Aggregating with `MAX(...)` (or sorting descending + `LIMIT 1`) guarantees any breach appears in Row 0, and `Just once` alerts only on the `OK → TRIGGERED` transition.

---

### Question 4.2.2 — Structured Streaming `lastProgress` Diagnosis
While monitoring a Structured Streaming job via `StreamingQueryListener` (`query.lastProgress`), an engineer observes:
- `inputRowsPerSecond`: `1,200`
- `processedRowsPerSecond`: `2,500`
- `stateOperators[0].numRowsTotal`: `15,000` → `32,000` → `49,000` → `68,000` over four consecutive micro-batches.

What is the correct diagnosis?

- **A.** The query is keeping up with input throughput (`processedRowsPerSecond > inputRowsPerSecond`), **but** RocksDB state is growing unboundedly—indicating a **missing or misconfigured `.withWatermark(...)`** that prevents old state keys from being evicted.
- **B.** The query is suffering from a processing backlog because `processedRowsPerSecond` must be lower than `inputRowsPerSecond`.
- **C.** `numRowsTotal` measures total rows written to the final Delta target table, so linear growth is expected and healthy.
- **D.** The query should be switched from `.trigger(availableNow=True)` to `Trigger.ProcessingTime('1 second')` to shrink `numRowsTotal`.

- **User Answer:** **A** ✅
- **Official Rationale:** `processedRowsPerSecond > inputRowsPerSecond` confirms throughput is healthy, but monotonically climbing `stateOperators[0].numRowsTotal` indicates state keys are never being evicted due to a missing or misconfigured watermark.
