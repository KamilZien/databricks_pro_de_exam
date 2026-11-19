# Section 4: Monitoring and Alerting (10%)

> **Official Weight:** 10% of total exam score  
> **Key Exam Standard:** `system.lakeflow.*` & `system.billing.usage`, Soft SLA `health.rules` vs `timeout_seconds`, Query Profile Bottlenecks, Databricks Lakehouse Alerts, Streaming `lastProgress`  
> **Official Docs:**  
> - [Monitor Lakeflow Jobs & System Tables (`system.lakeflow.*`)](https://docs.databricks.com/en/admin/system-tables/jobs.html)  
> - [Job Notifications & Duration Thresholds (`health`)](https://docs.databricks.com/en/jobs/notifications.html)  
> - [Query Profile (Serverless & SQL Execution Diagnosis)](https://docs.databricks.com/en/sql/user/queries/query-profile.html)  
> - [Databricks Lakehouse Alerts](https://docs.databricks.com/en/sql/user/alerts/index.html)

---

## 1. Subpoint 4.1: Lakeflow Jobs Monitoring, Soft SLAs & Query Profile

### A. System Tables for Job & Cost Telemetry
- `system.lakeflow.jobs`: Job definitions (SCD2 history).
- `system.lakeflow.job_run_timeline`: 1 row per job run slice (`result_state`: `SUCCEEDED`, `FAILED`, `TIMEDOUT`, `CANCELED`, `termination_code`).
- `system.lakeflow.job_task_run_timeline`: 1 row per task run slice (`task_key`, `compute_ids`, `period_start_time`, `period_end_time`).
- `system.billing.usage` + `system.billing.list_prices`: Track DBU usage and USD cost by `usage_metadata.job_id`, `usage_metadata.dlt_pipeline_id`, or `usage_metadata.warehouse_id`.

### B. Soft SLA Warnings vs. Hard Timeouts (`resources/jobs.yml`)

| Config Key | Behavior on Breach | Terminates Active Run? |
| :--- | :--- | :--- |
| `health.rules` (`RUN_DURATION_SECONDS`) + `on_duration_warning_threshold_exceeded` | Sends warning email/webhook when run exceeds SLA threshold. | **No** (run continues to completion). |
| `timeout_seconds` | Hard execution cutoff on job or task. | **Yes** (`TIMEDOUT` + `on_failure`). |

### C. Query Profile & Spark UI Bottleneck Diagnosis

| Symptom | Root Cause | Fix |
| :--- | :--- | :--- |
| 1 straggler task takes 45m while 199 finish in 4s (`Spill = 0`) | **Data Skew** | Ensure `spark.sql.adaptive.enabled = true` and `spark.sql.adaptive.skewJoin.enabled = true`; filter/salt skewed `NULL` keys or broadcast small lookup table. |
| `Spill (Memory)` & `Spill (Disk)` > 0 GB | **Memory Pressure** | Increase shuffle partitions / enable AQE coalescing or scale warehouse/compute. |
| `Files read: 100,000+` with `< 1 MB` avg file size | **Small File Problem** | Enable Predictive Optimization or run `OPTIMIZE` with Liquid Clustering (`CLUSTER BY`). |

---

## 2. Subpoint 4.2: Databricks Lakehouse Alerts & Streaming Monitoring

### A. Databricks Lakehouse Alerts (formerly Databricks SQL Alerts)
- Execute scheduled SQL queries on a **SQL Warehouse** (Serverless recommended).
- **Critical Rule:** Alerts **only inspect Row 0** of the query result! Always aggregate (`MAX(...)`, `COUNT(*)`) or `ORDER BY ... DESC LIMIT 1` so breaches are never hidden in Row 2+.
- **Alert States:** `OK`, `TRIGGERED`, `UNKNOWN` (empty result or query error).
- **Notification Frequency:** Use **`Just once`** to fire only on `OK → TRIGGERED` transitions and prevent alert fatigue.

### B. Structured Streaming `lastProgress` & `StreamingQueryListener`
- `inputRowsPerSecond > processedRowsPerSecond` persistently → **Processing Backlog** (falling behind input rate).
- `stateOperators[0].numRowsTotal` rising monotonically without dropping → **Missing/Misconfigured `.withWatermark(...)`** (unbounded RocksDB state growth / OOM risk).

---

## 3. Official Subpoints 4.2, 4.3 & 4.5: REST API / CLI / SDK, Jobs API & Pipeline Event Log

### A. Programmatic Monitoring (`4.2` & `4.5`)

| Task | REST API | Databricks CLI (`v0.200+`) | Python SDK (`WorkspaceClient()`) |
| :--- | :--- | :--- | :--- |
| **List Job Runs** | `GET /api/2.1/jobs/runs/list?job_id=<id>` | `databricks jobs list-runs --job-id <id>` | `w.jobs.list_runs(job_id=...)` |
| **Get Run Status & Tasks** | `GET /api/2.1/jobs/runs/get?run_id=<id>` | `databricks jobs get-run <run-id>` | `w.jobs.get_run(run_id=...)` |
| **Get Task Error / Output** | `GET /api/2.1/jobs/runs/get-output?run_id=<task_run_id>` | `databricks jobs get-run-output <task-run-id>` | `w.jobs.get_run_output(run_id=task_run_id)` |
| **List Pipeline Events** | `GET /api/2.0/pipelines/{id}/events` | `databricks pipelines list-pipeline-events <id>` | `w.pipelines.list_pipeline_events(pipeline_id=...)` |

- **`life_cycle_state` vs. `result_state`:** `result_state` (`SUCCESS`, `FAILED`, `TIMEDOUT`, `CANCELED`) is only populated after `life_cycle_state == "TERMINATED"`.
- **Multi-Task `runs/get-output` Rule:** Passing the parent job `run_id` fails on multi-task jobs; always look up the specific failed child task's `tasks[].run_id` from `runs/get` first.

### B. Complete Lakeflow Pipeline `event_log('<pipeline-id>')` `event_type` Reference (`4.3`)

> **Note:** `event_log('<pipeline-id>')` is a built-in SQL TVF reading the pipeline's internal Delta event log (not a `system.*` table). Publish it to Unity Catalog via `event_log` (`catalog`, `schema`, `name`) in `databricks.yml` to share across teams. There is **no** `job_action` event type.

| Official `event_type` | What It Records |
| :--- | :--- |
| **`flow_progress`** | Per-flow streaming/batch metrics (`num_output_rows`) and **Data Quality Expectations** (`details:flow_progress.data_quality.expectations`). |
| **`update_progress`** | Overall pipeline update lifecycle (`INITIALIZING`, `SETTING_UP_TABLES`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELED`). |
| **`user_action`** | Audit trail of actions performed on the pipeline (`user_name`, `action`: `CREATE`, `EDIT`, `START`, `STOP`, `DELETE`). |
| **`create_update`** | Metadata when a new pipeline update starts (full refresh vs incremental, trigger source, `cause`). |
| **`autoscale`** | Enhanced Autoscaling decisions (`status`, `requested_num_executors`, backlog metrics). |
| **`cluster_resources`** | Cluster compute metrics (task slots, executor counts, memory/CPU utilization). |
| **`flow_definition`** / **`dataset_definition`** | Table/view/flow schema, lineage, and configuration definitions. |
| **`maintenance_progress`** | Automated background maintenance (`OPTIMIZE`, `VACUUM`) progress on pipeline tables. |
| **`planning_information`** | Query/execution planning details and incrementalization decisions for Materialized Views. |
