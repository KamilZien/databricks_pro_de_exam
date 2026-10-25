# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2: Process Success Report (Consumer)
# MAGIC Runs only if Task 1 succeeds (`run_if: ALL_SUCCESS`).
# MAGIC Retrieves `taskValues` published by Task 1.

# COMMAND ----------

# Retrieve dynamic task values emitted by upstream task
clean = dbutils.jobs.taskValues.get(taskKey="ingest_and_score", key="clean_records", default=0)
quarantined = dbutils.jobs.taskValues.get(taskKey="ingest_and_score", key="quarantined_records", default=0)
status = dbutils.jobs.taskValues.get(taskKey="ingest_and_score", key="pipeline_status", default="UNKNOWN")

print("[REPORT] Retrieved from Task 1 via taskValues:")
print(f"  - Status: {status}")
print(f"  - Clean Records: {clean}")
print(f"  - Quarantined Records: {quarantined}")

# Generate simple summary
total = clean + quarantined
quarantine_pct = round((quarantined / total * 100), 2) if total > 0 else 0.0

print(f"[REPORT] Quarantine Rate: {quarantine_pct}%")
