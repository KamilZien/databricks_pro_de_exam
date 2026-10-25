# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1: Ingest & Score Producer
# MAGIC Calculates row counts and emits dynamic `taskValues` for downstream tasks.

# COMMAND ----------

clean_count = spark.table("portfolio_catalog.silver.clean_events").count()
quarantine_count = spark.table("portfolio_catalog.silver.quarantine_events").count()

print(f"[*] Clean events: {clean_count}")
print(f"[*] Quarantined events: {quarantine_count}")

# Emit taskValues for downstream tasks to consume
dbutils.jobs.taskValues.set(key="clean_records", value=clean_count)
dbutils.jobs.taskValues.set(key="quarantined_records", value=quarantine_count)
dbutils.jobs.taskValues.set(key="pipeline_status", value="HEALTHY")

print("[+] Successfully published taskValues: clean_records, quarantined_records, pipeline_status")
