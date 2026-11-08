# Databricks notebook source
# MAGIC %md
# MAGIC # Regional Settlement Processor (For Each Child Task)
# MAGIC Executed iteratively by `for_each_task` using `base_parameters: {"region_code": "{{input}}"}`.

# COMMAND ----------

# Retrieve the loop iteration parameter injected via "{{input}}"
dbutils.widgets.text("region_code", "GLOBAL")
region = dbutils.widgets.get("region_code")

print(f"[*] [FOR_EACH_ITERATION] Processing settlement for region: {region}")
print(f"[+] Completed regional FinOps settlement for {region}.")
