# Databricks notebook source
# MAGIC %md
# MAGIC # Subpoint 1.3: User-Defined Functions (UDFs) & Unity Catalog
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. **Photon-accelerated SQL UDF** (`calculate_finops_tax`) in Unity Catalog.
# MAGIC 2. **Governed Python UDF** (`score_customer_churn`) in Unity Catalog.
# MAGIC 3. **SQL & PySpark Interoperability** across data analysts and data engineers.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TASK 1: Create a Photon-accelerated SQL UDF in Unity Catalog
# MAGIC CREATE OR REPLACE FUNCTION portfolio_catalog.silver.calculate_finops_tax(amount DOUBLE, tax_rate DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC DETERMINISTIC
# MAGIC CONTAINS SQL
# MAGIC COMMENT 'Computes tax with standard rounding (Photon accelerated)'
# MAGIC RETURN ROUND(amount * tax_rate, 2);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TASK 2: Create a Centralized Python UDF in Unity Catalog
# MAGIC CREATE OR REPLACE FUNCTION portfolio_catalog.silver.score_customer_churn(days_inactive INT, support_tickets INT)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Scores churn risk (0.0 to 1.0) using Python rules'
# MAGIC AS $$
# MAGIC if days_inactive is None or support_tickets is None:
# MAGIC     return 0.0
# MAGIC base_risk = min(days_inactive / 90.0, 0.7)
# MAGIC ticket_risk = min(support_tickets * 0.1, 0.3)
# MAGIC return round(base_risk + ticket_risk, 2)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TASK 3: Query Registered UDFs via SQL (Data Analyst Workflow)
# MAGIC SELECT
# MAGIC     event_id,
# MAGIC     event_type,
# MAGIC     amount,
# MAGIC     portfolio_catalog.silver.calculate_finops_tax(amount, 0.23) AS tax,
# MAGIC     portfolio_catalog.silver.score_customer_churn(30, 2) AS churn_risk
# MAGIC FROM portfolio_catalog.silver.clean_events
# MAGIC LIMIT 10;

# COMMAND ----------

# TASK 4: Query Registered UDFs via PySpark (Data Engineer Workflow)
import pyspark.sql.functions as F

events_df = spark.table("portfolio_catalog.silver.clean_events").limit(10)

enriched_df = events_df.select(
    F.col("event_id"),
    F.col("event_type"),
    F.col("amount"),
    F.expr("portfolio_catalog.silver.calculate_finops_tax(amount, 0.23)").alias("tax_calculated"),
    F.expr("portfolio_catalog.silver.score_customer_churn(30, 2)").alias("churn_score"),
)

display(enriched_df)
