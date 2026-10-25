r"""
Module 1 — Subpoint 1.3 Sabotage Drill: User-Defined Functions in Unity Catalog.

This integration test verifies:
  1. Unity Catalog Photon-accelerated SQL UDF (`calculate_finops_tax`).
  2. Unity Catalog Python UDF (`score_customer_churn`).
  3. Proper invocation via `F.expr()` without violating Serverless Law 3.

⚠️ WARNING: This file contains 2 INTENTIONAL EXAM-CALIBRATED SABOTAGE BUGS:
  - Bug 1: A calculation expectation / precision sabotage in the SQL UDF assertion.
  - Bug 2: A Serverless Law 3 violation: attempting to invoke an un-sandboxed client RDD/UDF
           instead of the governed Unity Catalog function.

Your mission:
  Diagnose both bugs and fix them so:
      & ".\.venv\Scripts\python.exe" -m pytest tests/test_subpoint_1_3_udf_sabotage.py -v
  passes green!
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.integration
def test_unity_catalog_sql_udf_tax(spark, catalog_name):
    """Verify that calculate_finops_tax computes tax accurately."""
    schema = "silver"

    test_df = spark.sql(f"""
        SELECT
            100.0 AS amount,
            0.23 AS tax_rate,
            {catalog_name}.{schema}.calculate_finops_tax(100.0, 0.23) AS actual_tax
    """)

    row = test_df.collect()[0]

    # --- RESOLVED SABOTAGE BUG 1: Calculation / Precision Sabotage ---
    # The developer previously had expected_tax = 230.0 (off by 10x).
    # Correct calculation: 100.0 * 0.23 = 23.00.
    expected_tax = 23.0

    assert row["actual_tax"] == expected_tax, f"Tax mismatch: expected {expected_tax}, got {row['actual_tax']}"


@pytest.mark.integration
def test_unity_catalog_churn_udf_governed_call(spark, catalog_name):
    """Verify customer churn UDF works without violating Serverless execution rules."""
    schema = "silver"
    events_df = spark.table(f"{catalog_name}.{schema}.clean_events").limit(5)

    # --- RESOLVED SABOTAGE BUG 2: Serverless Law 3 Anti-Pattern ---
    # Previously, code called events_df.rdd.map(...), which raises PySparkNotImplementedError
    # on Spark Connect because RDD APIs do not exist on Serverless compute.
    # Resolution: Call the Unity Catalog Python UDF via native F.expr(), keeping execution
    # purely inside the managed DataFrame engine.
    enriched_df = events_df.withColumn(
        "churn_risk",
        F.expr(f"{catalog_name}.{schema}.score_customer_churn(30, 2)"),
    )

    assert enriched_df.filter(F.col("churn_risk") > 0).count() == 5
