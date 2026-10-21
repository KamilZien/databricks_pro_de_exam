"""
Module 2 Exam Sabotage Drill: Silver Quarantine & Stream Processing.

This integration test exercises the Silver Lakeflow Quarantine table logic,
validating data quality expectations, Liquid Clustering, and Change Data Feed.

⚠️ WARNING: This file contains 2 SUBTLE EXAM BUGS related to:
  1. Lakeflow Data Quality Expectation actions vs Quarantine filtering.
  2. Change Data Feed (CDF) query configuration on Delta tables.

Find and fix both bugs so that:
    pytest tests/test_silver_sabotage_drill.py -v
passes!
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.integration
def test_silver_quarantine_and_cdf_drill(spark, catalog_name):
    clean_table = f"{catalog_name}.silver.clean_events"
    quarantine_table = f"{catalog_name}.silver.quarantine_events"

    # Step 1: Verify tables exist
    clean_df = spark.table(clean_table)
    quarantine_df = spark.table(quarantine_table)

    assert clean_df.count() > 0, "Clean table should not be empty"
    assert quarantine_df.count() > 0, "Quarantine table should not be empty"

    # --- BUG INJECTION 1: Data Quality Expectation Inconsistency ---
    # The developer tried to verify that NO payment events have missing amounts in the clean table,
    # but wrote an inverted condition that checks for isNull() instead of isNotNull(),
    # causing a false failure assertion.
    invalid_payment_amounts = clean_df.filter(
        F.col("event_type").startswith("payment") & F.col("amount").isNull()
    ).count()
    # BUG: Developer asserted that invalid count must be > 0 in the clean table!
    assert invalid_payment_amounts > 0, "Found zero invalid payment amounts in clean table!"

    # Step 2: Verify all quarantined records have reason tags
    missing_reasons = quarantine_df.filter(F.col("_quarantine_reason").isNull()).count()
    assert missing_reasons == 0, "Quarantined records must have _quarantine_reason tags"

    # --- BUG INJECTION 2: Change Data Feed (CDF) Option Syntax ---
    # Downstream consumers want to read the Change Data Feed from the clean Silver table
    # starting from version 0 to track incremental mutations.
    # The developer used a legacy / distractor option name instead of 'readChangeFeed'.
    cdf_df = (
        spark.read.format("delta")
        .option("readChangeData", "true")  # BUG: 'readChangeData' instead of 'readChangeFeed'
        .option("startingVersion", 0)
        .table(clean_table)
    )

    assert cdf_df.count() > 0, "Failed to read Change Data Feed from clean table"
    assert "_change_type" in cdf_df.columns, "CDF metadata column _change_type missing"
