"""
Module 1 Exam Sabotage Drill.

This test simulates an ingestion validation job that reads Bronze data and
checks schema/lineage/performance constraints.

⚠️ WARNING: This file contains 2 SUBTLE BUGS directly violating Databricks
DE Pro (October 2026) Serverless & Architecture rules.
Find and fix both bugs so that:
    pytest tests/test_exam_drill.py -v
passes!
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.integration
def test_bronze_ingestion_and_optimization_drill(spark, catalog_name):
    bronze_table = f"{catalog_name}.bronze.raw_events"

    # Step 1: Read bronze data
    df = spark.table(bronze_table)

    # --- FIX 1 APPLIED ---
    # Serverless Spark Connect manages disk caching automatically; remove .cache()
    df_cached = df

    # Step 2: Validate row count
    row_count = df_cached.count()
    assert row_count >= 750, f"Expected >= 750 rows, got {row_count}"

    # Step 3: Validate ingestion lineage & schema
    # Ensure source metadata is present
    cols = df_cached.columns
    assert "_source_file" in cols, "Lineage column _source_file is missing"
    assert "_ingested_at" in cols, "Lineage column _ingested_at is missing"

    # Step 4: Extract amount from VARIANT for financial validation
    # In PySpark, colon navigation on VARIANT is evaluated via SQL expression (F.expr)
    amounts_df = df_cached.select(F.expr("raw_variant:amount::double").alias("parsed_amount")).filter(
        F.col("parsed_amount").isNotNull()
    )

    assert amounts_df.count() > 0, "Failed to parse amount from VARIANT"

    # --- FIX 2 APPLIED ---
    # For tables using Liquid Clustering (CLUSTER BY), ZORDER BY is invalid.
    # Run standard OPTIMIZE to trigger compaction and clustering.
    spark.sql(f"OPTIMIZE {bronze_table}")  # noqa: S608

    # Final assertion
    assert True
