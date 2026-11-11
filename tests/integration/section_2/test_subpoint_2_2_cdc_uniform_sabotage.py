"""
tests/integration/section_2/test_subpoint_2_2_cdc_uniform_sabotage.py — Subpoint 2.2 Sabotage Drill.

Verifies:
1. SCD Type 1 CDC event resolution (out-of-order sequence handling and delete tombstones).
2. UniForm Iceberg table properties on Databricks Delta Lake tables.

⚠️ WARNING: Contains 2 INTENTIONAL EXAM-CALIBRATED SABOTAGE BUGS:
  - Sabotage Bug 1: A CDC out-of-order sequencing assertion sabotage.
  - Sabotage Bug 2: A UniForm Iceberg table property name sabotage.

Your task:
  Run pytest on Databricks Serverless, diagnose both bugs, and fix them!
"""

import pytest
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.ingestion.cdc_uniform_pipeline import (
    get_uniform_iceberg_table_properties,
    process_cdc_scd1_batch,
)


@pytest.mark.integration
def test_cdc_scd1_out_of_order_and_delete_sabotage(spark):
    """
    Test CDC batch with:
    - Product P-1: Insert at ts=100 ($10), Update at ts=300 ($15), Late Update at ts=200 ($12).
      Correct state must reflect ts=300 ($15), ignoring late ts=200!
    - Product P-2: Insert at ts=100, Deleted at ts=150.
      Correct state must NOT contain P-2!
    - Product P-3: Insert at ts=50 ($50).
    """
    schema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("commit_ts", LongType(), False),
            StructField("op", StringType(), False),
        ]
    )

    raw_cdc_events = [
        # Product 1: out-of-order delivery
        ("P-1", "Cloud Compute Small", 10.0, 100, "i"),
        ("P-1", "Cloud Compute Large", 15.0, 300, "u"),
        ("P-1", "Cloud Compute Medium", 12.0, 200, "u"),  # Late arrival (ts=200 < ts=300)
        # Product 2: tombstone delete
        ("P-2", "Legacy Storage", 25.0, 100, "i"),
        ("P-2", "Legacy Storage", 25.0, 150, "d"),  # Deleted at ts=150
        # Product 3: normal insert
        ("P-3", "Managed DB", 50.0, 50, "i"),
    ]

    cdc_df = spark.createDataFrame(raw_cdc_events, schema=schema)
    resolved_df = process_cdc_scd1_batch(cdc_df)

    results = {row["product_id"]: row for row in resolved_df.collect()}

    # Verification 1: Product P-2 was deleted, so it must not exist in SCD1
    assert "P-2" not in results, "Product P-2 should have been deleted by tombstone op='d'!"

    # Verification 2: Product P-1 latest state
    # --- RESOLVED SABOTAGE BUG 1: Out-of-order sequencing expectation sabotage ---
    # SEQUENCE BY commit_ts correctly picks ts=300 ($15.0, 'Cloud Compute Large'),
    # ignoring the late-arriving ts=200 event ($12.0)!
    p1 = results["P-1"]
    assert p1["product_name"] == "Cloud Compute Large", f"Expected 'Cloud Compute Large', got {p1['product_name']}"
    assert p1["price"] == 15.0, f"Expected latest sequenced price 15.0, got {p1['price']}"


@pytest.mark.integration
def test_uniform_iceberg_table_properties_sabotage(spark, catalog_name):
    """
    Verify Delta table properties required for UniForm Iceberg metadata.
    """
    props = get_uniform_iceberg_table_properties()

    # --- RESOLVED SABOTAGE BUG 2: UniForm official property verification ---
    # The official Delta table property for UniForm is 'delta.universalFormat.enabledFormats'
    # (never distractor names like 'delta.icebergEnabled').
    assert "delta.universalFormat.enabledFormats" in props, "Official UniForm property missing!"
    assert props["delta.universalFormat.enabledFormats"] == "iceberg", "Expected UniForm format 'iceberg'"
    assert props["delta.enableDeletionVectors"] == "true", "UniForm requires Deletion Vectors to be enabled!"
