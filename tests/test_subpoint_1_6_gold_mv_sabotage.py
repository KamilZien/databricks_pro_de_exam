r"""
Module 1 — Subpoint 1.6 Sabotage Drill: Streaming Tables vs. Materialized Views.

This integration test verifies:
  1. Materialized View batch snapshot semantics vs. streaming state limitations.
  2. The exact reason why non-watermarked full aggregations cannot run as an append-only Streaming Table.
  3. Pre-computed Gold analytical outputs and window rankings in `portfolio_catalog.gold`.

⚠️ WARNING: This file contains 2 INTENTIONAL EXAM-CALIBRATED SABOTAGE BUGS:
  - Bug 1: An incorrect expectation on streaming aggregation semantics.
           The developer expects an unwatermarked streaming aggregation to succeed in append mode.
  - Bug 2: A sorting direction bug in the customer spend window ranking assertion.

Your mission:
  Diagnose both bugs and fix them so:
      & ".\.venv\Scripts\python.exe" -m pytest tests/test_subpoint_1_6_gold_mv_sabotage.py -v
  passes 100% green!
"""

import pytest
from pyspark.sql import functions as F
from pyspark.errors.exceptions.connect import AnalysisException


@pytest.mark.integration
def test_streaming_aggregation_state_limitation(spark, catalog_name):
    """
    Verifies that unwatermarked streaming aggregations cannot run in append mode,
    proving why Materialized Views are required for full-table Gold aggregations.
    """
    clean_table = f"{catalog_name}.silver.clean_events"

    # Read clean_events as an incremental stream
    streaming_df = spark.readStream.table(clean_table)

    # Perform full-table GROUP BY without an event-time watermark
    unwatermarked_agg = streaming_df.groupBy("user_id").count()

    # REAL ENGINE VERIFICATION:
    # Attempting to start an append stream on an unwatermarked aggregation
    # causes Spark Catalyst to throw AnalysisException:
    # "Append output mode not supported when there are streaming aggregations without watermark"
    with pytest.raises(AnalysisException) as exc_info:
        query = (
            unwatermarked_agg.writeStream.format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", f"/Volumes/{catalog_name}/silver/_checkpoints/test_sabotage_stream")
            .toTable(f"{catalog_name}.silver.test_invalid_stream_agg")
        )

    assert "This output mode is not supported for streaming aggregations without watermark" in str(exc_info.value), (
        f"Expected Append output mode error, got: {exc_info.value}"
    )


@pytest.mark.integration
def test_gold_materialized_view_ranking_and_clustering(spark, catalog_name):
    """
    Verifies that the Gold Materialized View computes the correct window ranking
    and maintains Liquid Clustering properties.
    """
    risk_table = f"{catalog_name}.gold.customer_churn_risk"
    df = spark.table(risk_table)

    assert df.count() > 0, f"{risk_table} should contain pre-computed rows"

    # Verify that rank 1 corresponds to the highest spend per day
    top_spenders = df.filter(F.col("daily_spend_rank") == 1).collect()
    rank_2_spenders = df.filter(F.col("daily_spend_rank") == 2).collect()

    assert len(top_spenders) > 0, "Should have rank 1 records"
    assert len(rank_2_spenders) > 0, "Should have rank 2 records"

    # Verified fix: Rank 1 spend must be >= Rank 2 spend
    for r1 in top_spenders:
        matching_r2 = [r2 for r2 in rank_2_spenders if r2["billing_date"] == r1["billing_date"]]
        if matching_r2:
            r2_val = matching_r2[0]
            assert r1["total_spend_usd"] >= r2_val["total_spend_usd"], (
                f"Rank 1 spend ({r1['total_spend_usd']}) should be >= Rank 2 spend ({r2_val['total_spend_usd']})"
            )
