"""
Module 4 — Gold Materialized Views Pipeline.

Subpoint 1.6: Streaming Tables vs. Materialized Views.
Demonstrates why Gold analytical rollups and BI serving require Materialized Views:
  1. Full non-watermarked aggregations (GROUP BY billing_date, customer_id)
  2. Arbitrary window functions (ROW_NUMBER over partition)
  3. Pre-computed query performance with Liquid Clustering (CLUSTER BY)

Dual-mode architecture:
  - Declarative Lakeflow Pipeline mode (Materialized View via @dlt.table without read_stream)
  - Standalone Spark Connect execution mode (for IDE execution and tests)
"""

CATALOG = "portfolio_catalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

CLEAN_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.clean_events"
GOLD_BILLING_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.daily_billing_summary"
GOLD_CHURN_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.customer_churn_risk"


def _is_lakeflow_context() -> bool:
    try:
        import dlt  # noqa: F401

        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Pure Transformation Functions (Unit-testable with DataFrame.transform)
# ---------------------------------------------------------------------------
def compute_daily_billing_summary(df):
    """
    Computes daily billing metrics grouped by date and user.
    Demonstrates full-dataset aggregation (impossible in Streaming Tables without watermarks).
    """
    from pyspark.sql import functions as F

    return (
        df.groupBy(F.col("event_date").alias("billing_date"), "user_id")
        .agg(
            F.count(F.lit(1)).alias("total_api_calls"),
            F.round(F.sum("amount"), 2).alias("total_spend_usd"),
            F.round(F.avg("amount"), 2).alias("avg_spend_per_call"),
            F.max("amount").alias("max_spend_single_call"),
        )
    )


def compute_customer_ranking(df):
    """
    Computes daily customer spend ranking using arbitrary window functions.
    Demonstrates window functions (unsupported in Streaming Tables).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window_spec = Window.partitionBy("billing_date").orderBy(F.desc("total_spend_usd"))
    return df.withColumn("daily_spend_rank", F.row_number().over(window_spec))


# ---------------------------------------------------------------------------
# MODE 1: Lakeflow Declarative Pipeline Definitions
# ---------------------------------------------------------------------------
def define_gold_materialized_views():
    """Declarative definitions for Lakeflow Pipeline Materialized Views."""
    import dlt

    @dlt.table(
        name="daily_billing_summary",
        comment="Gold Materialized View: Daily FinOps billing aggregations",
        table_properties={
            "quality": "gold",
            "delta.enableDeletionVectors": "true",
        },
        cluster_by=["billing_date", "user_id"],
    )
    def daily_billing_summary():
        # Notice: dlt.read() (batch snapshot), NOT dlt.read_stream()
        # This compiles as a Materialized View in Lakeflow Pipelines
        return compute_daily_billing_summary(dlt.read("clean_events"))

    @dlt.table(
        name="customer_churn_risk",
        comment="Gold Materialized View: Customer spend rank and latency risk",
        table_properties={
            "quality": "gold",
            "delta.enableDeletionVectors": "true",
        },
        cluster_by=["billing_date"],
    )
    def customer_churn_risk():
        return compute_customer_ranking(dlt.read("daily_billing_summary"))


# ---------------------------------------------------------------------------
# MODE 2: Standalone Spark Connect Execution (for tests and local dev)
# ---------------------------------------------------------------------------
def run_gold_pipeline_standalone(spark):
    """
    Executes Gold Materialized View logic over Spark Connect.
    Writes Delta tables with Liquid Clustering and Deletion Vectors.
    """
    print(f"[*] Reading Silver source table: {CLEAN_TABLE}")
    silver_df = spark.read.table(CLEAN_TABLE)

    # 1. Compute Daily Billing Summary
    billing_summary_df = silver_df.transform(compute_daily_billing_summary)

    print(f"[*] Writing Gold Materialized View: {GOLD_BILLING_TABLE}")
    (
        billing_summary_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_BILLING_TABLE)
    )

    spark.sql(f"ALTER TABLE {GOLD_BILLING_TABLE} CLUSTER BY (billing_date, user_id)")

    # 2. Compute Customer Ranking & Risk
    gold_billing_df = spark.read.table(GOLD_BILLING_TABLE)
    ranking_df = gold_billing_df.transform(compute_customer_ranking)

    print(f"[*] Writing Gold Risk Table: {GOLD_CHURN_TABLE}")
    (
        ranking_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_CHURN_TABLE)
    )

    spark.sql(f"ALTER TABLE {GOLD_CHURN_TABLE} CLUSTER BY (billing_date)")

    summary_count = spark.table(GOLD_BILLING_TABLE).count()
    risk_count = spark.table(GOLD_CHURN_TABLE).count()

    print(f"[SUCCESS] Gold Daily Billing Summary: {summary_count} rows written to {GOLD_BILLING_TABLE}")
    print(f"[SUCCESS] Gold Customer Risk: {risk_count} rows written to {GOLD_CHURN_TABLE}")

    return {"summary_count": summary_count, "risk_count": risk_count}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if _is_lakeflow_context():
    define_gold_materialized_views()
elif __name__ == "__main__":
    from databricks.connect import DatabricksSession

    spark_session = DatabricksSession.builder.serverless(True).profile("EXAM_PREP").getOrCreate()
    run_gold_pipeline_standalone(spark_session)
    spark_session.stop()
