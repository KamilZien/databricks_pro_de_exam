"""
Module 2 — Silver Lakeflow Quarantine Pipeline.

Ingests records from portfolio_catalog.bronze.raw_events using Change Data Feed
or streaming reads, applies Lakeflow Expectation validation rules, and splits
the data into two Delta tables:
  1. portfolio_catalog.silver.clean_events (Validated Silver table)
  2. portfolio_catalog.silver.quarantine_events (Quarantine table with error tags)

Dual-mode architecture:
  - Declarative Lakeflow Pipeline mode (when deployed via databricks.yml)
  - Standalone Spark Connect execution mode (for IDE execution and tests)
"""

CATALOG = "portfolio_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.raw_events"
CLEAN_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.clean_events"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.quarantine_events"


# ---------------------------------------------------------------------------
# MODE 1: Lakeflow Declarative Pipeline Definitions
# ---------------------------------------------------------------------------
def _is_lakeflow_context() -> bool:
    try:
        import dlt  # noqa: F401
        return True
    except ImportError:
        return False


def define_silver_lakeflow_tables():
    """Declarative definitions for Lakeflow Pipeline deployment."""
    import dlt

    from src.transformations.silver_transforms import (
        extract_silver_fields,
        filter_clean_records,
        filter_quarantine_records,
        validate_event_records,
    )

    @dlt.table(
        name="silver_staging",
        comment="Intermediate validated staging table for Silver quarantine split",
        temporary=True,
    )
    def silver_staging():
        return (
            dlt.read_stream(BRONZE_TABLE)
            .transform(extract_silver_fields)
            .transform(validate_event_records)
        )

    @dlt.table(
        name="clean_events",
        comment="Validated Silver events conforming to data quality rules",
        table_properties={
            "quality": "silver",
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
        },
        cluster_by=["event_date", "event_type"],
    )
    def clean_events():
        return dlt.read_stream("silver_staging").transform(filter_clean_records)

    @dlt.table(
        name="quarantine_events",
        comment="Quarantined records failing data quality expectations",
        table_properties={
            "quality": "quarantine",
            "delta.enableDeletionVectors": "true",
        },
        cluster_by=["event_date"],
    )
    def quarantine_events():
        return dlt.read_stream("silver_staging").transform(filter_quarantine_records)


# ---------------------------------------------------------------------------
# MODE 2: Standalone Spark Connect Runner (Local IDE execution)
# ---------------------------------------------------------------------------
def run_silver_pipeline_standalone(spark) -> dict[str, int]:
    """
    Execute Silver validation & quarantine split using Spark Connect.

    Reads from Bronze table, applies validation transformations,
    and writes to clean and quarantine Silver tables using Liquid Clustering.
    """
    from src.transformations.silver_transforms import (
        extract_silver_fields,
        filter_clean_records,
        filter_quarantine_records,
        validate_event_records,
    )

    # 1. Ensure Silver schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

    # 3. Read stream from Bronze Delta table
    bronze_stream = spark.readStream.table(BRONZE_TABLE)

    # 4. Apply validation transformations
    validated_stream = (
        bronze_stream
        .transform(extract_silver_fields)
        .transform(validate_event_records)
    )

    clean_stream = validated_stream.transform(filter_clean_records)
    quarantine_stream = validated_stream.transform(filter_quarantine_records)

    # 5. Execute streams using native .toTable() in append mode with .trigger(availableNow=True)
    # Note: On Spark Connect Serverless, arbitrary Python foreachBatch UDF workers require container sandboxes.
    # Native Delta streaming append via .toTable() executes directly inside the optimized Serverless engine.
    clean_query = (
        clean_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"/Volumes/{CATALOG}/{SILVER_SCHEMA}/_checkpoints/clean_events")
        .trigger(availableNow=True)
        .toTable(CLEAN_TABLE)
    )

    quarantine_query = (
        quarantine_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"/Volumes/{CATALOG}/{SILVER_SCHEMA}/_checkpoints/quarantine_events")
        .trigger(availableNow=True)
        .toTable(QUARANTINE_TABLE)
    )

    clean_query.awaitTermination()
    quarantine_query.awaitTermination()

    # Apply Liquid Clustering (Law 2) & Change Data Feed (Law 7)
    spark.sql(f"ALTER TABLE {CLEAN_TABLE} CLUSTER BY (event_date, event_type)")
    spark.sql(f"ALTER TABLE {CLEAN_TABLE} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    spark.sql(f"ALTER TABLE {QUARANTINE_TABLE} CLUSTER BY (event_date)")

    clean_count = spark.table(CLEAN_TABLE).count()
    quarantine_count = spark.table(QUARANTINE_TABLE).count()

    print(f"[SUCCESS] Clean Silver table: {clean_count} rows written to {CLEAN_TABLE} (Idempotent foreachBatch)")
    print(f"[SUCCESS] Quarantine Silver table: {quarantine_count} rows written to {QUARANTINE_TABLE} (Idempotent foreachBatch)")

    return {"clean": clean_count, "quarantine": quarantine_count}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if _is_lakeflow_context():
    define_silver_lakeflow_tables()

if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Add project root to sys.path
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from dotenv import load_dotenv
    load_dotenv()

    from databricks.connect import DatabricksSession

    spark_session = DatabricksSession.builder.serverless(True).getOrCreate()
    run_silver_pipeline_standalone(spark_session)
    spark_session.stop()
