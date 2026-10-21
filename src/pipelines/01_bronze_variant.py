"""
Module 1 — Bronze Ingestion Pipeline (Lakeflow Declarative / Standalone)

Ingests raw chaotic JSON webhook files from UC Volume landing zone into a
Bronze Delta table using Auto Loader (cloudFiles) + VARIANT data type.

Design decisions (all exam-relevant):
  - Auto Loader (`cloudFiles`) with `VARIANT` column (Law 5)
    → no brittle from_json struct schema; handles schema drift natively
  - Captures `_metadata` (file path, modification time) for lineage
  - NEVER specifies checkpointLocation inside Lakeflow pipeline (Law 4)
  - NEVER uses PARTITION BY — uses CLUSTER BY for Liquid Clustering (Law 2)
  - Uses .trigger(availableNow=True) for standalone mode (Law 3)
  - Reads from UC Volume /Volumes/... path (Law 1)
  - Enables Change Data Feed for downstream CDF consumers (Law 7)

This file supports TWO execution modes:
  1. Lakeflow Declarative Pipeline (inside databricks.yml pipeline resource)
  2. Standalone Spark Connect (for local dev/testing via databricks-connect)
"""

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "portfolio_catalog"
BRONZE_SCHEMA = "bronze"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/raw_events"
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.raw_events"


# ===========================================================================
# MODE 1: Lakeflow Declarative Pipeline (DLT)
# ===========================================================================
def _is_lakeflow_context() -> bool:
    """Check if we're running inside a Lakeflow/DLT pipeline."""
    try:
        import dlt  # noqa: F401

        return True
    except ImportError:
        return False


def define_lakeflow_tables():
    """
    Define Bronze streaming table for Lakeflow Declarative Pipeline.

    Key exam points:
      - dlt.table() with table_properties for CDF and deletion vectors
      - cloudFiles reads from UC Volume (Law 1)
      - VARIANT via parse_json — no from_json struct (Law 5)
      - No manual checkpointLocation or schemaLocation (Law 4)
      - CLUSTER BY for Liquid Clustering (Law 2)
    """
    import dlt
    from pyspark.sql import functions as F

    @dlt.table(
        name="raw_events_bronze",
        comment="Bronze: raw webhook events ingested via Auto Loader with VARIANT",
        table_properties={
            "quality": "bronze",
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
        },
        cluster_by=["event_date"],  # Liquid Clustering (Law 2)
    )
    def raw_events_bronze():
        return (
            dlt.read_stream("cloudFiles")
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")  # Read everything as string
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(LANDING_VOLUME_PATH)
            # Parse entire JSON body into VARIANT column (Law 5)
            .withColumn("raw_variant", F.expr("parse_json(to_json(struct(*)))"))
            # Extract event_date for clustering
            .withColumn("event_date", F.to_date(F.col("timestamp")))
            # Capture Auto Loader file metadata for lineage
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )


# ===========================================================================
# MODE 2: Standalone Spark Connect (for dev/testing)
# ===========================================================================
def ingest_bronze_standalone(spark) -> None:
    """
    Standalone Bronze ingestion using Spark Connect / databricks-connect.

    This is the local development equivalent of the Lakeflow pipeline above.
    Uses .trigger(availableNow=True) to process all available files and stop,
    preserving Free Edition Serverless quota (Law 3).

    Args:
        spark: DatabricksSession (Spark Connect session).
    """

    # Ensure Bronze schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

    # Read from UC Volume using Auto Loader (cloudFiles)
    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaLocation",
            f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/_schemas/raw_events",
        )
        .load(LANDING_VOLUME_PATH)
    )

    # Transform: parse into VARIANT + add metadata columns
    bronze_df = (
        raw_stream.transform(add_variant_column).transform(add_event_date_column).transform(add_ingestion_metadata)
    )

    # Write to Bronze Delta table
    # .trigger(availableNow=True) → process all available, then stop (Law 3)
    # NEVER use Trigger.ProcessingTime on Serverless
    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/_checkpoints/raw_events",
        )
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )

    query.awaitTermination()

    # Apply Liquid Clustering after first write (Law 2)
    spark.sql(f"ALTER TABLE {BRONZE_TABLE} CLUSTER BY (event_date)")

    # Enable CDF for downstream consumers (Law 7)
    spark.sql(f"ALTER TABLE {BRONZE_TABLE} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

    row_count = spark.table(BRONZE_TABLE).count()
    print(f"[SUCCESS] Bronze table {BRONZE_TABLE} now has {row_count} rows")


# ===========================================================================
# Pure Transform Functions (testable via DataFrame.transform)
# ===========================================================================
def add_variant_column(df):
    """
    Parse all columns into a single VARIANT column using parse_json.

    Exam point: VARIANT (Law 5) handles schema drift without brittle struct schemas.
    Uses parse_json(to_json(struct(*))) to capture the entire row as VARIANT.
    """
    from pyspark.sql import functions as F

    return df.withColumn("raw_variant", F.expr("parse_json(to_json(struct(*)))"))


def add_event_date_column(df):
    """
    Extract event_date from the timestamp field for Liquid Clustering.

    Exam point: CLUSTER BY event_date (Law 2), never PARTITION BY.
    """
    from pyspark.sql import functions as F

    return df.withColumn("event_date", F.to_date(F.col("timestamp")))


def add_ingestion_metadata(df):
    """
    Add ingestion metadata columns for lineage tracking.

    Captures _metadata.file_path (Auto Loader source file) and ingestion timestamp.
    """
    from pyspark.sql import functions as F

    return df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if _is_lakeflow_context():
    # Running inside Lakeflow Declarative Pipeline
    define_lakeflow_tables()
else:
    # Module-level: don't auto-execute; call ingest_bronze_standalone(spark) explicitly
    pass


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    ingest_bronze_standalone(spark)
    spark.stop()
