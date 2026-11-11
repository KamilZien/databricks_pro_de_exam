"""
src/ingestion/cdc_uniform_pipeline.py — Subpoint 2.2 CDC & UniForm Logic.

Handles:
1. process_cdc_scd1_batch: Pure transformation function that resolves out-of-order CDC
   records (INSERT, UPDATE, DELETE) using SEQUENCE BY semantics for SCD Type 1.
2. get_uniform_iceberg_table_properties: Returns table properties required to enable
   both Deletion Vectors and UniForm Iceberg metadata.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def process_cdc_scd1_batch(
    cdc_df: DataFrame,
    key_col: str = "product_id",
    seq_col: str = "commit_ts",
    op_col: str = "op",
    delete_val: str = "d",
) -> DataFrame:
    """
    Simulates AUTO CDC SCD Type 1 merge resolution within a batch / micro-batch:
      - Groups by primary key (KEYS)
      - Orders by sequencing column (SEQUENCE BY) descending to pick latest event
      - Applies tombstone delete: if latest event is DELETE ('d'), the record is evicted
      - Drops internal window metadata and CDC operation columns
    """
    # Window ordered by sequence column descending per key
    key_window = Window.partitionBy(key_col).orderBy(F.col(seq_col).desc())

    ranked_df = cdc_df.withColumn("_rank", F.row_number().over(key_window))

    # Keep only the newest event per key (SEQUENCE BY logic)
    latest_events = ranked_df.filter(F.col("_rank") == 1).drop("_rank")

    # APPLY AS DELETE WHEN op = 'd': filter out deleted rows
    active_rows = latest_events.filter(F.col(op_col) != delete_val)

    # Return clean current-state records (SCD Type 1)
    return active_rows.drop(op_col)


def get_uniform_iceberg_table_properties() -> dict[str, str]:
    """
    Returns the exact Delta table properties required for UniForm Iceberg support:
      1. delta.enableDeletionVectors = true
      2. delta.universalFormat.enabledFormats = iceberg
    """
    return {
        "delta.enableDeletionVectors": "true",
        "delta.universalFormat.enabledFormats": "iceberg",
    }
