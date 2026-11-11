"""
src/ingestion/lakeflow_connect_ingest.py — Subpoint 2.3 Lakeflow Connect Relational Ingestion.

Implements:
1. validate_lakeflow_connection_spec: Verifies Unity Catalog CREATE CONNECTION parameters
   for relational databases (PostgreSQL, MySQL, SQL Server).
2. unify_snapshot_and_cdc_stream: Simulates Lakeflow Connect's two-phase ingestion:
   - Phase 1: Baseline historical table snapshot (all rows tagged as initial snapshot state).
   - Phase 2: Incremental Write-Ahead Log (WAL / binlog) stream events (inserts, updates, deletes).
   - Unifies both phases and resolves current active state using commit sequence numbers.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_RELATIONAL_TYPES = {"POSTGRESQL", "MYSQL", "SQLSERVER", "ORACLE"}


def validate_lakeflow_connection_spec(conn_spec: dict[str, str]) -> bool:
    """
    Validates that a Unity Catalog CREATE CONNECTION definition has the required
    attributes for Lakeflow Connect relational ingestion.
    Requires:
      - 'connection_type': in VALID_RELATIONAL_TYPES
      - 'host': non-empty string
      - 'port': valid port string (e.g. '5432', '3306', '1433')
      - 'user': non-empty string
      - 'secret_scope' and 'secret_key' for password management (least-privilege credential security)
    """
    conn_type = conn_spec.get("connection_type", "").upper()
    if conn_type not in VALID_RELATIONAL_TYPES:
        return False

    required_keys = ["host", "port", "user", "secret_scope", "secret_key"]
    return all(conn_spec.get(k) for k in required_keys)


def unify_snapshot_and_cdc_stream(
    snapshot_df: DataFrame,
    cdc_stream_df: DataFrame,
    key_col: str = "customer_id",
    seq_col: str = "wal_lsn",
) -> DataFrame:
    """
    Simulates Lakeflow Connect's seamless snapshot + WAL incremental streaming:
      1. Baseline snapshot rows are initialized with op = 'snapshot' and lsn = 0.
      2. CDC stream rows carry actual WAL LSN (Log Sequence Number) or commit_ts with op in ('i', 'u', 'd').
      3. Union both datasets and resolve final state using SEQUENCE BY wal_lsn:
         - Newest LSN wins.
         - If newest event is 'd' (delete), row is evicted.
    """
    # Standardize snapshot schema
    standardized_snapshot = snapshot_df.withColumn("op", F.lit("snapshot")).withColumn(seq_col, F.lit(0).cast("long"))

    # Standardize CDC stream schema (ensure seq_col is cast to long)
    standardized_cdc = cdc_stream_df.withColumn(seq_col, F.col(seq_col).cast("long"))

    # Union Phase 1 snapshot with Phase 2 WAL mutations
    combined_df = standardized_snapshot.unionByName(standardized_cdc)

    # Window over key ordered by LSN descending (newest WAL entry wins)
    win = Window.partitionBy(key_col).orderBy(F.col(seq_col).desc())
    latest_ranked = combined_df.withColumn("_rank", F.row_number().over(win)).filter(F.col("_rank") == 1).drop("_rank")

    # Evict deleted rows
    active_records = latest_ranked.filter(F.col("op") != "d")

    return active_records.drop("op")
