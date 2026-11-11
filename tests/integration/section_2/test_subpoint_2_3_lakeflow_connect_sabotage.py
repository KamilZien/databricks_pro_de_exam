"""
tests/integration/section_2/test_subpoint_2_3_lakeflow_connect_sabotage.py — Sabotage Drill.

Verifies:
1. Unity Catalog relational CONNECTION specification validation for Lakeflow Connect.
2. Two-phase snapshot-to-stream WAL unification and state resolution.

⚠️ WARNING: Contains 2 INTENTIONAL EXAM-CALIBRATED SABOTAGE BUGS:
  - Sabotage Bug 1: A snapshot vs incremental WAL LSN resolution sabotage.
  - Sabotage Bug 2: A legacy JDBC credential anti-pattern vs UC Secret specification.

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

from src.ingestion.lakeflow_connect_ingest import (
    unify_snapshot_and_cdc_stream,
    validate_lakeflow_connection_spec,
)


@pytest.mark.integration
def test_lakeflow_connect_snapshot_and_cdc_unification_sabotage(spark):
    """
    Test Lakeflow Connect two-phase ingestion:
    - Customer C-1: In baseline snapshot ($100), updated via WAL at LSN=500 ($150).
      Correct state must reflect WAL update ($150).
    - Customer C-2: In baseline snapshot ($200), deleted via WAL at LSN=600 ('d').
      Correct state must NOT contain C-2.
    - Customer C-3: Not in baseline snapshot, inserted via WAL at LSN=700 ($300, 'i').
      Correct state must contain C-3 ($300).
    """
    snapshot_schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), True),
            StructField("balance", DoubleType(), True),
        ]
    )

    cdc_schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), True),
            StructField("balance", DoubleType(), True),
            StructField("wal_lsn", LongType(), False),
            StructField("op", StringType(), False),
        ]
    )

    # Phase 1: Initial full table snapshot from Postgres
    snapshot_records = [
        ("C-1", "Alice Corp", 100.0),
        ("C-2", "Bob LLC", 200.0),
    ]

    # Phase 2: Ongoing WAL change records streamed via Lakeflow Connect
    cdc_records = [
        ("C-1", "Alice Corp", 150.0, 500, "u"),  # Update at LSN 500
        ("C-2", "Bob LLC", 200.0, 600, "d"),  # Delete at LSN 600
        ("C-3", "Charlie Inc", 300.0, 700, "i"),  # Insert at LSN 700
    ]

    snapshot_df = spark.createDataFrame(snapshot_records, schema=snapshot_schema)
    cdc_df = spark.createDataFrame(cdc_records, schema=cdc_schema)

    unified_df = unify_snapshot_and_cdc_stream(snapshot_df, cdc_df)
    results = {row["customer_id"]: row for row in unified_df.collect()}

    # Verify C-2 was deleted by WAL
    assert "C-2" not in results, "Customer C-2 should have been evicted by WAL delete 'd'!"

    # Verify C-3 was inserted by WAL
    assert "C-3" in results, "Customer C-3 should exist from WAL insert 'i'!"
    assert results["C-3"]["balance"] == 300.0

    # --- RESOLVED SABOTAGE BUG 1: Snapshot vs. WAL LSN resolution ---
    # Lakeflow Connect prioritizes newer incremental WAL events (LSN 500 = 150.0)
    # over the initial snapshot baseline (LSN 0 = 100.0).
    c1 = results["C-1"]
    assert c1["balance"] == 150.0, f"Expected latest WAL balance 150.0, got {c1['balance']}"


@pytest.mark.integration
def test_lakeflow_connection_spec_sabotage():
    """
    Verify Unity Catalog CREATE CONNECTION parameter governance.
    """
    # --- RESOLVED SABOTAGE BUG 2: Governed Unity Catalog Secret Reference ---
    # In Unity Catalog, connections must reference governed secret scopes
    # instead of embedding hardcoded plaintext credentials.
    secure_conn_spec = {
        "connection_type": "POSTGRESQL",
        "host": "postgres-prod.corporate.internal",
        "port": "5432",
        "user": "databricks_cdc_reader",
        "secret_scope": "finops_credentials",
        "secret_key": "pg_cdc_reader_password",
    }

    is_valid = validate_lakeflow_connection_spec(secure_conn_spec)
    assert is_valid is True, "Expected governed secret connection spec to pass validation!"
