"""
Sabotage Drill — Section 5, Subpoint 5.1: Liquid Clustering, Predictive Optimization & Deletion Vectors
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_5/test_subpoint_5_1_clustering_and_po_sabotage.py -v
"""

import pytest

from src.optimization.storage_layout import (
    simulate_gdpr_physical_purge,
    validate_table_clustering_and_po,
)


@pytest.mark.unit
def test_liquid_clustering_and_predictive_optimization_sabotage():
    """
    Scenario:
    A wide 40-column Silver FinOps table has `tenant_id` at column index 38 (after 38 nested columns).
    The engineer wants:
      1. Liquid Clustering on `["tenant_id", "event_date"]` (with Data Skipping Stats covering `tenant_id`!).
      2. Predictive Optimization automatically running `OPTIMIZE`, `VACUUM`, and `ANALYZE`.
    """
    wide_columns = [f"col_{i}" for i in range(38)] + ["tenant_id", "event_date"]

    # --- FIXED BUG 1: Use UC `MANAGED` table (required for Predictive Optimization) AND
    # raise `data_skipping_num_indexed_cols=40` so column index 38 (`tenant_id`) has Data Skipping Stats! ---
    result = validate_table_clustering_and_po(
        table_type="MANAGED",
        partition_cols=[],
        cluster_cols=["tenant_id", "event_date"],
        column_order=wide_columns,
        predictive_optimization=True,
        data_skipping_num_indexed_cols=40,
    )

    assert result["valid"] is True
    assert result["predictive_optimization_active"] is True


@pytest.mark.unit
def test_gdpr_deletion_vectors_physical_purge_sabotage():
    """
    Scenario:
    A customer exercises GDPR Right-to-be-Forgotten on a Delta table with Deletion Vectors enabled.
    The engineer must ensure PII bytes are physically purged from cloud storage.
    """
    # --- FIXED BUG 2: Insert `REORG_PURGE` (`REORG TABLE ... APPLY (PURGE)`) between DELETE and VACUUM ---
    purge_status = simulate_gdpr_physical_purge(
        operations_executed=["DELETE", "REORG_PURGE", "VACUUM"],
        deletion_vectors_enabled=True,
    )

    assert purge_status["physical_pii_purged"] is True, (
        f"PII was NOT purged! Reason: {purge_status['reason']}"
    )
