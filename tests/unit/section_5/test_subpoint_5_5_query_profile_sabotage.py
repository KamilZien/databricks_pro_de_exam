"""
Sabotage Drill — Section 5, Subpoint 5.5: Query Profile Bottlenecks
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_5/test_subpoint_5_5_query_profile_sabotage.py -v
"""

import pytest

from src.optimization.query_profile_analyzer import (
    classify_join_operator,
    simulate_scan_data_skipping,
)


@pytest.mark.unit
def test_data_skipping_non_sargable_predicate_sabotage():
    """
    Scenario:
    `portfolio_catalog.silver.finops_events` has 10,000 Parquet files and is clustered by `event_ts`.
    The engineer filters for all 2026 events, expecting Delta Data Skipping to prune ~92% of files.
    """
    # --- FIXED BUG 1: Use sargable range predicate on `event_ts` so Delta Data Skipping prunes files ---
    scan_metrics = simulate_scan_data_skipping(
        where_sql="event_ts >= '2026-01-01' AND event_ts < '2027-01-01'",
        clustered_col="event_ts",
        total_files=10_000,
    )

    assert scan_metrics["skipping_active"] is True, scan_metrics["reason"]
    assert scan_metrics["files_pruned"] >= 9_000


@pytest.mark.unit
def test_join_strategy_nested_loop_sabotage():
    """
    Scenario:
    A pipeline enriches click events (`a`, 500 GB) with pricing tiers (`b`, 5 MB) by timestamp window.
    In the Query Profile, the join operator shows `BroadcastNestedLoopJoin` and runs for 2 hours!
    """
    # --- FIXED BUG 2: Include an equi-join key (`a.tenant_id = b.tenant_id`) before the range condition ---
    join_plan = classify_join_operator(
        join_condition_sql="a.tenant_id = b.tenant_id AND a.ts BETWEEN b.start_ts AND b.end_ts",
        right_table_mb=5.0,
    )

    assert join_plan["operator"] == "BroadcastHashJoin", (
        f"Expected BroadcastHashJoin, got {join_plan['operator']}! {join_plan['warning']}"
    )
