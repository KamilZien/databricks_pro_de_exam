"""
Sabotage Drill — Section 5, Subpoint 5.3: Caching (Disk Cache vs Spark Cache vs Query Result Cache)
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_5/test_subpoint_5_3_caching_sabotage.py -v
"""

import pytest

from src.optimization.caching_strategy import (
    simulate_cache_behavior_after_update,
    validate_caching_strategy,
)


@pytest.mark.unit
def test_serverless_caching_strategy_sabotage():
    """
    Scenario:
    A Lakeflow Job running on Databricks **Serverless** compute joins 2 large tables and writes the
    intermediate result to two downstream Gold tables. To avoid recomputing the join twice,
    the engineer called `df.cache()`.

    Requirement: Must run cleanly on Serverless without raising `[NOT_SUPPORTED_ON_SERVERLESS]`.
    """
    # --- FIXED BUG 1: Use `"materialize_delta"` (or `"disk_cache"`) on Serverless compute ---
    result = validate_caching_strategy(
        compute_mode="serverless",
        strategy="materialize_delta",
    )

    assert result["valid"] is True


@pytest.mark.unit
def test_cache_staleness_after_delta_update_sabotage():
    """
    Scenario:
    A BI team runs repeated identical queries on `portfolio_catalog.silver.clean_events` while
    an upstream pipeline commits an `UPDATE` (Delta v1 -> v2).
    The engineer wants a caching layer that NEVER serves stale v1 data without manual `.unpersist()`.
    """
    # --- FIXED BUG 2: Use `disk_cache` (or `query_result_cache`) which never serves stale v1 rows ---
    outcome = simulate_cache_behavior_after_update(
        cache_layer="disk_cache",
    )

    assert outcome["serves_stale_data"] is False, (
        f"Stale v1 rows were served! Reason: {outcome['reason']}"
    )
