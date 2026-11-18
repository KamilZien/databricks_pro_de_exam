"""
Sabotage Drill — Section 5, Subpoint 5.4: Change Data Feed (CDF) Incremental Processing
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_5/test_subpoint_5_4_cdf_sabotage.py -v
"""

import pytest

from src.optimization.cdf_incremental import (
    prepare_cdf_merge_batch,
    validate_cdf_read_options,
)


@pytest.mark.unit
def test_cdf_read_option_sabotage():
    """
    Scenario:
    A downstream consumer reads incremental updates/deletes from `portfolio_catalog.silver.customers`
    starting at version 6 (earliest non-VACUUMed CDF version is 5).
    """
    # --- FIXED BUG 1: Use official PySpark read option `"readChangeFeed": "true"` ---
    # (Note: `delta.enableChangeDataFeed = true` is the TBLPROPERTY to enable CDF on the table;
    #  `readChangeFeed` is the DataFrameReader option to read it!)
    good_options = {"readChangeFeed": "true", "startingVersion": "6"}

    assert (
        validate_cdf_read_options(
            read_options=good_options,
            starting_version=6,
            earliest_available_cdf_version=5,
        )
        is True
    )


@pytest.mark.unit
def test_cdf_merge_dedup_sabotage():
    """
    Scenario:
    Between Delta version 6 and 9, customer `C-1` was updated twice (`SILVER` -> `GOLD`).
    `table_changes` outputs `update_preimage` and `update_postimage` rows for both commits.
    We must prepare the batch for `MERGE INTO gold_customers` so only the latest state (`GOLD`) remains.
    """
    cdf_rows = [
        {"customer_id": "C-1", "tier": "BRONZE", "_change_type": "update_preimage", "_commit_version": 6},
        {"customer_id": "C-1", "tier": "SILVER", "_change_type": "update_postimage", "_commit_version": 6},
        {"customer_id": "C-1", "tier": "SILVER", "_change_type": "update_preimage", "_commit_version": 9},
        {"customer_id": "C-1", "tier": "GOLD", "_change_type": "update_postimage", "_commit_version": 9},
    ]

    # --- FIXED BUG 2: Filter out `update_preimage` and deduplicate to latest `_commit_version` ---
    merged_source = prepare_cdf_merge_batch(
        cdf_rows,
        filter_preimages=True,
        deduplicate_latest=True,
    )

    assert len(merged_source) == 1
    assert merged_source[0]["tier"] == "GOLD"
    assert merged_source[0]["_change_type"] == "update_postimage"
