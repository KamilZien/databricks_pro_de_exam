"""
Sabotage Drill — Section 3, Subpoint 3.4: Data Quality Expectations, Quarantine & Event Log
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_3/test_subpoint_3_4_expectations_sabotage.py -v
"""

import pytest

from src.transformations.expectations_processor import (
    parse_event_log_expectations,
    split_clean_and_quarantine,
)


@pytest.mark.unit
def test_quarantine_staging_expectation_mode_sabotage():
    """
    Scenario:
    We ingest 3 FinOps events into a Lakeflow Quarantine pipeline:
      - E1: valid (`amount=50.0`)
      - E2: invalid (`event_id=None`, `amount=10.0`)
      - E3: invalid (`amount=-25.0`)
    Requirements:
      - `clean_events` receives 1 valid row (`E1`).
      - `quarantine_events` receives 2 invalid rows (`E2`, `E3`) for auditing & replay.
    """
    records = [
        {"event_id": "E1", "amount": 50.0},
        {"event_id": None, "amount": 10.0},
        {"event_id": "E3", "amount": -25.0},
    ]
    rules = {
        "valid_event_id": lambda r: r["event_id"] is not None,
        "positive_amount": lambda r: r["amount"] > 0,
    }

    # --- FIXED BUG 1: Use `staging_expectation_mode="retain"` (`@dp.expect_all`) on `silver_staging` ---
    # Retains invalid rows so they can be routed to `quarantine_events` while still recording metrics!
    clean_rows, quarantine_rows, metrics = split_clean_and_quarantine(
        records,
        rules,
        staging_expectation_mode="retain",
    )

    assert len(clean_rows) == 1
    assert len(quarantine_rows) == 2, (
        f"Expected 2 rows in quarantine_events, but got {len(quarantine_rows)}! "
        "Using `@dp.expect_all_or_drop` on the staging view dropped invalid rows before quarantine!"
    )
    assert metrics["valid_event_id"]["failed_records"] == 1
    assert metrics["positive_amount"]["failed_records"] == 1


@pytest.mark.unit
def test_event_log_expectation_metrics_sabotage():
    """
    Scenario:
    Query the Lakeflow pipeline Event Log to extract `data_quality.expectations` metrics.
    """
    # --- FIXED BUG 2: Use `event_type = "flow_progress"` where Lakeflow emits expectation metrics ---
    event_log_rows = [
        {
            "event_type": "flow_progress",
            "details": {
                "flow_progress": {
                    "data_quality": {
                        "expectations": [
                            {
                                "name": "valid_event_id",
                                "dataset": "silver_staging",
                                "passed_records": 2,
                                "failed_records": 1,
                            }
                        ]
                    }
                }
            },
        }
    ]

    parsed = parse_event_log_expectations(event_log_rows)
    assert len(parsed) == 1, (
        "Expected 1 expectation metric row from Event Log, got 0! "
        "Check the `event_type` where Lakeflow writes `data_quality.expectations`."
    )
    assert parsed[0]["failed_records"] == 1
