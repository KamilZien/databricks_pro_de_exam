"""
Sabotage Drill — Section 4, Subpoint 4.2: Lakehouse Alerts & Streaming Progress Monitoring
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_4/test_subpoint_4_2_alerts_and_streaming_sabotage.py -v
"""

import pytest

from src.monitoring.lakehouse_alerts import (
    diagnose_streaming_progress,
    evaluate_lakehouse_alert,
)


@pytest.mark.unit
def test_lakehouse_alert_first_row_trap_sabotage():
    """
    Scenario:
    A Databricks Lakehouse Alert is configured to fire (`TRIGGERED`) whenever ANY region's
    `quarantine_count > 50`.
    Today, `US` has 2 quarantined rows and `EU` has 150 quarantined rows.

    Because Databricks Lakehouse Alerts ONLY evaluate the FIRST row returned by the query,
    an unaggregated `GROUP BY region` ordered alphabetically returns `US` (2) first and stays `OK`!
    """
    # --- FIXED BUG 1: Aggregate (`MAX(quarantine_count)`) or `ORDER BY quarantine_count DESC LIMIT 1` ---
    # Ensures Row 0 always reflects the worst region breach (`150`)!
    aggregated_query_rows = [
        {"region": "EU", "quarantine_count": 150},
    ]

    result = evaluate_lakehouse_alert(
        query_rows=aggregated_query_rows,
        metric_col="quarantine_count",
        threshold=50,
    )

    assert result["state"] == "TRIGGERED", (
        f"Alert returned {result['state']} (evaluated Row 0 = {result['evaluated_value']}) "
        "even though EU had 150 quarantined rows! Lakehouse Alerts only inspect Row 0."
    )


@pytest.mark.unit
def test_streaming_progress_state_growth_sabotage():
    """
    Scenario:
    A stateful Structured Streaming deduplication/aggregation query emits 4 micro-batch `lastProgress` reports:
      - `inputRowsPerSecond`: ~500, `processedRowsPerSecond`: ~800 (throughput is healthy, no backlog!)
      - `stateOperators[0].numRowsTotal`: 1,000 -> 2,500 -> 4,200 -> 6,100 (climbing every batch!)
    """
    batch_history = [
        {"batchId": 0, "inputRowsPerSecond": 500, "processedRowsPerSecond": 800, "stateOperators": [{"numRowsTotal": 1000}]},  # noqa: E501
        {"batchId": 1, "inputRowsPerSecond": 510, "processedRowsPerSecond": 810, "stateOperators": [{"numRowsTotal": 2500}]},  # noqa: E501
        {"batchId": 2, "inputRowsPerSecond": 490, "processedRowsPerSecond": 790, "stateOperators": [{"numRowsTotal": 4200}]},  # noqa: E501
        {"batchId": 3, "inputRowsPerSecond": 520, "processedRowsPerSecond": 805, "stateOperators": [{"numRowsTotal": 6100}]},  # noqa: E501
    ]

    # --- FIXED BUG 2: Enable `check_state_growth=True` to catch missing `.withWatermark(...)` ---
    health = diagnose_streaming_progress(batch_history, check_state_growth=True)

    assert "UNBOUNDED_STATE_GROWTH" in health["alerts"], (
        f"Expected UNBOUNDED_STATE_GROWTH alert for monotonically rising numRowsTotal, got {health}!"
    )
