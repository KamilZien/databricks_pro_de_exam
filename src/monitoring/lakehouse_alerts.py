"""
Section 4, Subpoint 4.2: Databricks Lakehouse Alerts, Streaming Metrics & Billing Alerts
Official Docs:
  - https://docs.databricks.com/en/sql/user/alerts/index.html
  - https://docs.databricks.com/en/structured-streaming/query-monitoring.html
  - https://docs.databricks.com/en/admin/system-tables/billing.html

Provides:
  1. `evaluate_lakehouse_alert`: Faithfully emulates Databricks Lakehouse Alerts, which ONLY
     inspect the FIRST row (`query_rows[0]`) returned by the backing SQL query!
  2. `diagnose_streaming_progress`: Inspects `query.lastProgress` snapshots across micro-batches
     to detect:
       - `PROCESSING_BACKLOG`: `inputRowsPerSecond > processedRowsPerSecond` across consecutive batches.
       - `UNBOUNDED_STATE_GROWTH`: `stateOperators[0].numRowsTotal` monotonically climbing without eviction
         (classic symptom of a missing watermark in stateful streaming).
"""

from typing import Any


def evaluate_lakehouse_alert(
    query_rows: list[dict[str, Any]],
    metric_col: str,
    threshold: float,
) -> dict[str, Any]:
    """
    Emulates Databricks Lakehouse Alert evaluation:
      - If `query_rows` is empty -> state is `"UNKNOWN"`.
      - Otherwise inspects ONLY `query_rows[0][metric_col]` > `threshold`!
        (Multi-row unaggregated queries silently ignore Row 2..N!)
    """
    if not query_rows:
        return {"state": "UNKNOWN", "evaluated_value": None}

    first_row_val = float(query_rows[0][metric_col])
    state = "TRIGGERED" if first_row_val > threshold else "OK"
    return {"state": state, "evaluated_value": first_row_val}


def diagnose_streaming_progress(
    batch_progress_list: list[dict[str, Any]],
    check_state_growth: bool = True,
) -> dict[str, Any]:
    """
    Analyze Structured Streaming `lastProgress` snapshots across micro-batches.
    """
    if not batch_progress_list:
        return {"status": "NO_DATA", "alerts": []}

    alerts: list[str] = []

    # 1. Check if input rate consistently exceeds processing rate
    falling_behind_batches = sum(
        1
        for b in batch_progress_list
        if b.get("inputRowsPerSecond", 0.0) > b.get("processedRowsPerSecond", 0.0)
    )
    if falling_behind_batches >= max(2, len(batch_progress_list) - 1):
        alerts.append("PROCESSING_BACKLOG")

    # 2. Check if RocksDB state (`numRowsTotal`) is strictly monotonically increasing (missing watermark)
    if check_state_growth:
        state_sizes = [
            b["stateOperators"][0]["numRowsTotal"]
            for b in batch_progress_list
            if b.get("stateOperators")
        ]
        if len(state_sizes) >= 3 and all(
            state_sizes[i] < state_sizes[i + 1] for i in range(len(state_sizes) - 1)
        ):
            alerts.append("UNBOUNDED_STATE_GROWTH")

    return {
        "status": "ALERT" if alerts else "HEALTHY",
        "alerts": alerts,
    }
