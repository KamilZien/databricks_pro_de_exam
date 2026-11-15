"""
tests/unit/section_3/test_subpoint_3_1_window_and_joins_sabotage.py — Sabotage Drill.

Verifies:
1. Window Framing: `ROWS BETWEEN` (physical row-by-row running total) vs default `RANGE BETWEEN`
   (logical value ties lumped together when `ORDER BY` is specified without `rowsBetween`).
2. Stream-Stream Left Outer Join (Law 6): Mandatory watermarks on BOTH streams AND an
   event-time range condition (`BETWEEN ... AND ... + INTERVAL ...`).

⚠️ WARNING: Contains 2 REAL ARCHITECTURAL / CODE SABOTAGE BUGS:
  - Sabotage Bug 1: Calling `evaluate_window_running_totals` with `use_physical_rows_frame=False`
    (Spark's default `RANGE` frame when `ORDER BY` is used without `rowsBetween`), which lumps
    both Day-2 transactions together (`60.0` instead of `30.0` on T2)!
  - Sabotage Bug 2: A Stream-Stream `leftOuter` join that defines watermarks on both streams
    but OMITS the mandatory event-time range condition (`payment_ts BETWEEN order_ts AND order_ts + INTERVAL 1 HOUR`)!
"""

import pytest

from src.transformations.advanced_sql_transforms import (
    evaluate_window_running_totals,
    validate_stream_stream_join_spec,
)


@pytest.mark.unit
def test_window_rows_vs_range_running_total_sabotage():
    """
    Verify row-by-row running total when two transactions occur on the same event_day (Day 2):
      - T1 (Day 1): $10.0 -> running_total = 10.0
      - T2 (Day 2): $20.0 -> running_total = 30.0 (physical row-by-row)
      - T3 (Day 2): $30.0 -> running_total = 60.0
      - T4 (Day 5): $40.0 -> running_total = 100.0
    """
    transactions = [
        {"txn_id": "T1", "user_id": "U1", "event_day": 1, "amount": 10.0},
        {"txn_id": "T2", "user_id": "U1", "event_day": 2, "amount": 20.0},  # Tied on Day 2
        {"txn_id": "T3", "user_id": "U1", "event_day": 2, "amount": 30.0},  # Tied on Day 2
        {"txn_id": "T4", "user_id": "U1", "event_day": 5, "amount": 40.0},
    ]

    # --- FIXED BUG 1: Physical ROWS Frame (`use_physical_rows_frame=True`) ---
    # Using physical `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` processes tied Day-2 rows
    # sequentially row-by-row (`T2 = 10 + 20 = 30.0`, `T3 = 10 + 20 + 30 = 60.0`).
    rows = evaluate_window_running_totals(transactions, use_physical_rows_frame=True)

    assert rows["T1"]["running_total"] == 10.0
    assert rows["T2"]["running_total"] == 30.0, (
        f"Sequential row-by-row running total at T2 should be 30.0 (10+20), but got {rows['T2']['running_total']} "
        "because default RANGE frame summed both Day-2 rows (10+20+30=60.0)!"
    )
    assert rows["T3"]["running_total"] == 60.0
    assert rows["T4"]["running_total"] == 100.0


@pytest.mark.unit
def test_stream_stream_left_outer_join_sabotage():
    """
    Verify Stream-Stream Left Outer Join rules (Law 6):
    Requires watermarks on BOTH streams (`order_ts`, `payment_ts`) AND an event-time range
    condition (`payment_ts BETWEEN order_ts AND order_ts + INTERVAL 1 HOUR`).
    """
    # --- FIXED BUG 2: Event-Time Range Condition Added to Stream-Stream Left Outer Join ---
    # Both watermarks (`order_ts`, `payment_ts`) AND an explicit event-time range bound (`BETWEEN ... INTERVAL`)
    # are provided so Catalyst knows when an unmatched left order row can emit NULL and be evicted from RocksDB state.
    join_condition_sql = (
        "order_id = payment_order_id AND payment_ts BETWEEN order_ts AND order_ts + INTERVAL 1 HOUR"
    )

    is_valid = validate_stream_stream_join_spec(
        join_type="leftOuter",
        left_watermark_col="order_ts",
        right_watermark_col="payment_ts",
        join_condition_sql=join_condition_sql,
    )
    assert is_valid is True
