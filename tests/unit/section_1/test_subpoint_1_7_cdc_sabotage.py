r"""
Module 5 — Subpoint 1.7 Sabotage Drill: AUTO CDC, SCD Types & Out-Of-Order Sequencing.

This test suite rigorously validates:
  1. SCD Type 1 behavior (in-place overwrite, current state only, deleted keys removed).
  2. SCD Type 2 behavior (__start_at and __end_at generation, historical timeline).
  3. SEQUENCE BY correctness (handling out-of-order late arriving events without corrupting state).

⚠️ WARNING: This test contains 2 REAL-WORLD, EXAM-CALIBRATED SABOTAGE BUGS:
  - Bug 1 (The Out-of-Order Overwrite Trap):
    An engineer assumed incoming order dictates the state instead of SEQUENCE BY timestamp,
    causing an older delayed event to corrupt a newer record.
  - Bug 2 (The SCD Type 2 Active Record Flag Trap):
    An engineer expects active SCD Type 2 records to have __end_at set to '9999-12-31'
    instead of the official Databricks Lakeflow standard (NULL).

Your mission:
  Diagnose both bugs and fix them so the test passes green!
"""

import importlib

import pytest

from src.generator.cdc_stream_generator import generate_customer_cdc_events

cdc_pipeline_module = importlib.import_module("src.pipelines.05_auto_cdc_pipeline")
simulate_scd1_merge = cdc_pipeline_module.simulate_scd1_merge
simulate_scd2_merge = cdc_pipeline_module.simulate_scd2_merge


@pytest.mark.unit
def test_scd1_and_sequence_by_sabotage():
    """
    Verifies that SEQUENCE BY prevents late-arriving older events
    from overwriting newer committed data.
    """
    events = generate_customer_cdc_events()
    final_scd1 = simulate_scd1_merge(events)

    # Customer 102 was DELETED with operation='DELETE'
    assert "CUST-102" not in final_scd1, "CUST-102 should be removed under apply_as_deletes"

    # Customer 103 had an out-of-order event:
    # 10:00 -> TRIAL
    # 12:00 -> VIP (Arrived second in stream)
    # 11:00 -> TRIAL in FR (Arrived third in stream, but earlier timestamp!)
    cust_103 = final_scd1["CUST-103"]

    # --- RESOLVED SABOTAGE BUG 1: The Out-of-Order Trap ---
    # In AUTO CDC, because we specify `SEQUENCE BY event_timestamp`, the 12:00 event (VIP)
    # WINS over the late 11:00 event (TRIAL), regardless of arrival order.
    assert cust_103["tier"] == "VIP", f"Expected tier to be sequenced by timestamp (VIP), got {cust_103['tier']}"


@pytest.mark.unit
def test_scd2_timeline_and_null_end_at_sabotage():
    """
    Verifies SCD Type 2 timeline generation and system column standards.
    """
    events = generate_customer_cdc_events()
    timeline = simulate_scd2_merge(events)

    # Customer 101 had 3 non-delete events:
    # 1. 10:00 (STANDARD, DE) -> __start_at: 10:00, __end_at: 12:00
    # 2. 12:00 (ENTERPRISE, DE) -> __start_at: 12:00, __end_at: 15:00
    # 3. 15:00 (ENTERPRISE, CH) -> __start_at: 15:00, __end_at: NULL (CURRENT ACTIVE)
    cust_101_history = [r for r in timeline if r["customer_id"] == "CUST-101"]
    assert len(cust_101_history) == 3, f"Expected 3 historical versions, got {len(cust_101_history)}"

    latest_active_version = cust_101_history[-1]

    # --- RESOLVED SABOTAGE BUG 2: Databricks SCD2 Convention ---
    # Official Databricks Lakeflow AUTO CDC designates __end_at as NULL (None) for currently active rows.
    assert latest_active_version["__end_at"] is None, (
        "Exam Rule: Databricks AUTO CDC sets __end_at to NULL for the currently active row!"
    )
