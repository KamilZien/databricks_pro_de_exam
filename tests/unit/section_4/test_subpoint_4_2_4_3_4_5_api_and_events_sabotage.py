"""
Sabotage Drill — Section 4 Gap-Fill (4.2, 4.3, 4.5): Jobs API Multi-Task Output & Pipeline Event Log
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_4/test_subpoint_4_2_4_3_4_5_api_and_events_sabotage.py -v
"""

import pytest

from src.monitoring.api_and_event_log import (
    extract_pipeline_audit_events,
    fetch_failed_task_error_trace,
)


@pytest.mark.unit
def test_multi_task_job_get_output_sabotage():
    """
    Scenario:
    A 2-task Lakeflow Job (`parent_run_id=9001`) failed on the `silver_transform` task (`task_run_id=9003`).
    Our monitoring script calls `runs/get-output` (`w.jobs.get_run_output`) to fetch the Python traceback.
    """
    run_payload = {
        "run_id": 9001,
        "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"},
        "tasks": [
            {"task_key": "bronze_ingest", "run_id": 9002, "state": {"result_state": "SUCCESS"}},
            {"task_key": "silver_transform", "run_id": 9003, "state": {"result_state": "FAILED"}},
        ],
    }
    outputs_by_run_id = {
        9002: {"error_trace": ""},
        9003: {"error_trace": "AnalysisException: Column 'amount' not found"},
    }

    # --- FIXED BUG 1: Pass the failed child task's `run_id` (9003), NOT parent job `run_id` (9001) ---
    trace = fetch_failed_task_error_trace(
        run_payload=run_payload,
        outputs_by_run_id=outputs_by_run_id,
        target_run_id=9003,
    )

    assert "AnalysisException" in trace


@pytest.mark.unit
def test_pipeline_event_log_user_action_sabotage():
    """
    Scenario:
    An audit query checks `event_log('<pipeline-id>')` to find who manually triggered the Lakeflow Pipeline.
    """
    event_log_rows = [
        {
            "event_type": "user_action",
            "details": {"user_action": {"user_name": "chinskagiera123@gmail.com", "action": "START"}},
        },
        {
            "event_type": "update_progress",
            "details": {"update_progress": {"state": "COMPLETED"}},
        },
    ]

    # --- FIXED BUG 2: Use official `event_type = "user_action"` ("job_action" does not exist in event_log!) ---
    audit_events = extract_pipeline_audit_events(event_log_rows, event_type_filter="user_action")

    assert len(audit_events) == 1
    assert audit_events[0]["details"]["user_action"]["action"] == "START"
