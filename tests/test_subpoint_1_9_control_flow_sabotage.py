r"""
Module 7 — Subpoint 1.9 Sabotage Drill: Lakeflow Jobs Control-Flow (If/Else & For Each).

This test suite verifies:
  1. `condition_task` evaluation and `outcome` routing.
  2. `for_each_task` parameter binding (`{{input}}` vs `{{inputs}}` trap).
  3. The architectural restriction against nesting a `for_each_task` inside another `for_each_task`.

⚠️ WARNING: This file contains 2 INTENTIONAL EXAM-CALIBRATED SABOTAGE BUGS:
  - Bug 1 (The Plural "{{inputs}}" Parameter Trap):
    An engineer configured `base_parameters: {"region_code": "{{inputs}}"}` (plural)
    instead of `"{{input}}"` (singular), causing the child task to fail!
  - Bug 2 (The Nested For-Each Trap):
    An engineer assumed a `for_each_task` can directly contain another `for_each_task` without raising an error.

Your mission:
  Diagnose both bugs and fix them so the test suite passes 100% green!
"""

import pytest

from tests.helpers.control_flow_sim import evaluate_condition_task, execute_for_each_task


@pytest.mark.unit
def test_condition_task_and_for_each_input_binding_sabotage():
    """
    Verifies condition_task evaluation and for_each_task "{{input}}" parameter resolution.
    """
    # 1. Verify condition_task evaluation (58 > 50 -> "true")
    outcome = evaluate_condition_task(left_val=58, op="GREATER_THAN", right_val=50)
    assert outcome == "true"

    # --- RESOLVED SABOTAGE BUG 1: Singular "{{input}}" ---
    child_spec = {
        "task_key": "regional_settlement_iteration",
        "notebook_task": {
            "notebook_path": "../src/notebooks/task_regional_processor.py",
            "base_parameters": {"region_code": "{{input}}"},
        },
    }

    iterations = execute_for_each_task(inputs_json='["US", "DE", "UK"]', child_task_spec=child_spec)
    assert len(iterations) == 3
    assert iterations[0]["resolved_parameters"]["region_code"] == "US", (
        f"Expected 'US', but got '{iterations[0]['resolved_parameters']['region_code']}' "
        "because base_parameters used '{{inputs}}' instead of '{{input}}'!"
    )


@pytest.mark.unit
def test_nested_for_each_restriction_sabotage():
    """
    Verifies the Databricks Lakeflow rule that a `for_each_task` CANNOT directly nest
    another `for_each_task` inside it.
    """
    invalid_nested_spec = {
        "task_key": "inner_loop",
        "for_each_task": {
            "inputs": '["A", "B"]',
            "task": {"task_key": "leaf_task"},
        },
    }

    # --- RESOLVED SABOTAGE BUG 2: Nested For-Each is Forbidden ---
    with pytest.raises(ValueError, match="NESTED_FOR_EACH_NOT_SUPPORTED"):
        execute_for_each_task(inputs_json='["US", "DE"]', child_task_spec=invalid_nested_spec)

    is_nested_for_each_allowed = False
    assert is_nested_for_each_allowed is False, (
        "Exam Rule: Lakeflow Jobs do NOT allow nesting a For Each task directly inside another For Each task!"
    )
