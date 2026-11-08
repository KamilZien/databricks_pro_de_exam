"""
Test Helper — Local Simulator for Lakeflow Job Control-Flow Operators (Subpoint 1.9).

Used exclusively by unit tests (`tests/test_subpoint_1_9_control_flow_sabotage.py`)
to test `condition_task` and `for_each_task` parameter resolution locally and in GitHub CI/CD.
"""

import json
from typing import Any


def evaluate_condition_task(left_val: Any, op: str, right_val: Any) -> str:
    """
    Simulates the Databricks Serverless Control Plane evaluation of a `condition_task`.
    Returns the string outcome ("true" or "false") used by downstream `depends_on.outcome`.
    """
    try:
        lhs = float(left_val)
        rhs = float(right_val)
    except (ValueError, TypeError):
        lhs = str(left_val)
        rhs = str(right_val)

    ops = {
        "EQUAL_TO": lambda a, b: a == b,
        "NOT_EQUAL": lambda a, b: a != b,
        "GREATER_THAN": lambda a, b: a > b,
        "GREATER_THAN_OR_EQUAL": lambda a, b: a >= b,
        "LESS_THAN": lambda a, b: a < b,
        "LESS_THAN_OR_EQUAL": lambda a, b: a <= b,
    }

    if op not in ops:
        raise ValueError(f"Unsupported condition_task operator: {op}")

    result = ops[op](lhs, rhs)
    return "true" if result else "false"


def execute_for_each_task(inputs_json: str, child_task_spec: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Simulates a Lakeflow `for_each_task` iterating over a JSON array and binding `{{input}}`.
    Enforces the Databricks architectural rule that a `for_each_task` CANNOT contain another nested `for_each_task`.
    """
    if "for_each_task" in child_task_spec:
        raise ValueError(
            "NESTED_FOR_EACH_NOT_SUPPORTED: A for_each_task cannot directly contain another nested for_each_task."
        )

    parsed_inputs = json.loads(inputs_json)
    if not isinstance(parsed_inputs, list):
        raise ValueError("FOR_EACH_INPUTS_MUST_BE_ARRAY: `inputs` must evaluate to a valid JSON array.")

    base_params = child_task_spec.get("notebook_task", {}).get("base_parameters", {})
    executed_iterations = []

    for item in parsed_inputs:
        resolved_params = {}
        for k, v in base_params.items():
            if v == "{{input}}":
                resolved_params[k] = item
            elif isinstance(item, dict) and v.startswith("{{input.") and v.endswith("}}"):
                field_name = v[len("{{input.") : -2]
                resolved_params[k] = item.get(field_name)
            else:
                resolved_params[k] = v

        executed_iterations.append(
            {
                "child_task_key": child_task_spec["task_key"],
                "input_element": item,
                "resolved_parameters": resolved_params,
                "status": "SUCCESS",
            }
        )

    return executed_iterations
