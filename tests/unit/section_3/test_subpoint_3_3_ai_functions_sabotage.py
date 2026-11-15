"""
Sabotage Drill — Section 3, Subpoint 3.3: Leveraging Built-In AI Functions (`ai_query` & Task-Specific AI Functions)
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_3/test_subpoint_3_3_ai_functions_sabotage.py -v
"""

import pytest

from src.transformations.ai_functions_processor import (
    simulate_ai_extract,
    simulate_ai_query_batch,
)


@pytest.mark.unit
def test_ai_query_fail_on_error_envelope_sabotage():
    """
    Scenario:
    We call `ai_query('databricks-meta-llama-3-3-70b-instruct', prompt, failOnError => ...)`
    across a micro-batch of 2 rows where Row 2 hits a transient HTTP 429 Rate Limit error (`"TRIGGER_429"`).

    Requirement:
    The query must NOT crash when Row 2 fails; instead it should return the `STRUCT<result, errorMessage>`
    envelope so valid rows succeed (`row["result"]`) and failed rows go to quarantine (`row["errorMessage"]`).
    """
    prompts = ["Billing issue #101", "TRIGGER_429"]

    # --- FIXED BUG 1: Set `fail_on_error=False` (`failOnError => false`) ---
    # Captures transient HTTP 429 errors in `STRUCT<result, errorMessage>` without aborting the batch!
    batch_results = simulate_ai_query_batch(
        endpoint_name="databricks-meta-llama-3-3-70b-instruct",
        requests=prompts,
        return_type="STRING",
        fail_on_error=False,
    )

    assert batch_results[0]["result"] == "CLASSIFIED:Billing issue #101"
    assert batch_results[0]["errorMessage"] is None
    assert batch_results[1]["result"] is None
    assert "429" in batch_results[1]["errorMessage"]


@pytest.mark.unit
def test_task_specific_ai_extract_and_custom_ml_struct_sabotage():
    """
    Scenario:
      1. Use `ai_extract(body, ARRAY('person', 'invoice'))` to extract a STRUCT of entities from support text.
      2. Use `ai_query('finops-churn-xgb', named_struct(...), returnType => 'DOUBLE')` to pass multiple
         numeric columns (`monthly_spend`, `support_calls`) to a custom ML endpoint.
    """
    ticket_body = "Hi, I am Alice and my invoice #99 is wrong!"

    # --- FIXED BUG 2A: Pass `labels` as an ARRAY (`list[str]`) ---
    entities = simulate_ai_extract(ticket_body, labels=["person", "invoice"])
    assert entities["person"] == "Alice"
    assert entities["invoice"] == "#99"

    # --- FIXED BUG 2B: Package multi-feature ML columns in a `named_struct` dict ---
    ml_predictions = simulate_ai_query_batch(
        endpoint_name="finops-churn-xgb",
        requests=[{"monthly_spend": 200.0, "support_calls": 3}],
        return_type="DOUBLE",
        fail_on_error=True,
        expect_multi_feature_struct=True,
    )
    assert ml_predictions[0] == 0.5
