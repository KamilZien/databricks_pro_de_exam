"""
Section 3, Subpoint 3.3: Leveraging Built-In AI Functions (`ai_query` & Task-Specific AI Functions)
Official Docs:
  - https://docs.databricks.com/en/large-language-models/ai-functions.html
  - https://docs.databricks.com/en/sql/language-manual/functions/ai_query.html

Provides:
  1. SQL query builders for Databricks task-specific AI functions (`ai_mask`, `ai_extract`, `ai_classify`)
     and general-purpose `ai_query()` (with `returnType`, `failOnError => false`, and `named_struct`).
  2. Deterministic emulators that enforce Databricks Spark SQL semantics for:
     - `ai_extract(text, labels)` requiring an `ARRAY<STRING>` (list of strings) and returning a `STRUCT` (dict).
     - `ai_query(endpoint, request, return_type, fail_on_error)`:
       * When `fail_on_error=True` (default): raises runtime exception if any row fails endpoint inference.
       * When `fail_on_error=False`: returns `{"result": <val_or_None>, "errorMessage": <err_or_None>}`.
       * When querying a multi-feature custom ML model, requires `request` to be passed as a `named_struct` (`dict`).
"""

from typing import Any


def simulate_ai_extract(text: str, labels: list[str] | str) -> dict[str, str | None]:
    """
    Emulates Databricks `ai_extract(content, labels)`.
    - `labels` MUST be an `ARRAY<STRING>` (`list[str]`), never a comma-separated `str`!
    - Returns a `STRUCT` (`dict`) keyed by each label in `labels`.
    """
    if not isinstance(labels, list):
        raise TypeError(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] `ai_extract(content, labels)` requires parameter 2 (`labels`) "
            f"to be of type `ARRAY<STRING>`, but received `{type(labels).__name__}` ({labels!r}). "
            "Use `ARRAY('person', 'invoice')` in SQL or a list of strings."
        )

    extracted: dict[str, str | None] = {}
    for label in labels:
        if label == "person" and "Alice" in text:
            extracted["person"] = "Alice"
        elif label == "invoice" and "#99" in text:
            extracted["invoice"] = "#99"
        else:
            extracted[label] = None
    return extracted


def simulate_ai_query_batch(
    endpoint_name: str,
    requests: list[Any],
    return_type: str = "STRING",
    fail_on_error: bool = True,
    expect_multi_feature_struct: bool = False,
) -> list[Any]:
    """
    Emulates Databricks `ai_query(endpoint, request, returnType => ..., failOnError => ...)`.

    Rules enforced:
      1. If `expect_multi_feature_struct=True` (custom MLflow / XGBoost endpoint expecting multiple features),
         each item in `requests` MUST be a `named_struct` (`dict`), not a flat tuple/list of unlabelled values.
      2. If a row triggers an endpoint failure (e.g., payload contains `"TRIGGER_429"`):
         - When `fail_on_error=True` (default `failOnError => true`), raises RuntimeError and aborts the entire batch.
         - When `fail_on_error=False` (`failOnError => false`), wraps every row's output in a 2-field STRUCT envelope:
           `{"result": <value_or_None>, "errorMessage": <error_or_None>}`.
    """
    if not endpoint_name or not isinstance(endpoint_name, str):
        raise ValueError("`ai_query` requires a valid Mosaic AI Model Serving endpoint name as the 1st argument.")

    outputs: list[Any] = []
    for req in requests:
        if expect_multi_feature_struct and not isinstance(req, dict):
            raise TypeError(
                "[AI_QUERY_INVALID_REQUEST_TYPE] Multi-feature ML endpoint requires features wrapped in "
                f"`named_struct('col1', col1, ...)` (a dict), but received `{type(req).__name__}`: {req!r}."
            )

        # Simulate a transient 429 rate-limit error on poison/throttled input
        if req == "TRIGGER_429" or (isinstance(req, dict) and req.get("trigger_error")):
            err_msg = "HTTP 429: Rate limit exceeded on endpoint " + endpoint_name
            if fail_on_error:
                    raise RuntimeError(
                        f"[AI_QUERY_ENDPOINT_ERROR] {err_msg}. "
                        "Set `failOnError => false` (`fail_on_error=False`) to capture errors in "
                        "`STRUCT<result, errorMessage>` without aborting the entire query."
                    )
            outputs.append({"result": None, "errorMessage": err_msg})
            continue

        # Compute simulated inference value
        if expect_multi_feature_struct and isinstance(req, dict):
            val: Any = round(float(req.get("monthly_spend", 0.0)) * 0.001 + float(req.get("support_calls", 0)) * 0.1, 2)
        elif return_type.upper() == "STRING":
            val = f"CLASSIFIED:{req}"
        else:
            val = {"urgency": "HIGH", "score": 0.95}

        if fail_on_error:
            outputs.append(val)
        else:
            outputs.append({"result": val, "errorMessage": None})

    return outputs
