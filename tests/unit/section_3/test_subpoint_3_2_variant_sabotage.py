"""
Sabotage Drill — Section 3, Subpoint 3.2: Using the VARIANT Data Type for Semi-Structured Data
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_3/test_subpoint_3_2_variant_sabotage.py -v
"""

import pytest

from src.transformations.variant_processor import (
    explode_variant_collection,
    extract_variant_field,
    is_variant_null,
    parse_json_variant,
)


@pytest.mark.unit
def test_variant_schema_drift_and_null_semantics_sabotage():
    """
    Scenario:
    We ingest two webhook payloads into a VARIANT column `payload`:
      - Row 1 (`raw_row_1`): '{"event_id": "E1", "amount": "49.95", "discount": null}'
      - Row 2 (`raw_row_2`): '{"event_id": "E2", "amount": "free", "discount": 5.0}'
        (Upstream marketing team sent `"amount": "free"` instead of a number!)

    Requirements:
      1. Extract `amount` as a `DOUBLE` safely so `"free"` becomes `None` (SQL `NULL`) without crashing.
      2. Check whether Row 1's `"discount": null` is a JSON null using the proper VARIANT null check.
    """
    v1 = parse_json_variant('{"event_id": "E1", "amount": "49.95", "discount": null}', safe_mode=True)
    v2 = parse_json_variant('{"event_id": "E2", "amount": "free", "discount": 5.0}', safe_mode=True)

    # --- FIXED BUG 1A: Use `TRY_VARIANT_GET` (`safe_cast=True`) so `"amount": "free"` returns `None` (`NULL`) ---
    amount_1 = extract_variant_field(v1, "amount", target_type="DOUBLE", safe_cast=True)
    amount_2 = extract_variant_field(v2, "amount", target_type="DOUBLE", safe_cast=True)

    assert amount_1 == 49.95
    assert amount_2 is None

    # --- FIXED BUG 1B: Use `IS_VARIANT_NULL(payload:discount)` (`is_variant_null`) to check explicit JSON `null` ---
    raw_discount_variant = extract_variant_field(v1, "discount", target_type="VARIANT")
    assert is_variant_null(raw_discount_variant) is True, (
        "Expected True for explicit JSON `null` inside an uncast VARIANT column using IS_VARIANT_NULL()."
    )


@pytest.mark.unit
def test_variant_array_flattening_sabotage():
    """
    Scenario:
    A webhook row contains a JSON array of tags inside the `VARIANT` column `payload`:
      '{"event_id": "E1", "tags": ["finops", "serverless"]}'
    The engineer wants to flatten `payload:tags` into one row per tag.
    """
    v1 = parse_json_variant('{"event_id": "E1", "tags": ["finops", "serverless"]}', safe_mode=True)

    # --- FIXED BUG 2: Use `VARIANT_EXPLODE` (`LATERAL VARIANT_EXPLODE(payload:tags)`) on VARIANT arrays ---
    exploded_rows = explode_variant_collection(v1, key="tags", generator_fn="VARIANT_EXPLODE")

    assert len(exploded_rows) == 2
    assert exploded_rows[0] == {"pos": 0, "key": None, "value": "finops"}
    assert exploded_rows[1] == {"pos": 1, "key": None, "value": "serverless"}
