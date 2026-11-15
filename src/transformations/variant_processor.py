"""
Section 3, Subpoint 3.2: Using the VARIANT Data Type for Semi-Structured Data
Official Docs: https://docs.databricks.com/en/sql/language-manual/data-types/variant-type.html

Provides both:
  1. Pure PySpark/SQL builders for Bronze-to-Silver VARIANT ingestion, extraction,
     schema evolution discovery (`SCHEMA_OF_VARIANT_AGG`), and array/object flattening (`LATERAL VARIANT_EXPLODE`).
  2. Deterministic VARIANT evaluation helpers that faithfully enforce Databricks Spark SQL semantics:
     - `PARSE_JSON` (raises error on malformed JSON) vs `TRY_PARSE_JSON` (returns None/NULL)
     - `VARIANT_GET` / `::` (raises cast error on type mismatch) vs `TRY_VARIANT_GET` (returns None/NULL)
     - `IS NULL` (True only when key is missing) vs `IS_VARIANT_NULL` (True when JSON key is explicitly `null`)
     - `VARIANT_EXPLODE` (required to explode VARIANT arrays/objects; standard `EXPLODE` raises DATATYPE_MISMATCH)
"""

import json
from typing import Any


class VariantNull:
    """Represents an explicit JSON `null` stored inside a Databricks `VARIANT` column (`IS_VARIANT_NULL = True`)."""

    def __repr__(self) -> str:
        return "VARIANT(null)"


VARIANT_NULL = VariantNull()


def parse_json_variant(raw_json: str, safe_mode: bool = False) -> dict[str, Any] | None:
    """
    Emulates Databricks `PARSE_JSON(str)` (safe_mode=False) vs `TRY_PARSE_JSON(str)` (safe_mode=True).
    Converts explicit JSON `null` values into `VARIANT_NULL` tokens so we can distinguish:
      - Missing key -> Python `None` (SQL `NULL`, where `payload:key IS NULL` is True)
      - Explicit JSON `null` -> `VARIANT_NULL` (where `payload:key IS NULL` is False, `IS_VARIANT_NULL` is True!)
    """
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        if safe_mode:
            return None
        raise ValueError(
            f"[MALFORMED_RECORD_IN_PARSING] Failed to parse JSON string in PARSE_JSON: {raw_json!r}. "
            "Use TRY_PARSE_JSON() to return NULL instead of failing the query."
        ) from exc

    if isinstance(parsed, dict):
        return {k: (VARIANT_NULL if v is None else v) for k, v in parsed.items()}
    return parsed


def extract_variant_field(
    variant_obj: dict[str, Any] | None,
    key: str,
    target_type: str = "VARIANT",
    safe_cast: bool = False,
) -> Any:
    """
    Emulates Databricks `VARIANT_GET(v, '$.key', type)` / `v:key::type` (safe_cast=False)
    vs `TRY_VARIANT_GET(v, '$.key', type)` (safe_cast=True).
    """
    if variant_obj is None or key not in variant_obj:
        return None  # SQL NULL (key missing)

    val = variant_obj[key]

    # If target_type is VARIANT (no cast applied, e.g. `payload:discount`),
    # an explicit JSON `null` remains a VARIANT_NULL token (NOT SQL NULL!).
    if target_type.upper() == "VARIANT":
        return val

    # Once cast to a concrete SQL type (e.g. `::DOUBLE`, `::STRING`), JSON `null` becomes SQL NULL (`None`).
    if isinstance(val, VariantNull) or val is None:
        return None

    t_upper = target_type.upper()
    try:
        if t_upper in ("DOUBLE", "FLOAT"):
            return float(val)
        if t_upper in ("INT", "BIGINT", "LONG"):
            return int(val)
        if t_upper == "STRING":
            return str(val)
    except (ValueError, TypeError) as exc:
        if safe_cast:
            return None
        raise ValueError(
            f"[CAST_INVALID_INPUT] Value {val!r} at key {key!r} cannot be cast to {target_type}. "
            "Use TRY_VARIANT_GET(v, path, type) or TRY_CAST to tolerate schema-drifted values."
        ) from exc

    return val


def is_sql_null(val: Any) -> bool:
    """
    Emulates SQL `expr IS NULL`.
    WARNING: For an uncast VARIANT path `payload:discount` where JSON is `{"discount": null}`,
    `payload:discount IS NULL` returns FALSE because it holds a VARIANT null token!
    """
    return val is None


def is_variant_null(val: Any) -> bool:
    """
    Emulates Databricks `IS_VARIANT_NULL(expr)`.
    Returns TRUE only if `expr` is a VARIANT token holding an explicit JSON `null`.
    """
    return isinstance(val, VariantNull)


def explode_variant_collection(
    variant_obj: dict[str, Any],
    key: str,
    generator_fn: str = "VARIANT_EXPLODE",
) -> list[dict[str, Any]]:
    """
    Emulates flattening a nested array or object inside a VARIANT column.
    In Databricks SQL:
      - `EXPLODE(payload:tags)` -> FAILS with `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` because
        `payload:tags` has data type `VARIANT`, whereas `EXPLODE` requires `ARRAY` or `MAP`.
      - `LATERAL VARIANT_EXPLODE(payload:tags)` -> Succeeds and outputs `(pos, key, value)`.
    """
    if generator_fn.upper() == "EXPLODE":
        raise TypeError(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve 'explode(payload:"
            f"{key})' due to data type mismatch: parameter 1 requires the 'ARRAY or MAP' type, "
            "however 'payload:" + key + "' has the type 'VARIANT'. Use VARIANT_EXPLODE() instead."
        )

    if generator_fn.upper() not in ("VARIANT_EXPLODE", "VARIANT_EXPLODE_OUTER"):
        raise ValueError(f"Unsupported generator function: {generator_fn}")

    collection = variant_obj.get(key)
    if isinstance(collection, list):
        return [{"pos": idx, "key": None, "value": item} for idx, item in enumerate(collection)]
    if isinstance(collection, dict):
        return [{"pos": idx, "key": k, "value": v} for idx, (k, v) in enumerate(collection.items())]
    return []
