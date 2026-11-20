"""
Section 6, Official Subpoint 6.2: Attribute-Based Access Control (ABAC) with Governed Tags
Official Docs:
  - https://docs.databricks.com/en/data-governance/unity-catalog/abac/index.html
  - https://docs.databricks.com/en/data-governance/unity-catalog/filters-and-masks/index.html
"""

from typing import Any


def validate_abac_policy_spec(policy_spec: dict[str, Any]) -> bool:
    """
    Validate a Unity Catalog ABAC Policy definition:
      1. Tag used in `MATCH COLUMNS hasTagValue(...)` MUST be a **Governed Tag** (`is_governed_tag=True`)
         so table owners cannot tamper with or remove security tags.
      2. Mask / Row Filter UDF body must use Unity Catalog account-level `is_account_group_member(...)`,
         never legacy workspace-local `is_member(...)`.
      3. `ROW FILTER` UDF must return `BOOLEAN`; `COLUMN MASK` UDF must return the target column's type.
    """
    if not policy_spec.get("is_governed_tag", False):
        raise ValueError(
            "[ABAC_UNGOVERNED_TAG] ABAC security policies require a **Governed Tag** "
            "(centrally controlled allowed values & ASSIGN permissions) to prevent tag tampering."
        )

    udf_sql = policy_spec.get("udf_sql", "")
    if "is_member(" in udf_sql and "is_account_group_member(" not in udf_sql:
        raise ValueError(
            "[ABAC_LEGACY_IDENTITY_FN] Detected legacy workspace-local `is_member(...)`. "
            "Use Unity Catalog account-level `is_account_group_member(...)` instead."
        )

    policy_type = policy_spec.get("policy_type", "").upper()
    return_type = policy_spec.get("udf_return_type", "").upper()
    target_col_type = policy_spec.get("target_col_type", "STRING").upper()

    if policy_type == "ROW_FILTER" and return_type != "BOOLEAN":
        raise TypeError(
            f"[ROW_FILTER_INVALID_RETURN_TYPE] Row Filter UDF must return `BOOLEAN`, got `{return_type}`."
        )
    if policy_type == "COLUMN_MASK" and return_type != target_col_type:
        raise TypeError(
            f"[COLUMN_MASK_TYPE_MISMATCH] Column Mask UDF must return `{target_col_type}` to match target column, "
            f"got `{return_type}`."
        )

    return True


def evaluate_abac_policy_on_table(
    policy_spec: dict[str, Any],
    table_column_tags: dict[str, tuple[str, str]],
    user_groups: set[str],
    row: dict[str, Any],
) -> dict[str, Any]:
    """
    Simulate Unity Catalog automatically applying a Catalog-level ABAC Column Mask
    to ANY current or newly created table whose columns carry the matching Governed Tag.
    """
    validate_abac_policy_spec(policy_spec)

    tag_key = policy_spec["match_tag_key"]
    tag_val = policy_spec["match_tag_value"]
    exempt_groups = set(policy_spec.get("except_groups", []))

    output = dict(row)
    if user_groups & exempt_groups:
        return output  # Exempt group sees raw unmasked values

    for col_name, (k, v) in table_column_tags.items():
        if k == tag_key and v == tag_val and col_name in output:
            output[col_name] = "***-**-****"
    return output
