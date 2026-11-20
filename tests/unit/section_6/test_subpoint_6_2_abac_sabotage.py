"""
Sabotage Drill — Section 6, Official Subpoint 6.2: ABAC Policies with Governed Tags
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_6/test_subpoint_6_2_abac_sabotage.py -v
"""

import pytest

from src.security.abac_policies import (
    evaluate_abac_policy_on_table,
    validate_abac_policy_spec,
)


@pytest.mark.unit
def test_abac_column_mask_governed_tag_and_identity_sabotage():
    """
    Scenario:
    A Catalog-level ABAC policy automatically masks any column tagged `('pii', 'ssn')` across
    all current and future tables in `portfolio_catalog` for non-HR users.
    """
    # --- FIXED BUG 1: Use a Governed Tag (`is_governed_tag=True`) and `is_account_group_member(...)` ---
    policy = {
        "policy_type": "COLUMN_MASK",
        "is_governed_tag": True,
        "match_tag_key": "pii",
        "match_tag_value": "ssn",
        "except_groups": ["hr_pii_readers"],
        "target_col_type": "STRING",
        "udf_return_type": "STRING",
        "udf_sql": "RETURN CASE WHEN is_account_group_member('hr_pii_readers') THEN ssn ELSE '***-**-****' END",
    }

    # Newly created future table `gold.customers_2027` tagged with ('pii', 'ssn')
    future_table_tags = {"customer_ssn": ("pii", "ssn")}
    sample_row = {"customer_id": "C1", "customer_ssn": "123-45-6789"}

    masked = evaluate_abac_policy_on_table(
        policy_spec=policy,
        table_column_tags=future_table_tags,
        user_groups={"finops_analysts"},
        row=sample_row,
    )
    assert masked["customer_ssn"] == "***-**-****"


@pytest.mark.unit
def test_abac_row_filter_return_type_sabotage():
    """
    Scenario:
    A Catalog-level ABAC Row Filter restricts non-admin users to rows where `region = 'EU'`.
    """
    # --- FIXED BUG 2: Row Filter UDF returns `BOOLEAN` ---
    row_filter_policy = {
        "policy_type": "ROW_FILTER",
        "is_governed_tag": True,
        "match_tag_key": "data_residency",
        "match_tag_value": "eu_only",
        "udf_return_type": "BOOLEAN",
        "udf_sql": "RETURN is_account_group_member('global_admins') OR region = 'EU'",
    }

    assert validate_abac_policy_spec(row_filter_policy) is True
