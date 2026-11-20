"""
Sabotage Drill — Section 6, Official Subpoint 6.1: Least-Privilege ACLs
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_6/test_subpoint_6_1_acls_sabotage.py -v
"""

import pytest

from src.security.uc_acl_validator import (
    validate_production_job_security,
    validate_uc_table_read_access,
)


@pytest.mark.unit
def test_uc_3_tier_traversal_sabotage():
    """
    Scenario:
    An admin wants `finops_analysts` to query `portfolio_catalog.gold.revenue_view`.
    They granted `BROWSE` on `portfolio_catalog` and `SELECT` on `revenue_view`,
    assuming `BROWSE` lets users run queries.
    """
    # --- FIXED BUG 1: Grant `USE CATALOG` on catalog and `USE SCHEMA` on schema alongside `SELECT` ---
    assert (
        validate_uc_table_read_access(
            catalog_grants={"USE CATALOG"},
            schema_grants={"USE SCHEMA"},
            table_grants={"SELECT"},
        )
        is True
    )


@pytest.mark.unit
def test_job_least_privilege_acl_and_run_as_sabotage():
    """
    Scenario:
    A production FinOps Lakeflow Job must:
      1. Run under a Service Principal (`service_principal_name`) so it never breaks if an employee leaves.
      2. Grant `oncall_ops` the least-privilege ACL needed to trigger, cancel, and Repair & Rerun failed runs
         WITHOUT permission to edit the job's code or tasks.
    """
    # --- FIXED BUG 2: Run as a Service Principal and grant `oncall_ops` least-privilege `CAN_MANAGE_RUN` ---
    job_spec = {
        "run_as": {"service_principal_name": "sp-finops-prod"},
        "permissions": {"oncall_ops": "CAN_MANAGE_RUN"},
    }

    assert validate_production_job_security(job_spec) is True
