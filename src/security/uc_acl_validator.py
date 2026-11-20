"""
Section 6, Official Subpoint 6.1: Least-Privilege ACLs (Unity Catalog & Workspace Resources)
Official Docs:
  - https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html
  - https://docs.databricks.com/en/security/auth/access-control/index.html
"""

from typing import Any


def validate_uc_table_read_access(
    catalog_grants: set[str],
    schema_grants: set[str],
    table_grants: set[str],
) -> bool:
    """
    Enforces the Unity Catalog 3-tier traversal rule for `SELECT` on a table or view:
      1. `USE CATALOG` on the parent catalog (`BROWSE` alone is NOT enough!)
      2. `USE SCHEMA` on the parent schema
      3. `SELECT` on the table/view (or inherited from schema/catalog)
    """
    has_use_catalog = "USE CATALOG" in catalog_grants or "ALL PRIVILEGES" in catalog_grants
    has_use_schema = "USE SCHEMA" in schema_grants or "ALL PRIVILEGES" in schema_grants
    has_select = (
        "SELECT" in table_grants
        or "SELECT" in schema_grants
        or "SELECT" in catalog_grants
        or "ALL PRIVILEGES" in table_grants
    )

    missing = []
    if not has_use_catalog:
        missing.append("USE CATALOG (catalog level)")
    if not has_use_schema:
        missing.append("USE SCHEMA (schema level)")
    if not has_select:
        missing.append("SELECT (table/view level)")

    if missing:
        raise PermissionError(
            f"[UC_INSUFFICIENT_PRIVILEGES] Missing required Unity Catalog grants: {', '.join(missing)}. "
            "Note: BROWSE only allows metadata discovery in Catalog Explorer, not query traversal."
        )
    return True


def validate_production_job_security(job_security_spec: dict[str, Any]) -> bool:
    """
    Enforces least-privilege workspace ACLs for a production Lakeflow Job:
      1. `run_as` must be a `service_principal_name` (not a human `user_name`).
      2. On-call operations group (`oncall_ops`) must have `CAN_MANAGE_RUN` (trigger/repair runs without
         editing production task code — never over-privileged `CAN_MANAGE`).
    """
    run_as = job_security_spec.get("run_as", {})
    if "service_principal_name" not in run_as:
        raise PermissionError(
            f"[JOB_RUN_AS_HUMAN_USER] Production job must execute as a Service Principal "
            f"(`service_principal_name`), not a human user: {run_as!r}."
        )

    ops_level = job_security_spec.get("permissions", {}).get("oncall_ops")
    if ops_level != "CAN_MANAGE_RUN":
        raise PermissionError(
            f"[JOB_ACL_VIOLATION] Group `oncall_ops` has {ops_level!r}; least-privilege for triggering/repairing "
            "without editing production job definitions is `CAN_MANAGE_RUN`."
        )

    return True
