"""
src/ingestion/opensharing_and_clean_rooms.py — Subpoint 2.4 OpenSharing & Clean Rooms.

Implements core protocol validation and logic:
1. classify_recipient_sharing_type: Distinguishes between Databricks-to-Databricks (D2D)
   and Databricks-to-Open (D2O) based on recipient configuration.
2. validate_d2o_credential_profile: Verifies external bearer token profile structure,
   ensuring proper endpoint, bearer token, and expiration timestamp.
3. generate_share_ddl: Constructs standard Unity Catalog SQL DDL for sharing tables,
   dynamic views, and Change Data Feed (WITH CHANGE DATA FEED).
"""

from datetime import UTC, datetime
from typing import Any


def classify_recipient_sharing_type(recipient_spec: dict[str, Any]) -> str:
    """
    Classifies recipient as:
      - 'D2D' (Databricks-to-Databricks): Configured using 'sharing_id' / 'metastore_id'.
        Requires zero credential files or bearer tokens.
      - 'D2O' (Databricks-to-Open): External client requiring token profile file.
    """
    if recipient_spec.get("sharing_id") or recipient_spec.get("metastore_id"):
        return "D2D"
    return "D2O"


def validate_d2o_credential_profile(profile_data: dict[str, Any]) -> tuple[bool, str]:
    """
    Validates Databricks-to-Open (D2O) credential profile JSON file:
      - 'shareCredentialsVersion': integer (typically 1)
      - 'endpoint': HTTPS URL of the sharing server
      - 'bearerToken': Non-empty string
      - 'expirationTime': ISO 8601 string; must not be expired
    """
    required_keys = ["shareCredentialsVersion", "endpoint", "bearerToken", "expirationTime"]
    for key in required_keys:
        if key not in profile_data or not profile_data[key]:
            return False, f"Missing required field: {key}"

    if not str(profile_data["endpoint"]).startswith("https://"):
        return False, "Endpoint must use HTTPS protocol"

    try:
        exp_time = datetime.fromisoformat(profile_data["expirationTime"].replace("Z", "+00:00"))
        if exp_time <= datetime.now(UTC):
            return False, "Bearer token is expired"
    except Exception as e:
        return False, f"Invalid expiration timestamp format: {e}"

    return True, "Valid credential profile"


def generate_share_table_ddl(share_name: str, table_name: str, include_cdf: bool = False) -> str:
    """
    Constructs official SQL DDL for adding a table to an OpenSharing SHARE.
    If include_cdf is True, appends WITH CHANGE DATA FEED.
    """
    cdf_clause = " WITH CHANGE DATA FEED" if include_cdf else ""
    return f"ALTER SHARE {share_name} ADD TABLE {table_name}{cdf_clause};"
