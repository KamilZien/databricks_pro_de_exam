"""
tests/integration/section_2/test_subpoint_2_4_opensharing_sabotage.py — Sabotage Drill.

Verifies:
1. D2D vs D2O protocol classification and credential mechanics.
2. D2O Bearer token profile expiration and schema validation.
3. OpenSharing Change Data Feed (CDF) SQL syntax rules.

⚠️ WARNING: Contains 2 INTENTIONAL EXAM-CALIBRATED ARCHITECTURAL BUGS:
  - Sabotage Bug 1: A D2D vs D2O credential file distribution anti-pattern.
  - Sabotage Bug 2: An OpenSharing CDF SQL syntax error.

Your task:
  Run pytest on Databricks Serverless, diagnose both architectural bugs, and fix them!
"""

import pytest

from src.ingestion.opensharing_and_clean_rooms import (
    classify_recipient_sharing_type,
    generate_share_table_ddl,
    validate_d2o_credential_profile,
)


@pytest.mark.integration
def test_opensharing_protocol_and_credential_profile_sabotage():
    """
    Test 1: Verify recipient classification and token validation.
    """
    # 1. Recipient 1: Partner on Databricks with Unity Catalog (D2D)
    d2d_recipient = {
        "recipient_name": "partner_finops_corp",
        "sharing_id": "aws:us-west-2:11112222-3333-4444-5555-666677778888",
    }
    sharing_type = classify_recipient_sharing_type(d2d_recipient)
    assert sharing_type == "D2D", f"Expected D2D, got {sharing_type}"
    # Architectural rule: D2D uses native Metastore-to-Metastore trust. No credential files exist!

    # 2. Recipient 2: External client on Pandas/PowerBI (D2O)
    # Valid D2O profile with HTTPS and future expiration
    valid_d2o_profile = {
        "shareCredentialsVersion": 1,
        "endpoint": "https://dbc-prod-sharing.cloud.databricks.com/delta-sharing/",
        "bearerToken": "valid_secure_bearer_token_xyz",
        "expirationTime": "2030-01-01T00:00:00Z",
    }
    is_valid, msg = validate_d2o_credential_profile(valid_d2o_profile)
    assert is_valid is True, f"Expected valid D2O profile to pass: {msg}"

    # Invalid D2O profile with HTTP or expired token must be rejected
    insecure_profile = {
        "shareCredentialsVersion": 1,
        "endpoint": "http://insecure-sharing-endpoint.com",  # Insecure HTTP
        "bearerToken": "token_123",
        "expirationTime": "2030-01-01T00:00:00Z",
    }
    is_insecure_valid, _ = validate_d2o_credential_profile(insecure_profile)
    assert is_insecure_valid is False, "Insecure HTTP endpoint must be rejected!"


@pytest.mark.integration
def test_opensharing_cdf_table_ddl_sabotage():
    """
    Test 2: Verify OpenSharing SQL DDL generation for tables with Change Data Feed.
    """
    share_name = "finops_partner_share"
    table_name = "portfolio_catalog.silver.clean_events"

    ddl = generate_share_table_ddl(share_name, table_name, include_cdf=True)

    # --- RESOLVED SABOTAGE BUG 2: Official OpenSharing CDF SQL Syntax ---
    # The official SQL clause is 'WITH CHANGE DATA FEED'
    assert "WITH CHANGE DATA FEED" in ddl, "Expected 'WITH CHANGE DATA FEED' in DDL!"
    assert ddl == f"ALTER SHARE {share_name} ADD TABLE {table_name} WITH CHANGE DATA FEED;"
    assert ddl.endswith(";"), "DDL must terminate with a semicolon"
