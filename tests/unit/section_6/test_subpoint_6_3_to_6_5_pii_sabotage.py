"""
Sabotage Drill — Section 6, Official Subpoints 6.3, 6.4 & 6.5: Privacy Methods & PII Vault
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_6/test_subpoint_6_3_to_6_5_pii_sabotage.py -v
"""

import pytest

from src.security.pii_compliance import (
    build_silver_compliant_record,
    pseudonymize_email_sha256,
    validate_pii_vault_separation,
)


@pytest.mark.unit
def test_salted_hashing_rainbow_table_sabotage():
    """
    Scenario:
    A Silver pipeline hashes `raw_email` using SHA-256 so analysts can join users across tables
    without exposing raw emails.
    """
    # --- FIXED BUG 1: Pass a strong secret salt retrieved from Databricks Secret Scope ---
    hashed = pseudonymize_email_sha256("alice@corp.com", secret_salt="prod_secret_scope_salt_v1")  # noqa: S106

    assert len(hashed) == 64


@pytest.mark.unit
def test_pii_vault_separation_suppression_sabotage():
    """
    Scenario:
    Raw PII (`raw_email`, `raw_ssn`) must live ONLY in `pii_vault.user_lookup` so a GDPR erasure
    only needs to purge the vault row (`DELETE -> REORG PURGE -> VACUUM`).
    `silver.clean_events` must contain ONLY `token_id`, `email_hash`, and generalized `age_bucket`.
    """
    raw_event = {"raw_email": "alice@corp.com", "raw_ssn": "123-45-6789", "age": 34}

    # --- FIXED BUG 2: Suppress raw PII (`suppress_raw_pii=True`) so Silver holds only `token_id` ---
    silver_row = build_silver_compliant_record(
        raw_record=raw_event,
        token_id="TOK_98123",  # noqa: S106
        secret_salt="prod_secret_scope_salt_v1",  # noqa: S106
        suppress_raw_pii=True,
    )

    assert (
        validate_pii_vault_separation(
            vault_columns={"token_id", "raw_email", "raw_ssn"},
            silver_columns=set(silver_row.keys()),
            raw_pii_fields={"raw_email", "raw_ssn"},
        )
        is True
    )
    assert silver_row["age_bucket"] == "30-39"
