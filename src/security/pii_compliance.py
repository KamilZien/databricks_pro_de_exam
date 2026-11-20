"""
Section 6, Official Subpoints 6.3, 6.4 & 6.5:
Anonymization/Pseudonymization (Salted Hashing, Tokenization, Suppression, Generalization),
Compliant PII Pipelines, and GDPR Physical Purge.
Official Docs: https://docs.databricks.com/en/security/privacy/gdpr-delta.html
"""

import hashlib
from typing import Any


def pseudonymize_email_sha256(email: str, secret_salt: str) -> str:
    """
    Apply salted SHA-256 hashing (6.3).
    Enforces that a non-empty secret salt (retrieved from Databricks Secret Scope) MUST be provided
    to prevent rainbow-table re-identification attacks!
    """
    if not secret_salt or len(secret_salt) < 8:
        raise ValueError(
            "[UNSALTED_HASH_VULNERABILITY] Hashing PII without a strong secret salt (>= 8 chars) "
            "is vulnerable to rainbow-table attacks! Retrieve a salt via `dbutils.secrets.get(scope, key)`."
        )
    digest = hashlib.sha256(f"{email.strip().lower()}:{secret_salt}".encode()).hexdigest()
    return digest


def generalize_age_bucket(age: int) -> str:
    """Apply Generalization (6.3): convert exact age into a 10-year cohort bucket (e.g. 34 -> '30-39')."""
    lower = (age // 10) * 10
    return f"{lower}-{lower + 9}"


def validate_pii_vault_separation(
    vault_columns: set[str],
    silver_columns: set[str],
    raw_pii_fields: set[str],
) -> bool:
    """
    Enforces compliant PII Vault architecture (6.4 & 6.5):
      - `pii_vault.user_lookup` holds `token_id` + raw PII (`raw_email`, `raw_ssn`).
      - `silver.clean_events` must hold ONLY `token_id` (and generalized/suppressed attributes) —
        ZERO raw PII columns may leak into Silver/Gold!
    """
    leaked_pii = silver_columns & raw_pii_fields
    if leaked_pii:
        raise PermissionError(
            f"[PII_LEAK_IN_SILVER] Raw PII columns {sorted(leaked_pii)} leaked into Silver! "
            "Keep raw PII exclusively in the restricted PII Vault and store only `token_id` in Silver/Gold."
        )
    if "token_id" not in vault_columns or "token_id" not in silver_columns:
        raise ValueError("Both `pii_vault` and `silver` must share `token_id` for joinability.")
    return True


def build_silver_compliant_record(
    raw_record: dict[str, Any],
    token_id: str,
    secret_salt: str,
    suppress_raw_pii: bool = True,
) -> dict[str, Any]:
    """Build a compliant Silver record using Tokenization + Salted Hashing + Generalization + Suppression."""
    record: dict[str, Any] = {
        "token_id": token_id,
        "email_hash": pseudonymize_email_sha256(raw_record["raw_email"], secret_salt),
        "age_bucket": generalize_age_bucket(raw_record["age"]),
    }
    if not suppress_raw_pii:
        record["raw_email"] = raw_record["raw_email"]
        record["raw_ssn"] = raw_record["raw_ssn"]
    return record
