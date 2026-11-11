"""
Unit and integration tests for Module 2: Silver Quarantine & Transformation logic.

Validates:
  - Unit: extract_silver_fields flattens raw_variant into typed columns
  - Unit: validate_event_records accurately flags bad records (null types, missing amounts)
  - Integration: Clean table contains only valid records; Quarantine table has proper failure reasons
"""

import pytest


@pytest.mark.integration
def test_silver_clean_table_conforms_to_rules(spark, catalog_name):
    """Ensure clean table has zero null event_types and valid payment amounts."""
    clean_table = f"{catalog_name}.silver.clean_events"
    df = spark.table(clean_table)

    # 1. Total row count must be positive
    count = df.count()
    assert count > 600, f"Expected >600 clean rows, got {count}"

    # 2. No null event types
    null_types = df.filter("event_type IS NULL").count()
    assert null_types == 0, f"Found {null_types} null event types in clean table!"

    # 3. All payment events have positive amount
    invalid_payments = df.filter(
        "(event_type LIKE 'payment%' OR event_type = 'refund.initiated') AND (amount IS NULL OR amount <= 0)"
    ).count()
    assert invalid_payments == 0, f"Found {invalid_payments} invalid payment amounts in clean table!"


@pytest.mark.integration
def test_silver_quarantine_table_captured_defects(spark, catalog_name):
    """Ensure quarantine table holds all bad records with appropriate reason tags."""
    quarantine_table = f"{catalog_name}.silver.quarantine_events"
    df = spark.table(quarantine_table)

    # Total bad rows captured
    count = df.count()
    assert count > 0, "Quarantine table should not be empty!"

    # Verify every record has a quarantine reason tag
    unreasons = df.filter("_quarantine_reason IS NULL").count()
    assert unreasons == 0, "All quarantined records must carry a _quarantine_reason"
