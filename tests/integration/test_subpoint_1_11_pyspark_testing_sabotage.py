"""
Subpoint 1.11 Drill: PySpark Testing (`assertDataFrameEqual`, `assertSchemaEqual`, `DataFrame.transform`).

Tests the real Silver pure transformation functions (`validate_event_records`, `filter_clean_records`)
from `src/transformations/silver_transforms.py` on a 3-row synthetic dataset over Databricks Serverless.

⚠️ THIS FILE CONTAINS 2 INTENTIONAL EXAM-CALIBRATED BUGS:
  - Bug 1 (`test_schema_validation_with_assert_schema_equal`):
    `actual_df` has nullable columns (`nullable=True`), while `expected_schema` defines `event_id`
    as non-nullable (`nullable=False`). Calling `assertSchemaEqual` with `ignoreNullable=False`
    fails! Fix the `ignoreNullable` parameter so the schema assertion succeeds.
  - Bug 2 (`test_clean_filter_transform_with_assert_dataframe_equal`):
    `actual_clean_df` has rows in order `["E-1", "E-2"]`, while `expected_df` has the exact same rows
    in reversed order `["E-2", "E-1"]`. Because the developer passed `checkRowOrder=True`,
    `assertDataFrameEqual` fails on row order! Fix `checkRowOrder` so order-independent comparison passes.
"""

import pytest
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from src.transformations.silver_transforms import filter_clean_records, validate_event_records


@pytest.mark.integration
def test_schema_validation_with_assert_schema_equal(spark):
    """
    Tests `validate_event_records` schema output using `assertSchemaEqual`.
    """
    input_rows = [
        ("E-1", "payment.processed", 100.0),
        (None, "payment.processed", 50.0),
    ]
    input_df = spark.createDataFrame(input_rows, ["event_id", "event_type", "amount"])

    # Chain pure transformation via DataFrame.transform()
    actual_df = input_df.transform(validate_event_records)

    expected_schema = StructType(
        [
            StructField("event_id", StringType(), nullable=False),  # Notice: nullable=False here!
            StructField("event_type", StringType(), nullable=True),
            StructField("amount", DoubleType(), nullable=True),
            StructField("_is_valid", BooleanType(), nullable=True),
            StructField("_quarantine_reason", StringType(), nullable=True),
        ]
    )

    # Resolved Bug 1: ignoreNullable=True ignores nullable=True/False differences across schemas.
    assertSchemaEqual(actual_df.schema, expected_schema, ignoreNullable=True)


@pytest.mark.integration
def test_clean_filter_transform_with_assert_dataframe_equal(spark):
    """
    Tests chaining `.transform(validate_event_records).transform(filter_clean_records)`
    on a 3-row synthetic dataset (2 valid rows, 1 invalid negative payment row)
    and verifies the result with `assertDataFrameEqual`.
    """
    input_rows = [
        ("E-1", "payment.processed", 120.50),  # Valid
        ("E-BAD", "payment.processed", -10.00),  # Invalid (negative payment -> quarantined)
        ("E-2", "user.login", 0.0),  # Valid (not a payment/refund event)
    ]
    input_df = spark.createDataFrame(input_rows, ["event_id", "event_type", "amount"])

    # Chain both pure transformations using DataFrame.transform()
    actual_clean_df = input_df.transform(validate_event_records).transform(filter_clean_records)

    # Expected clean rows (intentionally constructed in reverse order: E-2 first, E-1 second)
    expected_rows = [
        ("E-2", "user.login", 0.0),
        ("E-1", "payment.processed", 120.50),
    ]
    expected_df = spark.createDataFrame(expected_rows, ["event_id", "event_type", "amount"])

    # Resolved Bug 2: checkRowOrder=False compares DataFrames regardless of row order.
    assertDataFrameEqual(actual_clean_df, expected_df, checkRowOrder=False)
