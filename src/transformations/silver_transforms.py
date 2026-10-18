"""
Module 2 Pure Transformation Logic (Testable via DataFrame.transform).

Contains decoupled transformation functions:
1. extract_silver_fields: Flattens VARIANT column into strongly-typed Silver attributes.
2. validate_event_records: Evaluates data quality rules and tags records with validation status.
3. split_clean_and_quarantine: Partitions DataFrame into clean and quarantined datasets.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def extract_silver_fields(df: DataFrame) -> DataFrame:
    """
    Extract and strongly-type fields from Bronze raw_variant column.

    Enforces:
      - event_id (string)
      - event_type (string)
      - event_timestamp (timestamp)
      - user_id (string)
      - user_name (string)
      - user_email (string)
      - amount (double)
      - currency (string)
      - source (string from nested metadata)
    """
    return df.select(
        F.expr("raw_variant:event_id::string").alias("event_id"),
        F.expr("raw_variant:event_type::string").alias("event_type"),
        F.to_timestamp(F.expr("raw_variant:timestamp::string")).alias("event_timestamp"),
        F.expr("raw_variant:user_id::string").alias("user_id"),
        F.expr("raw_variant:user_name::string").alias("user_name"),
        F.expr("raw_variant:user_email::string").alias("user_email"),
        F.expr("raw_variant:amount::double").alias("amount"),
        F.expr("raw_variant:currency::string").alias("currency"),
        F.expr("raw_variant:metadata.source::string").alias("source"),
        F.col("event_date"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
    )


def validate_event_records(df: DataFrame) -> DataFrame:
    """
    Apply Lakeflow Data Quality Expectation rules to Silver records.

    Rules:
      1. valid_event_id: event_id must not be null.
      2. valid_event_type: event_type must not be null.
      3. valid_amount_for_payments: If event_type starts with 'payment' or 'refund',
         amount must be > 0 and not null.
    """
    is_payment = F.col("event_type").startswith("payment") | (F.col("event_type") == "refund.initiated")

    amount_valid = (~is_payment) | (F.col("amount").isNotNull() & (F.col("amount") > 0))
    id_valid = F.col("event_id").isNotNull()
    type_valid = F.col("event_type").isNotNull()

    is_valid_record = id_valid & type_valid & amount_valid

    # Capture reason for quarantine
    quarantine_reason = F.when(~id_valid, "NULL_EVENT_ID") \
                         .when(~type_valid, "NULL_EVENT_TYPE") \
                         .when(~amount_valid, "INVALID_OR_MISSING_PAYMENT_AMOUNT") \
                         .otherwise(None)

    return df.withColumn("_is_valid", is_valid_record) \
             .withColumn("_quarantine_reason", quarantine_reason)


def filter_clean_records(df: DataFrame) -> DataFrame:
    """Filter records that passed all data quality expectations."""
    return df.filter(F.col("_is_valid") == True).drop("_is_valid", "_quarantine_reason")  # noqa: E712


def filter_quarantine_records(df: DataFrame) -> DataFrame:
    """Filter records that failed one or more data quality expectations."""
    return df.filter(F.col("_is_valid") == False).withColumn("_quarantined_at", F.current_timestamp())  # noqa: E712
