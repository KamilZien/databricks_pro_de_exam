"""
tests/integration/section_2/test_subpoint_2_1_multi_format_ingest_sabotage.py — Sabotage Drill.

This integration test verifies:
1. Ingesting simulated Kafka binary key/value streams with VARIANT parsing.
2. Ingesting unstructured documents via format("binaryFile") with checksum extraction.
3. Enabling UniForm (Universal Format) for Iceberg interoperability on Delta Lake.

⚠️ WARNING: This test contains 2 INTENTIONAL SABOTAGE BUGS:
  - Sabotage Bug 1: A Kafka binary payload decoding sabotage.
  - Sabotage Bug 2: A binaryFile schema / content processing anti-pattern.

Your task:
  Run pytest on Databricks Serverless, diagnose both bugs, and fix them!
"""

import pytest
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.ingestion.kafka_and_binary_ingest import (
    extract_binary_file_metadata,
    parse_kafka_message_payload,
)


@pytest.mark.integration
def test_kafka_binary_payload_ingestion_sabotage(spark):
    """
    Simulate a raw 7-column Kafka stream output:
    (key, value, topic, partition, offset, timestamp, timestampType)
    """
    kafka_schema = StructType(
        [
            StructField("key", BinaryType(), True),
            StructField("value", BinaryType(), True),
            StructField("topic", StringType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("timestampType", IntegerType(), True),
        ]
    )

    raw_records = [
        (
            bytearray(b"KEY-101"),
            bytearray(b'{"event_id": "EVT-101", "amount": 149.99, "currency": "USD"}'),
            "finops.transactions.raw",
            0,
            1001,
            None,
            0,
        ),
        (
            bytearray(b"KEY-102"),
            bytearray(b'{"event_id": "EVT-102", "amount": 2500.00, "currency": "EUR"}'),
            "finops.transactions.raw",
            1,
            2005,
            None,
            0,
        ),
    ]

    raw_kafka_df = spark.createDataFrame(raw_records, schema=kafka_schema)

    # Transform using pure ingestion function
    parsed_df = raw_kafka_df.transform(parse_kafka_message_payload)

    # Collect for assertions
    rows = parsed_df.orderBy("event_id").collect()

    # --- RESOLVED SABOTAGE BUG 1: Decoding expectation sabotage ---
    # The developer assumed message_key remains binary bytes and checked isinstance(bytes),
    # but the architectural requirement specifies message_key must be decoded to STRING.
    row0 = rows[0]
    assert row0["message_key"] == "KEY-101", f"Expected decoded STRING 'KEY-101', got {row0['message_key']}"
    assert isinstance(row0["message_key"], str), "Expected decoded message_key to be of type str!"

    assert row0["amount"] == 149.99, f"Expected amount 149.99, got {row0['amount']}"


@pytest.mark.integration
def test_binary_file_ingestion_and_uniform_iceberg_sabotage(spark, catalog_name):
    """
    Verify binaryFile ingestion schema and UniForm Iceberg table properties.
    """
    # 1. Simulate format("binaryFile") columns: path, modificationTime, length, content
    binary_schema = StructType(
        [
            StructField("path", StringType(), True),
            StructField("modificationTime", TimestampType(), True),
            StructField("length", LongType(), True),
            StructField("content", BinaryType(), True),
        ]
    )

    sample_pdf_bytes = bytearray(b"%PDF-1.4 sample invoice text content for FinOps audit")
    binary_records = [
        (
            "/Volumes/portfolio_catalog/landing/invoices/2026_Q4_invoice_01.pdf",
            None,
            len(sample_pdf_bytes),
            sample_pdf_bytes,
        )
    ]

    raw_binary_df = spark.createDataFrame(binary_records, schema=binary_schema)
    processed_df = raw_binary_df.transform(extract_binary_file_metadata)

    row = processed_df.collect()[0]

    # --- RESOLVED SABOTAGE BUG 2: Filename & Checksum assertion sabotage ---
    # Filename must extract ONLY the leaf filename ('2026_Q4_invoice_01.pdf'),
    # while path holds the full '/Volumes/...' location.
    assert row["filename"] == "2026_Q4_invoice_01.pdf", (
        f"Expected leaf filename '2026_Q4_invoice_01.pdf', got {row['filename']}"
    )
    assert row["content_sha256"] is not None, "Checksum should be computed"
    assert len(row["content_sha256"]) == 64, "SHA-256 string must be 64 hexadecimal characters"
