"""
src/ingestion/kafka_and_binary_ingest.py — Ingestion Functions for Subpoint 2.1.

Implements pure transformation and parsing functions:
1. parse_kafka_message_payload: Decodes binary key/value from message bus feeds into typed columns & VARIANT.
2. extract_binary_file_metadata: Processes format("binaryFile") columns (path, modificationTime, length, content).
3. configure_uniform_iceberg_properties: Returns DDL table properties for Delta UniForm (Iceberg metadata).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def parse_kafka_message_payload(df: DataFrame) -> DataFrame:
    """
    Parse a raw 7-column Kafka DataFrame:
      (key, value, topic, partition, offset, timestamp, timestampType)

    Decodes:
      - key (BINARY) -> STRING
      - value (BINARY) -> VARIANT via parse_json(cast(value AS STRING))
      - Extracts event_id and amount from raw_variant
      - Keeps topic, partition, offset, timestamp for lineage
    """
    return (
        df.select(
            F.col("key").cast("string").alias("message_key"),
            F.expr("parse_json(cast(value AS STRING))").alias("raw_variant"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "event_id",
            F.expr("raw_variant:event_id::string"),
        )
        .withColumn(
            "amount",
            F.expr("raw_variant:amount::double"),
        )
    )


def extract_binary_file_metadata(df: DataFrame) -> DataFrame:
    """
    Process raw format("binaryFile") DataFrame:
      (path, modificationTime, length, content)

    Extracts:
      - filename from path
      - file_size_kb (length / 1024)
      - content_byte_count (length of content binary)
      - sha256 checksum of content for data integrity / deduping
    """
    return df.select(
        F.col("path"),
        F.element_at(F.split(F.col("path"), "/"), -1).alias("filename"),
        F.col("modificationTime"),
        F.col("length"),
        (F.col("length") / 1024.0).alias("size_kb"),
        F.sha2(F.col("content"), 256).alias("content_sha256"),
        F.col("content"),
    )


def configure_uniform_iceberg_properties() -> dict[str, str]:
    """
    Return table properties required to enable Delta UniForm (Iceberg metadata generation).
    UniForm requires:
      - delta.enableDeletionVectors = true
      - delta.universalFormat.enabledFormats = 'iceberg'
    """
    return {
        "delta.enableDeletionVectors": "true",
        "delta.universalFormat.enabledFormats": "iceberg",
    }
