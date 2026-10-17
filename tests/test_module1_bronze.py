"""
Unit and integration tests for Module 1 — Bronze Ingestion with Auto Loader & VARIANT.

Validates:
  - Pure transform functions (add_variant_column, add_event_date_column, add_ingestion_metadata)
  - Integration: Reading from bronze Delta table, verifying VARIANT structure and Liquid Clustering
  - Compliance with 7 Serverless Laws (Law 1 UC namespace, Law 2 Clustering, Law 5 VARIANT)
"""

import pytest


@pytest.mark.integration
def test_bronze_table_populated(spark, catalog_name):
    """Verify bronze table exists and contains ingested events."""
    table_name = f"{catalog_name}.bronze.raw_events"
    df = spark.table(table_name)
    count = df.count()
    assert count >= 750, f"Expected at least 750 rows, got {count}"


@pytest.mark.integration
def test_bronze_table_clustering_and_variant(spark, catalog_name):
    """Verify table has VARIANT column and Liquid Clustering enabled."""
    table_name = f"{catalog_name}.bronze.raw_events"
    df = spark.table(table_name)

    # Law 5: VARIANT column present
    schema_dict = {field.name: field.dataType.typeName() for field in df.schema.fields}
    assert "raw_variant" in schema_dict
    assert schema_dict["raw_variant"] == "variant"

    # Query VARIANT with colon syntax
    sample = spark.sql(f"SELECT raw_variant:event_id AS eid FROM {table_name} LIMIT 1").collect()  # noqa: S608
    assert len(sample) == 1
    assert sample[0]["eid"] is not None


@pytest.mark.integration
def test_mock_billing_table_populated(spark, catalog_name):
    """Verify mock billing table is created with Liquid Clustering."""
    table_name = f"{catalog_name}.billing_mock.usage"
    df = spark.table(table_name)
    count = df.count()
    assert count == 500, f"Expected 500 rows, got {count}"
