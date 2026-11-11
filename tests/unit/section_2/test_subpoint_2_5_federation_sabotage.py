"""
tests/integration/section_2/test_subpoint_2_5_federation_sabotage.py — Sabotage Drill.

Verifies:
1. Unity Catalog Lakehouse Federation Foreign Catalog DDL syntax rules.
2. Query pushdown mechanics (pushdown filter eligibility vs client-side fallback on custom UDFs).

⚠️ WARNING: Contains 2 INTENTIONAL EXAM-CALIBRATED ARCHITECTURAL BUGS:
  - Sabotage Bug 1: A Foreign Catalog DDL distractor syntax bug.
  - Sabotage Bug 2: A query pushdown execution plan misclassification.

Your task:
  Run pytest on Databricks Serverless, diagnose both bugs, and fix them!
"""

import pytest

from src.ingestion.lakehouse_federation import (
    analyze_query_pushdown_eligibility,
    generate_foreign_catalog_ddl,
)


@pytest.mark.unit
def test_foreign_catalog_ddl_syntax_sabotage():
    """
    Test 1: Verify exact Unity Catalog DDL for creating a Foreign Catalog.
    """
    catalog_name = "snowflake_sales_catalog"
    connection_name = "snowflake_corp_conn"
    database_name = "GLOBAL_SALES_DB"

    ddl = generate_foreign_catalog_ddl(catalog_name, connection_name, database_name)

    # --- RESOLVED SABOTAGE BUG 1: Official Foreign Catalog DDL Syntax ---
    # The official Unity Catalog keyword is 'CREATE FOREIGN CATALOG'
    # (never distractor terms like 'CREATE EXTERNAL CATALOG').
    assert ddl.startswith("CREATE FOREIGN CATALOG"), f"Expected DDL to start with 'CREATE FOREIGN CATALOG', got: {ddl}"
    assert f"USING CONNECTION {connection_name}" in ddl
    assert f"OPTIONS (database '{database_name}')" in ddl


@pytest.mark.unit
def test_query_pushdown_mechanics_sabotage():
    """
    Test 2: Verify Query Pushdown optimization and client-side execution boundaries.
    """
    # Query A: Standard SQL filter on external table (e.g. WHERE order_date >= '2026-01-01')
    pushdown_query = {
        "where_clause": "order_date >= '2026-01-01'",
        "aggregations": ["sum(amount)", "count(*)"],
        "custom_python_udf": None,
    }
    result_a = analyze_query_pushdown_eligibility(pushdown_query)
    assert result_a["can_pushdown_filter"] is True
    assert result_a["can_pushdown_aggregation"] is True
    assert result_a["execution_plan"] == "PUSHDOWN"

    # Query B: Query calling a custom local Python UDF
    # Because Snowflake/Postgres cannot run client-side Python bytecode,
    # Spark CANNOT push down execution and must fall back to a full network scan!
    client_udf_query = {
        "where_clause": "my_local_python_scorer(amount) > 0.8",
        "custom_python_udf": "my_local_python_scorer",
    }
    result_b = analyze_query_pushdown_eligibility(client_udf_query)

    # --- RESOLVED SABOTAGE BUG 2: Query Pushdown Boundary ---
    # Because external engines (Snowflake/PostgreSQL) cannot execute local Python bytecode,
    # Spark CANNOT push down execution and must fall back to a full network scan (FULL_SCAN_CLIENT_FILTER)!
    assert result_b["execution_plan"] == "FULL_SCAN_CLIENT_FILTER", (
        f"Expected 'FULL_SCAN_CLIENT_FILTER' for custom Python UDF query, got {result_b['execution_plan']}"
    )
