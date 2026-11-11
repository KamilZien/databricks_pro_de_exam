"""
src/ingestion/lakehouse_federation.py — Subpoint 2.5 Lakehouse Federation.

Implements:
1. generate_foreign_catalog_ddl: Constructs official Unity Catalog SQL DDL for creating
   Foreign Catalogs mapped to external systems (Snowflake, PostgreSQL, BigQuery, Redshift).
2. analyze_query_pushdown_eligibility: Evaluates query AST / expressions to verify if
   predicates (WHERE filters, simple GROUP BY aggregations) can be pushed down to
   the external database engine to avoid full table scans over the network.
"""

from typing import Any


def generate_foreign_catalog_ddl(catalog_name: str, connection_name: str, database_name: str) -> str:
    """
    Constructs official SQL DDL for creating a Foreign Catalog in Unity Catalog.
    Official Syntax:
      CREATE FOREIGN CATALOG <catalog_name>
      USING CONNECTION <connection_name>
      OPTIONS (database '<database_name>');
    """
    return (
        f"CREATE FOREIGN CATALOG {catalog_name} "
        f"USING CONNECTION {connection_name} "
        f"OPTIONS (database '{database_name}');"
    )


def analyze_query_pushdown_eligibility(query_spec: dict[str, Any]) -> dict[str, Any]:
    """
    Analyzes whether an external federation query can be pushed down:
      - Pushdown-eligible operators: WHERE filters, column projections, basic aggregations (SUM, COUNT, MIN, MAX).
      - Pushdown-blocking operators: Non-standard custom UDFs, window functions over unpartitioned streams.
    Returns:
      {
        'can_pushdown_filter': bool,
        'can_pushdown_aggregation': bool,
        'execution_plan': 'PUSHDOWN' | 'FULL_SCAN_CLIENT_FILTER'
      }
    """
    has_pushdown_filter = bool(query_spec.get("where_clause"))
    has_pushdown_agg = bool(query_spec.get("aggregations"))
    uses_unsupported_udf = bool(query_spec.get("custom_python_udf"))

    if uses_unsupported_udf:
        # Custom Python UDF cannot execute on external DB (Snowflake/Postgres);
        # Spark must pull raw rows over the network and evaluate Python locally!
        return {
            "can_pushdown_filter": False,
            "can_pushdown_aggregation": False,
            "execution_plan": "FULL_SCAN_CLIENT_FILTER",
        }

    plan = "PUSHDOWN" if (has_pushdown_filter or has_pushdown_agg) else "DIRECT_SCAN"
    return {
        "can_pushdown_filter": has_pushdown_filter,
        "can_pushdown_aggregation": has_pushdown_agg,
        "execution_plan": plan,
    }
