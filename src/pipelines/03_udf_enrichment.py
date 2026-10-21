"""
03_udf_enrichment.py — Unity Catalog UDF Registration & Table Enrichment
Part of Subpoint 1.3: User-Defined Functions in Unity Catalog.
"""

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def register_unity_catalog_functions(spark, catalog: str = "portfolio_catalog", schema: str = "silver") -> None:
    """Register governed SQL and Python UDFs in Unity Catalog."""
    # 1. Photon-accelerated SQL UDF
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {catalog}.{schema}.calculate_finops_tax(amount DOUBLE, tax_rate DOUBLE)
        RETURNS DOUBLE
        LANGUAGE SQL
        DETERMINISTIC
        CONTAINS SQL
        COMMENT 'Computes tax with standard rounding (Photon accelerated)'
        RETURN ROUND(amount * tax_rate, 2);
    """)

    # 2. Centralized Python UDF
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {catalog}.{schema}.score_customer_churn(days_inactive INT, support_tickets INT)
        RETURNS DOUBLE
        LANGUAGE PYTHON
        COMMENT 'Scores churn risk (0.0 to 1.0) using Python rules'
        AS $$
        if days_inactive is None or support_tickets is None:
            return 0.0
        base_risk = min(days_inactive / 90.0, 0.7)
        ticket_risk = min(support_tickets * 0.1, 0.3)
        return round(base_risk + ticket_risk, 2)
        $$;
    """)


def enrich_clean_events_with_udfs(
    df: DataFrame, catalog: str = "portfolio_catalog", schema: str = "silver"
) -> DataFrame:
    """
    Pure transformation function (testable with pyspark.testing).
    Enriches transactions with tax calculation and churn risk.
    """
    return df.select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("amount"),
        F.expr(f"{catalog}.{schema}.calculate_finops_tax(amount, 0.23)").alias("tax_amount"),
        F.expr(f"{catalog}.{schema}.score_customer_churn(30, 2)").alias("churn_score"),
    )


def main():
    spark = DatabricksSession.builder.profile("EXAM_PREP").serverless(True).getOrCreate()
    catalog = "portfolio_catalog"
    schema = "silver"

    print(f"[*] Registering UC functions in {catalog}.{schema}...")
    register_unity_catalog_functions(spark, catalog, schema)

    print("[*] Querying clean_events with registered UDFs...")
    clean_df = spark.table(f"{catalog}.{schema}.clean_events").limit(10)
    enriched_df = enrich_clean_events_with_udfs(clean_df, catalog, schema)
    enriched_df.show(5, truncate=False)
    print("[+] Done!")


if __name__ == "__main__":
    main()
