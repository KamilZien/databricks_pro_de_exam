r"""
Hands-On Exercise: Subpoint 1.3 — UDFs & Unity Catalog Functions
Run this script to create and test live functions on Databricks Serverless!

Usage:
    & ".\.venv\Scripts\python.exe" exercises/01_subpoint_1_3_udfs.py
"""

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


def main():
    print("=" * 70)
    print("[*] Connecting to Databricks Serverless Compute...")
    print("=" * 70)
    spark = DatabricksSession.builder.profile("EXAM_PREP").serverless(True).getOrCreate()

    catalog = "portfolio_catalog"
    schema = "silver"

    # --------------------------------------------------------------------------
    # TASK 1: Create a Unity Catalog SQL Function (Photon Native)
    # --------------------------------------------------------------------------
    print("\n[1/4] Registering Unity Catalog SQL Function: calculate_finops_tax...")
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {catalog}.{schema}.calculate_finops_tax(amount DOUBLE, tax_rate DOUBLE)
        RETURNS DOUBLE
        LANGUAGE SQL
        DETERMINISTIC
        CONTAINS SQL
        COMMENT 'Computes tax with standard rounding (Photon accelerated)'
        RETURN ROUND(amount * tax_rate, 2);
    """)
    print("   [+] SQL Function created in Unity Catalog!")

    # --------------------------------------------------------------------------
    # TASK 2: Create a Unity Catalog Python Function (Cross-language Governance)
    # --------------------------------------------------------------------------
    print("\n[2/4] Registering Unity Catalog Python Function: score_customer_churn...")
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {catalog}.{schema}.score_customer_churn(days_inactive INT, support_tickets INT)
        RETURNS DOUBLE
        LANGUAGE PYTHON
        COMMENT 'Scores churn probability (0.0 to 1.0) using Python rules'
        AS $$
        # Churn score calculation
        if days_inactive is None or support_tickets is None:
            return 0.0
        base_risk = min(days_inactive / 90.0, 0.7)
        ticket_risk = min(support_tickets * 0.1, 0.3)
        return round(base_risk + ticket_risk, 2)
        $$;
    """)
    print("   [+] Python Function created in Unity Catalog!")

    # --------------------------------------------------------------------------
    # TASK 3: Test Calling Both UC Functions via SQL & PySpark
    # --------------------------------------------------------------------------
    print("\n[3/4] Querying Unity Catalog Functions via SQL...")
    test_sql_df = spark.sql(f"""
        SELECT
            100.0 AS amount,
            {catalog}.{schema}.calculate_finops_tax(100.0, 0.23) AS tax_amount,
            {catalog}.{schema}.score_customer_churn(45, 2) AS churn_score
    """)
    test_sql_df.show()

    # --------------------------------------------------------------------------
    # TASK 4: Call UC Functions in PySpark on Real Table Data
    # --------------------------------------------------------------------------
    print("\n[4/4] Calling Unity Catalog Functions on Real Table Data (clean_events)...")

    events_df = spark.table(f"{catalog}.{schema}.clean_events").limit(5)

    # We call the UC SQL Function directly using F.expr()
    enriched_df = events_df.select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("amount"),
        F.expr(f"{catalog}.{schema}.calculate_finops_tax(amount, 0.23)").alias("tax_calculated"),
        F.expr(f"{catalog}.{schema}.score_customer_churn(30, 1)").alias("churn_risk"),
    )
    enriched_df.show(truncate=False)

    print("\n" + "=" * 70)
    print(">>> EXERCISE COMPLETED SUCCESSFULLY! <<<")
    print("Inspect your new functions in the Databricks Workspace:")
    print(f"--> Go to Catalog Explorer -> {catalog} -> {schema} -> Functions")
    print("=" * 70)


if __name__ == "__main__":
    main()
