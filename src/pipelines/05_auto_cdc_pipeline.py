"""
Module 5 — Subpoint 1.7: AUTO CDC Pipelines (SCD Type 1 & SCD Type 2).

Official Post-Oct 2026 Databricks Declarative Pipeline Implementation:
  - Ingests raw change logs from customer_cdc_raw
  - Merges into SCD Type 1 table (current state only)
  - Merges into SCD Type 2 table (tracks full historical timeline via __start_at / __end_at)
  - Utilizes SEQUENCE BY to gracefully handle out-of-order event arrivals
  - Employs apply_as_deletes to execute tombstone deletions
"""

CATALOG = "portfolio_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

SOURCE_CDC_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.customer_cdc_raw"
SCD1_TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_customers_scd1"
SCD2_TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_customers_scd2"


def _is_lakeflow_context() -> bool:
    try:
        import dlt  # noqa: F401

        return True
    except ImportError:
        return False


# ===========================================================================
# MODE 1: Declarative Lakeflow Pipeline Definitions (AUTO CDC)
# ===========================================================================
def define_cdc_pipeline_tables():
    """
    Defines the Bronze CDC staging table and the downstream SCD Type 1 & 2 flows.
    """
    import dlt

    # 1. Bronze CDC Ingestion Stream
    @dlt.table(
        name="customer_cdc_raw",
        comment="Bronze CDC incoming transaction change logs",
        table_properties={
            "quality": "bronze",
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true",
        },
    )
    def customer_cdc_raw():
        from src.generator.cdc_stream_generator import generate_customer_cdc_events
        import json
        from pyspark.sql import functions as F

        # Ingests CDC records as a DataFrame
        records = generate_customer_cdc_events()
        json_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
        return spark.read.json(json_rdd)

    # 2. SCD Type 1 Target Flow (Overwrites in-place: Current State Only)
    dlt.create_auto_cdc_flow(
        target="dim_customers_scd1",
        source="customer_cdc_raw",
        keys=["customer_id"],
        sequence_by="event_timestamp",
        apply_as_deletes="operation = 'DELETE'",
        stored_as_scd_type="1",
    )

    # 3. SCD Type 2 Target Flow (Historical Timeline with __start_at & __end_at)
    dlt.create_auto_cdc_flow(
        target="dim_customers_scd2",
        source="customer_cdc_raw",
        keys=["customer_id"],
        sequence_by="event_timestamp",
        apply_as_deletes="operation = 'DELETE'",
        stored_as_scd_type="2",
    )


# ===========================================================================
# MODE 2: Pure Local Logic & Verification Engine (Mocking CDC for Unit Testing)
# ===========================================================================
def simulate_scd1_merge(cdc_records):
    """
    Pure Python simulation of SCD Type 1 state machine to verify behavior
    independent of cluster connectivity.
    """
    # Sort events by key and sequence_by timestamp
    sorted_records = sorted(cdc_records, key=lambda x: (x["customer_id"], x["event_timestamp"]))
    state = {}

    for r in sorted_records:
        cid = r["customer_id"]
        op = r["operation"]
        if op == "DELETE":
            state.pop(cid, None)
        else:
            state[cid] = r

    return state


def simulate_scd2_merge(cdc_records):
    """
    Pure Python simulation of SCD Type 2 timeline builder with __start_at and __end_at.
    """
    from collections import defaultdict

    records_by_key = defaultdict(list)
    for r in cdc_records:
        records_by_key[r["customer_id"]].append(r)

    timeline = []
    for cid, events in records_by_key.items():
        # Order by sequence timestamp
        ordered = sorted(events, key=lambda x: x["event_timestamp"])
        for i, ev in enumerate(ordered):
            start_at = ev["event_timestamp"]
            is_last = (i == len(ordered) - 1)
            end_at = None if is_last else ordered[i + 1]["event_timestamp"]

            if ev["operation"] != "DELETE":
                record_version = dict(ev)
                record_version["__start_at"] = start_at
                record_version["__end_at"] = end_at
                timeline.append(record_version)

    return timeline


if _is_lakeflow_context():
    define_cdc_pipeline_tables()
elif __name__ == "__main__":
    import sys
    from pathlib import Path

    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from src.generator.cdc_stream_generator import generate_customer_cdc_events
    events = generate_customer_cdc_events()
    scd1 = simulate_scd1_merge(events)
    print(f"[*] SCD 1 Final Active Keys: {list(scd1.keys())}")
    for k, v in scd1.items():
        print(f"   {k}: {v['name']} ({v['tier']}, {v['country']})")
