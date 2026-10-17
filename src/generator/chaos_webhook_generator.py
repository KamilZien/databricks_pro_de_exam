"""
Module 1 — Chaos Webhook Generator

Generates realistic but messy fintech webhook JSON events and writes them as
individual JSON files to a Unity Catalog Volume landing zone. Also creates a
mock `system.billing.usage` table for FinOps exercises.

Design decisions (exam-relevant):
  - Writes to UC Volume path: /Volumes/portfolio_catalog/landing/raw_events/
    (NEVER dbfs:/mnt/ — Law 1)
  - Keeps dataset size to 500–1,000 rows (Free Edition quota — Law 3)
  - Uses DatabricksSession.builder.serverless(True) (Spark Connect — Law 3)
  - JSON is intentionally messy: missing fields, type drift, nested objects,
    nulls — forces VARIANT handling in Bronze (Law 5)
"""

import json
import random
import uuid
from datetime import UTC, datetime, timedelta

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "portfolio_catalog"
LANDING_SCHEMA = "landing"
VOLUME_NAME = "raw_events"
VOLUME_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{VOLUME_NAME}"

BILLING_SCHEMA = "billing_mock"
BILLING_TABLE = f"{CATALOG}.{BILLING_SCHEMA}.usage"

EVENT_TYPES = ["payment.completed", "payment.failed", "refund.initiated",
               "account.created", "account.updated", "kyc.verified"]

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "PLN"]

# PII fields — will exercise GDPR purge in Module 4
FAKE_NAMES = ["Alice Smith", "Bob Nowak", "Celine Dubois", "Daichi Tanaka",
              "Eva Müller", "Farid Khan", "Greta Johansson", "Hugo Silva"]

FAKE_EMAILS = [f"{n.split()[0].lower()}@example.com" for n in FAKE_NAMES]


# ---------------------------------------------------------------------------
# Chaos JSON Generator
# ---------------------------------------------------------------------------
def generate_chaos_event(event_id: int, base_ts: datetime) -> dict:
    """
    Generate a single messy webhook event. Intentional chaos:
      - Sometimes missing 'amount' field
      - Sometimes 'amount' is a string instead of float
      - Nested 'metadata' with variable schema
      - Random null fields
      - PII (name, email) for GDPR exercises
    """
    ts = base_ts + timedelta(
        seconds=random.randint(0, 86400),
        milliseconds=random.randint(0, 999),
    )
    event_type = random.choice(EVENT_TYPES)
    user_idx = random.randint(0, len(FAKE_NAMES) - 1)

    event = {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"evt-{event_id}")),
        "event_type": event_type,
        "timestamp": ts.isoformat(),
        "user_id": f"usr_{user_idx:04d}",
    }

    # ---- Chaos: PII fields (sometimes present, sometimes not) ----
    if random.random() > 0.15:
        event["user_name"] = FAKE_NAMES[user_idx]
        event["user_email"] = FAKE_EMAILS[user_idx]

    # ---- Chaos: amount field with type drift ----
    if event_type.startswith("payment") or event_type == "refund.initiated":
        amount = round(random.uniform(1.0, 9999.99), 2)
        if random.random() < 0.1:
            event["amount"] = str(amount)  # Bug: string instead of float
        elif random.random() < 0.05:
            pass  # Bug: missing amount entirely
        else:
            event["amount"] = amount
        event["currency"] = random.choice(CURRENCIES)

    # ---- Chaos: nested metadata with variable schema ----
    if random.random() > 0.2:
        meta = {"source": random.choice(["mobile_app", "web", "api", "partner_sdk"])}
        if random.random() > 0.5:
            meta["ip_address"] = f"192.168.{random.randint(0,255)}.{random.randint(1,254)}"
        if random.random() > 0.7:
            meta["device"] = random.choice(["iOS", "Android", "Desktop", None])
        event["metadata"] = meta

    # ---- Chaos: random nulls ----
    if random.random() < 0.05:
        event["event_type"] = None

    return event


def generate_events(num_events: int = 750, num_days: int = 3) -> list[dict]:
    """
    Generate a batch of chaotic webhook events spread across multiple days.

    Args:
        num_events: Total events to generate (default 750, within Free Edition quota).
        num_days: Number of days to spread events across.

    Returns:
        List of event dictionaries.
    """
    base_date = datetime(2026, 10, 1, tzinfo=UTC)
    events = []
    for i in range(num_events):
        day_offset = i % num_days
        base_ts = base_date + timedelta(days=day_offset)
        events.append(generate_chaos_event(i, base_ts))
    return events


# ---------------------------------------------------------------------------
# Write events to UC Volume as individual JSON files (for Auto Loader)
# ---------------------------------------------------------------------------
def write_events_to_volume(spark, events: list[dict]) -> str:
    """
    Write events as individual JSON files to UC Volume landing zone.

    Each file is a single JSON object (one event per file) to simulate
    realistic webhook file drops that Auto Loader will pick up.

    NEVER uses dbfs:/mnt/ — writes to /Volumes/ UC path (Law 1).
    """
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)

    # Ensure volume directory exists
    try:
        dbutils.fs.mkdirs(VOLUME_PATH)
    except Exception:
        pass  # Directory may already exist

    batch_id = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    batch_dir = f"{VOLUME_PATH}/batch_{batch_id}"
    dbutils.fs.mkdirs(batch_dir)

    for i, event in enumerate(events):
        file_path = f"{batch_dir}/event_{i:06d}.json"
        dbutils.fs.put(file_path, json.dumps(event), overwrite=True)

    print(f"✅ Wrote {len(events)} events to {batch_dir}")
    return batch_dir


# ---------------------------------------------------------------------------
# Mock system.billing.usage table for FinOps (Module 3)
# ---------------------------------------------------------------------------
def create_mock_billing_table(spark, num_rows: int = 500) -> None:
    """
    Create a mock billing usage table in Unity Catalog for FinOps exercises.

    Simulates system.billing.usage with realistic columns:
      - usage_date, workspace_id, sku_name, usage_unit, usage_quantity, etc.

    Uses 3-level UC namespace (Law 1), no PARTITION BY (Law 2).
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    sku_names = [
        "SERVERLESS_SQL_COMPUTE", "SERVERLESS_DLT_COMPUTE",
        "JOBS_COMPUTE", "INTERACTIVE_COMPUTE",
        "SERVERLESS_REALTIME_INFERENCE", "STORAGE",
    ]

    rows = []
    base_date = datetime(2026, 9, 1)
    for i in range(num_rows):
        day_offset = i % 30
        usage_date = (base_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        rows.append((
            usage_date,
            f"ws_{random.randint(1, 3):03d}",
            random.choice(sku_names),
            "DBU",
            round(random.uniform(0.1, 50.0), 4),
            f"pipeline_{random.randint(1, 5)}",
        ))

    schema = StructType([
        StructField("usage_date", StringType(), False),
        StructField("workspace_id", StringType(), False),
        StructField("sku_name", StringType(), False),
        StructField("usage_unit", StringType(), False),
        StructField("usage_quantity", DoubleType(), False),
        StructField("usage_metadata_job_id", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema).withColumn(
        "usage_date", F.col("usage_date").cast(DateType())
    )

    # Create schema if needed
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BILLING_SCHEMA}")

    # Write with Liquid Clustering (Law 2) — NEVER PARTITION BY
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BILLING_TABLE)
    )

    # Apply Liquid Clustering after table creation
    spark.sql(f"ALTER TABLE {BILLING_TABLE} CLUSTER BY (usage_date, sku_name)")

    print(f"✅ Created mock billing table {BILLING_TABLE} with {num_rows} rows")


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------
def main():
    """
    Run the chaos generator: create events and write to UC Volume.
    Optionally create mock billing table.
    """
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless(True).getOrCreate()

    # Ensure catalog and landing schema exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}.{VOLUME_NAME} "
        f"COMMENT 'Landing zone for raw webhook JSON files'"
    )

    # Generate and write chaotic events
    events = generate_events(num_events=750, num_days=3)
    write_events_to_volume(spark, events)

    # Create mock billing table for Module 3 FinOps exercises
    create_mock_billing_table(spark, num_rows=500)

    spark.stop()


if __name__ == "__main__":
    main()
