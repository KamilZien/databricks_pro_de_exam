"""
Subpoint 1.12 Drill: Stateful Streaming (`withWatermark`, `dropDuplicates`, `foreachBatch` Idempotency).

Tests two critical exam scenarios on Databricks Serverless:
  1. Deduplicating retry events where the same `event_id` arrives twice with slightly different
     timestamps (Law 6: never include the exact timestamp column inside `dropDuplicates`).
  2. Idempotent `foreachBatch` Delta writes using `txnVersion` and `txnAppId` so that a retried
     micro-batch (`batch_id = 1`) is automatically skipped by Delta Lake instead of duplicating rows.

⚠️ THIS FILE CONTAINS 2 INTENTIONAL EXAM-CALIBRATED BUGS:
  - Bug 1 (`test_watermark_deduplication_excludes_timestamp`):
    `dedup_columns` includes `"event_timestamp"`, so two retries of `"E-100"` arriving 4 seconds
    apart are treated as distinct rows and the duplicate is NOT dropped!
  - Bug 2 (`test_foreach_batch_idempotent_delta_write`):
    On the simulated retry of `batch_id = 1`, the developer passed `batch_id + 1` (`"2"`) as
    `txnVersion` instead of the retried batch's own `batch_id` (`"1"`), causing Delta Lake to
    insert the batch a second time!
"""

import uuid

import pytest
from pyspark.sql import functions as F


@pytest.mark.integration
def test_watermark_deduplication_excludes_timestamp(spark):
    """
    Webhook retry scenario:
      - Event "E-100" arrives at 10:00:01, and a network retry of "E-100" arrives at 10:00:05.
      - Event "E-200" arrives once at 10:01:00.
    Expected after deduplication: exactly 2 unique events ("E-100" and "E-200").
    """
    raw_rows = [
        ("E-100", "2026-10-25 10:00:01", 50.0),
        ("E-100", "2026-10-25 10:00:05", 50.0),  # Retry duplicate (4 seconds later!)
        ("E-200", "2026-10-25 10:01:00", 75.0),
    ]
    df = spark.createDataFrame(raw_rows, ["event_id", "ts_str", "amount"]).withColumn(
        "event_timestamp", F.to_timestamp("ts_str")
    )

    # Resolved Bug 1 (Law 6): Only include the business key ("event_id") in dropDuplicates,
    # never the exact timestamp column ("event_timestamp").
    dedup_columns = ["event_id"]
    deduped_df = df.withWatermark("event_timestamp", "2 hours").dropDuplicates(dedup_columns)

    assert deduped_df.count() == 2, (
        f"Expected 2 unique events after deduplication, but got {deduped_df.count()} "
        "because 'event_timestamp' was included inside dropDuplicates()!"
    )


@pytest.mark.integration
def test_foreach_batch_idempotent_delta_write(spark, catalog_name):
    """
    Simulates a `foreachBatch` writer that writes `batch_id = 1` to a Delta table, crashes,
    and then re-runs `batch_id = 1` on recovery.
    With identical `txnAppId` and `txnVersion = str(batch_id)`, Delta Lake skips the retry!
    """
    test_table = f"{catalog_name}.silver._test_idempotent_foreachbatch"
    app_id = f"app_{uuid.uuid4().hex[:8]}"
    spark.sql(f"DROP TABLE IF EXISTS {test_table}")

    try:
        batch_df = spark.createDataFrame([("E-1", 100.0), ("E-2", 200.0)], ["event_id", "amount"])
        batch_id = 1

        # 1st attempt of batch_id = 1 (Succeeds)
        (
            batch_df.write.format("delta")
            .mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", str(batch_id))
            .saveAsTable(test_table)
        )

        # Resolved Bug 2: On a retry of the SAME batch_id = 1, pass the SAME `str(batch_id)` ("1").
        # Delta Lake sees that (app_id, version 1) is already committed and skips the duplicate write!
        retry_txn_version = str(batch_id)
        (
            batch_df.write.format("delta")
            .mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", retry_txn_version)
            .saveAsTable(test_table)
        )

        final_count = spark.table(test_table).count()
        assert final_count == 2, (
            f"Expected 2 rows (idempotent skip on retry of batch_id=1), but got {final_count} rows!"
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
