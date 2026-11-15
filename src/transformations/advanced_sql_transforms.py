"""
src/transformations/advanced_sql_transforms.py — Subpoint 3.1 Advanced Data Transformations.

Implements pure PySpark transformation functions and plan validators for:
1. compute_running_spend_metrics: Window functions (DENSE_RANK, LAG, and ROWS vs RANGE framing).
2. compute_regional_rollup_summary: Hierarchical ROLLUP with GROUPING() to distinguish
   generated subtotal rows from genuine NULL source data.
3. validate_and_build_stream_outer_join: Validates Stream-Stream Join Catalyst rules (Law 6):
   Outer joins strictly require watermarks on BOTH streams AND an event-time range condition.
"""

from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def compute_running_spend_metrics(df: DataFrame, use_physical_rows_frame: bool = True) -> DataFrame:
    """
    Computes per-user spend analytics ordered by event_day:
      - running_total: cumulative spend per user
      - spend_dense_rank: dense rank of transaction amount (highest = 1, no gaps on ties)
      - prev_amount: previous transaction amount using LAG (default 0.0)
    """
    base_order_window = Window.partitionBy("user_id").orderBy(F.col("event_day").asc(), F.col("txn_id").asc())

    if use_physical_rows_frame:
        # Physical row-by-row running total (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        running_window = (
            Window.partitionBy("user_id")
            .orderBy(F.col("event_day").asc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    else:
        # Default logical RANGE frame when orderBy is specified without rowsBetween:
        # Lumps all rows sharing the same event_day together!
        running_window = Window.partitionBy("user_id").orderBy(F.col("event_day").asc())

    rank_window = Window.partitionBy("user_id").orderBy(F.col("amount").desc())

    return (
        df.withColumn("running_total", F.sum("amount").over(running_window))
        .withColumn("spend_dense_rank", F.dense_rank().over(rank_window))
        .withColumn("prev_amount", F.lag("amount", 1, 0.0).over(base_order_window))
    )


def evaluate_window_running_totals(
    records: list[dict[str, Any]], use_physical_rows_frame: bool = True
) -> dict[str, dict[str, Any]]:
    """
    Evaluates Spark SQL Window framing semantics on ordered transaction records:
      - When `use_physical_rows_frame=True` (`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`),
        accumulates row-by-row strictly in physical row order.
      - When `use_physical_rows_frame=False` (Spark's default `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
        when `ORDER BY` is present without an explicit `ROWS` frame), all peer rows sharing the same
        `event_day` are summed together on every tied row!
    """
    ordered = sorted(records, key=lambda r: (r["event_day"], r["txn_id"]))
    result: dict[str, dict[str, Any]] = {}

    if use_physical_rows_frame:
        running = 0.0
        for row in ordered:
            running += float(row["amount"])
            result[row["txn_id"]] = {**row, "running_total": running}
    else:
        # Default RANGE frame groups peers with identical ORDER BY key (event_day)
        for row in ordered:
            current_day = row["event_day"]
            range_sum = sum(float(r["amount"]) for r in ordered if r["event_day"] <= current_day)
            result[row["txn_id"]] = {**row, "running_total": range_sum}

    return result


def compute_regional_rollup_summary(df: DataFrame) -> DataFrame:
    """
    Computes hierarchical ROLLUP(region, product) and uses F.grouping() to label
    generated subtotal/grand-total NULLs vs genuine NULL values in source data.
    """
    return (
        df.rollup("region", "product")
        .agg(
            F.grouping("region").alias("is_region_subtotal"),
            F.grouping("product").alias("is_product_subtotal"),
            F.sum("revenue").alias("total_revenue"),
        )
        .withColumn(
            "region_label",
            F.when(F.col("is_region_subtotal") == 1, F.lit("ALL_REGIONS_SUBTOTAL")).otherwise(
                F.coalesce(F.col("region"), F.lit("UNKNOWN_SOURCE_REGION"))
            ),
        )
        .withColumn(
            "product_label",
            F.when(F.col("is_product_subtotal") == 1, F.lit("ALL_PRODUCTS_SUBTOTAL")).otherwise(F.col("product")),
        )
    )


def validate_stream_stream_join_spec(
    join_type: str,
    left_watermark_col: str | None,
    right_watermark_col: str | None,
    join_condition_sql: str,
) -> bool:
    """
    Enforces Spark Structured Streaming Catalyst validation rules for Stream-Stream Joins (Law 6):
      - Inner joins ('inner'): watermarks and time-range conditions are optional for compilation
        (though recommended for state cleanup).
      - Outer joins ('leftOuter', 'rightOuter', 'fullOuter', 'left', 'right', 'full'):
        1. BOTH streams MUST define a watermark (`left_watermark_col` and `right_watermark_col`).
        2. The join condition MUST include an event-time range constraint (`BETWEEN` or `<=` / `>=` interval bound)
           relating the two watermarked timestamp columns so Spark knows when an unmatched row can emit NULL!
    """
    normalized_type = join_type.lower()
    is_outer = any(k in normalized_type for k in ("left", "right", "full", "outer"))

    if not is_outer:
        return True

    if not left_watermark_col or not right_watermark_col:
        raise ValueError(
            "[STREAM_STREAM_OUTER_JOIN_MISSING_WATERMARK] "
            "Stream-stream outer join requires watermarks defined on BOTH input streams."
        )

    cond_upper = join_condition_sql.upper()
    has_time_range = (
        ("BETWEEN" in cond_upper and "INTERVAL" in cond_upper) or ("<=" in cond_upper and "INTERVAL" in cond_upper)
    ) and (left_watermark_col.upper() in cond_upper and right_watermark_col.upper() in cond_upper)

    if not has_time_range:
        raise ValueError(
            "[STREAM_STREAM_OUTER_JOIN_MISSING_TIME_RANGE] "
            f"Stream-stream {join_type} join requires an event-time range condition "
            f"(e.g. `{right_watermark_col} BETWEEN {left_watermark_col} AND {left_watermark_col} + INTERVAL 1 HOUR`) "
            "to bound state retention and emit unmatched NULL records."
        )

    return True
