"""
Section 5, Subpoint 5.3: Caching (Disk Cache vs Spark Cache vs Query Result Cache)
Official Docs:
  - https://docs.databricks.com/en/optimizations/disk-cache.html
  - https://docs.databricks.com/en/compute/serverless/limitations.html
  - https://docs.databricks.com/en/sql/user/queries/query-caching.html

Provides:
  1. `validate_caching_strategy`: Enforces Serverless / Spark Connect Law 3:
       - Manual `.cache()`, `.persist()`, and `CACHE TABLE` fail on Serverless compute.
       - Valid Serverless strategies are:
           * `"disk_cache"` (automatic managed SSD caching of remote Parquet files)
           * `"materialize_delta"` (write expensive intermediate result to a Delta table/MV)
           * `"query_result_cache"` (automatic SQL Warehouse result caching for identical queries)
  2. `simulate_cache_behavior_after_update`: Demonstrates why Disk Cache and Query Result Cache
     never serve stale data after a Delta `UPDATE`, whereas manual Spark `.cache()` (on Classic)
     serves stale rows unless `.unpersist()` is called.
"""

from typing import Any


def validate_caching_strategy(compute_mode: str, strategy: str) -> dict[str, Any]:
    """
    Validate caching strategy against compute environment constraints.
    """
    forbidden_on_serverless = {"spark_cache", "df.cache", "df.persist", "cache_table"}
    valid_serverless = {"disk_cache", "materialize_delta", "query_result_cache"}

    norm_compute = compute_mode.lower()
    norm_strategy = strategy.lower()

    if norm_compute == "serverless" and norm_strategy in forbidden_on_serverless:
        raise RuntimeError(
            f"[NOT_SUPPORTED_ON_SERVERLESS] Manual caching (`{strategy}`) is not supported on "
            "Serverless / Spark Connect because executors are shared and managed. "
            "Rely on automatic `disk_cache` for repeated table scans or `materialize_delta` "
            "(write to a temp Delta table / Materialized View) for expensive intermediate results."
        )

    if norm_compute == "serverless" and norm_strategy not in valid_serverless:
        raise ValueError(f"Unknown caching strategy {strategy!r}. Expected one of {valid_serverless}.")

    return {"compute_mode": norm_compute, "strategy": norm_strategy, "valid": True}


def simulate_cache_behavior_after_update(
    cache_layer: str,
    unpersisted_on_update: bool = False,
) -> dict[str, Any]:
    """
    Simulate what happens when an upstream `UPDATE` commits a new Delta version (v1 -> v2):
      - `disk_cache`: Stores immutable Parquet files; v2 reads new Parquet files -> NEVER stale!
      - `query_result_cache`: Automatically invalidated when underlying Delta table version changes -> NEVER stale!
      - `spark_cache`: Pins v1 transformed DataFrame in executor RAM; serves STALE v1 rows unless unpersisted!
    """
    layer = cache_layer.lower()
    if layer == "disk_cache":
        return {"serves_stale_data": False, "reason": "Delta files are immutable; v2 points to new Parquet files."}
    if layer == "query_result_cache":
        return {"serves_stale_data": False, "reason": "SQL Warehouse invalidates result cache on table version bump."}
    if layer == "spark_cache":
        if not unpersisted_on_update:
            return {
                "serves_stale_data": True,
                "reason": "Spark .cache() pinned v1 DataFrame in executor RAM without calling .unpersist()!",
            }
        return {"serves_stale_data": False, "reason": "Explicit .unpersist() cleared v1 cache."}
    raise ValueError(f"Unsupported cache_layer: {cache_layer}")
