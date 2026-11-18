"""
Section 5, Subpoint 5.5: Query Profile Bottlenecks (Data Skipping, Join Strategies, Shuffle)
Official Docs: https://docs.databricks.com/en/sql/user/queries/query-profile.html

Provides:
  1. `simulate_scan_data_skipping`: Evaluates whether a SQL `WHERE` predicate allows Delta
     min/max Data Skipping on a clustered column (`event_ts`), or defeats skipping due to
     a non-sargable function wrapper (e.g. `YEAR(event_ts) = 2026`, `LOWER(tenant_id) = ...`).
  2. `classify_join_operator`: Determines the Spark physical join operator (`BroadcastHashJoin`,
     `SortMergeJoin`, or `BroadcastNestedLoopJoin`) from join condition & table sizes.
"""

import re
from typing import Any


def simulate_scan_data_skipping(
    where_sql: str,
    clustered_col: str = "event_ts",
    total_files: int = 10_000,
) -> dict[str, Any]:
    """
    Simulate Delta Scan operator metrics (`files_read`, `files_pruned`) for a `WHERE` clause.
    Non-sargable expressions like `YEAR(event_ts)` or `DATE(event_ts)` prevent Delta min/max
    file-level data skipping (`files_pruned = 0`).
    """
    non_sargable_pattern = rf"\b[A-Za-z_]+\(\s*{re.escape(clustered_col)}\s*\)"
    if re.search(non_sargable_pattern, where_sql):
        return {
            "files_read": total_files,
            "files_pruned": 0,
            "skipping_active": False,
            "reason": (
                f"Non-sargable predicate wraps `{clustered_col}` in a function (`{where_sql}`), "
                "preventing Delta min/max statistics pruning!"
            ),
        }

    if clustered_col in where_sql and any(op in where_sql for op in (">=", "<=", ">", "<", "=", "BETWEEN")):
        pruned = int(total_files * 0.92)
        return {
            "files_read": total_files - pruned,
            "files_pruned": pruned,
            "skipping_active": True,
            "reason": "Sargable range/equality predicate pruned 92% of files via Delta Data Skipping.",
        }

    return {
        "files_read": total_files,
        "files_pruned": 0,
        "skipping_active": False,
        "reason": f"Filter does not reference clustered column `{clustered_col}`.",
    }


def classify_join_operator(
    join_condition_sql: str,
    right_table_mb: float,
    broadcast_threshold_mb: float = 10.0,
) -> dict[str, Any]:
    """
    Simulate Spark Catalyst physical join selection:
      - Requires at least one equality predicate (`a.col = b.col`, not inside `OR`) for Hash/SortMerge joins.
      - Pure range/inequality joins (`BETWEEN`, `<`, `>`) without an equi-join key fall back to
        `BroadcastNestedLoopJoin` (O(N * M) bottleneck)!
    """
    # Check if there is an equality join key (`left.col = right.col`) outside pure range predicates
    has_equi_key = bool(re.search(r"\b\w+\.\w+\s*=\s*\w+\.\w+\b", join_condition_sql))
    has_or = bool(re.search(r"\bOR\b", join_condition_sql, re.IGNORECASE))

    if not has_equi_key or has_or:
        return {
            "operator": "BroadcastNestedLoopJoin",
            "shuffle_required": False,
            "warning": "O(N*M) nested loop join! Add an equi-join key (e.g. `a.tenant_id = b.tenant_id`) and avoid OR.",
        }

    if right_table_mb <= broadcast_threshold_mb:
        return {
            "operator": "BroadcastHashJoin",
            "shuffle_required": False,
            "warning": None,
        }

    return {
        "operator": "SortMergeJoin",
        "shuffle_required": True,
        "warning": "Both sides shuffled via Exchange; consider pre-filtering or broadcasting if small enough.",
    }
