"""
Section 5, Subpoint 5.4: Change Data Feed (CDF) for Incremental Downstream Processing
Official Docs: https://docs.databricks.com/en/delta/delta-change-data-feed.html

Provides:
  1. `validate_cdf_read_options`: Enforces the exact PySpark CDF option (`"readChangeFeed": "true"`),
     rejecting distractors like `"readChangeData"` or `"enableChangeFeed"`, and verifies that
     `starting_version` has not been purged by `VACUUM`.
  2. `prepare_cdf_merge_batch`: Deduplicates CDF output before `MERGE INTO` so multiple updates
     to the same primary key (and `update_preimage` rows) do NOT trigger
     `[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE]`.
"""

from typing import Any


def validate_cdf_read_options(
    read_options: dict[str, str],
    starting_version: int,
    earliest_available_cdf_version: int = 0,
) -> bool:
    """
    Validate PySpark `.readStream` / `.read` options for Delta Change Data Feed.
    """
    if read_options.get("readChangeFeed", "").lower() != "true":
        raise ValueError(
            "[CDF_INVALID_READ_OPTION] To read Delta Change Data Feed in PySpark, you MUST set "
            '`.option("readChangeFeed", "true")`. '
            f"Received options: {read_options!r}"
        )

    if starting_version < earliest_available_cdf_version:
        raise RuntimeError(
            f"[CDF_VERSION_PURGED_BY_VACUUM] Cannot read CDF startingVersion={starting_version}: "
            f"versions < {earliest_available_cdf_version} were already removed by VACUUM!"
        )

    return True


def prepare_cdf_merge_batch(
    cdf_rows: list[dict[str, Any]],
    key_col: str = "customer_id",
    filter_preimages: bool = True,
    deduplicate_latest: bool = True,
) -> list[dict[str, Any]]:
    """
    Prepare a CDF micro-batch for `MERGE INTO` on a target Delta table:
      1. Filter out `_change_type == 'update_preimage'` (old values before UPDATE).
      2. Keep only the latest change per `key_col` ordered by `_commit_version DESC`
         (equivalent to `QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _commit_version DESC) = 1`).
    """
    rows = (
        [r for r in cdf_rows if r.get("_change_type") != "update_preimage"]
        if filter_preimages
        else list(cdf_rows)
    )

    if deduplicate_latest:
        latest_by_key: dict[Any, dict[str, Any]] = {}
        for row in sorted(rows, key=lambda r: r["_commit_version"]):
            latest_by_key[row[key_col]] = row
        rows = list(latest_by_key.values())

    # Simulate Delta MERGE duplicate source key check
    seen_keys: set[Any] = set()
    for row in rows:
        k = row[key_col]
        if k in seen_keys:
            raise RuntimeError(
                "[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] "
                f"Multiple CDF rows found for key {k!r}! "
                "Filter out `update_preimage` and deduplicate by `_commit_version DESC` before MERGE."
            )
        seen_keys.add(k)

    return rows
