"""
Section 5, Subpoint 5.1: Liquid Clustering, Predictive Optimization & Delta File Layout
Official Docs:
  - https://docs.databricks.com/en/delta/clustering.html
  - https://docs.databricks.com/en/optimizations/predictive-optimization.html
  - https://docs.databricks.com/en/delta/deletion-vectors.html

Provides:
  1. `validate_table_clustering_and_po`: Enforces 2026 Databricks storage & governance rules:
       - `PARTITION BY` and `CLUSTER BY` are mutually exclusive (Law 2: never use `PARTITION BY` on Liquid tables).
       - Clustering columns must be indexed by Delta Data Skipping Statistics
         (`col_index < data_skipping_num_indexed_cols`, default 32).
       - Predictive Optimization ONLY runs on Unity Catalog `MANAGED` tables (never `EXTERNAL` tables).
  2. `simulate_gdpr_physical_purge`: Enforces the 3-step physical purge sequence when Deletion Vectors
     are enabled (`DELETE` -> `REORG TABLE ... APPLY (PURGE)` -> `VACUUM`).
"""

from typing import Any


def validate_table_clustering_and_po(
    table_type: str,
    partition_cols: list[str],
    cluster_cols: list[str] | str,
    column_order: list[str],
    predictive_optimization: bool,
    data_skipping_num_indexed_cols: int = 32,
) -> dict[str, Any]:
    """
    Validate Delta table layout & Predictive Optimization configuration.
    """
    if partition_cols and cluster_cols:
        raise ValueError(
            "[CLUSTER_BY_WITH_PARTITION_BY] `PARTITION BY` and `CLUSTER BY` are mutually exclusive. "
            "Remove `PARTITION BY` and use Liquid Clustering (`CLUSTER BY (...)` or `CLUSTER BY AUTO`)."
        )

    if predictive_optimization and table_type.upper() != "MANAGED":
        raise ValueError(
            f"[PREDICTIVE_OPTIMIZATION_UNSUPPORTED_TABLE_TYPE] Predictive Optimization only runs on "
            f"Unity Catalog MANAGED tables, but table_type is {table_type!r}. "
            "Convert to a UC Managed table or schedule manual OPTIMIZE/VACUUM."
        )

    if isinstance(cluster_cols, list):
        if len(cluster_cols) > 4:
            raise ValueError(
                f"[TOO_MANY_CLUSTERING_COLUMNS] Liquid Clustering supports at most 4 keys, got {len(cluster_cols)}."
            )
        for col in cluster_cols:
            if col not in column_order:
                raise ValueError(f"Clustering column {col!r} not found in table schema.")
            col_idx = column_order.index(col)
            if col_idx >= data_skipping_num_indexed_cols:
                raise ValueError(
                    f"[MISSING_DATA_SKIPPING_STATS] Clustering column {col!r} is at position {col_idx} "
                    f"(>= `delta.dataSkippingNumIndexedCols`={data_skipping_num_indexed_cols}). "
                    "Move the clustering key within the first 32 columns or raise `delta.dataSkippingNumIndexedCols`."
                )

    return {
        "valid": True,
        "clustering": cluster_cols,
        "predictive_optimization_active": predictive_optimization and table_type.upper() == "MANAGED",
    }


def simulate_gdpr_physical_purge(
    operations_executed: list[str],
    deletion_vectors_enabled: bool = True,
) -> dict[str, Any]:
    """
    Verify whether PII bytes have been physically purged from cloud storage:
      - `DELETE`: Soft-marks rows in a Deletion Vector bitmap (old Parquet file still contains PII!).
      - `REORG_PURGE`: Rewrites Parquet files without deleted rows (required when DVs are enabled).
      - `VACUUM`: Physically removes unreferenced old Parquet files from storage.
    """
    has_delete = "DELETE" in operations_executed
    has_reorg_or_optimize = "REORG_PURGE" in operations_executed or "OPTIMIZE" in operations_executed
    has_vacuum = "VACUUM" in operations_executed

    if deletion_vectors_enabled and has_delete and not has_reorg_or_optimize:
        return {
            "logical_rows_hidden": True,
            "physical_pii_purged": False,
            "reason": (
                "Deletion Vectors enabled: DELETE only wrote a soft bitmap; run REORG TABLE ... APPLY (PURGE) first!"
            ),
        }

    if not has_vacuum:
        return {
            "logical_rows_hidden": has_delete,
            "physical_pii_purged": False,
            "reason": "Old unreferenced Parquet files still remain in cloud storage until VACUUM runs!",
        }

    return {
        "logical_rows_hidden": True,
        "physical_pii_purged": True,
        "reason": "PII physically purged via DELETE -> REORG PURGE -> VACUUM.",
    }
