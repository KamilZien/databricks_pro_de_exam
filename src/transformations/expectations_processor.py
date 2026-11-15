"""
Section 3, Subpoint 3.4: Data Quality Expectations, Quarantine Pattern & Event Log Auditing
Official Docs: https://docs.databricks.com/en/ldp/expectations.html

Provides:
  1. Pure Python emulators of Lakeflow Spark Declarative Pipeline expectation modes:
       - `expect` / `expect_all` (retain mode: keeps bad rows, logs passed/failed counts)
       - `expect_or_drop` / `expect_all_or_drop` (drop mode: drops bad rows silently, logs counts)
       - `expect_or_fail` / `expect_all_or_fail` (fail mode: aborts pipeline on first violation)
  2. Quarantine view splitter (`apply_quarantine_split`) demonstrating why a Quarantine pipeline
     MUST use `retain` (`expect_all`) on the staging view rather than `drop` (`expect_all_or_drop`)!
  3. Event Log expectation metric parser (`parse_event_log_expectations`) matching
     `details:flow_progress.data_quality.expectations`.
"""

from collections.abc import Callable
from typing import Any


def evaluate_expectations(
    records: list[dict[str, Any]],
    rules: dict[str, Callable[[dict[str, Any]], bool]],
    mode: str = "retain",
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    """
    Emulates Lakeflow `@dp.expect_all` (`mode='retain'`), `@dp.expect_all_or_drop` (`mode='drop'`),
    and `@dp.expect_all_or_fail` (`mode='fail'`).

    Returns:
      - `output_rows`: Rows emitted to the downstream dataset.
      - `metrics`: Dict mapping each rule name -> `{"passed_records": int, "failed_records": int}`.
    """
    valid_modes = {"retain", "drop", "fail"}
    if mode not in valid_modes:
        raise ValueError(f"Invalid expectation mode {mode!r}. Expected one of {valid_modes}.")

    metrics: dict[str, dict[str, int]] = {
        rule_name: {"passed_records": 0, "failed_records": 0} for rule_name in rules
    }
    output_rows: list[dict[str, Any]] = []

    for row in records:
        row_failed_any = False
        for rule_name, predicate in rules.items():
            if predicate(row):
                metrics[rule_name]["passed_records"] += 1
            else:
                metrics[rule_name]["failed_records"] += 1
                row_failed_any = True

        if row_failed_any:
            if mode == "fail":
                raise RuntimeError(
                    f"[DELTA_PIPELINE_EXPECTATION_VIOLATION] Expectation failed in `fail` mode on row: {row!r}"
                )
            if mode == "drop":
                continue  # Silently dropped! Never reaches downstream quarantine table!

        output_rows.append(dict(row, _is_valid=not row_failed_any))

    return output_rows, metrics


def split_clean_and_quarantine(
    records: list[dict[str, Any]],
    rules: dict[str, Callable[[dict[str, Any]], bool]],
    staging_expectation_mode: str = "retain",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, int]]]:
    """
    Emulates a Lakeflow Quarantine Pipeline:
      1. `silver_staging` view evaluates `rules` using `staging_expectation_mode`.
      2. `clean_events` keeps rows where `_is_valid == True`.
      3. `quarantine_events` keeps rows where `_is_valid == False`.

    CRITICAL TRAP: If `staging_expectation_mode="drop"` (`@dp.expect_all_or_drop`),
    bad rows are dropped BEFORE reaching `quarantine_events`, leaving `quarantine_events` empty!
    """
    staged_rows, metrics = evaluate_expectations(records, rules, mode=staging_expectation_mode)
    clean_rows = [r for r in staged_rows if r["_is_valid"]]
    quarantine_rows = [r for r in staged_rows if not r["_is_valid"]]
    return clean_rows, quarantine_rows, metrics


def parse_event_log_expectations(event_log_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Emulates querying Lakeflow `event_log('<pipeline-id>')` for `event_type = 'flow_progress'`
    and exploding `details.flow_progress.data_quality.expectations`.
    """
    extracted: list[dict[str, Any]] = []
    for event in event_log_rows:
        if event.get("event_type") != "flow_progress":
            continue
        expectations = (
            event.get("details", {})
            .get("flow_progress", {})
            .get("data_quality", {})
            .get("expectations", [])
        )
        for exp in expectations:
            extracted.append(
                {
                    "expectation_name": exp["name"],
                    "target_table": exp["dataset"],
                    "passed_records": exp["passed_records"],
                    "failed_records": exp["failed_records"],
                }
            )
    return extracted
