---
name: build-module
description: >
  Build the current active module from STATE.md. Separates pure testable DataFrame
  logic (DataFrame.transform) from pipeline wrappers, validates for Serverless quota
  safety, and updates STATE.md when complete.
---

# /build — Build Current Module

## Trigger
User types `/build` in chat.

## Instructions

### Step 1 — Read State
1. Read `STATE.md` to determine the **Current Module** and **Current Step**.
2. Read `AGENTS.md` §2 (Serverless Laws) to refresh constraints.

### Step 2 — Plan the Module Build
Based on the current module from STATE.md:

| Module | Files to Build |
|--------|---------------|
| 1 | `src/generator/chaos_webhook_generator.py`, `src/pipelines/01_bronze_variant.py` |
| 2 | `src/pipelines/02_silver_cdc_quarantine.py`, `src/transformations/stream_joins.py` |
| 3 | `src/pipelines/03_gold_finops_mv.sql` |
| 4 | `src/governance/04_uc_abac_and_gdpr.sql` |
| 5 | `tests/test_pipeline.py`, `.github/workflows/deploy.yml`, `README.md` |
| 6 | N/A (exam drill mode only — use `/drill`) |

### Step 3 — Code Generation Rules
1. **Separate concerns:** Pure DataFrame transformation functions go in `src/transformations/` using `DataFrame.transform()` pattern. Pipeline wrappers (Auto Loader, DLT/Lakeflow, streaming sinks) go in `src/pipelines/`.
2. **Every function must be testable** without a live Databricks cluster (use `DatabricksSession` in conftest).
3. **Validate all 7 Serverless Laws** from `AGENTS.md` §2 before writing any code:
   - No `.cache()`, `.persist()`, `.rdd`, `spark.sparkContext`
   - No `Trigger.ProcessingTime` — only `.trigger(availableNow=True)`
   - No `PARTITION BY` or `ZORDER BY` — only `CLUSTER BY`
   - No `dbfs:/mnt/` or `hive_metastore` — only UC 3-level namespace
   - No manual `checkpointLocation` inside Lakeflow pipelines
   - Datasets ≤ 1,000 rows
   - Use `VARIANT` for semi-structured Bronze data
4. **Write corresponding tests** in `tests/test_<module>.py`.

### Step 4 — Validate & Update State
1. Run `ruff check src/ tests/` to lint.
2. Run `pytest tests/ -m "not integration" -v` for unit tests.
3. Update `STATE.md`:
   - Set module status to `COMPLETE` if all tests pass.
   - Advance **Current Module** to the next module.
   - Write **Handoff Notes** for the next model.

### Step 5 — Summary
Print a concise summary:
- Files created/modified
- Tests passed/failed
- Exam concepts covered
- What the user should do next (e.g., `/drill` to practice, or `/build` for next module)
