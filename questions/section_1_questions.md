# Section 1: Developing Code — Calibrated Exam Question Bank

> **Domain:** Section 1: Developing Code for Data Processing using Python and SQL (23%)  
> **Source:** Post-October 2026 Databricks Certified Data Engineer Professional Syllabus  
> **Purpose:** Permanent retention, review, and exam revision.

---

## Question Bank Index

- [Subpoint 1.1: DABs Project Structures & Environments](#subpoint-11-dabs-project-structures--environments)
- [Subpoint 1.2: Dependency Conflicts & Resolution](#subpoint-12-dependency-conflicts--resolution)
- [Subpoint 1.3: User-Defined Functions (UDFs) & Unity Catalog](#subpoint-13-user-defined-functions-udfs--unity-catalog)
- [Subpoint 1.4: Lakeflow Pipelines & Auto Loader](#subpoint-14-lakeflow-pipelines--auto-loader)
- [Subpoint 1.5: Lakeflow Jobs Automation](#subpoint-15-lakeflow-jobs-automation)
- [Subpoint 1.6: Streaming Tables vs. Materialized Views](#subpoint-16-streaming-tables-vs-materialized-views)
- [Subpoint 1.7: AUTO CDC & SCD Type 1/2](#subpoint-17-auto-cdc--scd-type-12)
- [Subpoint 1.8: Structured Streaming vs. Lakeflow Declarative Pipelines](#subpoint-18-structured-streaming-vs-lakeflow-declarative-pipelines)
- [Subpoint 1.9: Lakeflow Jobs Control-Flow Operators](#subpoint-19-lakeflow-jobs-control-flow-operators)
- [Subpoint 1.10: Compute Selection & Serverless Constraints](#subpoint-110-compute-selection--serverless-constraints)
- [Subpoint 1.11: Unit & Integration Testing](#subpoint-111-unit--integration-testing)
- [Subpoint 1.12: Structured Streaming Stateful Processing & Idempotency](#subpoint-112-structured-streaming-stateful-processing--idempotency)

---

### Subpoint 1.1: DABs Project Structures & Environments

#### Question 1.1.1: Development vs. Production Mode Semantics
A data engineering team is deploying a Lakeflow Pipeline and a scheduled Lakeflow Job using Declarative Automation Bundles (DABs). A developer deploys their bundle using a custom target configured with `mode: development`.

Which set of behaviors will the Databricks CLI automatically enforce for this deployment?

- A) The pipeline is deployed without prefixes, schedules remain active, and code is synced to `/Workspace/.bundle/`.
- B) Deployed resource names are prefixed with `[dev <username>]`, scheduled job triggers are automatically paused, and files are deployed under the user's home workspace directory.
- C) The deployment fails unless a Service Principal token is provided in the `run_as` block.
- D) The bundle forces the execution of all unit tests in `tests/` before provisioning any workspace compute.

**Correct Answer:** **B**

> **Explanation:**  
> In Databricks Asset Bundles, setting `mode: development` provides three automatic safety guardrails:
> 1. It prefixes resource names with `[dev <username>]` so developers don't clash with shared or production assets.
> 2. It automatically pauses triggers and schedules on jobs so unintended cron runs don't consume compute during active development.
> 3. It targets the developer's personal workspace directory (`/Workspace/Users/<username>/.bundle/...`).
> Production mode (`mode: production`), conversely, keeps exact names, enables schedules, and typically runs under a service principal.

> **🎯 10-Second Elimination Rule:**  
> Look for "prefixed with `[dev <username>]`" and "schedules are automatically paused". Any option claiming schedules remain active in `development` mode is an immediate elimination.

---

#### Question 1.1.2: Modular Project Layout and Artifact Resolution
A data engineer is structuring a large-scale project containing multiple Lakeflow pipelines, daily batch jobs, and a shared Python utility package in `src/`. The team wants to ensure clean CI/CD integration, modular code review, and prevent `databricks.yml` from becoming a monolithic configuration file.

Which project structure represents the Databricks recommended best practice for Declarative Automation Bundles?

- A) Store all job definitions, pipeline definitions, and Python code inside a single monolithic `databricks.yml` file in the root directory.
- B) Separate pipeline and job definitions into individual YAML files inside a `resources/` directory, reference them in `databricks.yml` using `include: - resources/*.yml`, and configure the `artifacts` section to package `src/` into a wheel.
- C) Keep pipeline definitions in `dbfs:/FileStore/bundles/`, compile Python scripts into `.jar` files, and reference them via DBFS mounts.
- D) Store all pipeline configurations as JSON inside Unity Catalog Volume metadata tags and invoke them dynamically using `spark.sql()`.

**Correct Answer:** **B**

> **Explanation:**  
> The enterprise standard for DABs separates infrastructure resources into modular YAML files within a `resources/` directory (`resources/pipelines.yml`, `resources/jobs.yml`), which are imported into the root `databricks.yml` via `include: - resources/*.yml`. Shared Python logic in `src/` is packaged as an installable wheel using the `artifacts` block (`type: whl`), allowing automated build and deployment across `dev` and `prod` targets. DBFS mounts (Option C) and monolithic configurations (Option A) violate 2026 enterprise best practices.

> **🎯 10-Second Elimination Rule:**  
> Eliminate any option mentioning `dbfs:/` or mounts (violates Law 1). Eliminate monolithic configurations. Look for the `include:` directive with `resources/*.yml` and wheel packaging in `artifacts`.

