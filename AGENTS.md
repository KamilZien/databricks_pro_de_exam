# AGENTS.md — Databricks DE Professional (Oct 2026+) Official Exam Coach & Portfolio Engine

> **Cross-Model Standard:** Every AI model (Gemini 3.8, Claude Sonnet 4.6, Claude Opus 4.5, gpt-oss 120B)
> MUST read this file, [`EXAM_GUIDE.md`](./EXAM_GUIDE.md), and [`STATE.md`](./STATE.md) at the start of every session.
> Always ground claims in official Databricks docs (`https://docs.databricks.com/...`). If unverified, state `[UNVERIFIED - Check docs.databricks.com/<topic>]`.
> Before and after every task, read and update [`STATE.md`](./STATE.md).

---

## 1. USER PROFILE & ZERO-COST ENVIRONMENT

- **User Profile:** Passed Databricks Certified Data Engineer Associate in 2023. Taking 2 weeks to pass the **Databricks Certified Data Engineer Professional** exam (Oct 2026+ outline) and build a production-grade portfolio project (`databricks-serverless-finops-lakehouse`).
- **Zero-Cost Cloud Stack:** IDE + Databricks extension (`databricks.databricks`) + `databricks-connect` (Spark Connect) + MCP (`databricks-mcp-genie`) connected to **Databricks Free Edition Serverless** (`DatabricksSession.builder.serverless(True).getOrCreate()`).
- **Free Edition Guardrails:** Keep synthetic datasets between **500–1,000 rows**. Always use `.trigger(availableNow=True)` for micro-batch execution so serverless compute shuts down immediately.

---

## 2. STRICT 2026 SERVERLESS & EXAM LAWS

Every line of code and quiz question MUST adhere to these official rules:

### Law 1 — No Legacy Storage or Hive
- **NEVER** use `dbfs:/mnt/`, Mount Points, or `hive_metastore`.
- **ALWAYS** use 3-level Unity Catalog namespaces: `portfolio_catalog.<schema>.<table>`.
- **ALWAYS** use UC Volumes: `/Volumes/portfolio_catalog/landing/raw_events/`.

### Law 2 — Liquid Clustering Over Partition/ZOrder
- **NEVER** use `PARTITION BY` or `ZORDER BY` on Delta tables.
- **ALWAYS** use `CLUSTER BY (...)` or `CLUSTER BY AUTO` + Predictive Optimization.

### Law 3 — Serverless / Spark Connect Constraints
- **NEVER** call `.cache()`, `.persist()`, `CACHE TABLE`, `.rdd`, or `spark.sparkContext`.
  - Spark Connect throws runtime exceptions on manual caching and RDD APIs.
  - Relies on managed disk caching and lazy evaluation.
- **NEVER** use continuous streaming triggers (`Trigger.ProcessingTime`).
  - **ALWAYS** use `.trigger(availableNow=True)` to shut down Serverless compute immediately.

### Law 4 — Lakeflow Declarative Pipelines (formerly DLT)
- **NEVER** manually define `checkpointLocation` or `cloudFiles.schemaLocation` inside a Lakeflow pipeline definition.
- Use `AUTO CDC` (`CREATE FLOW ... AS AUTO CDC INTO` or `dlt.create_auto_cdc_flow`) with `KEYS`, `SEQUENCE BY`, and `STORED AS SCD TYPE 1/2`.
- Free Edition allows **1 active Lakeflow Pipeline** — bundle Bronze and Silver flows into a single pipeline in `databricks.yml`.

### Law 5 — Semi-Structured Data
- Prefer the **`VARIANT`** data type (`parse_json`, `variant_get`, `:` path syntax) over brittle `from_json()` struct schemas for schema-drifting Bronze JSON.

### Law 6 — Streaming Joins, Watermarks & Idempotency
- **Stream-Stream Inner Joins:** Watermarks + time-range constraints are *optional* for correctness, *required* for state cleanup.
- **Stream-Stream Outer Joins:** Watermarks on **BOTH** streams AND an event-time range condition (`BETWEEN ... AND ...`) are **mandatory**.
- **Deduplication:** `.withWatermark("ts", "X hours").dropDuplicates(["composite_key_1", "composite_key_2"])` — never include the exact timestamp in `dropDuplicates`.
- **`foreachBatch` Idempotency:** Write using `.option("txnVersion", batchId).option("txnAppId", app_id)`.
- **Testing:** Use `assertDataFrameEqual` and `assertSchemaEqual` from `pyspark.testing` with `pytest` and pure `DataFrame.transform()` functions.

### Law 7 — GDPR Physical Purge & Deletion Vectors
- Tables enable **Deletion Vectors** by default.
- To physically purge PII:
  1. `DELETE FROM <table> WHERE ...`
  2. `REORG TABLE <table> APPLY (PURGE)`
  3. `VACUUM <table> RETAIN <hours> HOURS`
- Propagate deletes downstream via **Change Data Feed** (`delta.enableChangeDataFeed = true`).

---

## 3. RENAMED PRODUCTS QUICK REFERENCE

| Deprecated / Distractor | Official Oct 2026+ Name |
|---|---|
| Delta Live Tables (DLT) | Lakeflow Pipelines |
| `APPLY CHANGES INTO` | `AUTO CDC` APIs (`CREATE FLOW ... AS AUTO CDC INTO`) |
| Databricks Asset Bundles / `dbx` | Declarative Automation Bundles (`databricks.yml`) |
| Databricks Workflows | Lakeflow Jobs |
| Databricks Repos | Databricks Git Folders |
| Delta Sharing | OpenSharing (D2D and Databricks-to-Open) |
| Custom JDBC Ingestion | Lakeflow Connect |
| Databricks SQL Alerts | Databricks Lakehouse Alerts |
| Hardcoded BI Metrics | Unity Catalog Metric Views |
| `PARTITION BY` + `ZORDER BY` | Liquid Clustering (`CLUSTER BY`) + Predictive Optimization |

---

## 4. SKILLS & WORKFLOW COMMANDS

| Command | Skill Path | Purpose |
|---|---|---|
| `/orient <section>` | `skills/orient/SKILL.md` | Plain-English 1-sentence overview per objective + repo mapping |
| `/diagnose <section>` | `skills/diagnose/SKILL.md` | 10 calibrated MCQs on section; wait for answers, grade with citations, log weak areas |
| `/deepdive <objective>` | `skills/deepdive/SKILL.md` | Deep dive into missed objective using docs.databricks.com citations + code sample |
| `/build <module>` | `skills/build/SKILL.md` | Vibe-code hands-on task on Serverless + inject 2 sabotage bugs in tests |
| `/sanitize` | `skills/sanitize/SKILL.md` | Verify community dump questions against 2026 syllabus + 10s elimination rule |
| `/mock` & `/repair` | `skills/mock-repair/SKILL.md` | Full weighted mock exam + Repair loop on missed questions |
