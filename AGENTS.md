# AGENTS.md — Databricks DE Professional (Oct 2026) Exam Prep & Portfolio Engine

> **Cross-Model Standard:** Every AI model (Gemini, Claude Sonnet 4.6, Claude Opus 4.5, gpt-oss 120B)
> MUST read this file at the start of every session and obey every rule below.
> Before and after every task, read and update [`STATE.md`](./STATE.md).

---

## 1. USER PROFILE & GOAL

- **Background:** Passed Databricks Certified Data Engineer Associate 3 years ago (2023). Taking 2 weeks off work to pass the **Databricks Certified Data Engineer Professional** exam (updated October 2026 syllabus) with minimal wasted effort while vibe-coding a senior-level portfolio project (`databricks-serverless-finops-lakehouse`) to show off in technical interviews.
- **Zero-Cost Cloud Stack:** IDE + official Databricks extension (`databricks.databricks`) + `databricks-connect` (Spark Connect) + MCP (`databricks-mcp-genie`) connected to **Databricks Free Edition Serverless** (`DatabricksSession.builder.serverless(True).getOrCreate()`). No local Java/Spark installation required.
- **Multi-Model Environment:** The user switches dynamically between models. Maintain project state on disk in `STATE.md` and rules in this file so any model can take over seamlessly in a new chat session.

---

## 2. STRICT 2026 DATABRICKS & FREE EDITION SERVERLESS LAWS

Every line of code generated or reviewed MUST obey these rules (which are also direct exam answers):

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
  - It uses automatic disk caching and lazy schema validation.
- **NEVER** use continuous streaming triggers (`Trigger.ProcessingTime`).
  - **ALWAYS** use `.trigger(availableNow=True)` so Free Edition Serverless compute shuts down immediately after processing micro-batches and preserves daily quota.
- Keep synthetic generated datasets around **500–1,000 rows** to protect Free Edition quotas.

### Law 4 — Lakeflow Declarative Pipelines (formerly DLT)
- **NEVER** manually define `checkpointLocation` or `cloudFiles.schemaLocation` inside a Lakeflow pipeline.
- Use `AUTO CDC` (`CREATE FLOW ... AS AUTO CDC INTO` or `dlt.create_auto_cdc_flow`) with `KEYS`, `SEQUENCE BY`, and `STORED AS SCD TYPE 1/2`.
- Free Edition allows **1 active Lakeflow Declarative Pipeline** — bundle Bronze and Silver flows into a single unified pipeline definition in `databricks.yml`.

### Law 5 — Semi-Structured Data
- Prefer the **`VARIANT`** data type (`parse_json`, `variant_get`, `:` path syntax) over brittle `from_json()` struct schemas for schema-drifting JSON in Bronze.

### Law 6 — Streaming Joins, Watermarks & Idempotency
- **Stream-Stream Inner Joins:** Watermarks + time-range constraints are *optional* for correctness, *required* for state cleanup (preventing OOM).
- **Stream-Stream Outer Joins** (Left/Right/Full): Watermarks on **BOTH** streams AND an event-time range condition (`BETWEEN`) are **mandatory** (fails compilation without them).
- **Deduplication:** Use `.withWatermark("ts", "X hours").dropDuplicates(["composite_key_1", "composite_key_2"])` — **never** include the exact timestamp column inside `dropDuplicates` if duplicates arrive hours apart!
- **`foreachBatch` Idempotency:** Always accept `(microBatchDF, batchId)` and write to Delta using `.option("txnVersion", batchId).option("txnAppId", app_id)`.
- **Testing Framework:** Use `assertDataFrameEqual` and `assertSchemaEqual` (from `pyspark.testing`) alongside `pytest` and `DataFrame.transform()` for modular pure functions.

### Law 7 — GDPR Physical Purge & Deletion Vectors
- Modern Delta tables enable **Deletion Vectors** (soft-delete bitmaps).
- To physically purge PII for GDPR compliance:
  1. `DELETE FROM <table> WHERE ...`
  2. `REORG TABLE <table> APPLY (PURGE)`
  3. `VACUUM <table> RETAIN <hours> HOURS`
- Propagate deletes downstream via **Change Data Feed** (`delta.enableChangeDataFeed = true` and `.option("readChangeFeed", "true")`).

---

## 3. THE 2-WEEK PORTFOLIO + EXAM ROADMAP

| Module | Days | Focus | Exam Weight |
|--------|------|-------|-------------|
| **1** | 1–2 | Workspace, DABs & Bronze Ingestion | Ingestion (7%), Developing Code (22%) |
| **2** | 3–5 | Silver Lakeflow Quarantine, AUTO CDC & Stream Joins | Developing Code (22%), Transformation & Quality (10%), Data Modelling (6%) |
| **3** | 6–7 | Gold Materialized Views, Liquid Clustering & FinOps | Cost & Performance (13%), Monitoring & Alerting (10%) |
| **4** | 8–9 | Unity Catalog Governance, Sharing & GDPR Purge | Security & Compliance (10%), Governance (7%), Sharing & Federation (5%) |
| **5** | 10–11 | CI/CD, Unit Tests & Interview Polish | Debugging & Deploying (10%) |
| **6** | 12–14 | Rapid Exam Question Blitz (350+ questions) | All domains |

### Module Details

**Module 1 (Days 1–2): Workspace, DABs & Bronze Ingestion**
- `databricks.yml` — DAB bundle configuration
- `src/generator/chaos_webhook_generator.py` — writes messy JSON to UC Volume + mock `system.billing.usage` table
- `src/pipelines/01_bronze_variant.py` — Auto Loader `cloudFiles` + `VARIANT` + `_metadata`

**Module 2 (Days 3–5): Silver Lakeflow Quarantine, AUTO CDC & Stream Joins**
- `src/pipelines/02_silver_cdc_quarantine.py` — Lakeflow Expectations quarantine split, `AUTO CDC` SCD Type 2
- `src/transformations/stream_joins.py` — watermarked outer joins + idempotent `foreachBatch`

**Module 3 (Days 6–7): Gold Materialized Views, Liquid Clustering & FinOps**
- `src/pipelines/03_gold_finops_mv.sql` — Streaming Tables vs. Materialized Views, `CLUSTER BY AUTO`
- DBU cost attribution queries against `system.billing.usage` + `system.billing.list_prices`

**Module 4 (Days 8–9): Unity Catalog Governance, Sharing & GDPR Purge**
- `src/governance/04_uc_abac_and_gdpr.sql` — `ALTER TABLE ... SET TAGS`, SQL UDF `ROW FILTER` and `COLUMN MASK`
- CDF delete propagation, `REORG APPLY (PURGE)` + `VACUUM`
- Lakehouse Federation & Delta Sharing D2D/D2O patterns

**Module 5 (Days 10–11): CI/CD, Unit Tests & Interview Polish**
- `tests/test_pipeline.py` — `pytest` + `assertDataFrameEqual`
- `.github/workflows/deploy.yml`
- Recruiter-ready `README.md` with Mermaid diagrams and Architectural Decision Records

**Module 6 (Days 12–14): Rapid Exam Question Blitz**
- Sanitizing and drilling 350+ questions using 10-second elimination rules

---

## 4. ON-DEMAND WORKFLOW SKILLS

| Command | Skill Location | Purpose |
|---------|---------------|---------|
| `/build` | `skills/build-module/SKILL.md` | Build the current active module from `STATE.md` |
| `/drill` | `skills/exam-sabotage-drill/SKILL.md` | Sabotage + code-diff quiz for exam drilling |
| `/sanitize` | `skills/sanitize-dump-questions/SKILL.md` | Verify community dump questions against Oct 2026 syllabus |
