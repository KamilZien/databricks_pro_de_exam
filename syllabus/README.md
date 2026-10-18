# Databricks Certified Data Engineer Professional Syllabus (Oct 2026+)

This directory contains deep-dive documentation, architectural blueprints, and exam rules mapped 1-to-1 with the 9 official exam domains.

---

## 9 Official Exam Domains & Weights

| Domain | Weight | Focus Areas | Key Files in Repo |
|---|---|---|---|
| **[Section 1: Developing Code for Data Processing](./section_1_developing_code.md)** | **23%** | Lakeflow Pipelines, AUTO CDC (SCD 1 & 2), UDFs, Watermarking, Idempotent `foreachBatch`, Serverless Caching rules | `src/pipelines/01_bronze_variant.py`, `src/pipelines/02_silver_cdc_quarantine.py`, `src/transformations/` |
| **[Section 2: Data Ingestion & Acquisition](./section_2_data_ingestion.md)** | **12%** | Auto Loader (`cloudFiles`), Lakeflow Connect, OpenSharing (D2D, D2O), Lakehouse Federation | `src/generator/chaos_webhook_generator.py` |
| **[Section 3: Data Manipulation](./section_3_data_manipulation.md)** | **12%** | `VARIANT` semi-structured data, Lakeflow Expectations (Drop, Fail, Quarantine), AI Functions (`ai_query`) | `src/transformations/silver_transforms.py` |
| **[Section 4: Monitoring & Alerting](./section_4_monitoring_alerting.md)** | **10%** | System Tables (`system.billing.usage`, `system.access`), Lakeflow Event Logs, Lakehouse Alerts | `portfolio_catalog.billing_mock.usage` |
| **[Section 5: Cost & Performance Optimization](./section_5_cost_performance.md)** | **15%** | Liquid Clustering (`CLUSTER BY`), Predictive Optimization, Change Data Feed (CDF), Serverless NVMe disk caching | `STATE.md`, `tests/test_exam_drill.py` |
| **[Section 6: Ensuring Security & Compliance](./section_6_security_compliance.md)** | **8%** | ABAC with Governed Tags, Row Filters, Column Masks, GDPR Purge (`DELETE` + `REORG APPLY (PURGE)` + `VACUUM`) | `src/governance/` *(Module 4)* |
| **[Section 7: Data Governance](./section_7_data_governance.md)** | **5%** | Unity Catalog Privilege Inheritance, Tags, Comments, Lineage | Unity Catalog 3-level namespace |
| **[Section 8: Debugging & Deploying](./section_8_debugging_deploying.md)** | **10%** | Declarative Automation Bundles (`databricks.yml`), Databricks Git Folders, Job Repair & Rerun | `databricks.yml`, `.github/workflows/deploy.yml` |
| **[Section 9: Data Modeling](./section_9_data_modeling.md)** | **5%** | Dimensional Modeling, Streaming Tables vs. Materialized Views, Unity Catalog Metric Views | `portfolio_catalog.gold` *(Module 3)* |
