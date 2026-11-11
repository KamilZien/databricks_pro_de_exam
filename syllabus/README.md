# Databricks Certified Data Engineer Professional Syllabus (Oct 2026+)

This directory contains deep-dive documentation, architectural blueprints, Master Q&A Vaults, and calibrated question banks mapped 1-to-1 with the 9 official exam domains.

---

## 9 Official Exam Domains & Weights

| Domain | Weight | Study Guide & Q&A Vault | Calibrated Question Bank | Key Files in Repo |
|---|---|---|---|---|
| **Section 1: Developing Code for Data Processing** | **23%** | [section_1_developing_code.md](./section_1_developing_code.md) | [section_1_questions.md](./section_1_questions.md) (`25/25` ✅) | `src/pipelines/`, `tests/unit/section_1/`, `tests/integration/section_1/` |
| **Section 2: Data Ingestion & Acquisition** | **12%** | [section_2_data_ingestion.md](./section_2_data_ingestion.md) | [section_2_questions.md](./section_2_questions.md) (`10/10` ✅) | `src/ingestion/`, `tests/unit/section_2/`, `tests/integration/section_2/` |
| **Section 3: Data Manipulation** | **12%** | [section_3_data_manipulation.md](./section_3_data_manipulation.md) | `section_3_questions.md` *(Next)* | `src/transformations/` |
| **Section 4: Monitoring & Alerting** | **10%** | `section_4_monitoring_alerting.md` | `section_4_questions.md` | `portfolio_catalog.billing_mock.usage` |
| **Section 5: Cost & Performance Optimization** | **15%** | `section_5_cost_performance.md` | `section_5_questions.md` | `tests/integration/section_1/test_exam_drill.py` |
| **Section 6: Ensuring Security & Compliance** | **8%** | `section_6_security_compliance.md` | `section_6_questions.md` | `src/governance/` |
| **Section 7: Data Governance** | **5%** | `section_7_data_governance.md` | `section_7_questions.md` | Unity Catalog 3-level namespace |
| **Section 8: Debugging & Deploying** | **10%** | `section_8_debugging_deploying.md` | `section_8_questions.md` | `databricks.yml`, `resources/` |
| **Section 9: Data Modeling** | **5%** | `section_9_data_modeling.md` | `section_9_questions.md` | `src/pipelines/04_gold_materialized_views.py` |
