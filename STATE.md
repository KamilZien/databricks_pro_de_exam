# STATE.md — Persistent Cross-Model Memory

> **Last Updated:** 2026-10-17T23:19:00+02:00
> **Last Model:** Claude Opus 4.6 (Thinking)
> **Last Action:** Initial project bootstrap

---

## Active Module

| Field | Value |
|-------|-------|
| **Current Module** | 1 — Workspace, DABs & Bronze Ingestion |
| **Status** | `IN_PROGRESS` |
| **Current Step** | Scaffolding complete. Ready to flesh out and test `chaos_webhook_generator.py` and `01_bronze_variant.py`. |
| **Next Action** | Connect to Databricks Free Edition, run generator to populate UC Volume, then run Bronze pipeline with Auto Loader. |

---

## Module Progress Tracker

| Module | Status | Notes |
|--------|--------|-------|
| 1 — Workspace, DABs & Bronze Ingestion | 🟡 `IN_PROGRESS` | Files scaffolded: `databricks.yml`, `chaos_webhook_generator.py`, `01_bronze_variant.py` |
| 2 — Silver Lakeflow Quarantine, AUTO CDC & Stream Joins | ⬜ `NOT_STARTED` | |
| 3 — Gold Materialized Views, Liquid Clustering & FinOps | ⬜ `NOT_STARTED` | |
| 4 — Unity Catalog Governance, Sharing & GDPR Purge | ⬜ `NOT_STARTED` | |
| 5 — CI/CD, Unit Tests & Interview Polish | ⬜ `NOT_STARTED` | |
| 6 — Rapid Exam Question Blitz | ⬜ `NOT_STARTED` | |

---

## Exam Drill Stats

| Metric | Value |
|--------|-------|
| Sabotage Drills Completed | 0 |
| Code-Diff Quizzes Answered | 0 |
| Dump Questions Sanitized | 0 |
| Accuracy Rate | N/A |

---

## Files Created

```
exam_prep/
├── AGENTS.md                          # Cross-model rules (canonical)
├── CLAUDE.md                          # → @AGENTS.md pointer
├── GEMINI.md                          # → @AGENTS.md pointer
├── STATE.md                           # This file (persistent memory)
├── databricks.yml                     # DAB bundle config
├── pyproject.toml                     # Python dependencies
├── .gitignore
├── .claudeignore
├── .geminiignore
├── skills/
│   ├── build-module/SKILL.md
│   ├── exam-sabotage-drill/SKILL.md
│   └── sanitize-dump-questions/SKILL.md
├── src/
│   ├── generator/
│   │   └── chaos_webhook_generator.py
│   └── pipelines/
│       └── 01_bronze_variant.py
└── tests/
    └── conftest.py
```

---

## Handoff Notes for Next Model

> If you are a new model picking up this session:
> 1. Read `AGENTS.md` in full — it contains all Databricks rules and the roadmap.
> 2. Module 1 is scaffolded but not yet tested against Databricks Free Edition.
> 3. The user's next likely commands: `/build` to flesh out Module 1, or `/drill` once code is running.
> 4. Respect all 7 Serverless Laws in `AGENTS.md` §2.
