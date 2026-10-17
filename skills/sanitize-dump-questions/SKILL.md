---
name: sanitize-dump-questions
description: >
  When the user pastes exposed/community dump questions, verify them against the
  October 2026 DE Pro syllabus, flag outdated 2023/2024 answers, map concepts to
  our repo code, and provide 10-second keyword elimination rules.
---

# /sanitize — Dump Question Sanitizer

## Trigger
User pastes exam dump questions into chat, or types `/sanitize`.

## Instructions

### Step 1 — Parse the Question
1. Extract the question stem and all answer options from the user's paste.
2. Identify the **exam domain** (Ingestion, Developing Code, Transformation & Quality, Data Modelling, Cost & Performance, Monitoring & Alerting, Security & Compliance, Governance, Sharing & Federation, Debugging & Deploying).

### Step 2 — Verify Against October 2026 Syllabus
Check every answer option against the rules in `AGENTS.md` §2. Common traps from outdated 2023/2024 dumps:

| Outdated Concept (2023/2024) | Correct 2026 Replacement |
|------------------------------|-------------------------|
| `PARTITION BY` / `ZORDER BY` on Delta | `CLUSTER BY (...)` or `CLUSTER BY AUTO` + Predictive Optimization |
| `dbfs:/mnt/` mount points | UC Volumes `/Volumes/catalog/schema/volume/` |
| `hive_metastore` | Unity Catalog 3-level namespace |
| `from_json()` with struct schema | `VARIANT` type + `parse_json` + `:` path syntax |
| Manual `checkpointLocation` in DLT | Lakeflow manages it automatically |
| `.cache()` / `.persist()` | Not available on Spark Connect / Serverless |
| `Trigger.ProcessingTime` / `Trigger.Continuous` | `.trigger(availableNow=True)` for Serverless |
| `DELETE` + `VACUUM` for GDPR | `DELETE` → `REORG APPLY (PURGE)` → `VACUUM` |
| `APPLY CHANGES INTO` (old DLT syntax) | `AUTO CDC` (`CREATE FLOW ... AS AUTO CDC INTO`) |
| `spark.sparkContext` / `.rdd` | Unavailable on Spark Connect |

### Step 3 — Deliver the Sanitized Answer
Format your response as:

```
## 📋 Question Sanitization

**Domain:** [exam domain]
**Question:** [restated question]

### ✅ Correct Answer: [letter]
[1-sentence explanation referencing AGENTS.md rule]

### ❌ Eliminated Options
- **[letter]:** [why it's wrong — reference specific 2026 change if outdated]
- **[letter]:** [why it's wrong]
- **[letter]:** [why it's wrong]

### 🎯 10-Second Elimination Rule
> "If you see `[keyword]` in an answer about `[topic]`, it's WRONG because `[reason]`."

### 🔗 Mapped to Our Code
- Relevant file: `[path in our repo]`
- The concept is demonstrated in: [brief description]

### ⚠️ Freshness Flag
- [ ] ✅ Question is valid for October 2026 syllabus
- [ ] ⚠️ Question is OUTDATED — [explanation of what changed]
```

### Step 4 — Update State
Increment the "Dump Questions Sanitized" counter in `STATE.md`.
