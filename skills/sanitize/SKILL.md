---
name: sanitize
description: >
  When community or dump questions are pasted, verify them against the official Oct 2026+
  9-section outline and Renamed Products Trap Table, flag outdated answers, map to repo code,
  and provide 10-second elimination rules.
---

# `/sanitize`

## Trigger
User types `/sanitize` or pastes dump/community exam questions into chat.

## Instructions
1. Parse the pasted question and answer choices.
2. Check against the 9-Section Outline and the Renamed Products Trap Table in `EXAM_GUIDE.md`.
3. Check for outdated pre-October 2026 concepts:
   - Mentions of DLT instead of Lakeflow Pipelines.
   - Mentions of `apply_changes` instead of `AUTO CDC`.
   - Mentions of `PARTITION BY` / `ZORDER BY` instead of Liquid Clustering (`CLUSTER BY`).
   - Mentions of `.cache()` / `.persist()` on Serverless.
   - Mentions of `dbfs:/mnt/` or `hive_metastore`.
4. Output:
   - **Official Domain & Section:** (e.g. Section 1.7)
   - **Verdict:** Valid for Oct 2026+ or Outdated Pre-2026 Trap.
   - **Correct Answer & Official Explanation:** Citing `https://docs.databricks.com/...`.
   - **Eliminated Options Breakdown:** Explaining why distractors fail.
   - **🎯 10-Second Elimination Rule:** Quick exam day keyword trigger.
   - **Repo Code Mapping:** Relevant file in our portfolio repository.
5. Increment sanitized question count in `STATE.md`.
