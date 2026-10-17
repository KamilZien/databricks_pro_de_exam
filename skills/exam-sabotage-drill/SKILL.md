---
name: exam-sabotage-drill
description: >
  Two-phase exam drill: (A) inject 2 subtle DE Pro bugs into test code for the user
  to find and fix, then (B) present 3 code-diff multiple-choice questions with
  10-second keyword elimination rules.
---

# /drill — Exam Sabotage & Code-Diff Quiz

## Trigger
User types `/drill` in chat.

## Instructions

### Phase A — Sabotage Task

1. **Read State:** Check `STATE.md` for the current/last-completed module.
2. **Select 2 subtle bugs** relevant to that module from this bug bank:

| Bug Category | Example Sabotage |
|-------------|-----------------|
| Stream Join | Remove watermark or time-range condition from outer join (will fail at runtime) |
| GDPR Purge | Forget `REORG TABLE ... APPLY (PURGE)` — only `DELETE` + `VACUUM` (soft deletes remain) |
| Deduplication | Include the timestamp column inside `dropDuplicates()` (duplicates arriving hours apart won't match) |
| Serverless | Add `.cache()` or `.persist()` call (Spark Connect throws exception) |
| Legacy Storage | Use `dbfs:/mnt/` path or `hive_metastore` (violates UC mandate) |
| Clustering | Use `PARTITION BY` or `ZORDER BY` instead of `CLUSTER BY` |
| Streaming Trigger | Use `Trigger.ProcessingTime("10 seconds")` instead of `.trigger(availableNow=True)` |
| DLT/Lakeflow | Manually specify `checkpointLocation` inside Lakeflow pipeline |
| Semi-Structured | Use brittle `from_json()` with struct schema instead of `VARIANT` |
| foreachBatch | Omit `txnVersion`/`txnAppId` options (not idempotent) |

3. **Generate `tests/test_exam_drill.py`** with the 2 bugs injected into realistic pipeline code.
4. **Present the challenge:** Tell the user the file has 2 bugs, explain which Serverless Laws or exam rules are violated (without revealing the exact lines), and ask them to find & fix so `pytest tests/test_exam_drill.py -v` passes.
5. **After the user submits fixes:** Validate their corrections, explain any they missed, and give the exam-relevant rule.

### Phase B — Code-Diff Quiz

1. **Generate 3 multiple-choice questions** (present one at a time):
   - Each question shows 4 code snippets (`A`, `B`, `C`, `D`) that look nearly identical.
   - Only ONE is correct per the October 2026 DE Pro syllabus and `AGENTS.md` rules.
   - Questions should cover the current module's exam weight areas.

2. **After each answer:**
   - Reveal the correct answer with a 1-sentence explanation.
   - Give the **10-Second Keyword Elimination Rule** — a quick mental shortcut to eliminate wrong answers on exam day. Format:

   > **🎯 10-Second Rule:** "If you see `[keyword/pattern]` in an answer about `[topic]`, it's WRONG because `[1-sentence reason]`."

3. **Update `STATE.md`** drill stats (drills completed, quiz accuracy).

### Example 10-Second Elimination Rules
- "If you see `ZORDER BY` in any 2026 answer, eliminate it — Liquid Clustering (`CLUSTER BY`) replaced it."
- "If you see `.cache()` in a Serverless/Connect answer, eliminate it — Spark Connect throws `UnsupportedOperationException`."
- "If you see `dbfs:/mnt/` in any Unity Catalog answer, eliminate it — Mount Points are legacy."
- "If you see only `DELETE` + `VACUUM` for GDPR purge, eliminate it — you need `REORG APPLY (PURGE)` between them to rewrite Deletion Vector files."
- "If `dropDuplicates` includes the event-time column, eliminate it — late-arriving duplicates won't match on different timestamps."
