---
name: deepdive
description: >
  Deep-dive into a specific missed exam objective using ONLY official Databricks
  documentation (https://docs.databricks.com/...) citations, architecture rules,
  tradeoffs, and runnable repository code.
---

# `/deepdive <objective>`

## Trigger
User types `/deepdive <objective>` (e.g. `/deepdive 1.7` or `/deepdive 5.3`).

## Instructions
1. Locate the objective in `EXAM_GUIDE.md` §2.
2. Structure the deep-dive strictly as follows:
   - **1. Core Concept & Official Definition:** State the principle, citing the exact official URL (`https://docs.databricks.com/...`).
   - **2. How It Works on Modern Databricks (Serverless / UC):** Mechanics, internal engines, state stores, or metadata handling.
   - **3. When to Use vs. Alternatives (Exam Tradeoffs):** Clear comparison table comparing the right pattern against common exam distractors.
   - **4. Common Exam Pitfalls & Traps:** Traps from the Renamed Products table or legacy Spark APIs.
   - **5. Hands-on Code Example in Our Repo:** Concise, runnable snippet adhering to Serverless laws.
3. Update `STATE.md` to reflect that the objective has been reviewed.
