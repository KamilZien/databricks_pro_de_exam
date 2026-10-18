---
name: orient
description: >
  Give a plain-English 1-sentence overview per objective in a target exam section,
  and map each objective to the implementation file in our portfolio repository.
---

# `/orient <section>`

## Trigger
User types `/orient <section>` (e.g. `/orient Section 1` or `/orient 1`).

## Instructions
1. Look up the specified Section in `EXAM_GUIDE.md` §2.
2. For every objective in that section:
   - Provide a concise, plain-English 1-sentence breakdown of what the exam actually tests.
   - Cite the official Databricks documentation URL (`https://docs.databricks.com/...`).
   - Map it directly to our portfolio file (existing or planned) in `databricks-serverless-finops-lakehouse`.
3. Provide the user with the next recommended command: `/diagnose <section>`.
