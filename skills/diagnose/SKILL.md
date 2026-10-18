---
name: diagnose
description: >
  Administer 10 calibrated multiple-choice questions on an exam section based on the
  10 official sample benchmark questions. Waits for user answers, grades with citations,
  and updates weak objectives in STATE.md.
---

# `/diagnose <section>`

## Trigger
User types `/diagnose <section>` (e.g. `/diagnose Section 1`).

## Instructions
1. Check `EXAM_GUIDE.md` for the specified Section.
2. Present **10 multiple-choice questions** calibrated to the difficulty and style of the 10 Official Sample Benchmark questions in `EXAM_GUIDE.md` §3.
   - Each question must have 4 options: **A, B, C, D**.
   - Incorporate realistic production constraints (Serverless, Spark Connect, Free Edition, Unity Catalog).
   - Use the post-Oct 2026 Renamed Products Table (Lakeflow Pipelines, AUTO CDC, OpenSharing, etc.). Include legacy terms only as realistic exam distractors.
3. Present the questions to the user and **WAIT** for the user's answers. Do not reveal the answers upfront.
4. When the user responds:
   - Grade each question (Correct/Incorrect).
   - Provide the **1-sentence explanation** citing the official URL (`https://docs.databricks.com/...`).
   - Give the **🎯 10-Second Elimination Rule** for each question.
   - Record score and any failed objectives in `STATE.md` under Section Diagnostics.
   - Recommend `/deepdive <objective>` for any missed objectives.
