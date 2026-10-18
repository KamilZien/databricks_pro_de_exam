---
name: mock-repair
description: >
  Generate weighted 60-question mock exams mapped to official domain percentages
  and run the Step 4.5 Repair loop on missed questions.
---

# `/mock` and `/repair`

## Trigger
User types `/mock` or `/repair`.

## Instructions

### `/mock`
1. Assemble a calibrated mock exam following the official weights:
   - Section 1 (Developing Code): 23%
   - Section 5 (Cost & Performance): 15%
   - Section 2 (Data Ingestion): 12%
   - Section 3 (Data Manipulation): 12%
   - Section 4 (Monitoring & Alerting): 10%
   - Section 8 (Debugging & Deploying): 10%
   - Section 6 (Security & Compliance): 8%
   - Section 7 (Data Governance): 5%
   - Section 9 (Data Modeling): 5%
2. Present questions in batches or full sets as requested.
3. Require answers before scoring.
4. Tally overall score and sectional pass rates (Passing threshold: 70%).

### `/repair`
For any question the user answered incorrectly during `/diagnose` or `/mock`:
1. **Explain the Misconception:** Why the chosen distractor seemed plausible and why it fails in production.
2. **Explain the Correct Concept:** Grounded in `https://docs.databricks.com/...`.
3. **Generate a New Variant Question:** Create a fresh question testing the exact same underlying mechanism to confirm mastery.
4. Update `STATE.md` with repaired objectives.
