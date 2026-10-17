# Branching & Release Strategy for Databricks Asset Bundles (DABs)

> This strategy ensures safe CI/CD for a Free Edition Serverless workspace
> where only **1 Lakeflow pipeline** can be active at a time.

---

## Branch Model

```
main (protected)          ← production-quality code, tagged releases
  │
  └── dev                 ← integration branch, DAB deploys in development mode
       │
       ├── feature/mod1-bronze-ingestion
       ├── feature/mod2-silver-cdc
       ├── feature/mod3-gold-finops
       ├── fix/watermark-bug
       └── ...
```

### Rules

| Branch | DAB Target | Deploys To | Who Merges |
|--------|-----------|------------|------------|
| `feature/*` | `dev` | Dev workspace (prefixed names) | Developer → PR into `dev` |
| `dev` | `dev` | Dev workspace (prefixed names) | Auto-merge after tests pass |
| `main` | `prod` | Prod workspace (clean names) | Squash-merge from `dev` after module completion |

---

## Workflow

### 1. Start a new module
```bash
git checkout dev
git pull origin dev
git checkout -b feature/mod1-bronze-ingestion
```

### 2. Develop & deploy to dev
```bash
# Deploy bundle to dev target (development mode — prefixed resource names)
databricks bundle deploy --target dev --profile EXAM_PREP

# Validate pipeline
databricks bundle validate --target dev --profile EXAM_PREP

# Run pipeline
databricks bundle run finops_lakehouse_pipeline --target dev --profile EXAM_PREP
```

### 3. Test locally
```bash
# Activate venv
.venv\Scripts\activate

# Run unit tests (no Databricks connection needed for pure transforms)
pytest tests/ -m "not integration" -v

# Run integration tests (requires Databricks connection)
pytest tests/ -m integration -v

# Lint
ruff check src/ tests/
```

### 4. Merge to dev
```bash
git add -A
git commit -m "feat(mod1): bronze ingestion with Auto Loader + VARIANT"
git push origin feature/mod1-bronze-ingestion
# Create PR: feature/mod1-bronze-ingestion → dev
# After review/tests pass → merge
```

### 5. Promote to main (module complete)
```bash
# After module is complete and tested on dev target:
git checkout main
git merge --squash dev
git commit -m "release(mod1): Bronze ingestion pipeline v1.0"
git tag -a v1.0.0-mod1 -m "Module 1: Bronze Ingestion complete"
git push origin main --tags

# Deploy to prod target
databricks bundle deploy --target prod --profile EXAM_PREP
```

---

## DAB Target Behavior

### `dev` target (development mode)
- Resource names are **prefixed** with `[dev ${user}]` to avoid collisions
- Deployed to: `/Workspace/Users/chinskagiera123@gmail.com/databricks_pro_de_exam/.bundle/.../dev`
- Safe to deploy frequently — won't interfere with prod resources
- **Default target** — `databricks bundle deploy` without `--target` goes here

### `prod` target (production mode)
- Resource names are **clean** (no prefix)
- Deployed to: `/Workspace/Users/chinskagiera123@gmail.com/databricks_pro_de_exam/.bundle/.../prod`
- Only deployed from `main` branch after squash-merge
- Permissions locked to owner

---

## Free Edition Constraints

> [!WARNING]
> Free Edition allows only **1 active Lakeflow Declarative Pipeline** at a time.
> Both dev and prod targets define the same pipeline — only deploy to one at a time.

### Safe deployment sequence:
1. Destroy dev pipeline before deploying prod: `databricks bundle destroy --target dev --profile EXAM_PREP`
2. Deploy prod: `databricks bundle deploy --target prod --profile EXAM_PREP`
3. To resume dev work: `databricks bundle deploy --target dev --profile EXAM_PREP`

---

## Tagging Convention

| Tag | Meaning |
|-----|---------|
| `v1.0.0-mod1` | Module 1 complete (Bronze Ingestion) |
| `v2.0.0-mod2` | Module 2 complete (Silver CDC + Stream Joins) |
| `v3.0.0-mod3` | Module 3 complete (Gold FinOps) |
| `v4.0.0-mod4` | Module 4 complete (Governance + GDPR) |
| `v5.0.0-mod5` | Module 5 complete (CI/CD + Interview Polish) |
| `v5.1.0` | Portfolio final — interview-ready |
