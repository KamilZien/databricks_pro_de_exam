# Section 6: Ensuring Data Security and Compliance (8%)

> **Official Weight:** 8% of total exam score  
> **Official Subpoints Covered:** 6.1, 6.2, 6.3, 6.4, 6.5  
> **Official Docs:**  
> - [Unity Catalog Privileges & Securable Objects](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html)  
> - [Unity Catalog ABAC Policies](https://docs.databricks.com/en/data-governance/unity-catalog/abac/index.html)  
> - [Row Filters & Column Masks](https://docs.databricks.com/en/data-governance/unity-catalog/filters-and-masks/index.html)  
> - [GDPR & Delta Lake](https://docs.databricks.com/en/security/privacy/gdpr-delta.html)

---

## 1. Official Subpoint 6.1: Least-Privilege ACLs
- **3-Tier Traversal Rule:** Querying any table or view requires `USE CATALOG` + `USE SCHEMA` + `SELECT` (on the table/view). For views, analysts do **not** need `SELECT` on underlying tables.
- **`BROWSE` vs. `USE CATALOG`/`USE SCHEMA`:** `BROWSE` only allows metadata discovery in Catalog Explorer; `USE CATALOG`/`USE SCHEMA` allow traversal (and do not grant `CREATE TABLE`/`CREATE SCHEMA`).
- **Workspace Resource ACLs:** Run production jobs/pipelines as a **Service Principal** (`run_as: service_principal_name`), grant privileges to **Account-level Groups**, and give on-call support **`CAN MANAGE RUN`** (trigger/cancel/repair without editing code).

---

## 2. Official Subpoint 6.2: ABAC Policies with Governed Tags
- **Governed Tags:** Account-level tags with controlled allowed values and `ASSIGN` permissions so table owners cannot tamper with security classifications.
- **Centralized ABAC (`CREATE POLICY`):** Bind **one** catalog/schema-level policy (`COLUMN MASK` or `ROW FILTER`) matching `hasTagValue('pii', 'ssn')` to automatically protect **all current and future** tagged tables.
- **UDF Rules:**
  - Always use account-level **`is_account_group_member('group')`** (never workspace-local `is_member`).
  - Column Mask UDF returns the **same type** as the target column; Row Filter UDF returns **`BOOLEAN`**.

---

## 3. Official Subpoints 6.3, 6.4 & 6.5: Privacy Methods, PII Vault & GDPR Purge

| Method | Category | Reversible? | Joinable? | Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **Salted Hashing** (`sha2(concat(col, salt), 256)`) | Pseudonymization | No | Yes | Joinable analytics; store salt in Databricks Secret Scope to defeat rainbow tables. |
| **Tokenization** (PII Lookup Vault) | Pseudonymization | Yes (via Vault) | Yes | Fraud reversal + fast GDPR erasure (purge 1 row in PII Vault instead of rewriting 50 TB Gold). |
| **Suppression** (`NULL` / drop / `ai_mask`) | Anonymization | No | No | Remove unneeded PII or scrub unstructured text with `ai_mask(text, ARRAY('person', 'email'))`. |
| **Generalization** (bucketing) | Anonymization | No | Group-level | Preserve cohort distributions (`age = 34` → `'30-39'`, `zip = '00-***'`). |

- **GDPR Physical Purge (Deletion Vectors enabled):** `DELETE` → `REORG TABLE ... APPLY (PURGE)` → `VACUUM ... RETAIN <hours> HOURS` (propagate deletes downstream via Change Data Feed).
