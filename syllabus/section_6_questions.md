# Section 6: Ensuring Data Security and Compliance — Exam Question Bank (8%)

> **Progress:** Official Subpoints 6.1–6.5 Complete (6/6 — 100%) ✅

---

## Official Subpoint 6.1: Least-Privilege ACLs (Unity Catalog & Workspace Resources)

### Question 6.1.1 — Least-Privilege Unity Catalog View Access
A BI team (`bi_analysts`) needs to query a governed view `portfolio_catalog.gold.monthly_revenue_v` that reads from `portfolio_catalog.silver.clean_events`. The BI team must **not** be able to query `silver.clean_events` directly or modify any data.

Which exact set of Unity Catalog grants enforces least privilege?

- **A.** `GRANT SELECT ON TABLE portfolio_catalog.silver.clean_events TO bi_analysts` and `GRANT SELECT ON VIEW portfolio_catalog.gold.monthly_revenue_v TO bi_analysts`.
- **B.** `GRANT BROWSE ON CATALOG portfolio_catalog TO bi_analysts` and `GRANT SELECT ON VIEW portfolio_catalog.gold.monthly_revenue_v TO bi_analysts`.
- **C.** `GRANT ALL PRIVILEGES ON SCHEMA portfolio_catalog.gold TO bi_analysts`.
- **D.** `GRANT USE CATALOG ON CATALOG portfolio_catalog TO bi_analysts`, `GRANT USE SCHEMA ON SCHEMA portfolio_catalog.gold TO bi_analysts`, and `GRANT SELECT ON VIEW portfolio_catalog.gold.monthly_revenue_v TO bi_analysts`.

- **User Answer:** **D** ✅
- **Official Rationale:** Unity Catalog requires 3-tier traversal (`USE CATALOG` + `USE SCHEMA` + `SELECT` on the view). Analysts do not need `SELECT` on the underlying `silver.clean_events` table because the view executes with the view owner's permissions.

---

### Question 6.1.2 — Production Lakeflow Job ACLs & Execution Identity
An operations support group (`ops_support`) needs to monitor a production Lakeflow Job, manually trigger ad-hoc reruns, and click **Repair and Rerun** on failed tasks, **without** being able to edit the job's notebook paths or task definitions. Additionally, the job must not break if any individual data engineer leaves the company.

Which workspace configuration meets both requirements?

- **A.** Set `run_as` to a **Service Principal**, and grant `ops_support` **`CAN MANAGE RUN`** on the job.
- **B.** Set `run_as` to the lead engineer's email, and grant `ops_support` `CAN MANAGE` on the job.
- **C.** Set `run_as` to a Service Principal, and grant `ops_support` `CAN VIEW` on the job.
- **D.** Grant `ops_support` `IS OWNER` on the job and `Storage Credential` access on the cloud bucket.

- **User Answer:** **A** ✅
- **Official Rationale:** `CAN MANAGE RUN` grants trigger/cancel/repair privileges without code-editing rights, and running as a Service Principal decouples production jobs from individual employee accounts.

---

## Official Subpoint 6.2: Attribute-Based Access Control (ABAC) Policies with Governed Tags

### Question 6.2.1 — Scaling Column Masking Across 300+ Current & Future Tables
A healthcare enterprise has 300 Unity Catalog tables containing `patient_email` and `national_id` columns, and data engineering teams deploy 15 new tables every month. Security compliance mandates that **all** existing and future columns classified as PII must be automatically masked for everyone except `hipaa_authorized`, and table owners must **not** be able to remove or edit the classification tag to bypass masking.

Which architecture satisfies these requirements with minimal operational overhead?

- **A.** Write a nightly Lakeflow Job that loops over `information_schema.columns` and executes `ALTER TABLE ... ALTER COLUMN ... SET MASK`.
- **B.** Create a separate dynamic view (`CREATE VIEW`) on top of each of the 300 tables using `is_member('hipaa_authorized')`.
- **C.** Define a centrally managed **Governed Tag** (e.g. `pii = 'national_id'`), create a Unity Catalog masking UDF using `is_account_group_member('hipaa_authorized')`, and bind a **Catalog-level ABAC `CREATE POLICY`** matching `hasTagValue('pii', 'national_id')`.
- **D.** Grant `BROWSE` on the catalog instead of `SELECT` so PII columns are hidden.

- **User Answer:** **C** ✅
- **Official Rationale:** Governed Tags prevent unauthorized tampering, and a Catalog-level ABAC `CREATE POLICY` enforces masking automatically across all current and future tables matching `hasTagValue(...)`.

---

### Question 6.2.2 — Row Filter UDF Requirements
A data engineer writes a Unity Catalog Row Filter function to restrict regional analysts to rows where `country = 'PL'` while allowing members of the account-level group `eu_leads` to view all rows.

Which SQL function definition is valid for a Unity Catalog Row Filter?

- **A.** `CREATE FUNCTION sec.filter_pl(country STRING) RETURNS BOOLEAN RETURN is_account_group_member('eu_leads') OR country = 'PL';`
- **B.** `CREATE FUNCTION sec.filter_pl(country STRING) RETURNS STRING RETURN CASE WHEN is_member('eu_leads') THEN country ELSE 'PL' END;`
- **C.** `CREATE FUNCTION sec.filter_pl(country STRING) RETURNS INT RETURN CASE WHEN country = 'PL' THEN 1 ELSE 0 END;`
- **D.** `CREATE FUNCTION sec.filter_pl(country STRING) RETURNS BOOLEAN RETURN is_workspace_user('eu_leads') AND country = 'PL';`

- **User Answer:** **A** ✅
- **Official Rationale:** Row Filter UDFs must return `BOOLEAN` and check Unity Catalog account-level groups using `is_account_group_member(...)`.

---

## Official Subpoints 6.3, 6.4 & 6.5: Anonymization, Compliant PII Pipelines & GDPR Purge

### Question 6.3.1 — Choosing Between Hashing, Tokenization, Suppression, and Generalization
A retail bank wants to share customer transaction analytics with its Data Science team under two strict requirements:
1. `customer_email` must remain **joinable** across tables and **reversible** *only* for authorized fraud investigators, while enabling fast GDPR erasure without rewriting 50 TB of historical Gold fact tables.
2. `customer_age` must preserve demographic cohort distributions (e.g. `20-29`, `30-39`) without exposing exact birth dates or exact ages.

Which pair of privacy methods matches requirements (1) and (2)?

- **A.** Unsalted SHA-256 Hashing for `customer_email`, and Suppression for `customer_age`.
- **B.** **Tokenization** (with a restricted Unity Catalog PII Lookup Vault table) for `customer_email`, and **Generalization** (age bucketing) for `customer_age`.
- **C.** Suppression for `customer_email`, and Salted Hashing for `customer_age`.
- **D.** Generalization for `customer_email`, and Tokenization for `customer_age`.

- **User Answer:** **B** ✅
- **Official Rationale:** Tokenization preserves joinability, allows controlled reversal via a restricted PII Lookup Vault, and enables GDPR erasure by purging the vault entry. Generalization (age bucketing) preserves cohort distributions while removing exact ages.

---

### Question 6.4.1 — Compliant PII Pipeline & GDPR Physical Purge
A streaming customer support pipeline ingests free-text chat transcripts (which sometimes contain customer phone numbers and emails) into `portfolio_catalog.bronze.raw_chats`, and also maintains a restricted `portfolio_catalog.pii_vault.customers` table with Deletion Vectors enabled.

Which combination correctly scrubs PII from the free-text transcripts in Silver **and** physically purges a customer's record from `pii_vault.customers` upon a GDPR erasure request?

- **A.** Use `PARSE_JSON(transcript)` to mask phone numbers, and run `DELETE` + `VACUUM` on `pii_vault.customers`.
- **B.** Use `is_member('pii')` on `raw_chats`, and run `ALTER TABLE pii_vault.customers DROP COLUMN email`.
- **C.** Use `TRY_VARIANT_GET(transcript, '$.phone', 'NULL')`, and run `OPTIMIZE pii_vault.customers ZORDER BY (customer_id)`.
- **D.** Use **`ai_mask(transcript, ARRAY('person', 'email', 'phone'))`** in the Silver pipeline, and execute **`DELETE`** → **`REORG TABLE pii_vault.customers APPLY (PURGE)`** → **`VACUUM pii_vault.customers RETAIN <hours> HOURS`** (propagating deletes downstream via Change Data Feed if needed).

- **User Answer:** **D** ✅
- **Official Rationale:** `ai_mask(text, ARRAY(...))` redacts PII from unstructured text, and with Deletion Vectors enabled, GDPR physical erasure requires `DELETE` → `REORG TABLE ... APPLY (PURGE)` → `VACUUM`.
