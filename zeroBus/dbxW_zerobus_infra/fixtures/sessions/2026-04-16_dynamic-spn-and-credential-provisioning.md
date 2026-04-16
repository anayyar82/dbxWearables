# dbxW_zerobus_infra

## Session: Dynamic Service Principal Creation, OAuth Credential Provisioning, and Variable Cleanup

**Date:** 2026-04-16

---

### Summary

Created a new Python notebook (`ensure-service-principal`) that dynamically creates a least-privilege service principal for the ZeroBus ingestion pipeline, generates OAuth credentials, derives the ZeroBus endpoint URL, and populates the secret scope with all values the Databricks App needs at runtime. Refactored the UC setup job into a two-task workflow with task value handoff. Removed the `var.zerobus_service_principal` bundle variable and all its declarative YAML references — SPN permissions are now fully managed by the notebooks. Updated both READMEs and the job name/description.

---

### Problems Encountered

#### 1. Bundle validator requires file extensions on `notebook_path`

The validator rejected `notebook_path: ../src/uc_setup/target-table-ddl` (no extension) with:
```
Error: notebook "src/uc_setup/target-table-ddl" not found.
Did you mean "src/uc_setup/target-table-ddl.sql"?
Local notebook references are expected to contain one of the following
file extensions: [.py, .r, .scala, .sql, .ipynb]
```

**Root cause:** The bundle validator always requires an extension matching the notebook's default language, regardless of whether it's a workspace or file-based bundle.

**Fix:** Added `.sql` for the SQL-default DDL notebook. The ensure-service-principal notebook (Python-default) needs `.py`. Updated `.assistant_instructions.md` with a complete extension-to-language mapping table — the previous instruction ("workspace bundles omit extensions") was incorrect.

#### 2. Session summary dates were wrong (2025-07 instead of 2026-04)

Both existing session files had July 2025 dates despite being created on April 16, 2026.

**Root cause:** LLM training data cutoff produced incorrect date assumptions. No `datetime.now()` check was performed at write time.

**Fix:** Renamed both files with correct `2026-04-16` prefix, updated internal `**Date:**` lines, updated INDEX.md. Added a "CRITICAL — Date accuracy" rule to `.assistant_instructions.md` requiring `datetime.now()` for all future session summaries.

#### 3. `IDENTIFIER()` not supported in `ALTER SCHEMA` position (from earlier session)

Carried over from the query tags session — `EXECUTE IMMEDIATE` with string concatenation is the correct pattern for `ALTER SCHEMA ... ENABLE PREDICTIVE OPTIMIZATION` with dynamic schema names.

---

### Changes Made

#### New Notebook: `src/uc_setup/ensure-service-principal` (ID: `3647522242741063`)

Python-default notebook with 8 cells:

| Cell | Title | Purpose |
| --- | --- | --- |
| 1 | Ensure ZeroBus Service Principal | Markdown — purpose, secret scope key table, rotation instructions |
| 2 | Install latest Databricks SDK | `%pip install --upgrade databricks-sdk` + `restartPython()` |
| 3 | Read Job Parameters | `catalog_use`, `schema_use`, `secret_scope_name` via `dbutils.widgets.get()` |
| 4 | Find or Create the Service Principal | `w.service_principals.list/create()`, naming: `dbxw-zerobus-{schema}`, tracks `is_new_spn` |
| 5 | Provision OAuth Credentials and Populate Secret Scope | Derives ZeroBus endpoint from workspace ID + region, creates OAuth secret, stores 5 keys |
| 6 | Ensure Secret Scope READ Access | `w.secrets.put_acl(READ)` for the SPN |
| 7 | Output Task Value | `dbutils.jobs.taskValues.set("spn_application_id", ...)` |
| 8 | Summary | Status report with all values |

**Secret scope keys provisioned by cell 5:**

| Key | Source | Refreshed on re-run? |
| --- | --- | --- |
| `client_id` | SPN `application_id` | No (skip if exists) |
| `client_secret` | `service_principal_secrets.create()` | No (skip if exists) |
| `workspace_url` | `w.config.host` | Yes |
| `zerobus_endpoint` | `{workspace_id}.zerobus.{region}.cloud.databricks.com` | Yes |
| `target_table_name` | `{catalog}.{schema}.wearables_zerobus` | Yes |

**ZeroBus endpoint derivation logic:**
- Workspace ID: from `w.get_workspace_id()` or notebook context fallback
- Region: from `spark.conf.get("spark.databricks.clusterUsageTags.region")` or URL parsing
- Fallback region: `us-east-1`

#### Updated: `resources/uc_setup.job.yml`

| Change | Before | After |
| --- | --- | --- |
| Job name | `dbxWearables ZeroBus UC Setup — Table DDL & Grants` (53 chars) | `dbxW ZeroBus — UC Setup` (24 chars) |
| Description | Mentioned only DDL + grants | Full two-task workflow including SPN, OAuth, secret scope |
| Tasks | 1 task (`create_wearables_table`) | 2 tasks: `ensure_service_principal` → `create_wearables_table` |
| Parameters | `catalog_use`, `schema_use`, `spn_application_id` | `catalog_use`, `schema_use`, `secret_scope_name` |
| SPN handoff | `${var.zerobus_service_principal}` | `{{tasks.ensure_service_principal.values.spn_application_id}}` via `base_parameters` |

#### Updated: `databricks.yml`

| Change | Detail |
| --- | --- |
| Removed | `var.zerobus_service_principal` variable declaration |
| Removed | `zerobus_service_principal: acf021b4-...` from `hls_fde` target variables |

#### Updated: `resources/zerobus.secret_scope.yml`

| Change | Detail |
| --- | --- |
| Removed | `service_principal_name: ${var.zerobus_service_principal}` + `level: READ` from all 3 targets |
| Added | Header comments documenting auto-provisioned keys and manual fallback CLI commands |
| Added | NOTE comment explaining SPN READ is handled by the notebook |

#### Updated: `resources/wearables.schema.yml`

| Change | Detail |
| --- | --- |
| Removed | ZeroBus SPN grant block from `hls_fde` target (USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE, CREATE_VOLUME) |
| Added | NOTE comment explaining SPN grants are handled by the DDL notebook |

#### Updated: `README.md` (bundle)

| Section | Change |
| --- | --- |
| "What This Bundle Manages" | Rewrote table — added Service Principal and Bronze Table rows; Secret Scope row now lists 5 auto-provisioned keys |
| New: "UC Setup Job" | Documents two-task workflow, task value handoff, idempotency |
| New: "Secret Scope Contents" | Table of all 5 keys with source and rotation instructions |
| "Deployment Order" | Now 3-step: deploy → run job → app bundle |
| "Quick Start" | "Run the UC setup job" section describes full scope (SPN + credentials + DDL + grants) |
| "Predictive Optimization" | Removed stale `.sql` file note; updated query description |
| "Documentation" | Added ZeroBus Ingest overview, connector, and limitations links |

#### Updated: `zeroBus/README.md` (parent)

| Change | Detail |
| --- | --- |
| Rewrote from placeholder | Now describes directory structure, two-bundle architecture, bundle status table, deploy.sh usage, documentation links |

#### Updated: `.assistant_instructions.md`

| Change | Detail |
| --- | --- |
| Notebook paths convention | Rewrote to require extensions for ALL bundle types; added language-to-extension mapping table |
| Session date accuracy | Added "CRITICAL — Date accuracy" rule requiring `datetime.now()` |
| Region fallback | Updated to `us-east-1` |

#### Renamed: Session summary files

| Old name | New name |
| --- | --- |
| `2025-07-16_infra-bundle-scaffold.md` | `2026-04-16_infra-bundle-scaffold.md` |
| `2025-07-17_query-tags-and-query-relocation.md` | `2026-04-16_query-tags-and-query-relocation.md` |

---

### Design Decisions

1. **Dynamic SPN creation over static variable** — Eliminates the chicken-and-egg problem of needing to know the SPN's application_id at bundle deploy time. The SPN is created (or found) at job runtime and its ID flows via task values.

2. **SPN naming convention: `dbxw-zerobus-{schema}`** — Ties the SPN to a specific schema, ensuring least privilege. Different schemas get different SPNs. The naming is deterministic, so re-runs find the existing SPN instead of creating duplicates.

3. **Credential skip-if-exists pattern** — OAuth credentials (`client_id`, `client_secret`) are only generated when the keys don't already exist in the secret scope. This prevents unnecessary secret rotation. Non-sensitive keys (`workspace_url`, `zerobus_endpoint`, `target_table_name`) are always refreshed.

4. **ZeroBus endpoint derived from workspace metadata** — The endpoint format `{workspace_id}.zerobus.{region}.cloud.databricks.com` is constructed from runtime metadata (workspace ID + AWS region). This avoids hardcoding environment-specific URLs. Fallback region: `us-east-1`.

5. **Task value handoff over shared variables** — The SPN's `application_id` passes from task 1 to task 2 via `dbutils.jobs.taskValues.set()` / `{{tasks.*.values.*}}`. This is the standard Databricks pattern for inter-task data flow and keeps the job self-contained.

6. **Notebook-managed permissions over declarative YAML** — SPN grants (secret scope READ, UC catalog/schema/table grants) are now fully managed by the two notebooks. This means permissions are applied at the same time as the SPN creation, rather than requiring the SPN to pre-exist for YAML deployment.

7. **Job name shortened: `dbxW ZeroBus — UC Setup`** — 24 characters vs 53. Fits the Workflows viewer without truncation. The `[dev matthew]` or `[prod]` prefix from bundle presets is appended automatically.

8. **Separate Python + SQL tasks** — Task 1 (Python) uses the Databricks SDK for SPN/credential management; Task 2 (SQL) runs on the SQL warehouse for DDL/GRANT statements. Each task uses the compute best suited to its workload.

---

### Files Modified Summary

| File | Path (relative to bundle root) | Action |
| --- | --- | --- |
| ensure-service-principal | `src/uc_setup/ensure-service-principal` | Created (8 cells, Python) |
| uc_setup.job.yml | `resources/uc_setup.job.yml` | Rewritten — 2-task workflow, new name/description |
| databricks.yml | `databricks.yml` | Removed `zerobus_service_principal` var + target assignment |
| zerobus.secret_scope.yml | `resources/zerobus.secret_scope.yml` | Removed SPN READ permissions, updated header comments |
| wearables.schema.yml | `resources/wearables.schema.yml` | Removed SPN grants from hls_fde target |
| README.md (bundle) | `README.md` | Major rewrite — UC Setup Job, Secret Scope Contents sections |
| README.md (parent) | `../README.md` | Rewrote from placeholder to architecture overview |
| .assistant_instructions.md | `~/.assistant_instructions.md` | Notebook paths convention, date accuracy rule |
| Session files (2) | `fixtures/sessions/` | Renamed with correct 2026-04-16 dates |
| INDEX.md | `fixtures/sessions/INDEX.md` | Updated filenames + added this session |
