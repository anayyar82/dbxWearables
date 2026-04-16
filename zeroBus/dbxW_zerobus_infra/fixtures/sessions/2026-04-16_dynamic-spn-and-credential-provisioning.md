# dbxW_zerobus_infra

## Session: Dynamic Service Principal Creation, OAuth Credential Provisioning, and Variable Cleanup

**Date:** 2026-04-16

---

### Summary

Created a new Python notebook (`ensure-service-principal`) that dynamically creates a least-privilege service principal for the ZeroBus ingestion pipeline, derives the ZeroBus endpoint URL, and populates the secret scope with all values the Databricks App needs at runtime. Refactored the UC setup job into a two-task workflow with task value handoff. Removed the `var.zerobus_service_principal` bundle variable and all its declarative YAML references — SPN permissions are now fully managed by the notebooks. Updated both READMEs and the job name/description.

Later in the session: corrected the OAuth credential split so `client_id` is auto-provisioned (stored by the notebook from the SPN's `application_id`) and only `client_secret` requires manual admin provisioning. Added infrastructure readiness gates to `deploy.sh` — checks all 5 secret scope keys and the bronze table before allowing app bundle deployment. Fixed a `SET QUERY_TAGS` syntax error in the DDL notebook.

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

#### 4. OAuth `client_id` incorrectly classified as admin-provisioned

The initial implementation placed both `client_id` and `client_secret` in the "admin-provisioned" category, requiring manual storage after deployment. However, `client_id` is simply the SPN's `application_id` — already available at notebook runtime.

**Root cause:** Overly conservative design treated all OAuth fields as requiring admin action, when only the `client_secret` (generated via an account-level API) is unavailable to the notebook.

**Fix:** Moved `client_id` to auto-provisioned. The notebook now stores it via `w.secrets.put_secret(scope, key="client_id", string_value=spn_application_id)`. Updated all documentation (notebook markdown, YAML headers, README tables) to reflect only `client_secret` as admin-provisioned.

#### 5. `SET QUERY_TAGS` comma-chaining syntax error

The DDL notebook cell 3 used comma-separated key assignments in a single `SET` statement:
```sql
SET QUERY_TAGS['project'] = 'dbxWearables ZeroBus',
    QUERY_TAGS['component'] = 'uc_setup',
    ...
```

**Root cause:** `SET QUERY_TAGS` only supports one key per statement — comma-chaining is not valid Databricks SQL syntax.

**Fix:** Split into 5 separate `SET` statements. For the `catalog` and `schema` tags (which reference session variables), used `EXECUTE IMMEDIATE` to interpolate the variable values:
```sql
SET QUERY_TAGS['project'] = 'dbxWearables ZeroBus';
SET QUERY_TAGS['component'] = 'uc_setup';
SET QUERY_TAGS['pipeline'] = 'dbxw_zerobus_infra';
EXECUTE IMMEDIATE "SET QUERY_TAGS['catalog'] = '" || catalog_use || "';";
EXECUTE IMMEDIATE "SET QUERY_TAGS['schema'] = '" || schema_use || "';";
```

---

### Changes Made

#### New Notebook: `src/uc_setup/ensure-service-principal` (ID: `3647522242741063`)

Python-default notebook with 8 cells:

| Cell | Title | Purpose |
| --- | --- | --- |
| 1 | Ensure ZeroBus Service Principal | Markdown — purpose, auto-provisioned key table (4 keys), admin-provisioned table (1 key: `client_secret` only) |
| 2 | Install latest Databricks SDK | `%pip install --upgrade databricks-sdk` + `restartPython()` |
| 3 | Read Job Parameters | `catalog_use`, `schema_use`, `secret_scope_name` via `dbutils.widgets.get()` |
| 4 | Find or Create the Service Principal | `w.service_principals.list/create()`, naming: `dbxw-zerobus-{schema}`, tracks `is_new_spn` |
| 5 | Populate Secret Scope and Check Credentials | Derives ZeroBus endpoint, stores `client_id` + 3 derived values, checks for `client_secret` |
| 6 | Ensure Secret Scope READ Access | `w.secrets.put_acl(READ)` for the SPN |
| 7 | Output Task Value | `dbutils.jobs.taskValues.set("spn_application_id", ...)` |
| 8 | Summary | Status report — shows `client_id: stored`, `client_secret: PRESENT/MISSING` |

**Secret scope keys provisioned by cell 5:**

| Key | Source | Category | Refreshed on re-run? |
| --- | --- | --- | --- |
| `client_id` | SPN `application_id` | Auto-provisioned | Yes (always refreshed) |
| `workspace_url` | `w.config.host` | Auto-provisioned | Yes |
| `zerobus_endpoint` | `{workspace_id}.zerobus.{region}.cloud.databricks.com` | Auto-provisioned | Yes |
| `target_table_name` | `{catalog}.{schema}.wearables_zerobus` | Auto-provisioned | Yes |
| `client_secret` | Admin-generated | Admin-provisioned | N/A — notebook only checks presence |

**ZeroBus endpoint derivation logic:**
- Workspace ID: from `w.get_workspace_id()` or notebook context fallback
- Region: from `spark.conf.get("spark.databricks.clusterUsageTags.region")` or URL parsing
- Fallback region: `us-east-1`

#### Updated: `resources/uc_setup.job.yml`

| Change | Before | After |
| --- | --- | --- |
| Job name | `dbxWearables ZeroBus UC Setup — Table DDL & Grants` (53 chars) | `dbxW ZeroBus — UC Setup` (24 chars) |
| Description | Mentioned only DDL + grants | Full two-task workflow; documents `client_id` as auto-stored, `client_secret` as admin-provisioned |
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
| Updated | Header comments — `client_id` under AUTO-PROVISIONED; only `client_secret` under ADMIN-PROVISIONED |
| Added | NOTE comment explaining SPN READ is handled by the notebook |

#### Updated: `resources/wearables.schema.yml`

| Change | Detail |
| --- | --- |
| Removed | ZeroBus SPN grant block from `hls_fde` target (USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE, CREATE_VOLUME) |
| Added | NOTE comment explaining SPN grants are handled by the DDL notebook |

#### Updated: `src/uc_setup/target-table-ddl` (ID: `3647522242740894`)

| Cell | Change |
| --- | --- |
| 3 (Set Query Tags) | Split comma-chained `SET QUERY_TAGS` into 5 separate statements; used `EXECUTE IMMEDIATE` for variable-based tags (`catalog`, `schema`) |

#### Updated: `zeroBus/deploy.sh`

Major update — added infrastructure readiness gates between infra and app bundle deployment.

**New flags:**

| Flag | Purpose |
| --- | --- |
| `--run-setup` | Run the UC setup job after deploying the infra bundle |
| `--skip-checks` | Bypass infrastructure readiness checks before app deploy |

**New functions:**

| Function | Purpose |
| --- | --- |
| `resolve_infra_vars()` | Extracts `SCOPE_NAME`, `CATALOG`, `SCHEMA` from `databricks bundle summary --output json` via python3 |
| `run_uc_setup()` | Runs `databricks bundle run wearables_uc_setup --target ${TARGET}` |
| `verify_infra_readiness()` | Checks all 5 secret scope keys + bronze table existence |

**Readiness gate logic:**

| Check | Missing → behaviour |
| --- | --- |
| Auto-provisioned keys (`client_id`, `workspace_url`, `zerobus_endpoint`, `target_table_name`) | **Fail** — "run UC setup job" instructions |
| Bronze table (`wearables_zerobus`) | **Fail** — "run UC setup job" instructions |
| Admin-provisioned key (`client_secret`) | **Fail** — admin provisioning instructions + `--skip-checks` hint |

**Typical workflows:**
- First deploy: `./deploy.sh --target dev --run-setup` → infra + job + checks (client_secret will fail → admin action)
- After admin provisions: `./deploy.sh --target dev --app` → checks pass → app deploys
- Force deploy: `./deploy.sh --target dev --app --skip-checks`

#### Updated: `README.md` (bundle)

| Section | Change |
| --- | --- |
| "What This Bundle Manages" | Rewrote table — added Service Principal and Bronze Table rows |
| "UC Setup Job" | Documents two-task workflow; `client_id` listed as auto-stored, credential check for `client_secret` only |
| "Secret Scope Contents" | `client_id` in auto-provisioned table; only `client_secret` in admin-provisioned table |
| "Deployment Order" | Rewritten with ASCII readiness gate diagram showing all 5 keys + table check; added check behaviour table and deploy.sh flags reference |
| "Quick Start" | Reorganized: "First deployment" flow first (`--run-setup` → admin → `--app`); full flag matrix; standalone deploy section; admin step shows only `client_secret` |
| "Predictive Optimization" | Removed stale `.sql` file note |
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

#### Renamed: Session summary files

| Old name | New name |
| --- | --- |
| `2025-07-16_infra-bundle-scaffold.md` | `2026-04-16_infra-bundle-scaffold.md` |
| `2025-07-17_query-tags-and-query-relocation.md` | `2026-04-16_query-tags-and-query-relocation.md` |

---

### Design Decisions

1. **Dynamic SPN creation over static variable** — Eliminates the chicken-and-egg problem of needing to know the SPN's application_id at bundle deploy time. The SPN is created (or found) at job runtime and its ID flows via task values.

2. **SPN naming convention: `dbxw-zerobus-{schema}`** — Ties the SPN to a specific schema, ensuring least privilege. Different schemas get different SPNs. The naming is deterministic, so re-runs find the existing SPN instead of creating duplicates.

3. **`client_id` auto-provisioned, `client_secret` admin-only** — The SPN's `application_id` is available at runtime and is stored as `client_id` automatically. Only `client_secret` requires admin action (account-level API). This eliminates a manual step from the deployment flow.

4. **Non-sensitive value refresh on every run** — `client_id`, `workspace_url`, `zerobus_endpoint`, and `target_table_name` are always overwritten. This ensures values stay current if the workspace moves or the schema changes. Only `client_secret` is untouched (admin-managed).

5. **ZeroBus endpoint derived from workspace metadata** — The endpoint format `{workspace_id}.zerobus.{region}.cloud.databricks.com` is constructed from runtime metadata (workspace ID + AWS region). This avoids hardcoding environment-specific URLs. Fallback region: `us-east-1`.

6. **Task value handoff over shared variables** — The SPN's `application_id` passes from task 1 to task 2 via `dbutils.jobs.taskValues.set()` / `{{tasks.*.values.*}}`. This is the standard Databricks pattern for inter-task data flow and keeps the job self-contained.

7. **Notebook-managed permissions over declarative YAML** — SPN grants (secret scope READ, UC catalog/schema/table grants) are now fully managed by the two notebooks. This means permissions are applied at the same time as the SPN creation, rather than requiring the SPN to pre-exist for YAML deployment.

8. **Job name shortened: `dbxW ZeroBus — UC Setup`** — 24 characters vs 53. Fits the Workflows viewer without truncation. The `[dev matthew]` or `[prod]` prefix from bundle presets is appended automatically.

9. **Separate Python + SQL tasks** — Task 1 (Python) uses the Databricks SDK for SPN/credential management; Task 2 (SQL) runs on the SQL warehouse for DDL/GRANT statements. Each task uses the compute best suited to its workload.

10. **deploy.sh readiness gates** — Infrastructure checks (5 secret scope keys + bronze table) are enforced as an explicit gate between infra and app bundle deployment. Auto-provisioned missing → "run UC setup job"; admin-provisioned missing → "admin action required" with `--skip-checks` escape hatch. Resolved values come from `databricks bundle summary --output json` parsed with python3.

11. **`SET QUERY_TAGS` one-key-per-statement** — Databricks SQL requires separate `SET` statements for each query tag key. Variable-based tags use `EXECUTE IMMEDIATE` to interpolate session variable values into the string literal.

---

### Files Modified Summary

| File | Path (relative to bundle root) | Action |
| --- | --- | --- |
| ensure-service-principal | `src/uc_setup/ensure-service-principal` | Created (8 cells, Python) |
| target-table-ddl | `src/uc_setup/target-table-ddl` | Updated cell 3 — SET QUERY_TAGS fix |
| uc_setup.job.yml | `resources/uc_setup.job.yml` | Rewritten — 2-task workflow, updated description |
| databricks.yml | `databricks.yml` | Removed `zerobus_service_principal` var + target assignment |
| zerobus.secret_scope.yml | `resources/zerobus.secret_scope.yml` | Removed SPN READ, updated header (client_id auto-provisioned) |
| wearables.schema.yml | `resources/wearables.schema.yml` | Removed SPN grants from hls_fde target |
| deploy.sh | `../deploy.sh` | Added readiness gates, --run-setup, --skip-checks |
| README.md (bundle) | `README.md` | Major rewrite — readiness gate diagram, Quick Start overhaul |
| README.md (parent) | `../README.md` | Rewrote from placeholder to architecture overview |
| .assistant_instructions.md | `~/.assistant_instructions.md` | Notebook paths convention, date accuracy rule |
| Session files (2) | `fixtures/sessions/` | Renamed with correct 2026-04-16 dates |
| INDEX.md | `fixtures/sessions/INDEX.md` | Updated filenames + session description |
