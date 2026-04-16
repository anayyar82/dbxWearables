# Session: dbxW_zerobus_infra Bundle Scaffold

**Date:** 2025-07-16
**Bundle:** `dbxW_zerobus_infra`
**Project:** dbxWearables-ZeroBus

## Summary

Scaffolded the `dbxW_zerobus_infra` Databricks Asset Bundle from scratch, using the existing `fhir_zerobus` solution (in `synthea-on-fhir/zerobus/`) as a reference architecture. This bundle manages shared infrastructure (secret scopes, UC schemas, bronze table DDL, grants) that must be deployed before the primary `dbxW_zerobus` application bundle.

## Reference Analysis

Reviewed the complete FHIR ZeroBus solution to identify infrastructure-first resources:

| Resource | FHIR Location | Infra-First? |
| --- | --- | --- |
| Secret scope (`fhir_zerobus_credentials`) | `fhir_zerobus/resources/zerobus.secret_scope.yml` | Yes — moved to infra bundle |
| Bronze table DDL + grants | `fhir_zerobus/src/uc_setup/target-table-ddl.ipynb` | Yes — moved to infra bundle |
| UC schema (assumed to exist) | Not declared | Yes — now declarative via DAB `schemas` resource |
| Volumes (planned, not implemented) | `fhir_zerobus_infra/README.md` only | Yes |
| Service principal permissions | Dynamic SQL in DDL notebook | Yes — both declarative (schema grants) and notebook (table grants) |

## Changes Made

### databricks.yml — Full rewrite

* **Variables:** `catalog`, `schema`, `secret_scope_name`, `client_id_dbs_key`, `run_as_user`, `higher_level_service_principal`, `zerobus_service_principal`, `serverless_environment_version`, `dashboard_embed_credentials`, plus 5 tag variables
* **Convention block:** `catalog` and `schema` vars feed ONLY into `resources/wearables.schema.yml`. All other resources must use `${resources.schemas.wearables_schema.catalog_name}`, `.name`, or `.id`
* **Includes:** `*.schema.yml`, `*.secret_scope.yml`, `*.job.yml`, `*.yml`, `*/*.yml`
* **`dev` target:** host `fevm-hls-fde.cloud.databricks.com`, catalog `hls_fde_dev`, schema `wearables`, run_as matthew.giglia
* **`hls_fde` target:** production mode, same workspace, catalog `hls_fde`, SP `acf021b4-...` (matching FHIR hls_fde target), full permissions block
* **`prod` target:** left as original placeholder skeleton

### resources/wearables.schema.yml — New file

* Declares `wearables_schema` as a DAB `schemas` resource — the ONLY file that references `${var.catalog}` and `${var.schema}` directly
* `catalog_name: ${var.catalog}`, `name: ${var.schema}`
* `lifecycle.prevent_destroy: true`
* Per-target grants:
  * **dev:** `ALL_PRIVILEGES` to `run_as_user`, `USE_SCHEMA` to `account users`
  * **hls_fde:** `USE_SCHEMA` + `SELECT` + `MODIFY` + `CREATE_TABLE` + `CREATE_VOLUME` to ZeroBus SP, `ALL_PRIVILEGES` to deploy SP + admin, `USE_SCHEMA` + `SELECT` to `account users`
  * **prod:** `ALL_PRIVILEGES` to admin, `USE_SCHEMA` + `SELECT` to `account users`
* Header comment documents substitution paths for downstream consumers

### resources/zerobus.secret_scope.yml — New file

* Databricks-managed backend, `prevent_destroy: true`
* Per-target permissions: dev (user MANAGE + SP READ), hls_fde (admin MANAGE + deploy SP MANAGE + zerobus SP READ), prod (same pattern)
* Header comments with CLI commands for populating secrets

### resources/uc_setup.job.yml — New file

* Job `wearables_uc_setup` — runs the DDL notebook to create/maintain the bronze table and grants
* Parameters use `${resources.schemas.wearables_schema.*}` convention (NOT `${var.catalog/schema}`):
  * `catalog_use` → `${resources.schemas.wearables_schema.catalog_name}`
  * `schema_use` → `${resources.schemas.wearables_schema.name}`
  * `spn_application_id` → `${var.zerobus_service_principal}`
* Serverless environment (`${var.serverless_environment_version}`), 10-minute timeout, max 1 concurrent run
* No automatic schedule (manual run or via `deploy.sh`)
* Tags: `component: setup`, `resource_type: table`, `table_name: wearables_zerobus`

### src/uc_setup/target-table-ddl.ipynb — New notebook (13 cells)

SQL notebook invoked by the `wearables_uc_setup` job. Cells:

| # | Title | Purpose |
| --- | --- | --- |
| 1 | (Markdown) Title | Describes notebook purpose and bundle context |
| 2 | Set Catalog and Schema from Parameters | `DECLARE` `:catalog_use`, `:schema_use`; `USE IDENTIFIER(...)` |
| 3 | Target Table DDL — wearables_zerobus | `CREATE TABLE IF NOT EXISTS` with `record_id` (PK), `ingested_at`, `body` (VARIANT), `headers` (VARIANT), `record_type`; CDF, auto-optimize, VARIANT support |
| 4 | Optimization Strategy (Z-ORDER) | `OPTIMIZE ... ZORDER BY (ingested_at, record_type)` with notes on predictive optimization |
| 5 | Declare Service Principal Variable | `DECLARE` `:spn_application_id` |
| 6 | Grant USE CATALOG to SP | Dynamic SQL — builds `GRANT USE CATALOG` statement |
| 7 | Execute USE CATALOG Grant | `EXECUTE IMMEDIATE` |
| 8 | Grant USE SCHEMA to SP | Dynamic SQL — builds `GRANT USE SCHEMA` statement |
| 9 | Execute USE SCHEMA Grant | `EXECUTE IMMEDIATE` |
| 10 | Grant MODIFY and SELECT on Table to SP | Dynamic SQL — builds table-level grant |
| 11 | Execute Table Grant | `EXECUTE IMMEDIATE` |
| 12 | Verify Grants | `SHOW GRANTS ON TABLE wearables_zerobus` |
| 13 | Show Table Definition | `SHOW CREATE TABLE wearables_zerobus` |

### Earlier in session (prior context)

* Updated `dbxW_zerobus_infra/README.md` — rewritten to describe infra-first purpose
* Created `zeroBus/deploy.sh` — shared deployment orchestrator with `--infra`/`--app` flags

## Design Decisions

1. **Schema name `wearables`** — domain-oriented, not transport-oriented (avoided `zerobus`)
2. **Secret scope in infra, not app** — fixes the chicken-and-egg problem from the FHIR bundle where the scope was in the app bundle but needed before app startup
3. **SP values copied from FHIR `hls_fde`** — `acf021b4-87c6-44ff-b3d7-45c59d63fe4d` for both `higher_level_service_principal` and `zerobus_service_principal` (same SP in this workspace)
4. **`source_linked_deployment: false`** for all targets — matches FHIR pattern, avoids symlink issues
5. **Schema as declarative DAB resource** — improvement over FHIR where schemas were assumed to pre-exist. Uses `resources.schemas` with grants and `prevent_destroy`
6. **`${resources.schemas.*}` convention** — all resources except the schema YAML itself reference catalog/schema via deployed-resource substitutions, not variables. Prevents drift between variable values and actual deployed objects
7. **Bronze table DDL in infra bundle** — moved from app bundle (where FHIR had it) to infra, since the table must exist before the ZeroBus app can stream into it
8. **Dynamic SQL for grants** — follows FHIR pattern; `EXECUTE IMMEDIATE` with string-built GRANT statements allows parameterized SP application IDs

## Files Modified

| File | Action | Path |
| --- | --- | --- |
| `databricks.yml` | Rewritten (×2) | `zeroBus/dbxW_zerobus_infra/databricks.yml` |
| `wearables.schema.yml` | Created | `zeroBus/dbxW_zerobus_infra/resources/wearables.schema.yml` |
| `zerobus.secret_scope.yml` | Created | `zeroBus/dbxW_zerobus_infra/resources/zerobus.secret_scope.yml` |
| `uc_setup.job.yml` | Created | `zeroBus/dbxW_zerobus_infra/resources/uc_setup.job.yml` |
| `target-table-ddl.ipynb` | Created (13 cells) | `zeroBus/dbxW_zerobus_infra/src/uc_setup/target-table-ddl.ipynb` |
| `README.md` | Rewritten | `zeroBus/dbxW_zerobus_infra/README.md` |
| `deploy.sh` | Created | `zeroBus/deploy.sh` |

## Next Steps

* Add volume declarations as YAML resources (app_config, checkpoints, archive)
* Scaffold the primary `dbxW_zerobus` app bundle alongside this one (AppKit app, SDP pipeline, jobs)
* Populate secrets via CLI after first deploy
* Validate bundle: `databricks bundle validate --target dev`
* First deploy: `./deploy.sh --target dev --infra`
