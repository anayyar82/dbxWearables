# ZeroBus

Databricks-side ingestion infrastructure and application for the **dbxWearables** project. This folder hosts the two Databricks Asset Bundles, the shared deployment script, and all configuration needed to stand up **another workspace or logical environment** by editing bundle targets only—then running the same `deploy.sh` commands.

## Layout

```
zeroBus/
├── deploy.sh                 # Shared script: infra → (optional UC setup) → readiness → app
├── dbxW_zerobus_infra/       # UC schema, secret scope, SQL warehouse, Lakebase, UC setup job
└── dbxW_zerobus_app/         # AppKit app, medallion DLT, jobs, Lakeview, analytics wiring
```

| Bundle | README |
| --- | --- |
| Infrastructure | [dbxW_zerobus_infra/README.md](dbxW_zerobus_infra/README.md) |
| Application | [dbxW_zerobus_app/README.md](dbxW_zerobus_app/README.md) |

**Rule:** Anything that identifies the environment (workspace, catalog, schema, Lakebase branch/database, warehouse IDs, secret key strategy) lives in **`databricks.yml`** under a **target** name. The script and jobs read that target via `--target <name>`—no code changes are required for a new environment once the target blocks exist and match between bundles.

---

## Deploy to a new environment (step-by-step)

Use this when you want a **new** logical environment (e.g. `staging`, another catalog/schema, or another workspace). Existing targets such as `dev` / `hls_fde` / `prod` are examples only.

### 0. Prerequisites

1. **Databricks CLI** installed and authenticated for the workspace you will use.  
   - Configure a **CLI profile** whose name you will put in `workspace.profile` in both bundles (see `~/.databrickscfg` or `databricks auth login --profile <name>`).
2. **Git clone** of this repo and shell access from `zeroBus/`.

### 1. Choose a target name

Pick a short identifier, e.g. `staging`. You will pass it everywhere as:

`./deploy.sh --target staging ...`

The name must exist **identically** in:

- `dbxW_zerobus_infra/databricks.yml` → `targets.staging`
- `dbxW_zerobus_app/databricks.yml` → `targets.staging`

### 2. Add the infrastructure target (copy and edit)

In **`dbxW_zerobus_infra/databricks.yml`**, duplicate an existing target (e.g. `dev`) under `targets:` and adjust at minimum:

| Area | What to set |
| --- | --- |
| `workspace.host` | HTTPS URL of the Databricks workspace |
| `workspace.profile` | CLI profile that authenticates to that host |
| `workspace.root_path` | Bundle state path (often under `/Workspace/Users/<you>/.bundle/...` for dev) |
| `mode` | `development` or `production` |
| `variables.catalog` / `variables.schema` | UC catalog and schema for wearable objects |
| `variables.run_as_user` | User email or service principal **application id** string used to run jobs |
| `variables.client_id_dbs_key` / `client_secret_dbs_key` | Usually `client_id_${var.schema}` and `client_secret_${var.schema}` so multiple schemas can share one scope |
| `presets.tags` | Optional: tags are driven by `variables.tags_*` at the top of the file |

If you use **production-style** permissions (see `hls_fde` in the file), copy that block and adjust `user_name` / `group_name` / `service_principal_name` as needed.

Validate:

```bash
cd zeroBus/dbxW_zerobus_infra
databricks bundle validate --target staging
```

### 3. Deploy infrastructure once

```bash
cd zeroBus
./deploy.sh --target staging --infra
```

This creates (among other things) the **SQL warehouse** and **Lakebase** project for that target, as defined under `dbxW_zerobus_infra/resources/`.

### 4. Resolve values for the **application** target

The app bundle cannot read the infra bundle’s deployed resource IDs at YAML compile time. After infra is deployed, collect:

**A. SQL warehouse ID (required for Insights / Lakeview / analytics plugin)**  
The infra bundle deploys `infra_warehouse`. Read its id from the infra bundle summary:

```bash
cd zeroBus/dbxW_zerobus_infra
databricks bundle summary --target staging --output json | python3 -c "
import json, sys
data = json.load(sys.stdin)
wh = data.get('resources', {}).get('sql_warehouses', {}).get('infra_warehouse', {})
wid = wh.get('id') or wh.get('warehouse_id')
print(wid or 'Could not find id — search JSON for infra_warehouse')
"
```

If the one-liner prints nothing useful, search the same JSON for `infra_warehouse` and copy the warehouse **id** (UUID) into the app bundle target’s `sql_warehouse_id`.

**B. Lakebase `postgres_branch` and `postgres_database`**  
If the Lakebase project was just created, list branch and database resource names:

```bash
databricks postgres list-branches projects/<project-id>
databricks postgres list-databases projects/<project-id>/branches/production
```

Use the full resource strings in the app bundle (same format as existing targets).

**C. Optional: medallion pipeline UUID**  
After the app bundle has been deployed at least once, the pipeline `wearable_medallion` exists in the workspace. Copy its UUID from **Workspace → Lakeflow Pipelines** into `variables.wearable_medallion_pipeline_id` for that target so the DLT status page and triggers can resolve it without extra API permissions. You can leave it empty on first deploy and fill it in on a later deploy.

### 5. Add the application target (copy and edit)

In **`dbxW_zerobus_app/databricks.yml`**, duplicate an existing target and set the **same** `catalog`, `schema`, `secret_scope_name`, `client_id_dbs_key`, and `client_secret_dbs_key` as the infra target for this environment. Also set:

| Area | Must match / notes |
| --- | --- |
| `workspace.*` | Same workspace **host** and **profile** as infra for this environment |
| `variables.postgres_branch` / `postgres_database` | From step 4B |
| `variables.sql_warehouse_id` | From step 4A (**do not** rely on the default id—it belongs to another workspace) |
| `variables.wearable_medallion_pipeline_id` | From step 4C when available |
| `variables.run_as_user` / `run_as` / `permissions` | Mirror the pattern used by `dev` or `hls_fde` for your use case |

Validate:

```bash
cd zeroBus/dbxW_zerobus_app
databricks bundle validate --target staging
```

### 6. Run UC setup, store the client secret, deploy the app

```bash
cd zeroBus

# Deploy infra (if not already), run UC setup job: SPN, scope keys, bronze table DDL
./deploy.sh --target staging --run-setup
```

On **first** run, the job stores the OAuth **client id** and fixed keys (`workspace_url`, `zerobus_endpoint`, `target_table_name`) in the secret scope. An **admin** must create the OAuth **client secret** for the `dbxw-zerobus-<schema>` service principal and store it under the **same** key name as `client_secret_dbs_key` (e.g. `client_secret_my_schema`). The infra README has exact CLI examples.

Then:

```bash
./deploy.sh --target staging
```

This runs **readiness checks** (secrets + bronze table), then deploys the **app** bundle. Equivalent to full stack after infra is already up:

```bash
./deploy.sh --target staging --infra
./deploy.sh --target staging --run-setup   # if you skipped --run-setup above
# provision client_secret in the scope, then:
./deploy.sh --target staging --app
```

**Optional:** `databricks bundle deploy` supports `-var name=value` overrides for declared variables; useful for ad hoc tweaks without committing YAML. Persistent environments should still use per-target blocks.

---

## Command reference (`deploy.sh`)

All commands are run from **`zeroBus/`**. `--target <name>` must match a target defined in **both** bundles.

| Command | Effect |
| --- | --- |
| `./deploy.sh --target <name>` | Deploy infra (if selected), readiness gate, deploy app |
| `./deploy.sh --target <name> --run-setup` | After infra deploy, run `wearables_uc_setup` job |
| `./deploy.sh --target <name> --infra` | Infra bundle only |
| `./deploy.sh --target <name> --app` | App bundle only (readiness checks first) |
| `./deploy.sh --target <name> --app --skip-checks` | App only, skip gate (not recommended until secrets exist) |
| `./deploy.sh --target <name> --validate` | Validate both bundles, no deploy |
| `./deploy.sh --target <name> --destroy` | Destroy resources (use with care) |

The script sets **`DATABRICKS_CONFIG_PROFILE`** from the infra bundle’s `workspace.profile` when resolving secrets and table checks, so checks hit the correct workspace.

---

## Documentation links

* [dbxWearables project README](../README.md)
* [ZeroBus Ingest overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview/)
* [ZeroBus Ingest connector](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest/)
* [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
