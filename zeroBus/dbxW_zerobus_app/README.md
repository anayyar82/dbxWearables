# dbxW_zerobus_app

Application bundle for the **dbxWearables ZeroBus** solution. This Databricks Asset Bundle manages the runtime application layer — the AppKit REST API, ZeroBus SDK consumer, Lakebase operational database, Spark Declarative Pipelines (silver/gold), jobs, and dashboards — that sits on top of the shared infrastructure provisioned by the companion [`dbxW_zerobus_infra`](../dbxW_zerobus_infra/README.md) bundle.

## Relationship to dbxWearables

The [dbxWearables](../../README.md) project ingests wearable and health app data into Databricks using AppKit, ZeroBus, Spark Declarative Pipelines, Lakebase, and AI/BI. The end-to-end flow is:

```
Client (Apple HealthKit app | /docs demo NDJSON | future Android / APIs)
  → Databricks App (AppKit REST API)              ← this bundle
    ├─ ZeroBus SDK → UC bronze (wearables_zerobus) ← same schema for demo + prod
    │    └─ DLT medallion (wearable_medallion.py) — streaming bronze/silver STs + gold MV-style @dlt.table
    └─ Lakebase (Postgres) → app state               ← infra bundle creates the project
```

**Replacing demo traffic with HealthKit:** keep the bronze table and headers contract; only the HTTP client changes. DLT reads JSON fields via snake_case keys (same as the iOS mappers).

This application bundle owns everything **above** the foundational infrastructure: the AppKit app that receives data, the ZeroBus consumer that streams it, the Lakebase connection for operational state, and the Spark Declarative Pipelines that refine data through the medallion layers.

## Prerequisites — Infrastructure Bundle

The [`dbxW_zerobus_infra`](../dbxW_zerobus_infra/README.md) bundle **must** be deployed and its UC setup job run before this bundle can be deployed. The infra bundle provisions:

| Shared Resource | How This Bundle References It |
| --- | --- |
| UC schema (`wearables`) | `${var.catalog}.${var.schema}` — per-target values kept in sync |
| Secret scope (`dbxw_zerobus_credentials`) | `${var.secret_scope_name}` — same default across both bundles |
| SQL warehouse (2X-Small serverless PRO) | By warehouse ID or name where needed |
| Service principal (`dbxw-zerobus-{schema}`) | OAuth credentials read from the secret scope at runtime |
| Bronze table (`wearables_zerobus`) | ZeroBus SDK streams directly to this table |
| Lakebase project (`dbxw-zerobus-wearables`) | Referenced via `${var.postgres_branch}` and `${var.postgres_database}` |

> **Cross-bundle convention:** DAB does not support cross-bundle resource substitutions (`${resources.*}`). This bundle maintains its own `catalog`, `schema`, and `secret_scope_name` variables with per-target values that **must match** the infra bundle. If the infra target values change, update both bundles.

The shared [`deploy.sh`](../deploy.sh) script enforces deployment order and runs readiness checks (all 5 secret scope keys + bronze table existence) before allowing this bundle to deploy.

## AppKit Application

The app is a **TypeScript/Node.js** project built with `@databricks/appkit` (Express + React + Vite). Source code lives in `src/app/` and is uploaded as the Databricks App source via `source_code_path: ../src/app` in the resource YAML.

### Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │  AppKit App (src/app/)                       │
                    │                                              │
  HealthKit POST ──►│  Express Server (server/server.ts)           │
                    │    ├─ ZeroBus routes → SDK → bronze table    │
                    │    └─ Lakebase routes → pg.Pool → Postgres   │
                    │                                              │
  Browser ─────────►│  React Client (client/src/)                  │
                    │    └─ Vite + Tailwind + appkit-ui             │
                    └─────────────────────────────────────────────┘
```

### Medallion (single DLT pipeline)

`resources/wearable_medallion.pipeline.yml` → `src/dlt/wearable_medallion.py` (continuous): **ZeroBus Delta → `01_wearable_bronze_stream` (append-only ST)** → **silver as `create_streaming_table` + `@append_flow`** → **gold as `@dlt.table` aggregations (MV-style in Lakeflow)**. UC names use the **`01_`** prefix. Refresh on demand via `wearable_medallion_refresh.job.yml`.

Bronze rows are tagged for DLT filtering: **`x-ingest-channel`** = `notebook_simulator` (seed notebook) or `rest_app` (AppKit `/api/v1/healthkit/ingest`). Pipeline config **`wearables_ingest_channel_filter`** (`all` \| `notebook_simulator` \| `rest_app`) is set per target in `databricks.yml` (`var.wearables_ingest_channel_filter`). Legacy notebook seeds without the header still match `notebook_simulator` when `x-device-id` is `demo-notebook-seed`.

If you see **“Table … is already managed by pipeline …”**, an older Lakeflow pipeline still owns those object names. This bundle uses the **`01_`** table prefix (for example `01_wearable_deletes_silver`) so the new `wearable_medallion` pipeline avoids colliding with legacy unprefixed tables. To reset only the prefixed tables, run `src/sql/drop_wearable_medallion_managed_objects.sql` (adjust catalog/schema), then start a pipeline update.

Lakebase connectivity checks should use `GET /api/lakebase/health` (`SELECT 1`); the sample todos CRUD requires DDL on `app.todos` and may still fail if the database role cannot create schemas — that no longer blocks the Health page from showing “Lakebase OK”.

### DLT live page (`/dlt`)

The React **DLT live** page calls workspace Pipelines REST via the app’s service principal (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`, `scope=all-apis`) and `ZEROBUS_WORKSPACE_URL`. Grant the app SPN **CAN RUN** (or **CAN VIEW**) on each pipeline.

Set optional environment variables on the **Databricks App** deployment (UI or API), using pipeline UUIDs from **Workspace → Lakeflow Pipelines**:

| Variable | Purpose |
| --- | --- |
| `WEARABLE_PIPELINE_ID` | Single medallion DLT pipeline UUID (`wearable_medallion` in the bundle) |

Aliases: `WEARABLE_PIPELINE_BATCH_ID`, `WEARABLE_MEDALLION_PIPELINE_ID`. Optional label: `WEARABLE_PIPELINE_LABEL`.

### Plugins

Configured in `src/app/appkit.plugins.json`:

| Plugin | Package | Purpose | Required |
| --- | --- | --- | --- |
| `server` | `@databricks/appkit` | Express HTTP server, static files, Vite dev mode | Yes (template) |
| `lakebase` | `@databricks/appkit` | Postgres wire protocol via `pg.Pool` with OAuth token rotation | Yes (template) |
| `analytics` | `@databricks/appkit` | SQL query execution against Databricks SQL Warehouses | Optional |
| `files` | `@databricks/appkit` | File operations against Volumes and Unity Catalog | Optional |
| `genie` | `@databricks/appkit` | AI/BI Genie space integration | Optional |

### App Resources (6 total)

Defined in `resources/zerobus_ingest.app.yml` and mapped to environment variables in `src/app/app.yaml`:

| Resource | Type | `valueFrom` | Env Var |
| --- | --- | --- | --- |
| `postgres` | Lakebase Postgres | `postgres` | `LAKEBASE_ENDPOINT` |
| `zerobus-client-id` | Secret scope | `zerobus-client-id` | `ZEROBUS_CLIENT_ID` |
| `zerobus-client-secret` | Secret scope | `zerobus-client-secret` | `ZEROBUS_CLIENT_SECRET` |
| `zerobus-workspace-url` | Secret scope | `zerobus-workspace-url` | `ZEROBUS_WORKSPACE_URL` |
| `zerobus-endpoint` | Secret scope | `zerobus-endpoint` | `ZEROBUS_ENDPOINT` |
| `zerobus-target-table` | Secret scope | `zerobus-target-table` | `ZEROBUS_TARGET_TABLE` |

Platform-injected (no `valueFrom` needed): `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGSSLMODE`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`.

### What This Bundle Manages

| Resource Type | Resource | Purpose | Status |
| --- | --- | --- | --- |
| Databricks App | `dbxw-zerobus-ingest-${var.schema}` | AppKit REST API + Lakebase + ZeroBus SDK | Defined |
| Spark Declarative Pipeline | `resources/wearable_medallion.pipeline.yml` → `wearable_medallion.py` | Streaming `01_wearable_bronze_stream` + silver STs + gold MVs (`01_` prefix); optional ingest-channel filter | Defined in bundle |
| Jobs | `seed_wearables_bronze`, `wearable_medallion_refresh` | Seed bronze; trigger pipeline update | Defined |
| Dashboards | Lakeview | Wearable health data visualizations | Defined |

## Bundle Structure

```
dbxW_zerobus_app/
├── databricks.yml                          # Bundle configuration (variables, targets, includes)
├── README.md                               # This file
├── .gitignore                              # Excludes .databricks/, build artifacts, node_modules
├── resources/
│   └── zerobus_ingest.app.yml              # AppKit app resource (6 resources, per-target permissions)
├── src/
│   └── app/                                # AppKit source (source_code_path target)
│       ├── app.yaml                        # Runtime command + env var bindings
│       ├── appkit.plugins.json             # Plugin registry (lakebase, server, analytics, etc.)
│       ├── package.json                    # Node.js dependencies (@databricks/appkit 0.20.3)
│       ├── package-lock.json               # Locked dependency tree
│       ├── server/                         # Express backend
│       │   ├── server.ts                   # Entry point — createApp + plugin init
│       │   └── routes/
│       │       └── lakebase/
│       │           └── todo-routes.ts      # Sample Lakebase CRUD routes (scaffold)
│       ├── client/                         # React frontend
│       │   ├── index.html                  # HTML entry point
│       │   ├── vite.config.ts              # Vite build configuration
│       │   ├── tailwind.config.ts          # Tailwind CSS configuration
│       │   ├── src/
│       │   │   ├── App.tsx                 # Root React component
│       │   │   ├── main.tsx                # React DOM entry
│       │   │   └── pages/lakebase/         # Lakebase demo page
│       │   └── public/                     # Static assets (favicons, manifest)
│       ├── tests/
│       │   └── smoke.spec.ts              # Playwright smoke test
│       ├── tsconfig.json                   # Root TypeScript config
│       ├── tsconfig.server.json            # Server-specific TS config
│       ├── tsconfig.client.json            # Client-specific TS config
│       ├── tsconfig.shared.json            # Shared TS config
│       ├── tsdown.server.config.ts         # Server bundler config
│       ├── vitest.config.ts                # Vitest test runner config
│       ├── playwright.config.ts            # Playwright E2E config
│       ├── eslint.config.js                # ESLint config
│       ├── .prettierrc.json                # Prettier config
│       ├── .env.example                    # Environment variable template
│       ├── CLAUDE.md                       # AppKit AI assistant instructions
│       └── .gitignore                      # AppKit-specific ignores
└── fixtures/
    ├── sessions/                           # Development session logs
    └── AppKit App Bundle Setup Session.ipynb
```

## Variables

All variables are declared in `databricks.yml` and assigned per-target. Variables shared with the infra bundle use identical defaults and per-target values.

### Shared with infra bundle (must stay in sync)

| Variable | Default | Purpose |
| --- | --- | --- |
| `catalog` | *(per-target)* | Unity Catalog catalog — `users` |
| `schema` | *(per-target)* | Schema name — `ankur_nayyar` |
| `secret_scope_name` | `dbxw_zerobus_credentials` | Secret scope for ZeroBus OAuth credentials |
| `client_id_dbs_key` | `client_id` | Key name for the M2M client ID in the secret scope |
| `client_secret_dbs_key` | `client_secret` | Key name for the M2M client secret in the secret scope |
| `run_as_user` | *(per-target)* | User or service principal for workflow execution |
| `higher_level_service_principal` | `acf021b4-...` | SP application ID for production deployments |
| `serverless_environment_version` | `5` | Serverless environment version for tasks |

#### Schema-qualified secret key names

The `dev` and `hls_fde` targets override `client_id_dbs_key` and `client_secret_dbs_key` to schema-qualified names, enabling multiple schemas to share a single secret scope without key collisions:

| Target | `client_id_dbs_key` | `client_secret_dbs_key` |
| --- | --- | --- |
| `dev` | `client_id_${var.schema}` → `client_id_ankur_nayyar` | `client_secret_${var.schema}` → `client_secret_ankur_nayyar` |
| `hls_fde` | `client_id_${var.schema}` → `client_id_ankur_nayyar` | `client_secret_${var.schema}` → `client_secret_ankur_nayyar` |
| `prod` | `client_id` *(default)* | `client_secret` *(default)* |

### Lakebase Postgres

| Variable | Purpose |
| --- | --- |
| `postgres_branch` | Full branch resource name: `projects/dbxw-zerobus-wearables/branches/production` |
| `postgres_database` | Full database resource name: `projects/.../databases/db-0k31-aj7nvq8pgr` |

Obtain these by running:
```bash
databricks postgres list-branches projects/dbxw-zerobus-wearables
databricks postgres list-databases projects/dbxw-zerobus-wearables/branches/production
```

### App-specific

| Variable | Default | Purpose |
| --- | --- | --- |
| `dashboard_embed_credentials` | `false` | Dashboard credential mode (`true` = owner, `false` = viewer) |

### Tags (applied to all resources via presets)

| Variable | Default |
| --- | --- |
| `tags_project` | `dbxWearables ZeroBus` |
| `tags_businessUnit` | `Healthcare and Life Sciences` |
| `tags_developer` | `ankur.nayyar@databricks.com` |
| `tags_requestedBy` | `Healthcare Providers and Health Plans` |
| `tags_RemoveAfter` | `2027-03-04` |

## Targets

| Target | Mode | Workspace | Catalog | Schema | Default |
| --- | --- | --- | --- | --- | --- |
| `dev` | development | `e2-demo-field-eng.cloud.databricks.com` | `users` | `ankur_nayyar` | Yes |
| `hls_fde` | production | `e2-demo-field-eng.cloud.databricks.com` | `users` | `ankur_nayyar` | No |
| `prod` | production | `e2-demo-field-eng.cloud.databricks.com` | `users` | `ankur_nayyar` | No |

All three targets mirror the infra bundle's target definitions — same workspace hosts, root paths, presets, and permissions.

## Development

### AppKit local dev

```bash
cd src/app

# Install dependencies
npm install

# Start dev server (hot-reload, Vite dev mode)
npm run dev

# Build for production
npm run build

# Run tests
npm test

# Lint and format
npm run lint
npm run format
```

Local dev requires a `.env` file (see `.env.example`) with Lakebase connection details and Databricks host.

### Deployment

#### Via shared script (recommended)

```bash
cd zeroBus

# Full deployment — infra first, readiness checks, then app
./deploy.sh --target dev

# First-time setup — infra + UC setup job + app
./deploy.sh --target dev --run-setup

# App bundle only (with infrastructure readiness checks)
./deploy.sh --target dev --app

# App bundle only (skip readiness checks)
./deploy.sh --target dev --app --skip-checks

# Validate without deploying
./deploy.sh --target dev --validate

# Destroy app resources
./deploy.sh --target dev --app --destroy
```

#### Standalone (without deploy.sh)

```bash
cd zeroBus/dbxW_zerobus_app
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

> **Warning:** Standalone deployment bypasses the readiness gate. Ensure the infra bundle is deployed, the UC setup job has run, and `client_secret` is provisioned before deploying standalone.

#### Workspace UI

1. Click the **deployment rocket** in the left sidebar to open the **Deployments** panel
2. Click **Deploy** to deploy the bundle
3. Hover over a resource and click **Run** to execute a job or pipeline

#### Managing Resources

* Use the **Add** dropdown in the Deployments panel to add new resources
* Click **Schedule** on a notebook to create a job definition

## Documentation

* [dbxWearables project README](../../README.md)
* [ZeroBus directory README](../README.md)
* [Infrastructure bundle README](../dbxW_zerobus_infra/README.md)
* [Declarative Automation Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
* [Declarative Automation Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
* [ZeroBus Ingest overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview/)
* [ZeroBus Ingest connector](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest/)
* [Databricks Apps (AppKit)](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
* [Lakebase Autoscaling](https://docs.databricks.com/aws/en/lakebase/)
* [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/delta-live-tables/)
