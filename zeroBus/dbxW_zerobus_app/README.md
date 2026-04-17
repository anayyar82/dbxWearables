# dbxW_zerobus_app

Application bundle for the **dbxWearables ZeroBus** solution. This Databricks Asset Bundle manages the runtime application layer — the AppKit REST API, ZeroBus SDK consumer, Spark Declarative Pipelines (silver/gold), jobs, and dashboards — that sits on top of the shared infrastructure provisioned by the companion [`dbxW_zerobus_infra`](../dbxW_zerobus_infra/README.md) bundle.

## Relationship to dbxWearables

The [dbxWearables](../../README.md) project ingests wearable and health app data into Databricks using AppKit, ZeroBus, Spark Declarative Pipelines, Lakebase, and AI/BI. The end-to-end flow is:

```
Client App (HealthKit, etc.)
  → Databricks App (AppKit REST API)        ← this bundle
    → ZeroBus SDK → UC Bronze Table         ← infra bundle creates the table
      → Spark Declarative Pipeline          ← this bundle
        (silver → gold)
```

This application bundle owns everything **above** the foundational infrastructure: the AppKit app that receives data, the ZeroBus consumer that streams it, and the Spark Declarative Pipelines that refine it through the medallion layers.

## Prerequisites — Infrastructure Bundle

The [`dbxW_zerobus_infra`](../dbxW_zerobus_infra/README.md) bundle **must** be deployed and its UC setup job run before this bundle can be deployed. The infra bundle provisions:

| Shared Resource | How This Bundle References It |
| --- | --- |
| UC schema (`wearables`) | `${var.catalog}.${var.schema}` — per-target values kept in sync |
| Secret scope (`dbxw_zerobus_credentials`) | `${var.secret_scope_name}` — same default across both bundles |
| SQL warehouse (2X-Small serverless PRO) | By warehouse ID or name where needed |
| Service principal (`dbxw-zerobus-{schema}`) | OAuth credentials read from the secret scope at runtime |
| Bronze table (`wearables_zerobus`) | ZeroBus SDK streams directly to this table |

> **Cross-bundle convention:** DAB does not support cross-bundle resource substitutions (`${resources.*}`). This bundle maintains its own `catalog`, `schema`, and `secret_scope_name` variables with per-target values that **must match** the infra bundle. If the infra target values change, update both bundles.

The shared [`deploy.sh`](../deploy.sh) script enforces deployment order and runs readiness checks (all 5 secret scope keys + bronze table existence) before allowing this bundle to deploy.

## What This Bundle Will Manage

| Resource Type | Resource | Purpose |
| --- | --- | --- |
| Databricks App | AppKit REST API | Receives HealthKit JSON POSTs, forwards to ZeroBus SDK |
| ZeroBus Consumer | SDK within the app process | Streams request payload + headers to the bronze table |
| Spark Declarative Pipeline | Silver/gold processing | Reads bronze → cleaned/validated silver → aggregated gold |
| Jobs | Pipeline orchestration | Scheduled runs of the Spark Declarative Pipeline |
| Dashboards | AI/BI analytics | Wearable health data visualizations and monitoring |

> **Note:** Resources are being added incrementally. The bundle skeleton is deployed first; resource YAML files are added to `resources/` as each component is built.

## Bundle Structure

```
dbxW_zerobus_app/
├── databricks.yml              # Bundle configuration (variables, targets, includes)
├── README.md                   # This file
├── .gitignore                  # Excludes .databricks/ state directory
├── resources/                  # Resource YAML definitions (created as resources are added)
│   ├── *.app.yml               # AppKit app definitions
│   ├── *.pipeline.yml          # Spark Declarative Pipeline definitions
│   ├── *.job.yml               # Job definitions
│   └── *.dashboard.yml         # Dashboard definitions
├── src/                        # Source code (created as components are built)
│   ├── app/                    # AppKit REST API + ZeroBus SDK consumer
│   ├── pipelines/              # Spark Declarative Pipeline notebooks
│   └── transforms/             # Shared transformation logic
└── fixtures/                   # Session summaries and examples
    └── sessions/               # Development session logs
```

## Variables

All variables are declared in `databricks.yml` and assigned per-target. Variables shared with the infra bundle use identical defaults and per-target values.

### Shared with infra bundle (must stay in sync)

| Variable | Default | Purpose |
| --- | --- | --- |
| `catalog` | *(per-target)* | Unity Catalog catalog — `hls_fde_dev` (dev), `hls_fde` (hls_fde) |
| `schema` | *(per-target)* | Schema name — `wearables` across all targets |
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
| `dev` | `client_id_${var.schema}` → `client_id_wearables` | `client_secret_${var.schema}` → `client_secret_wearables` |
| `hls_fde` | `client_id_${var.schema}` → `client_id_wearables` | `client_secret_${var.schema}` → `client_secret_wearables` |
| `prod` | `client_id` *(default)* | `client_secret` *(default)* |

### App-specific

| Variable | Default | Purpose |
| --- | --- | --- |
| `dashboard_embed_credentials` | `false` | Dashboard credential mode (`true` = owner, `false` = viewer) |

### Tags (applied to all resources via presets)

| Variable | Default |
| --- | --- |
| `tags_project` | `dbxWearables ZeroBus` |
| `tags_businessUnit` | `Healthcare and Life Sciences` |
| `tags_developer` | `matthew.giglia@databricks.com` |
| `tags_requestedBy` | `Healthcare Providers and Health Plans` |
| `tags_RemoveAfter` | `2027-03-04` |

## Targets

| Target | Mode | Workspace | Catalog | Schema | Default |
| --- | --- | --- | --- | --- | --- |
| `dev` | development | `fevm-hls-fde.cloud.databricks.com` | `hls_fde_dev` | `wearables` | Yes |
| `hls_fde` | production | `fevm-hls-fde.cloud.databricks.com` | `hls_fde` | `wearables` | No |
| `prod` | production | `fevm-hls-fde.cloud.databricks.com` | *(TBD)* | *(TBD)* | No |

All three targets mirror the infra bundle's target definitions — same workspace hosts, root paths, presets, and permissions.

## Deployment

### Via shared script (recommended)

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

### Standalone (without deploy.sh)

```bash
cd zeroBus/dbxW_zerobus_app
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

> **Warning:** Standalone deployment bypasses the readiness gate. Ensure the infra bundle is deployed, the UC setup job has run, and `client_secret` is provisioned before deploying standalone.

### Workspace UI

1. Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel
2. Click **Deploy** to deploy the bundle
3. Hover over a resource and click **Run** to execute a job or pipeline

### Managing Resources

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
* [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/delta-live-tables/)
