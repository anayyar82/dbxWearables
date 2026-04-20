# Lakeview dashboard — wearables / Apple Health demo

This folder holds **SQL queries** you attach to a **Databricks SQL / Lakeview** dashboard. Defaults use `users.ankur_nayyar`; change the catalog/schema in each file if needed.

## Recommended layout (shows “real” Databricks value)

| Tile | Query file | Why it matters |
|------|----------------|----------------|
| KPI | `08_bronze_freshness_latency.sql` | Data freshness / SLA |
| KPI | `06_silver_gold_volumes.sql` (sum or top row) | Lakeflow / DLT materialization footprint |
| Trend | `01_bronze_ingest_overview.sql` | Ingest volume over time |
| Trend | `02_hk_quantity_daily_gold.sql` | Curated analytics on gold |
| Table | `03_recent_workouts.sql` | Drill-down |
| Bar / area | `04_sleep_stage_minutes.sql` | Stage mix |
| Combo | `05_activity_ring_adherence.sql` | Goals vs actuals |
| Table | `07_bronze_demo_seed_footprint.sql` | Isolate notebook-seeded demo data |

## Build steps (Lakeview)

1. **SQL** workspace → paste a query from `*.sql` → run.
2. **Save** → add to **Dashboard** (create new dashboard on first save).
3. For each visualization: pick **visualization type** (counter, line, bar, table), set **x / y** fields, and enable **auto-refresh** if your workspace allows it.
4. **Parameters (optional):** create dashboard parameters `catalog`, `schema` and replace literals in SQL with `{{catalog}}` / `{{schema}}` once the dashboard supports them.

## Platform features to mention in a demo

- **Unity Catalog** — governed tables (`users.ankur_nayyar.*`), ACLs, audit.
- **Delta + VARIANT** — flexible bronze, structured silver/gold via DLT.
- **Lakeflow Declarative Pipelines** — tested expectations, incremental runs, lineage in UI.
- **Query tags** — set in jobs/notebooks (`SET QUERY_TAGS['project']=...`) for chargeback; filter in **system.query.history** / Lakehouse Monitoring where available.
- **Predictive optimization** — keep enabled on the bronze schema for liquid-clustered tables (see infra UC setup docs).

## Seed data

Run the bundled notebook `zeroBus/dbxW_zerobus_app/src/notebooks/seed_wearables_bronze_demo.ipynb` to append (or replace) demo rows tagged with `demo-notebook-seed`, then refresh the DLT pipeline.
