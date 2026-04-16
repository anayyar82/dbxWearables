# ZeroBus

Databricks-side ingestion infrastructure and application for the **dbxWearables** project. This folder hosts all ZeroBus-related bundles, deployment scripts, and configuration for streaming wearable health data into Unity Catalog.

## Architecture

```
zeroBus/
├── deploy.sh                  # Shared deployment script (infra-first ordering)
├── dbxW_zerobus_infra/        # Infrastructure bundle (schemas, secrets, SPN, warehouse, DDL)
└── dbxW_zerobus/              # Application bundle (AppKit app, pipelines) — coming soon
```

## Bundles

| Bundle | Purpose | Status |
| --- | --- | --- |
| [`dbxW_zerobus_infra`](dbxW_zerobus_infra/README.md) | Shared infrastructure — UC schema, secret scope, SQL warehouse, service principal, bronze table DDL | Active |
| `dbxW_zerobus` | Application — AppKit REST API, ZeroBus SDK consumer, Spark Declarative Pipelines | Planned |

Infrastructure must be deployed **before** the application bundle. Use `deploy.sh` to handle ordering automatically:

```bash
./deploy.sh --target dev            # deploys infra first, then app
./deploy.sh --target dev --infra    # deploy only infra bundle
```

## Documentation

* [dbxWearables project README](../README.md)
* [Infrastructure bundle README](dbxW_zerobus_infra/README.md)
* [ZeroBus Ingest overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview/)
* [ZeroBus Ingest connector](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest/)
