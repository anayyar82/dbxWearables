import { analytics, createApp, lakebase, server } from '@databricks/appkit';
import { setupSampleLakebaseRoutes } from './routes/lakebase/todo-routes';
import { setupPipelineRoutes } from './routes/pipelines/pipeline-routes';
import { setupZeroBusRoutes } from './routes/zerobus/ingest-routes';

const hasSqlWarehouse = Boolean(process.env.DATABRICKS_WAREHOUSE_ID?.trim());

const plugins = [server({ autoStart: false }), lakebase()];

if (hasSqlWarehouse) {
  // Warehouse ID is read from DATABRICKS_WAREHOUSE_ID (see appkit.plugins.json / app.yaml valueFrom).
  plugins.push(analytics({}));
  console.log('[appkit] Analytics plugin enabled (DATABRICKS_WAREHOUSE_ID is set)');
} else {
  console.warn('[appkit] DATABRICKS_WAREHOUSE_ID not set — Insights SQL routes are disabled (local dev)');
}

createApp({
  plugins,
  // Lakebase often cannot create the AppKit `appkit` cache schema with CAN_CONNECT_AND_CREATE;
  // disabling global cache avoids migration failures while keeping Lakebase for app queries.
  cache: {
    enabled: false,
  },
})
  .then(async (appkit) => {
    // Lakebase CRUD routes (sample scaffold)
    await setupSampleLakebaseRoutes(appkit);

    // ZeroBus HealthKit ingestion routes
    await setupZeroBusRoutes(appkit);

    setupPipelineRoutes(appkit);

    await appkit.server.start();
  })
  .catch(console.error);
