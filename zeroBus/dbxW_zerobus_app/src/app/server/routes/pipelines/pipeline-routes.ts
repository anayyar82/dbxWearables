import type { Application, Request, Response } from 'express';
import {
  getPipeline,
  getUpdate,
  listConfiguredPipelineIdsResolved,
  listEventsPaged,
  listUpdates,
  triggerUpdate,
} from '../../services/pipelines-service';
import { derivePipelineInsights } from '../../services/pipeline-insights';
import { workspaceApiConfigured, workspaceOrigin } from '../../services/workspace-api-client';

interface AppKitWithServer {
  server: {
    extend: (fn: (app: Application) => void) => void;
  };
}

export function setupPipelineRoutes(appkit: AppKitWithServer) {
  appkit.server.extend((app) => {
    app.get('/api/pipelines/config', async (_req: Request, res: Response) => {
      const pipelines = await listConfiguredPipelineIdsResolved();
      res.json({
        workspace_api_configured: workspaceApiConfigured(),
        workspace_origin: workspaceOrigin(),
        pipelines,
        wearable_pipeline_name: process.env.WEARABLE_PIPELINE_NAME?.trim() ?? null,
      });
    });

    /** Poll a single pipeline update (state, progress) — used after ingest trigger + /docs UI. */
    app.get('/api/pipelines/:pipelineId/updates/:updateId', async (req: Request, res: Response) => {
      try {
        if (!workspaceApiConfigured()) {
          res.status(503).json({ error: 'Workspace API not configured' });
          return;
        }
        const { pipelineId, updateId } = req.params;
        const update = await getUpdate(pipelineId, updateId);
        if (!update) {
          res.status(404).json({ error: 'Update not found' });
          return;
        }
        res.json({ update });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error('[pipelines] get-update error:', message);
        res.status(500).json({ error: message });
      }
    });

    app.get('/api/pipelines/:pipelineId/status', async (req: Request, res: Response) => {
      try {
        const { pipelineId } = req.params;
        if (!workspaceApiConfigured()) {
          res.status(503).json({
            error: 'Workspace API not configured',
            hint: 'Requires ZEROBUS_WORKSPACE_URL and DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET; grant the app SPN access to pipelines.',
          });
          return;
        }

        const [pipeline, updates, events] = await Promise.all([
          getPipeline(pipelineId),
          listUpdates(pipelineId, 20),
          listEventsPaged(pipelineId, 400, 10),
        ]);

        let latestUpdateDetail = null as Awaited<ReturnType<typeof getUpdate>>;
        const latestId =
          updates?.updates?.[0]?.update_id ?? pipeline?.latest_updates?.[0]?.update_id;
        if (latestId) {
          latestUpdateDetail = await getUpdate(pipelineId, latestId);
        }

        const evList = Array.isArray(events) ? events : [];
        const insights = derivePipelineInsights(evList as Array<Record<string, unknown>>);

        res.json({
          pipeline,
          updates: updates?.updates ?? [],
          latest_update: latestUpdateDetail,
          events: evList,
          insights,
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error('[pipelines] status error:', message);
        res.status(500).json({ error: message });
      }
    });

    app.post('/api/pipelines/:pipelineId/trigger', async (req: Request, res: Response) => {
      try {
        if (!workspaceApiConfigured()) {
          res.status(503).json({ error: 'Workspace API not configured' });
          return;
        }
        const { pipelineId } = req.params;
        const body = await triggerUpdate(pipelineId);
        res.status(200).json({ status: 'started', ...body });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error('[pipelines] trigger error:', message);
        res.status(500).json({ error: message });
      }
    });
  });
}
