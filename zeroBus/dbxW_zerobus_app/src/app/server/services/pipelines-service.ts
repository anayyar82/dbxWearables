/**
 * Thin wrappers around Databricks Pipelines REST API 2.0.
 * https://docs.databricks.com/api/workspace/pipelines
 */

import { workspaceApiConfigured, workspaceFetchJson } from './workspace-api-client';

export type PipelineSummary = {
  pipeline_id: string;
  name?: string;
  state?: string;
  health?: string;
  latest_updates?: Array<{
    update_id?: string;
    state?: string;
    creation_time?: number;
    complete_time?: number;
  }>;
  spec?: unknown;
};

export type PipelineUpdateDetail = {
  update_id?: string;
  state?: string;
  creation_time?: number;
  complete_time?: number;
  progress?: unknown;
  update_details?: unknown;
  config_update?: unknown;
};

export type PipelineEvent = {
  time_stamp?: string;
  timestamp?: string;
  level?: string;
  message?: string;
  event_type?: string;
  details?: unknown;
  data?: unknown;
  dataset_name?: string;
  sink_name?: string;
  origin?: unknown;
};

function configuredOrThrow() {
  if (!workspaceApiConfigured()) {
    throw new Error(
      'Pipelines API unavailable: configure ZEROBUS_WORKSPACE_URL and app DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET',
    );
  }
}

export async function getPipeline(pipelineId: string): Promise<PipelineSummary | null> {
  configuredOrThrow();
  const r = await workspaceFetchJson<PipelineSummary>(
    `/api/2.0/pipelines/${encodeURIComponent(pipelineId)}`,
  );
  if (!r.ok) return null;
  return r.json;
}

export async function listUpdates(
  pipelineId: string,
  maxResults = 15,
): Promise<{ updates?: PipelineUpdateDetail[] } | null> {
  configuredOrThrow();
  const q = new URLSearchParams({ max_results: String(maxResults) });
  const r = await workspaceFetchJson<{ updates?: PipelineUpdateDetail[] }>(
    `/api/2.0/pipelines/${encodeURIComponent(pipelineId)}/updates?${q}`,
  );
  if (!r.ok) return null;
  return r.json;
}

export async function getUpdate(
  pipelineId: string,
  updateId: string,
): Promise<PipelineUpdateDetail | null> {
  configuredOrThrow();
  const r = await workspaceFetchJson<PipelineUpdateDetail>(
    `/api/2.0/pipelines/${encodeURIComponent(pipelineId)}/updates/${encodeURIComponent(updateId)}`,
  );
  if (!r.ok) return null;
  return r.json;
}

export async function triggerUpdate(pipelineId: string): Promise<{ update_id?: string } | null> {
  configuredOrThrow();
  const r = await workspaceFetchJson<{ update_id?: string; message?: string; error_code?: string }>(
    `/api/2.0/pipelines/${encodeURIComponent(pipelineId)}/updates`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    },
  );
  if (!r.ok) {
    const msg = r.json?.message || r.text || `HTTP ${r.status}`;
    throw new Error(msg);
  }
  const j = r.json as { update_id?: string; id?: string } | null;
  if (!j) return {};
  if (!j.update_id && typeof j.id === 'string') {
    return { ...j, update_id: j.id };
  }
  return j;
}

export async function listEvents(
  pipelineId: string,
  maxResults = 100,
  pageToken?: string,
): Promise<{ events?: PipelineEvent[]; next_page_token?: string } | null> {
  configuredOrThrow();
  const q = new URLSearchParams({ max_results: String(maxResults) });
  if (pageToken) q.set('page_token', pageToken);
  const r = await workspaceFetchJson<{ events?: PipelineEvent[]; next_page_token?: string }>(
    `/api/2.0/pipelines/${encodeURIComponent(pipelineId)}/events?${q}`,
  );
  if (!r.ok) return null;
  return r.json;
}

/**
 * Pull several pages of pipeline events so older `flow_progress` rows (e.g. gold MVs)
 * are not dropped when the tail of the log is noisy.
 */
export async function listEventsPaged(
  pipelineId: string,
  pageSize = 400,
  maxPages = 10,
): Promise<PipelineEvent[]> {
  configuredOrThrow();
  const out: PipelineEvent[] = [];
  let token: string | undefined;
  for (let page = 0; page < maxPages; page++) {
    const batch = await listEvents(pipelineId, pageSize, token);
    if (!batch?.events?.length) break;
    out.push(...batch.events);
    token = batch.next_page_token;
    if (!token) break;
  }
  return out;
}

/** Bundle substitution into Apps env sometimes leaves `${...}` literals — treat as unset. */
function sanitizePipelineEnvValue(raw: string | undefined): string | null {
  const v = raw?.trim();
  if (!v || v.startsWith('${')) return null;
  return v;
}

type ListPipelinesPage = {
  statuses?: unknown[];
  pipelines?: unknown[];
  results?: unknown[];
  next_page_token?: string;
};

const PIPELINE_UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function isPipelineUuidShape(s: string): boolean {
  return PIPELINE_UUID_RE.test(s.trim());
}

function pipelineIdFromRow(row: Record<string, unknown>): string | null {
  const id = row.pipeline_id ?? row.id;
  return typeof id === 'string' && id.trim() ? id.trim() : null;
}

function displayNameFromRow(row: Record<string, unknown>): string | null {
  if (typeof row.name === 'string' && row.name.trim()) return row.name.trim();
  const spec = row.spec;
  if (spec && typeof spec === 'object' && spec !== null && 'name' in spec) {
    const n = (spec as { name?: unknown }).name;
    if (typeof n === 'string' && n.trim()) return n.trim();
  }
  return null;
}

function listPageRows(j: ListPipelinesPage | null): Record<string, unknown>[] {
  const raw = j?.statuses ?? j?.pipelines ?? j?.results ?? [];
  const out: Record<string, unknown>[] = [];
  for (const x of raw) {
    if (x && typeof x === 'object') out.push(x as Record<string, unknown>);
  }
  return out;
}

function idFromPipelineSummary(p: PipelineSummary | null): string | null {
  if (!p) return null;
  if (typeof p.pipeline_id === 'string' && p.pipeline_id.trim()) return p.pipeline_id.trim();
  const anyp = p as { id?: string };
  if (typeof anyp.id === 'string' && anyp.id.trim()) return anyp.id.trim();
  return null;
}

/**
 * Resolve a pipeline UUID by display `name` (see bundle `wearable_medallion.pipeline.yml`).
 * Used when WEARABLE_PIPELINE_ID is not injected from the bundle into the App env.
 */
export async function findPipelineIdByName(exactName: string): Promise<string | null> {
  configuredOrThrow();
  const want = exactName.trim();
  if (!want) return null;
  const wantLc = want.toLowerCase();

  let pageToken: string | undefined;
  for (let guard = 0; guard < 25; guard++) {
    const q = new URLSearchParams({ max_results: '100' });
    if (pageToken) q.set('page_token', pageToken);
    const r = await workspaceFetchJson<ListPipelinesPage>(`/api/2.0/pipelines?${q}`);
    if (!r.ok) {
      console.warn(
        `[pipelines] list pipelines failed HTTP ${r.status}: ${(r.text || '').slice(0, 240)}`,
      );
      return null;
    }
    const j = r.json;
    for (const row of listPageRows(j)) {
      const display = displayNameFromRow(row);
      const pid = pipelineIdFromRow(row);
      if (pid && display && display.toLowerCase() === wantLc) {
        return pid;
      }
    }
    pageToken = j?.next_page_token;
    if (!pageToken) break;
  }
  return null;
}

let resolvedCache: { name: string; id: string | null } | null = null;

export function listConfiguredPipelineIds(): Array<{
  key: string;
  label: string;
  id: string;
}> {
  const primary =
    sanitizePipelineEnvValue(process.env.WEARABLE_PIPELINE_ID) ||
    sanitizePipelineEnvValue(process.env.WEARABLE_PIPELINE_BATCH_ID) ||
    sanitizePipelineEnvValue(process.env.WEARABLE_MEDALLION_BATCH_PIPELINE_ID) ||
    sanitizePipelineEnvValue(process.env.WEARABLE_MEDALLION_PIPELINE_ID);

  if (!primary) {
    return [];
  }
  return [
    {
      key: 'medallion',
      label: process.env.WEARABLE_PIPELINE_LABEL?.trim() || 'Wearable medallion (DLT)',
      id: primary,
    },
  ];
}

/** Same UUID as bundle env / legacy aliases — sync only (no name lookup). */
export function primaryPipelineIdOrNull(): string | null {
  const rows = listConfiguredPipelineIds();
  return rows[0]?.id ?? null;
}

/** Prefer explicit UUID env vars; otherwise resolve WEARABLE_PIPELINE_NAME via Pipelines list API. */
export async function primaryPipelineIdResolved(): Promise<string | null> {
  const direct = primaryPipelineIdOrNull();
  if (direct) return direct;

  const name = sanitizePipelineEnvValue(process.env.WEARABLE_PIPELINE_NAME);
  if (!name || !workspaceApiConfigured()) return null;

  if (resolvedCache?.name === name) {
    return resolvedCache.id;
  }

  let id: string | null = null;
  if (isPipelineUuidShape(name)) {
    const p = await getPipeline(name);
    id = idFromPipelineSummary(p);
  }
  if (!id) {
    id = await findPipelineIdByName(name);
  }
  resolvedCache = { name, id };
  if (!id) {
    console.warn(
      `[pipelines] WEARABLE_PIPELINE_NAME="${name}" did not resolve to a pipeline_id (set WEARABLE_PIPELINE_ID, or use exact pipeline name / UUID + app SPN CAN VIEW on the pipeline).`,
    );
  }
  return id;
}

/** For /api/pipelines/config — includes pipeline id after name resolution when needed. */
export async function listConfiguredPipelineIdsResolved(): Promise<
  Array<{
    key: string;
    label: string;
    id: string;
  }>
> {
  const id = await primaryPipelineIdResolved();
  if (!id) return [];
  return [
    {
      key: 'medallion',
      label: process.env.WEARABLE_PIPELINE_LABEL?.trim() || 'Wearable medallion (DLT)',
      id,
    },
  ];
}
