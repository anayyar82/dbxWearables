/**
 * Derive flow-level signals from Pipelines REST `events` for the status UI.
 * Event shapes vary slightly by platform; parsing is defensive.
 */

import { canonicalPublishedTableSuffix } from './pipeline-dataset-aliases';

export type FlowInsight = {
  dataset_name: string;
  status?: string;
  num_output_rows?: number;
  num_upserted_rows?: number;
  event_type?: string;
  time_stamp?: string;
};

export type DatasetDefinitionInsight = {
  dataset_name: string;
  dataset_type?: string;
  num_flows?: number;
  time_stamp?: string;
};

function parseJsonField(raw: unknown): Record<string, unknown> | null {
  if (raw == null) return null;
  if (typeof raw === 'object' && !Array.isArray(raw)) return raw as Record<string, unknown>;
  if (typeof raw === 'string') {
    try {
      const o = JSON.parse(raw) as unknown;
      return o && typeof o === 'object' && !Array.isArray(o) ? (o as Record<string, unknown>) : null;
    } catch {
      return null;
    }
  }
  return null;
}

function num(v: unknown): number | undefined {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  }
  return undefined;
}

function pickDatasetName(ev: Record<string, unknown>, details: Record<string, unknown>): string {
  const a = ev.dataset_name ?? ev.sink_name;
  if (typeof a === 'string' && a.trim()) return a.trim();
  const b = details.output_dataset ?? details.dataset_name ?? details.name;
  if (typeof b === 'string' && b.trim()) return b.trim();
  return '';
}

/** Pipelines events often nest identifiers under `origin` and use `timestamp` vs `time_stamp`. */
function normalizeEvent(ev: Record<string, unknown>): Record<string, unknown> {
  const origin =
    ev.origin && typeof ev.origin === 'object' && !Array.isArray(ev.origin)
      ? (ev.origin as Record<string, unknown>)
      : {};
  const merged: Record<string, unknown> = { ...origin, ...ev };
  if (!merged.time_stamp && merged.timestamp) {
    merged.time_stamp = merged.timestamp;
  }
  return merged;
}

function flowDatasetFromMessage(msg: unknown): string {
  if (typeof msg !== 'string') return '';
  const m = msg.match(/Flow '([^']+)'/);
  return m?.[1]?.trim() ?? '';
}

/** Some payloads nest lifecycle fields under `details.flow_progress`. */
function flattenFlowProgressDetails(details: Record<string, unknown> | null): Record<string, unknown> | null {
  if (!details) return null;
  const nested = details.flow_progress;
  if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
    const fp = nested as Record<string, unknown>;
    return { ...details, ...fp };
  }
  return details;
}

/**
 * Events are usually newest-first from the REST API; we still sort by time_stamp.
 */
export function derivePipelineInsights(events: Array<Record<string, unknown>>): {
  update_progress_state?: string;
  flow_progress: FlowInsight[];
  dataset_definitions: DatasetDefinitionInsight[];
} {
  const normalized = events.map((e) => normalizeEvent(e));
  const sorted = [...normalized].sort((a, b) => {
    const ta = Date.parse(String(a.time_stamp ?? '')) || 0;
    const tb = Date.parse(String(b.time_stamp ?? '')) || 0;
    return tb - ta;
  });

  let updateProgressState: string | undefined;
  const latestFlowByDataset = new Map<string, FlowInsight>();
  const latestDefByDataset = new Map<string, DatasetDefinitionInsight>();

  for (const ev of sorted) {
    const et = String(ev.event_type ?? '');
    const details = parseJsonField(ev.details) ?? parseJsonField(ev.data);

    if (
      et === 'update_progress' &&
      details &&
      typeof details.state === 'string' &&
      updateProgressState === undefined
    ) {
      updateProgressState = details.state;
    }

    if (et === 'flow_progress' || et === 'FLOW_PROGRESS') {
      const flat = flattenFlowProgressDetails(details);
      let dataset = flat ? pickDatasetName(ev, flat) : '';
      if (!dataset) {
        dataset = flowDatasetFromMessage(ev.message);
      }
      if (!dataset) continue;

      const metrics =
        flat && flat.metrics && typeof flat.metrics === 'object'
          ? (flat.metrics as Record<string, unknown>)
          : {};
      const ts =
        typeof ev.time_stamp === 'string'
          ? ev.time_stamp
          : typeof ev.timestamp === 'string'
            ? ev.timestamp
            : undefined;
      const publishedSuffix = canonicalPublishedTableSuffix(dataset);
      const stableKey =
        publishedSuffix.startsWith('01_wearable_') || publishedSuffix.startsWith('01_wearable_vw_')
          ? publishedSuffix
          : dataset;
      const row: FlowInsight = {
        dataset_name: stableKey.startsWith('01_wearable_') || stableKey.startsWith('01_wearable_vw_')
          ? stableKey
          : dataset,
        status: flat && typeof flat.status === 'string' ? flat.status : undefined,
        num_output_rows: num(metrics.num_output_rows),
        num_upserted_rows: num(metrics.num_upserted_rows),
        event_type: et,
        time_stamp: ts,
      };
      if (!latestFlowByDataset.has(stableKey)) {
        latestFlowByDataset.set(stableKey, row);
      }
    }

    if (et === 'dataset_definition' && details) {
      const dataset = pickDatasetName(ev, details);
      if (!dataset) continue;
      const ts =
        typeof ev.time_stamp === 'string'
          ? ev.time_stamp
          : typeof ev.timestamp === 'string'
            ? ev.timestamp
            : undefined;
      const row: DatasetDefinitionInsight = {
        dataset_name: dataset,
        dataset_type: typeof details.dataset_type === 'string' ? details.dataset_type : undefined,
        num_flows: num(details.num_flows),
        time_stamp: ts,
      };
      if (!latestDefByDataset.has(dataset)) {
        latestDefByDataset.set(dataset, row);
      }
    }
  }

  const flow_progress = [...latestFlowByDataset.values()].sort((x, y) =>
    x.dataset_name.localeCompare(y.dataset_name),
  );
  const dataset_definitions = [...latestDefByDataset.values()].sort((x, y) =>
    x.dataset_name.localeCompare(y.dataset_name),
  );

  return { update_progress_state: updateProgressState, flow_progress, dataset_definitions };
}
