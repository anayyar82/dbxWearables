/** Medallion names aligned with `wearable_medallion.py` (UC `01_` prefix). */

export const BRONZE_STREAM_TABLE = '01_wearable_bronze_stream';

/** Internal append_flow name in wearable_medallion.py (matches event log Flow '…' names). */
export const BRONZE_DELTA_APPEND_FLOW = 'zerobus_bronze_delta_stream';

/** Map append_flow dataset suffix → published UC streaming table suffix. */
export const SILVER_APPEND_FLOW_TO_TABLE: Record<string, string> = {
  bronze_stream_into_events_silver: '01_wearable_events_silver',
  bronze_stream_into_hk_samples_silver: '01_wearable_hk_quantity_samples_silver',
  bronze_stream_into_workouts_silver: '01_wearable_workouts_silver',
  bronze_stream_into_sleep_stages_silver: '01_wearable_sleep_stages_silver',
  bronze_stream_into_activity_ring_silver: '01_wearable_activity_ring_daily_silver',
  bronze_stream_into_deletes_silver: '01_wearable_deletes_silver',
};

export const SILVER_STREAMING_TABLES = [
  '01_wearable_events_silver',
  '01_wearable_hk_quantity_samples_silver',
  '01_wearable_workouts_silver',
  '01_wearable_sleep_stages_silver',
  '01_wearable_activity_ring_daily_silver',
  '01_wearable_deletes_silver',
] as const;

export const GOLD_MATERIALIZED_TABLES = [
  '01_wearable_hk_quantity_daily_gold',
  '01_wearable_subject_daily_gold',
  '01_wearable_gold_daily_steps',
  '01_wearable_gold_activity_enriched_daily',
  '01_wearable_gold_sleep_nightly',
  '01_wearable_gold_weekly_workout_summary',
  '01_wearable_gold_cardio_vitals_daily',
  '01_wearable_gold_heart_rate_intraday_daily',
  '01_wearable_gold_bronze_ingest_daily',
  '01_wearable_gold_hk_family_weekly',
] as const;

export const GOLD_VIEWS = [
  '01_wearable_vw_gold_activity_last_45d',
  '01_wearable_vw_gold_sleep_last_30_sessions',
  '01_wearable_vw_gold_workouts_recent',
] as const;

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

export type PipelineInsightsPayload = {
  update_progress_state?: string;
  flow_progress: FlowInsight[];
  dataset_definitions: DatasetDefinitionInsight[];
};

export function tableSuffix(fqn: string): string {
  const parts = fqn.split('.');
  return parts[parts.length - 1] ?? fqn;
}

/** Python @dlt.table function suffix → published gold / view name (when events use function names). */
const GOLD_FLOW_FN_TO_PUBLISHED: Record<string, string> = {
  wearable_hk_quantity_daily_gold: '01_wearable_hk_quantity_daily_gold',
  wearable_subject_daily_gold: '01_wearable_subject_daily_gold',
  wearable_gold_daily_steps: '01_wearable_gold_daily_steps',
  wearable_gold_activity_enriched_daily: '01_wearable_gold_activity_enriched_daily',
  wearable_gold_sleep_nightly: '01_wearable_gold_sleep_nightly',
  wearable_gold_weekly_workout_summary: '01_wearable_gold_weekly_workout_summary',
  wearable_gold_cardio_vitals_daily: '01_wearable_gold_cardio_vitals_daily',
  wearable_gold_heart_rate_intraday_daily: '01_wearable_gold_heart_rate_intraday_daily',
  wearable_gold_bronze_ingest_daily: '01_wearable_gold_bronze_ingest_daily',
  wearable_gold_hk_family_weekly: '01_wearable_gold_hk_family_weekly',
  wearable_vw_gold_activity_last_45d: '01_wearable_vw_gold_activity_last_45d',
  wearable_vw_gold_sleep_last_30_sessions: '01_wearable_vw_gold_sleep_last_30_sessions',
  wearable_vw_gold_workouts_recent: '01_wearable_vw_gold_workouts_recent',
};

export function findInsightForTable(
  shortName: string,
  flows: FlowInsight[],
): FlowInsight | undefined {
  return flows.find(
    (f) =>
      tableSuffix(f.dataset_name) === shortName ||
      f.dataset_name.endsWith(`.${shortName}`) ||
      f.dataset_name.includes(shortName),
  );
}

/** Gold / presentation views — match `01_…` names or internal Python flow names from `flow_progress`. */
export function findInsightForGoldTable(shortName: string, flows: FlowInsight[]): FlowInsight | undefined {
  const direct = findInsightForTable(shortName, flows);
  if (direct) return direct;
  for (const [fn, pub] of Object.entries(GOLD_FLOW_FN_TO_PUBLISHED)) {
    if (pub !== shortName) continue;
    const hit = flows.find(
      (f) =>
        tableSuffix(f.dataset_name) === fn ||
        f.dataset_name.endsWith(`.${fn}`) ||
        f.dataset_name.includes(`.${fn}`),
    );
    if (hit) return hit;
  }
  return undefined;
}

/** Match Pipelines events that use internal flow names (append_flow) to published UC table names. */
export function findInsightForPublishedTable(
  publishedShortName: string,
  flows: FlowInsight[],
): FlowInsight | undefined {
  const direct = findInsightForTable(publishedShortName, flows);
  if (direct) return direct;

  if (publishedShortName === BRONZE_STREAM_TABLE) {
    const byFlow = flows.find(
      (f) =>
        tableSuffix(f.dataset_name) === BRONZE_DELTA_APPEND_FLOW ||
        f.dataset_name.endsWith(`.${BRONZE_DELTA_APPEND_FLOW}`) ||
        f.dataset_name.toLowerCase().includes('zerobus_bronze') ||
        f.dataset_name.includes(BRONZE_STREAM_TABLE) ||
        f.dataset_name.toLowerCase().includes('wearable_bronze_stream'),
    );
    if (byFlow) return byFlow;
  }

  for (const [flowSuffix, pub] of Object.entries(SILVER_APPEND_FLOW_TO_TABLE)) {
    if (pub !== publishedShortName) continue;
    const hit = flows.find(
      (f) =>
        tableSuffix(f.dataset_name) === flowSuffix || f.dataset_name.endsWith(`.${flowSuffix}`),
    );
    if (hit) return hit;
  }

  return undefined;
}

export function formatRelativeTime(ms: number | null | undefined): string {
  if (ms == null) return '—';
  const s = Math.max(0, Math.floor((Date.now() - ms) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

export function formatInt(n: number | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return n.toLocaleString();
}
