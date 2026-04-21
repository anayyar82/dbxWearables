/**
 * Map internal DLT / append_flow dataset names from pipeline events → published UC table suffixes.
 * Must stay aligned with `src/dlt/wearable_medallion.py` and client `dltStatusModel.ts`.
 */

export const BRONZE_STREAM_PUBLISHED = '01_wearable_bronze_stream';

/** Internal append_flow name in wearable_medallion.py. */
export const BRONZE_DELTA_APPEND_FLOW = 'zerobus_bronze_delta_stream';

/** append_flow suffix → published silver streaming table suffix. */
export const SILVER_APPEND_FLOW_TO_TABLE: Record<string, string> = {
  bronze_stream_into_events_silver: '01_wearable_events_silver',
  bronze_stream_into_hk_samples_silver: '01_wearable_hk_quantity_samples_silver',
  bronze_stream_into_workouts_silver: '01_wearable_workouts_silver',
  bronze_stream_into_sleep_stages_silver: '01_wearable_sleep_stages_silver',
  bronze_stream_into_activity_ring_silver: '01_wearable_activity_ring_daily_silver',
  bronze_stream_into_deletes_silver: '01_wearable_deletes_silver',
};

/** Python @dlt.table function name suffix → published gold / view table suffix. */
export const GOLD_OR_VIEW_FLOW_TO_TABLE: Record<string, string> = {
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

export function datasetSuffix(dataset: string): string {
  const t = dataset.trim();
  const i = t.lastIndexOf('.');
  return i >= 0 ? t.slice(i + 1) : t;
}

/**
 * Normalize event dataset identifiers to the published UC table suffix (`01_…`)
 * so UI `findInsightForTable` and gold card keys match `flow_progress` rows.
 */
export function canonicalPublishedTableSuffix(dataset: string): string {
  const raw = dataset.trim();
  if (!raw) return raw;
  const suf = datasetSuffix(raw);

  if (suf === BRONZE_DELTA_APPEND_FLOW || suf.toLowerCase().includes('zerobus_bronze')) {
    return BRONZE_STREAM_PUBLISHED;
  }
  if (suf.includes(BRONZE_STREAM_PUBLISHED) || suf.toLowerCase().includes('wearable_bronze_stream')) {
    return BRONZE_STREAM_PUBLISHED;
  }

  const silverHit = SILVER_APPEND_FLOW_TO_TABLE[suf];
  if (silverHit) return silverHit;

  const goldHit = GOLD_OR_VIEW_FLOW_TO_TABLE[suf];
  if (goldHit) return goldHit;

  if (suf.startsWith('01_wearable_')) return suf;

  return suf;
}
