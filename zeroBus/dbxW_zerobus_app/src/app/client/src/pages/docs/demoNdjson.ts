/**
 * Demo NDJSON builders for /docs "Try it out".
 * Field names use snake_case to match the iOS HealthKit mappers and DLT silver parsers
 * (get_json_object paths like $.start_date).
 */

export type DemoRecordType = 'samples' | 'workouts' | 'sleep' | 'activity_summaries' | 'deletes';

/** Same stable personas as notebook seed (Lakeview / DLT multi-user charts). */
export const WEB_DEMO_USER_IDS: readonly string[] = [
  'demo.wearables+aurora@dbx.demo',
  'demo.wearables+blake@dbx.demo',
  'demo.wearables+chen@dbx.demo',
  'demo.wearables+dana@dbx.demo',
  'demo.wearables+eli@dbx.demo',
  'demo.wearables+noa@dbx.demo',
];

export function demoUserForLine(i: number, userSlots: number): string | undefined {
  const slots = Math.floor(userSlots);
  if (!Number.isFinite(slots) || slots <= 1) return undefined;
  const j = i % slots;
  if (j < WEB_DEMO_USER_IDS.length) return WEB_DEMO_USER_IDS[j];
  return `demo.rest+synth${j + 1}@dbx.demo`;
}

export function randomUuid(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/** ISO-8601 with Z suffix, no millis (common HealthKit shape). */
export function isoZ(d: Date): string {
  return d.toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function clamp(n: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, n));
}

/** Map index 0..count-1 to a day offset in [0, spanDays-1] spread across the range. */
function dayOffsetForIndex(i: number, count: number, spanDays: number): number {
  if (count <= 1 || spanDays <= 1) return 0;
  return Math.floor((i / (count - 1)) * (spanDays - 1));
}

function atDaysAgo(dayOffset: number, hour: number, minute: number): Date {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - dayOffset);
  d.setUTCHours(hour % 24, minute % 60, 0, 0);
  return d;
}

function addMinutes(base: Date, m: number): Date {
  return new Date(base.getTime() + m * 60_000);
}

export function oneSampleLine(
  i: number,
  spanDays: number,
  count: number,
  demoUserId?: string,
): string {
  const day = dayOffsetForIndex(i, count, spanDays);
  const start = atDaysAgo(day, 8 + (i % 8), (i * 7) % 60);
  const end = addMinutes(start, 15 + (i % 45));
  const types = [
    ['HKQuantityTypeIdentifierStepCount', 'count', 2000 + ((i * 73) % 12000)],
    ['HKQuantityTypeIdentifierAppleExerciseTime', 'min', 5 + (i % 90)],
    ['HKQuantityTypeIdentifierHeartRate', 'count/min', 55 + (i % 45)],
    ['HKQuantityTypeIdentifierDistanceWalkingRunning', 'm', 800 + ((i * 101) % 12000)],
  ] as const;
  const [t, unit, value] = types[i % types.length];
  const tag = demoUserId ? demoUserId.split('@')[0].replace(/[^a-zA-Z0-9._-]+/g, '') : '';
  const row: Record<string, unknown> = {
    uuid: randomUuid(),
    type: t,
    value,
    unit,
    start_date: isoZ(start),
    end_date: isoZ(end),
    source_name: tag ? `dbxWearables-web-docs/${tag}` : 'dbxWearables-web-docs',
    source_bundle_id: 'com.databricks.wearables.demo',
  };
  if (demoUserId) row.demo_user_id = demoUserId;
  return JSON.stringify(row);
}

export function oneWorkoutLine(
  i: number,
  spanDays: number,
  count: number,
  demoUserId?: string,
): string {
  const day = dayOffsetForIndex(i, count, spanDays);
  const start = atDaysAgo(day, 6 + (i % 6), (i * 11) % 60);
  const durMin = 20 + (i % 90);
  const end = addMinutes(start, durMin);
  const activities = [
    'running',
    'walking',
    'cycling',
    'traditionalStrengthTraining',
    'yoga',
    'swimming',
    'hiking',
    'elliptical',
  ];
  const activity = activities[i % activities.length];
  const tag = demoUserId ? demoUserId.split('@')[0].replace(/[^a-zA-Z0-9._-]+/g, '') : '';
  const row: Record<string, unknown> = {
    uuid: randomUuid(),
    activity_type: activity,
    activity_type_raw: 37 + (i % 50),
    start_date: isoZ(start),
    end_date: isoZ(end),
    duration_seconds: durMin * 60,
    total_energy_burned_kcal: 120 + ((i * 17) % 600),
    total_distance_meters: activity === 'cycling' || activity === 'running' ? 3000 + ((i * 99) % 25000) : null,
    source_name: tag ? `dbxWearables-web-docs/${tag}` : 'dbxWearables-web-docs',
  };
  if (demoUserId) row.demo_user_id = demoUserId;
  return JSON.stringify(row);
}

export function oneSleepLine(
  i: number,
  spanDays: number,
  count: number,
  demoUserId?: string,
): string {
  const day = dayOffsetForIndex(i, count, spanDays);
  const sessionStart = atDaysAgo(day, 23, 30);
  const sessionEnd = addMinutes(sessionStart, 7 * 60 + 20);
  const t0 = sessionStart.getTime();
  const stages = [
    {
      uuid: randomUuid(),
      stage: 'asleepCore',
      start_date: isoZ(sessionStart),
      end_date: isoZ(new Date(t0 + 90 * 60_000)),
    },
    {
      uuid: randomUuid(),
      stage: 'asleepDeep',
      start_date: isoZ(new Date(t0 + 90 * 60_000)),
      end_date: isoZ(new Date(t0 + 150 * 60_000)),
    },
    {
      uuid: randomUuid(),
      stage: 'asleepREM',
      start_date: isoZ(new Date(t0 + 150 * 60_000)),
      end_date: isoZ(new Date(t0 + 200 * 60_000)),
    },
    {
      uuid: randomUuid(),
      stage: 'awake',
      start_date: isoZ(new Date(t0 + 200 * 60_000)),
      end_date: isoZ(sessionEnd),
    },
  ];
  const tag = demoUserId ? demoUserId.split('@')[0].replace(/[^a-zA-Z0-9._-]+/g, '') : '';
  const row: Record<string, unknown> = {
    uuid: randomUuid(),
    start_date: isoZ(sessionStart),
    end_date: isoZ(sessionEnd),
    source_name: tag ? `dbxWearables-web-docs/${tag}` : 'dbxWearables-web-docs',
    stages,
  };
  if (demoUserId) row.demo_user_id = demoUserId;
  return JSON.stringify(row);
}

export function oneActivitySummaryLine(
  i: number,
  spanDays: number,
  count: number,
  demoUserId?: string,
): string {
  const day = dayOffsetForIndex(i, count, spanDays);
  const d = atDaysAgo(day, 22, 0);
  const dateStr = d.toISOString().slice(0, 10);
  const tag = demoUserId ? demoUserId.split('@')[0].replace(/[^a-zA-Z0-9._-]+/g, '') : '';
  const row: Record<string, unknown> = {
    date: dateStr,
    active_energy_burned_kcal: 180 + ((i * 41) % 800),
    active_energy_burned_goal_kcal: 600,
    exercise_minutes: 15 + (i % 90),
    exercise_minutes_goal: 30,
    stand_hours: 8 + (i % 4),
    stand_hours_goal: 12,
    source_name: tag ? `dbxWearables-web-docs/${tag}` : 'dbxWearables-web-docs',
  };
  if (demoUserId) row.demo_user_id = demoUserId;
  return JSON.stringify(row);
}

export function oneDeleteLine(i: number, demoUserId?: string): string {
  const row: Record<string, unknown> = {
    uuid: randomUuid(),
    sample_type: i % 2 === 0 ? 'HKQuantityTypeIdentifierStepCount' : 'HKQuantityTypeIdentifierHeartRate',
  };
  if (demoUserId) row.demo_user_id = demoUserId;
  return JSON.stringify(row);
}

/**
 * Build an NDJSON string with `count` lines, spread across the last `spanDays` calendar days.
 * When `demoUserSlots` > 1, each line includes `demo_user_id` (rotating personas) so bronze
 * `user_id` differs per row; the ingest route strips `demo_user_id` from the stored body.
 */
export function generateBatchNdjson(
  recordType: DemoRecordType,
  count: number,
  spanDays: number,
  demoUserSlots = 1,
): string {
  const n = clamp(Math.floor(count), 1, 10_000);
  const span = clamp(Math.floor(spanDays), 1, 730);
  const slots = clamp(Math.floor(demoUserSlots), 1, 48);
  const lines: string[] = [];
  for (let i = 0; i < n; i++) {
    const demoUserId = demoUserForLine(i, slots);
    switch (recordType) {
      case 'samples':
        lines.push(oneSampleLine(i, span, n, demoUserId));
        break;
      case 'workouts':
        lines.push(oneWorkoutLine(i, span, n, demoUserId));
        break;
      case 'sleep':
        lines.push(oneSleepLine(i, span, n, demoUserId));
        break;
      case 'activity_summaries':
        lines.push(oneActivitySummaryLine(i, span, n, demoUserId));
        break;
      case 'deletes':
        lines.push(oneDeleteLine(i, demoUserId));
        break;
      default:
        lines.push(oneSampleLine(i, span, n, demoUserId));
    }
  }
  return lines.join('\n');
}

/** Single-line templates for quick copy / reset. */
export const TEMPLATE_BY_TYPE: Record<DemoRecordType, string> = {
  samples: JSON.stringify(
    {
      uuid: '00000000-0000-4000-8000-000000000001',
      type: 'HKQuantityTypeIdentifierStepCount',
      value: 8432,
      unit: 'count',
      start_date: '2026-01-15T08:00:00Z',
      end_date: '2026-01-15T08:30:00Z',
      source_name: 'com.apple.health',
      source_bundle_id: 'com.apple.Health',
    },
    null,
    2,
  ),
  workouts: JSON.stringify(
    {
      uuid: '00000000-0000-4000-8000-000000000002',
      activity_type: 'running',
      activity_type_raw: 37,
      start_date: '2026-01-15T06:30:00Z',
      end_date: '2026-01-15T07:15:00Z',
      duration_seconds: 2700,
      total_energy_burned_kcal: 412,
      total_distance_meters: 8420,
      source_name: 'Apple Watch',
    },
    null,
    2,
  ),
  sleep: JSON.stringify(
    {
      uuid: '00000000-0000-4000-8000-000000000003',
      start_date: '2026-01-14T23:00:00Z',
      end_date: '2026-01-15T06:30:00Z',
      source_name: 'Apple Watch',
      stages: [
        {
          uuid: '00000000-0000-4000-8000-000000000031',
          stage: 'asleepCore',
          start_date: '2026-01-14T23:15:00Z',
          end_date: '2026-01-15T01:00:00Z',
        },
        {
          uuid: '00000000-0000-4000-8000-000000000032',
          stage: 'asleepDeep',
          start_date: '2026-01-15T01:00:00Z',
          end_date: '2026-01-15T03:00:00Z',
        },
        {
          uuid: '00000000-0000-4000-8000-000000000033',
          stage: 'asleepREM',
          start_date: '2026-01-15T03:00:00Z',
          end_date: '2026-01-15T05:00:00Z',
        },
      ],
    },
    null,
    2,
  ),
  activity_summaries: JSON.stringify(
    {
      date: '2026-01-15',
      active_energy_burned_kcal: 420,
      active_energy_burned_goal_kcal: 600,
      exercise_minutes: 32,
      exercise_minutes_goal: 30,
      stand_hours: 10,
      stand_hours_goal: 12,
      source_name: 'Apple Watch',
    },
    null,
    2,
  ),
  deletes: JSON.stringify(
    {
      uuid: '00000000-0000-4000-8000-000000000099',
      sample_type: 'HKQuantityTypeIdentifierStepCount',
    },
    null,
    2,
  ),
};
