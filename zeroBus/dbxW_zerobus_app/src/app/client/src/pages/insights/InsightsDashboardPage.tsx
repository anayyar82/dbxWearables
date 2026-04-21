import { useCallback, useMemo, useState, type ComponentType } from 'react';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Activity,
  BarChart2,
  CalendarRange,
  Database,
  Filter,
  Footprints,
  HeartPulse,
  RotateCcw,
  Search,
  Users,
} from 'lucide-react';
import { InteractiveSeriesChart } from '@/components/insights/InteractiveSeriesChart';

type KpiRow = {
  bronze_rows_in_window: number;
  bronze_rows_all_time: number;
  active_users_in_window: number;
  users_with_steps_in_window: number;
  last_bronze_ingest_in_window: string | null;
};

type TrendRow = {
  day: string;
  active_users: number;
  avg_move_ratio: number;
  avg_steps: number;
};

type LeaderRow = {
  user_id: string;
  avg_steps: number;
  peak_steps: number;
  sum_steps: number;
};

type IngestRow = {
  record_type: string;
  row_count: number;
};

type SleepRow = {
  user_id: string;
  avg_deep_min: number;
  avg_rem_min: number;
  avg_tracked_min: number;
  nights: number;
};

type BronzeDailyRow = {
  ingest_day: string;
  row_count: number;
};

type VitalsRow = {
  day: string;
  avg_resting_hr: number;
  users_reporting: number;
};

type WorkoutRow = {
  user_id: string;
  workouts: number;
  total_minutes: number;
  total_kcal: number;
};

const RECORD_TYPES = ['ALL', 'samples', 'workouts', 'sleep', 'activity_summaries', 'deletes'] as const;
type RecordTypeFilter = (typeof RECORD_TYPES)[number];

function toYmd(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function defaultRange(days: number): { start: string; end: string } {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - days);
  return { start: toYmd(start), end: toYmd(end) };
}

function normalizeRange(start: string, end: string): { start: string; end: string } {
  if (start <= end) return { start, end };
  return { start: end, end: start };
}

function formatNumber(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return '—';
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(n);
}

function formatTs(s: string | null | undefined): string {
  if (!s) return '—';
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? s : d.toLocaleString();
}

function KpiCard({
  label,
  value,
  hint,
  icon: Icon,
}: {
  label: string;
  value: string;
  hint?: string;
  icon: ComponentType<{ className?: string }>;
}) {
  return (
    <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-5 shadow-lg shadow-black/10 transition hover:border-[var(--dbx-navy-500)]/25 dark:hover:border-white/15">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-xs font-medium uppercase tracking-wider text-[var(--muted-foreground)]">{label}</p>
          <p className="mt-2 text-3xl font-bold tabular-nums text-[var(--foreground)] tracking-tight break-all">{value}</p>
          {hint ? <p className="mt-1 text-xs text-[var(--muted-foreground)] leading-snug">{hint}</p> : null}
        </div>
        <div className="rounded-lg bg-[var(--muted)] p-2.5 text-[var(--dbx-lava-600)] dark:text-[var(--dbx-lava-400)] shrink-0">
          <Icon className="h-5 w-5" />
        </div>
      </div>
    </div>
  );
}

function Section({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="mb-12">
      <div className="mb-5 flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 className="text-lg font-bold text-[var(--foreground)]">{title}</h2>
          {subtitle ? (
            <p className="text-sm text-[var(--muted-foreground)] mt-1 max-w-3xl leading-relaxed">{subtitle}</p>
          ) : null}
        </div>
      </div>
      {children}
    </section>
  );
}

function QueryState({ loading, error }: { loading: boolean; error: string | null }) {
  if (loading) {
    return (
      <div className="animate-pulse rounded-xl border border-[var(--border)] bg-[var(--muted)] h-32 flex items-center justify-center text-sm text-[var(--muted-foreground)]">
        Loading lakehouse metrics…
      </div>
    );
  }
  if (error) {
    return (
      <div className="rounded-xl border border-red-500/30 bg-red-950/20 px-4 py-3 text-sm text-red-200">
        <p className="font-medium">Unable to load SQL results</p>
        <p className="mt-1 text-red-300/80">{error}</p>
        <p className="mt-2 text-xs text-red-200/70">
          Confirm the app service principal has <span className="font-mono">CAN USE</span> on the SQL warehouse and{' '}
          <span className="font-mono">SELECT</span> on UC gold tables. After seeding bronze, run the medallion refresh
          job so gold is populated.
        </p>
      </div>
    );
  }
  return null;
}

function inputClass() {
  return [
    'w-full rounded-lg border border-[var(--border)] bg-[var(--background)] px-3 py-2 text-sm text-[var(--foreground)]',
    'placeholder:text-[var(--muted-foreground)] focus:outline-none focus:ring-2 focus:ring-[var(--dbx-lava-500)]/40 focus:border-[var(--dbx-lava-500)]/50',
    'dark:bg-[var(--card)]',
  ].join(' ');
}

export function InsightsDashboardPage() {
  const range30 = useMemo(() => defaultRange(30), []);
  const [draftStart, setDraftStart] = useState(range30.start);
  const [draftEnd, setDraftEnd] = useState(range30.end);
  const [draftUserSearch, setDraftUserSearch] = useState('');
  const [draftRecordType, setDraftRecordType] = useState<RecordTypeFilter>('ALL');

  const [appliedStart, setAppliedStart] = useState(range30.start);
  const [appliedEnd, setAppliedEnd] = useState(range30.end);
  const [appliedUserPattern, setAppliedUserPattern] = useState('%');
  const [appliedRecordType, setAppliedRecordType] = useState<RecordTypeFilter>('ALL');
  const [ingestHoverType, setIngestHoverType] = useState<string | null>(null);

  const applyFilters = useCallback(() => {
    const { start, end } = normalizeRange(draftStart, draftEnd);
    setAppliedStart(start);
    setAppliedEnd(end);
    const q = draftUserSearch.trim();
    setAppliedUserPattern(q === '' ? '%' : `%${q}%`);
    setAppliedRecordType(draftRecordType);
  }, [draftStart, draftEnd, draftUserSearch, draftRecordType]);

  const resetFilters = useCallback(() => {
    const r = defaultRange(30);
    setDraftStart(r.start);
    setDraftEnd(r.end);
    setDraftUserSearch('');
    setDraftRecordType('ALL');
    setAppliedStart(r.start);
    setAppliedEnd(r.end);
    setAppliedUserPattern('%');
    setAppliedRecordType('ALL');
  }, []);

  const preset = useCallback((days: number) => {
    const r = defaultRange(days);
    setDraftStart(r.start);
    setDraftEnd(r.end);
    setAppliedStart(r.start);
    setAppliedEnd(r.end);
    const q = draftUserSearch.trim();
    setAppliedUserPattern(q === '' ? '%' : `%${q}%`);
    setAppliedRecordType(draftRecordType);
  }, [draftUserSearch, draftRecordType]);

  const dateUserParams = useMemo(
    () => ({
      startDate: sql.date(appliedStart),
      endDate: sql.date(appliedEnd),
      userPattern: sql.string(appliedUserPattern),
    }),
    [appliedStart, appliedEnd, appliedUserPattern],
  );

  const ingestParams = useMemo(
    () => ({
      ...dateUserParams,
      recordType: sql.string(appliedRecordType),
    }),
    [dateUserParams, appliedRecordType],
  );

  const kpis = useAnalyticsQuery('wearables_dashboard_kpis', dateUserParams) as {
    data: KpiRow[] | null;
    loading: boolean;
    error: string | null;
  };

  const trend = useAnalyticsQuery('wearables_activity_trend', dateUserParams) as {
    data: TrendRow[] | null;
    loading: boolean;
    error: string | null;
  };

  const leaders = useAnalyticsQuery('wearables_user_steps_leaderboard', dateUserParams) as {
    data: LeaderRow[] | null;
    loading: boolean;
    error: string | null;
  };

  const ingest = useAnalyticsQuery('wearables_bronze_ingest_mix', ingestParams) as {
    data: IngestRow[] | null;
    loading: boolean;
    error: string | null;
  };

  const sleep = useAnalyticsQuery('wearables_sleep_summary', dateUserParams) as {
    data: SleepRow[] | null;
    loading: boolean;
    error: string | null;
  };

  const bronzeDaily = useAnalyticsQuery('wearables_bronze_ingest_daily_trend', dateUserParams) as {
    data: BronzeDailyRow[] | null;
    loading: boolean;
    error: string | null;
  };

  const vitals = useAnalyticsQuery('wearables_vitals_rhr_trend', dateUserParams) as {
    data: VitalsRow[] | null;
    loading: boolean;
    error: string | null;
  };

  const workouts = useAnalyticsQuery('wearables_workout_volume_by_user', dateUserParams) as {
    data: WorkoutRow[] | null;
    loading: boolean;
    error: string | null;
  };

  const kpiRow = kpis.data?.[0];

  const movePoints = useMemo(
    () =>
      (trend.data ?? [])
        .map((r) => ({
          xLabel: r.day,
          y: Number(r.avg_move_ratio),
          tooltipExtra: [
            { label: 'Avg steps', value: formatNumber(r.avg_steps) },
            { label: 'Active users', value: formatNumber(r.active_users) },
          ],
        }))
        .filter((p) => Number.isFinite(p.y)),
    [trend.data],
  );

  const stepsPoints = useMemo(
    () =>
      (trend.data ?? [])
        .map((r) => ({
          xLabel: r.day,
          y: Number(r.avg_steps),
          tooltipExtra: [
            {
              label: 'Move ratio',
              value: Number.isFinite(Number(r.avg_move_ratio)) ? Number(r.avg_move_ratio).toFixed(3) : '—',
            },
            { label: 'Active users', value: formatNumber(r.active_users) },
          ],
        }))
        .filter((p) => Number.isFinite(p.y)),
    [trend.data],
  );

  const bronzeVolPoints = useMemo(
    () =>
      (bronzeDaily.data ?? [])
        .map((r) => ({
          xLabel: r.ingest_day,
          y: Number(r.row_count),
          tooltipExtra: [{ label: 'Bronze rows', value: formatNumber(Number(r.row_count)) }],
        }))
        .filter((p) => Number.isFinite(p.y)),
    [bronzeDaily.data],
  );

  const rhrPoints = useMemo(
    () =>
      (vitals.data ?? [])
        .map((r) => ({
          xLabel: r.day,
          y: Number(r.avg_resting_hr),
          tooltipExtra: [{ label: 'Users reporting', value: formatNumber(r.users_reporting) }],
        }))
        .filter((p) => Number.isFinite(p.y)),
    [vitals.data],
  );

  const maxIngest = Math.max(0, ...(ingest.data ?? []).map((r) => Number(r.row_count)));

  const rangeLabel = `${appliedStart} → ${appliedEnd}`;

  return (
    <div className="min-h-screen bg-[var(--background)]">
      <section className="gradient-hero text-white border-b border-white/10 py-16 px-6 relative overflow-hidden">
        <div className="absolute inset-0 opacity-[0.04] pointer-events-none">
          <img
            src="/images/databricks-symbol-light.svg"
            alt=""
            aria-hidden
            className="absolute -top-16 -right-16 w-80 h-80 rotate-12 opacity-90"
          />
        </div>
        <div className="max-w-7xl mx-auto relative z-10">
          <p className="text-xs font-bold uppercase tracking-widest text-[var(--dbx-lava-400)] mb-2">Lakehouse analytics</p>
          <h1 className="text-4xl md:text-5xl font-bold tracking-tight">Wearables insights</h1>
          <p className="mt-4 text-lg text-gray-300 max-w-2xl leading-relaxed">
            Filterable KPIs and trends from Unity Catalog gold tables via Databricks SQL. Adjust the window, narrow by
            user id substring, and slice ingest by record type — then apply to refresh every panel together.
          </p>
        </div>
      </section>

      <div className="sticky top-0 z-20 border-b border-[var(--border)] bg-[var(--background)]/95 backdrop-blur-md">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="rounded-2xl border border-[var(--border)] bg-[var(--card)] p-4 shadow-lg shadow-black/10">
            <div className="flex flex-wrap items-center gap-2 mb-4">
              <div className="inline-flex items-center gap-2 text-xs font-bold uppercase tracking-wider text-[var(--muted-foreground)]">
                <Filter className="h-4 w-4 text-[var(--dbx-lava-600)] dark:text-[var(--dbx-lava-400)]" />
                Filters
              </div>
              <span className="text-xs text-[var(--muted-foreground)] ml-auto sm:ml-0">Active window: {rangeLabel}</span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-12 gap-3 items-end">
              <div className="xl:col-span-2">
                <label className="block text-xs text-[var(--muted-foreground)] mb-1">Start date</label>
                <div className="relative">
                  <CalendarRange className="absolute left-2.5 top-2.5 h-4 w-4 text-[var(--muted-foreground)] pointer-events-none" />
                  <input
                    type="date"
                    className={`${inputClass()} pl-9`}
                    value={draftStart}
                    onChange={(e) => setDraftStart(e.target.value)}
                  />
                </div>
              </div>
              <div className="xl:col-span-2">
                <label className="block text-xs text-[var(--muted-foreground)] mb-1">End date</label>
                <input type="date" className={inputClass()} value={draftEnd} onChange={(e) => setDraftEnd(e.target.value)} />
              </div>
              <div className="xl:col-span-3">
                <label className="block text-xs text-[var(--muted-foreground)] mb-1">User contains</label>
                <div className="relative">
                  <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-[var(--muted-foreground)] pointer-events-none" />
                  <input
                    type="search"
                    placeholder="Substring of user_id (empty = all)"
                    className={`${inputClass()} pl-9`}
                    value={draftUserSearch}
                    onChange={(e) => setDraftUserSearch(e.target.value)}
                  />
                </div>
              </div>
              <div className="xl:col-span-3">
                <label className="block text-xs text-[var(--muted-foreground)] mb-1">Ingest record type</label>
                <select
                  className={inputClass()}
                  value={draftRecordType}
                  onChange={(e) => setDraftRecordType(e.target.value as RecordTypeFilter)}
                >
                  {RECORD_TYPES.map((t) => (
                    <option key={t} value={t} className="bg-[var(--card)] text-[var(--foreground)]">
                      {t}
                    </option>
                  ))}
                </select>
              </div>
              <div className="xl:col-span-3 flex flex-wrap gap-2 justify-end">
                <button
                  type="button"
                  onClick={() => preset(7)}
                  className="rounded-lg border border-[var(--border)] px-3 py-2 text-xs font-medium text-[var(--foreground)] hover:bg-[var(--muted)]"
                >
                  Last 7d
                </button>
                <button
                  type="button"
                  onClick={() => preset(30)}
                  className="rounded-lg border border-[var(--border)] px-3 py-2 text-xs font-medium text-[var(--foreground)] hover:bg-[var(--muted)]"
                >
                  Last 30d
                </button>
                <button
                  type="button"
                  onClick={() => preset(90)}
                  className="rounded-lg border border-[var(--border)] px-3 py-2 text-xs font-medium text-[var(--foreground)] hover:bg-[var(--muted)]"
                >
                  Last 90d
                </button>
              </div>
            </div>

            <div className="mt-4 flex flex-wrap gap-2 justify-end">
              <button
                type="button"
                onClick={resetFilters}
                className="inline-flex items-center gap-2 rounded-lg border border-[var(--border)] px-4 py-2 text-sm text-[var(--foreground)] hover:bg-[var(--muted)]"
              >
                <RotateCcw className="h-4 w-4" />
                Reset
              </button>
              <button
                type="button"
                onClick={applyFilters}
                className="gradient-red inline-flex items-center gap-2 rounded-lg px-5 py-2 text-sm font-semibold text-white shadow-lg shadow-[var(--dbx-lava-600)]/25 hover:opacity-95"
              >
                Apply filters
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-6 py-10">
        <QueryState loading={kpis.loading} error={kpis.error} />
        {!kpis.loading && !kpis.error ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 mb-12">
            <KpiCard
              label="Bronze rows (window)"
              value={formatNumber(kpiRow?.bronze_rows_in_window)}
              hint={`ingested_at in range · user LIKE filter`}
              icon={Database}
            />
            <KpiCard
              label="Bronze rows (all time)"
              value={formatNumber(kpiRow?.bronze_rows_all_time)}
              hint="Unfiltered catalog total"
              icon={BarChart2}
            />
            <KpiCard
              label="Active users (window)"
              value={formatNumber(kpiRow?.active_users_in_window)}
              hint="Distinct users in activity gold"
              icon={Users}
            />
            <KpiCard
              label="Users with steps"
              value={formatNumber(kpiRow?.users_with_steps_in_window)}
              hint="Daily steps gold in window"
              icon={Footprints}
            />
            <KpiCard
              label="Latest bronze ingest"
              value={formatTs(kpiRow?.last_bronze_ingest_in_window)}
              hint="Max ingested_at in filtered window"
              icon={Activity}
            />
          </div>
        ) : null}

        <Section
          title="Activity quality & volume"
          subtitle="Population averages per day in your window — Move ring ratio (0–1) and mean steps when goals exist."
        >
          <QueryState loading={trend.loading} error={trend.error} />
          {!trend.loading && !trend.error ? (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6">
                  <p className="text-sm font-semibold text-[var(--foreground)] mb-1">Move ring ratio</p>
                <InteractiveSeriesChart
                  points={movePoints}
                  accent="#FF6A33"
                  valueLabel="Move ratio (0–1)"
                  formatY={(n) => n.toFixed(3)}
                />
                <p className="text-xs text-[var(--muted-foreground)] mt-2">
                  {trend.data?.length ?? 0} daily buckets · filtered cohort
                </p>
              </div>
              <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6">
                  <p className="text-sm font-semibold text-[var(--foreground)] mb-1">Average steps (cohort)</p>
                <InteractiveSeriesChart
                  points={stepsPoints}
                  accent="#00A972"
                  valueLabel="Avg steps"
                  formatY={(n) => formatNumber(Math.round(n))}
                />
                <p className="text-xs text-[var(--muted-foreground)] mt-2">Mean total_steps across users with a row that day</p>
              </div>
            </div>
          ) : null}
        </Section>

        <Section
          title="Bronze ingest volume"
          subtitle="Raw row counts per calendar day from wearables_zerobus — complements the typed ingest mix panel."
        >
          <QueryState loading={bronzeDaily.loading} error={bronzeDaily.error} />
          {!bronzeDaily.loading && !bronzeDaily.error ? (
            <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6">
              <InteractiveSeriesChart
                points={bronzeVolPoints}
                accent="#7C3AED"
                valueLabel="Rows ingested"
                formatY={(n) => formatNumber(Math.round(n))}
              />
              <p className="text-xs text-[var(--muted-foreground)] mt-2">{bronzeDaily.data?.length ?? 0} days with ingest in range</p>
            </div>
          ) : null}
        </Section>

        <Section
          title="Resting heart rate trend"
          subtitle="Gold cardio vitals — daily average resting HR across users who reported a value."
        >
          <QueryState loading={vitals.loading} error={vitals.error} />
          {!vitals.loading && !vitals.error ? (
            <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6">
              <InteractiveSeriesChart
                points={rhrPoints}
                accent="#38BDF8"
                valueLabel="Avg resting HR (bpm)"
                formatY={(n) => `${Math.round(n)} bpm`}
              />
              <p className="text-xs text-[var(--muted-foreground)] mt-2">
                {(vitals.data ?? []).length} days · avg bpm (population mean of daily user averages)
              </p>
            </div>
          ) : null}
        </Section>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
          <Section title="Step leaderboard" subtitle="Ranked by average daily steps in the selected window.">
            <QueryState loading={leaders.loading} error={leaders.error} />
            {!leaders.loading && !leaders.error ? (
              <div className="overflow-hidden rounded-xl border border-[var(--border)] bg-[var(--card)] shadow-lg shadow-black/10">
                <table className="w-full text-sm">
                  <thead className="bg-[var(--muted)] text-left text-xs uppercase tracking-wide text-[var(--muted-foreground)]">
                    <tr>
                      <th className="px-4 py-3 font-medium">User</th>
                      <th className="px-4 py-3 font-medium text-right">Avg / day</th>
                      <th className="px-4 py-3 font-medium text-right">Peak day</th>
                      <th className="px-4 py-3 font-medium text-right">Sum</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[var(--border)]">
                    {(leaders.data ?? []).map((r, i) => (
                      <tr
                        key={r.user_id}
                        className={i % 2 === 0 ? 'bg-[var(--muted)]/40' : 'bg-transparent hover:bg-[var(--muted)]/60'}
                      >
                        <td className="px-4 py-2.5 font-mono text-xs text-[var(--foreground)] truncate max-w-[220px]">
                          {r.user_id}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--foreground)] font-medium">
                          {formatNumber(r.avg_steps)}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--muted-foreground)]">
                          {formatNumber(r.peak_steps)}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--muted-foreground)]">
                          {formatNumber(r.sum_steps)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}
          </Section>

          <Section
            title="Ingest mix by record type"
            subtitle="From gold ingest rollup — honor the record-type filter above (ALL shows every type in the window)."
          >
            <QueryState loading={ingest.loading} error={ingest.error} />
            {!ingest.loading && !ingest.error ? (
              <div className="space-y-3 rounded-xl border border-[var(--border)] bg-[var(--muted)] p-5">
                {(ingest.data ?? []).length === 0 ? (
                  <p className="text-sm text-[var(--muted-foreground)]">No ingest rows for this combination of filters.</p>
                ) : null}
                {(ingest.data ?? []).map((r) => {
                  const pct = maxIngest > 0 ? Math.round((Number(r.row_count) / maxIngest) * 100) : 0;
                  const count = Number(r.row_count);
                  const hovered = ingestHoverType === r.record_type;
                  return (
                    <div
                      key={r.record_type}
                      className="relative"
                      onMouseEnter={() => setIngestHoverType(r.record_type)}
                      onMouseLeave={() => setIngestHoverType(null)}
                    >
                      <div className="flex justify-between text-xs text-[var(--muted-foreground)] mb-1 gap-2">
                        <span className="font-mono text-[var(--foreground)] truncate">{r.record_type}</span>
                        <span className="tabular-nums shrink-0">{formatNumber(count)} rows</span>
                      </div>
                      <div className="h-2.5 rounded-full bg-[var(--muted)] overflow-hidden transition-colors">
                        <div
                          className={`h-full rounded-full bg-gradient-to-r from-[var(--dbx-lava-600)] to-[#FF9F7A] transition-[filter] ${
                            hovered ? 'brightness-110' : ''
                          }`}
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      {hovered ? (
                        <div className="absolute z-10 left-1/2 -translate-x-1/2 top-full mt-1 whitespace-nowrap rounded-md border border-[var(--border)] bg-[var(--card)] px-2.5 py-1.5 text-[11px] text-[var(--foreground)] shadow-lg">
                          <span className="font-mono font-medium">{r.record_type}</span>
                          <span className="mx-1.5 text-[var(--muted-foreground)]">·</span>
                          <span className="tabular-nums">{formatNumber(count)} rows</span>
                          <span className="mx-1.5 text-[var(--muted-foreground)]">·</span>
                          <span className="tabular-nums text-[var(--muted-foreground)]">{pct}% of max type in window</span>
                        </div>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            ) : null}
          </Section>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
          <Section title="Workout volume by user" subtitle="Weekly workout summary rolled up for weeks starting in the window.">
            <QueryState loading={workouts.loading} error={workouts.error} />
            {!workouts.loading && !workouts.error ? (
              <div className="overflow-hidden rounded-xl border border-[var(--border)] bg-[var(--card)] shadow-lg shadow-black/10">
                <table className="w-full text-sm">
                  <thead className="bg-[var(--muted)] text-left text-xs uppercase tracking-wide text-[var(--muted-foreground)]">
                    <tr>
                      <th className="px-4 py-3 font-medium">User</th>
                      <th className="px-4 py-3 font-medium text-right">Workouts</th>
                      <th className="px-4 py-3 font-medium text-right">Minutes</th>
                      <th className="px-4 py-3 font-medium text-right">kcal</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[var(--border)]">
                    {(workouts.data ?? []).map((r, i) => (
                      <tr
                        key={r.user_id}
                        className={i % 2 === 0 ? 'bg-[var(--muted)]/40' : 'bg-transparent hover:bg-[var(--muted)]/60'}
                      >
                        <td className="px-4 py-2.5 font-mono text-xs text-[var(--foreground)] truncate max-w-[200px]">
                          {r.user_id}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--foreground)] font-medium">
                          {formatNumber(r.workouts)}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--muted-foreground)]">
                          {formatNumber(r.total_minutes)}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--muted-foreground)]">
                          {formatNumber(r.total_kcal)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}
          </Section>

          <Section title="Sleep depth profile" subtitle="Per-user averages across sleep nights in the window.">
            <QueryState loading={sleep.loading} error={sleep.error} />
            {!sleep.loading && !sleep.error ? (
              <div className="overflow-hidden rounded-xl border border-[var(--border)] bg-[var(--card)] shadow-lg shadow-black/10">
                <table className="w-full text-sm">
                  <thead className="bg-[var(--muted)] text-left text-xs uppercase tracking-wide text-[var(--muted-foreground)]">
                    <tr>
                      <th className="px-4 py-3 font-medium">User</th>
                      <th className="px-4 py-3 font-medium text-right">Deep (min)</th>
                      <th className="px-4 py-3 font-medium text-right">REM (min)</th>
                      <th className="px-4 py-3 font-medium text-right">Tracked (min)</th>
                      <th className="px-4 py-3 font-medium text-right">Nights</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[var(--border)]">
                    {(sleep.data ?? []).map((r, i) => (
                      <tr
                        key={r.user_id}
                        className={i % 2 === 0 ? 'bg-[var(--muted)]/40' : 'bg-transparent hover:bg-[var(--muted)]/60'}
                      >
                        <td className="px-4 py-2.5 font-mono text-xs text-[var(--foreground)] truncate max-w-[200px]">
                          {r.user_id}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--foreground)]">
                          {r.avg_deep_min?.toFixed?.(1) ?? r.avg_deep_min}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--foreground)]">
                          {r.avg_rem_min?.toFixed?.(1) ?? r.avg_rem_min}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--foreground)]">
                          {formatNumber(r.avg_tracked_min)}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums text-[var(--muted-foreground)]">{r.nights}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}
          </Section>
        </div>

        <div className="rounded-xl border border-[var(--border)] bg-[var(--muted)]/50 px-5 py-4 flex items-start gap-3 text-sm text-[var(--muted-foreground)]">
          <HeartPulse className="h-5 w-5 shrink-0 text-[var(--dbx-lava-600)] dark:text-[var(--dbx-lava-400)] mt-0.5" />
          <p>
            SQL definitions live under <span className="font-mono text-[var(--foreground)]">config/queries/</span> and execute as
            the app service principal. Parameters use <span className="font-mono text-[var(--foreground)]">-- @param</span> with{' '}
            <span className="font-mono text-[var(--foreground)]">sql.date</span> / <span className="font-mono text-[var(--foreground)]">sql.string</span>{' '}
            from <span className="font-mono text-[var(--foreground)]">@databricks/appkit-ui/js</span>. For static Lakeview
            dashboards, deploy the bundle Lakeview resource; this page is for interactive, app-native analytics with
            shared filters.
          </p>
        </div>
      </div>
    </div>
  );
}
