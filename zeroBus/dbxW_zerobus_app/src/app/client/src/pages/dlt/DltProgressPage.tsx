import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  AlertCircle,
  ArrowDown,
  CheckCircle2,
  Circle,
  CircleDot,
  Database,
  ExternalLink,
  GitBranch,
  Info,
  Layers,
  Loader2,
  Pause,
  Play,
  Radio,
  RefreshCw,
  Sparkles,
  Zap,
} from 'lucide-react';
import {
  BRONZE_STREAM_TABLE,
  findInsightForPublishedTable,
  findInsightForGoldTable,
  findInsightForTable,
  formatInt,
  formatRelativeTime,
  GOLD_MATERIALIZED_TABLES,
  GOLD_VIEWS,
  type FlowInsight,
  type PipelineInsightsPayload,
  SILVER_STREAMING_TABLES,
  tableSuffix,
} from './dltStatusModel';
import { buildMedallionSteps, isDltUpdateActive, type MedallionStepKind } from './medallionSteps';
import { MedallionVerticalDag } from './MedallionVerticalDag';

type PipelineConfig = {
  workspace_api_configured: boolean;
  workspace_origin: string | null;
  pipelines: Array<{ key: string; label: string; id: string }>;
  wearable_pipeline_name?: string | null;
};

type StatusPayload = {
  pipeline: {
    pipeline_id?: string;
    name?: string;
    state?: string;
    health?: string;
    latest_updates?: Array<{ update_id?: string; state?: string; creation_time?: number }>;
  } | null;
  updates: Array<{
    update_id?: string;
    state?: string;
    creation_time?: number;
    complete_time?: number;
  }>;
  latest_update: {
    update_id?: string;
    state?: string;
    creation_time?: number;
    complete_time?: number;
    progress?: unknown;
  } | null;
  events: Array<{
    time_stamp?: string;
    level?: string;
    message?: string;
    event_type?: string;
  }>;
  insights?: PipelineInsightsPayload;
};

type IngestStatsPayload = {
  zerobus_env_configured: boolean;
  target_table: string | null;
  started_at_ms: number;
  successful_ingests: number;
  failed_ingests: number;
  total_records_ingested: number;
  last_ingest_at_ms: number | null;
  last_error_at_ms: number | null;
  last_error_message: string | null;
  by_record_type: Array<{ record_type: string; requests: number; records: number }>;
};

/** Pipelines API often returns epoch seconds; some fields are ms — normalize for display. */
function normalizeEpochToMs(raw: number | undefined): number | undefined {
  if (raw == null || !Number.isFinite(raw)) return undefined;
  if (raw < 1_000_000_000_000) return Math.round(raw * 1000);
  return Math.round(raw);
}

function formatTs(raw: number | undefined): string {
  const ms = normalizeEpochToMs(raw);
  if (ms == null) return '—';
  const d = new Date(ms);
  return Number.isNaN(d.getTime()) ? String(raw) : d.toLocaleString();
}

type StepKind = MedallionStepKind;

function stepRingClass(kind: StepKind): string {
  const base =
    'relative flex h-12 w-12 shrink-0 items-center justify-center rounded-xl border transition-all duration-300';
  switch (kind) {
    case 'done':
      return `${base} border-emerald-500/35 bg-emerald-950/30 text-emerald-200/95 ring-1 ring-emerald-500/10`;
    case 'active':
      return `${base} border-sky-500/40 bg-sky-950/40 text-sky-100 ring-1 ring-sky-500/15`;
    case 'warn':
      return `${base} border-amber-500/35 bg-amber-950/25 text-amber-100`;
    case 'error':
      return `${base} border-red-500/35 bg-red-950/25 text-red-200`;
    default:
      return `${base} border-[var(--border)] bg-[var(--muted)] text-[var(--muted-foreground)]`;
  }
}

function StepGlyph({ kind }: { kind: StepKind }) {
  if (kind === 'active') return <Loader2 className="h-5 w-5 animate-spin" aria-hidden />;
  if (kind === 'done') return <CheckCircle2 className="h-5 w-5" aria-hidden />;
  if (kind === 'warn') return <AlertCircle className="h-5 w-5" aria-hidden />;
  if (kind === 'error') return <AlertCircle className="h-5 w-5" aria-hidden />;
  return <Circle className="h-5 w-5 opacity-40" aria-hidden />;
}

function MedallionArchitectureBar({
  zerobusReady,
  ingestFailures,
  hasIngestActivity,
  totalRecordsIngested,
  bronzeInsight,
  silverDone,
  silverTotal,
  goldDone,
  goldTotal,
  dltUpdateState,
}: {
  zerobusReady: boolean;
  ingestFailures: number;
  hasIngestActivity: boolean;
  totalRecordsIngested: number;
  bronzeInsight: FlowInsight | undefined;
  silverDone: number;
  silverTotal: number;
  goldDone: number;
  goldTotal: number;
  dltUpdateState: string | undefined;
}) {
  const dltActive = isDltUpdateActive(dltUpdateState);
  const steps = buildMedallionSteps({
    zerobusReady,
    ingestFailures,
    hasIngestActivity,
    totalRecordsIngested,
    bronzeInsight,
    silverDone,
    silverTotal,
    goldDone,
    goldTotal,
    dltUpdateState,
  });

  return (
    <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6 md:p-8 ring-1 ring-black/[0.04] dark:ring-white/[0.06] shadow-xl shadow-black/10">
      <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-5 mb-6">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-sky-500/80 mb-1.5">Architecture</p>
          <h2 className="text-xl md:text-2xl font-bold text-[var(--foreground)] tracking-tight">ZeroBus medallion progress</h2>
          <p className="text-sm text-[var(--muted-foreground)] mt-2.5 max-w-2xl leading-relaxed">
            Live read of ingest counters in this app plus DLT{' '}
            <span className="font-mono text-[var(--muted-foreground)]">flow_progress</span> from your workspace pipeline. The vertical graph
            echoes the DLT UI: animated flow on live connectors, a running chevron under active stages, and a progress rail
            on the active stage.
          </p>
        </div>
        <div className="rounded-lg border border-[var(--border)] bg-[var(--muted)] px-4 py-3 text-xs text-[var(--muted-foreground)] shrink-0">
          <p>
            DLT update:{' '}
            <span className={`font-medium font-mono ${dltActive ? 'text-sky-300' : 'text-[var(--foreground)]'}`}>
              {dltUpdateState ?? '—'}
            </span>
          </p>
          {bronzeInsight?.status ? (
            <p className="mt-1.5">
              Bronze stream:{' '}
              <span className="font-mono text-emerald-400/90">{bronzeInsight.status}</span>
            </p>
          ) : null}
        </div>
      </div>

      <div className="mb-6 flex flex-wrap items-center gap-x-8 gap-y-2 rounded-lg border border-[var(--border)] bg-[var(--muted)] px-4 py-3 text-xs text-[var(--muted-foreground)]">
        <span>
          <span className="text-[var(--muted-foreground)] uppercase tracking-wider">Ingested (this app)</span>{' '}
          <span className="font-semibold tabular-nums text-[var(--foreground)]">{formatInt(totalRecordsIngested)}</span>{' '}
          <span className="text-[var(--muted-foreground)]">rows</span>
        </span>
        {bronzeInsight?.num_output_rows != null || bronzeInsight?.num_upserted_rows != null ? (
          <span>
            <span className="text-[var(--muted-foreground)] uppercase tracking-wider">Bronze ST (DLT)</span>{' '}
            <span className="font-semibold tabular-nums text-emerald-400/90">
              {formatInt(bronzeInsight.num_output_rows ?? bronzeInsight.num_upserted_rows)}
            </span>{' '}
            <span className="text-[var(--muted-foreground)]">rows (latest flow_progress)</span>
          </span>
        ) : (
          <span className="text-[var(--muted-foreground)] max-w-prose">
            Bronze ST DLT row counts appear when the pipeline emits{' '}
            <span className="font-mono text-[var(--muted-foreground)]">flow_progress</span> for this update (or infer from silver/gold
            below).
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-[minmax(280px,340px)_1fr] gap-8 xl:gap-10 items-start">
        <MedallionVerticalDag steps={steps} dltPipelineActive={dltActive} />
        <div className="min-w-0">
          <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-[var(--muted-foreground)] mb-3 hidden xl:block">
            Stage strip
          </p>
          <div className="relative">
            <div
              className="absolute left-[6%] right-[6%] top-[22px] hidden lg:block h-px bg-gradient-to-r from-transparent via-[var(--border)] to-transparent rounded-full"
              aria-hidden
            />
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-5 md:gap-3">
              {steps.map((s) => (
                <div key={s.key} className="flex flex-col items-center text-center gap-2">
                  <div className={stepRingClass(s.kind)}>
                    <s.Icon className={`h-6 w-6 ${s.kind === 'pending' ? 'opacity-40' : 'opacity-95'}`} />
                    {s.kind !== 'pending' ? (
                      <span className="absolute -bottom-1 -right-1 flex h-5 w-5 items-center justify-center rounded-full bg-[var(--background)] border border-[var(--border)] shadow-sm">
                        <StepGlyph kind={s.kind} />
                      </span>
                    ) : null}
                  </div>
                  <div>
                    <p className="text-[11px] font-semibold uppercase tracking-wider text-[var(--muted-foreground)]">{s.title}</p>
                    <p className="text-[10px] text-[var(--muted-foreground)] mt-0.5 leading-snug px-1 line-clamp-2">{s.subtitle}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function stateColor(state: string | undefined): string {
  const s = (state ?? '').toUpperCase();
  if (s.includes('RUN') || s === 'ACTIVE') return 'text-emerald-300 bg-emerald-500/10 border-emerald-500/25';
  if (s.includes('FAIL') || s.includes('ERROR')) return 'text-red-300 bg-red-500/10 border-red-500/25';
  if (s.includes('CANCEL')) return 'text-amber-200 bg-amber-500/10 border-amber-500/25';
  if (s.includes('WAIT') || s.includes('INIT') || s.includes('SETUP') || s.includes('RESET'))
    return 'text-sky-300 bg-sky-500/10 border-sky-500/25';
  if (s.includes('COMPLET') || s === 'IDLE')
    return 'text-[var(--foreground)] bg-[var(--muted)] border-[var(--border)]';
  return 'text-[var(--muted-foreground)] bg-[var(--muted)] border-[var(--border)]';
}

function statusBadgeClass(status: string | undefined): string {
  const u = (status ?? '').toUpperCase();
  if (u === 'COMPLETED' || u === 'IDLE') return 'bg-emerald-500/10 text-emerald-200/95 border-emerald-500/25';
  if (u === 'FAILED' || u === 'STOPPED') return 'bg-red-500/10 text-red-200/95 border-red-500/25';
  if (u === 'RUNNING' || u === 'STARTING' || u === 'QUEUED') return 'bg-sky-500/10 text-sky-200/95 border-sky-500/25';
  return 'bg-[var(--muted)] text-[var(--muted-foreground)] border-[var(--border)]';
}

function DltStateIcon({ state, className = 'h-4 w-4' }: { state: string | undefined; className?: string }) {
  const u = (state ?? '').toUpperCase();
  if (u.includes('FAIL') || u.includes('ERROR')) return <AlertCircle className={`${className} text-red-400`} aria-hidden />;
  if (isDltUpdateActive(state))
    return <Loader2 className={`${className} text-sky-300 animate-spin`} aria-hidden />;
  if (u.includes('COMPLET') || u === 'IDLE')
    return <CheckCircle2 className={`${className} text-emerald-400`} aria-hidden />;
  return <CircleDot className={`${className} text-[var(--muted-foreground)]`} aria-hidden />;
}

function FlowNode({
  icon: Icon,
  title,
  subtitle,
  accent,
}: {
  icon: typeof Database;
  title: string;
  subtitle: string;
  accent?: boolean;
}) {
  return (
    <div
      className={`relative flex flex-col items-center text-center min-w-[140px] max-w-[180px] px-3 py-4 rounded-xl border transition-colors ${
        accent
          ? 'border-sky-500/35 bg-sky-950/35 ring-1 ring-sky-500/10'
          : 'border-[var(--border)] bg-[var(--card)]'
      }`}
    >
      <div
        className={`mb-2 flex h-11 w-11 items-center justify-center rounded-lg ${
          accent ? 'bg-sky-500/15 text-sky-300' : 'bg-[var(--muted)] text-[var(--muted-foreground)]'
        }`}
      >
        <Icon className="h-5 w-5" />
      </div>
      <p className="text-sm font-medium text-[var(--foreground)] leading-snug">{title}</p>
      <p className="text-[11px] text-[var(--muted-foreground)] mt-1 leading-relaxed">{subtitle}</p>
    </div>
  );
}

function FlowArrow() {
  return (
    <>
      <div className="flex md:hidden items-center py-1 text-[var(--muted-foreground)]">
        <ArrowDown className="h-4 w-4 mx-auto opacity-70" />
      </div>
      <div className="hidden md:flex items-center justify-center text-[var(--muted-foreground)] shrink-0 px-1">
        <div className="w-8 h-px bg-gradient-to-r from-[var(--border)] to-transparent relative">
          <div className="absolute -right-0.5 -top-1 w-0 h-0 border-t-[5px] border-t-transparent border-b-[5px] border-b-transparent border-l-[6px] border-l-[var(--muted-foreground)]" />
        </div>
      </div>
    </>
  );
}

export function DltProgressPage() {
  const [config, setConfig] = useState<PipelineConfig | null>(null);
  const [selectedId, setSelectedId] = useState<string>('');
  const [status, setStatus] = useState<StatusPayload | null>(null);
  const [ingestStats, setIngestStats] = useState<IngestStatsPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const [polling, setPolling] = useState(true);
  const [triggering, setTriggering] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastSyncedAtMs, setLastSyncedAtMs] = useState<number | null>(null);

  const selectedIdRef = useRef(selectedId);
  selectedIdRef.current = selectedId;

  const loadIngestStats = useCallback(async () => {
    try {
      const res = await fetch('/api/v1/healthkit/ingest-stats');
      if (res.ok) {
        setIngestStats((await res.json()) as IngestStatsPayload);
      }
    } catch {
      /* non-fatal */
    }
  }, []);

  const fetchPipelineStatusForId = useCallback(
    async (pipelineId: string) => {
      if (!pipelineId) {
        setLoading(false);
        return;
      }
      try {
        const [pipeRes] = await Promise.all([
          fetch(`/api/pipelines/${encodeURIComponent(pipelineId)}/status`),
          loadIngestStats(),
        ]);
        if (!pipeRes.ok) {
          const t = await pipeRes.text();
          setError(t || `HTTP ${pipeRes.status}`);
          setStatus(null);
          return;
        }
        setError(null);
        setStatus((await pipeRes.json()) as StatusPayload);
        setLastSyncedAtMs(Date.now());
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    },
    [loadIngestStats],
  );

  const fetchPipelineStatusRef = useRef(fetchPipelineStatusForId);
  fetchPipelineStatusRef.current = fetchPipelineStatusForId;

  /** Re-resolve pipeline id from bundle config + pull latest status (fixes stale selectedId on Refresh). */
  const reloadConfigAndStatus = useCallback(async () => {
    try {
      const res = await fetch('/api/pipelines/config');
      const j = (await res.json()) as PipelineConfig;
      setConfig(j);
      const current = selectedIdRef.current;
      const next = j.pipelines.find((p) => p.id === current)?.id ?? j.pipelines[0]?.id ?? '';
      setSelectedId(next);
      if (!next) {
        setStatus(null);
        setLoading(false);
        return;
      }
      setLoading(true);
      await fetchPipelineStatusForId(next);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setLoading(false);
    }
  }, [fetchPipelineStatusForId]);

  useEffect(() => {
    void loadIngestStats();
  }, [loadIngestStats]);

  useEffect(() => {
    void reloadConfigAndStatus();
  }, [reloadConfigAndStatus]);

  useEffect(() => {
    if (!polling) return;
    const id = window.setInterval(() => {
      const pid = selectedIdRef.current;
      if (pid) void fetchPipelineStatusRef.current(pid);
    }, 3500);
    return () => window.clearInterval(id);
  }, [polling]);

  const pipelineName = status?.pipeline?.name ?? 'Pipeline';
  const topState = status?.latest_update?.state ?? status?.pipeline?.state ?? '—';
  const insights = status?.insights;
  const workspaceLink = useMemo(() => {
    const o = config?.workspace_origin;
    if (!o || !selectedId) return null;
    return `${o}/pipelines/${encodeURIComponent(selectedId)}`;
  }, [config?.workspace_origin, selectedId]);

  const goldInsights = useMemo(() => {
    const flows = insights?.flow_progress ?? [];
    return GOLD_MATERIALIZED_TABLES.map((name) => ({
      name,
      insight: findInsightForGoldTable(name, flows),
    }));
  }, [insights?.flow_progress]);

  const silverInsights = useMemo(() => {
    const flows = insights?.flow_progress ?? [];
    return SILVER_STREAMING_TABLES.map((name) => ({
      name,
      insight: findInsightForPublishedTable(name, flows),
    }));
  }, [insights?.flow_progress]);

  const bronzeStreamInsight = useMemo(
    () => findInsightForPublishedTable(BRONZE_STREAM_TABLE, insights?.flow_progress ?? []),
    [insights?.flow_progress],
  );

  const dltRowTotals = useMemo(() => {
    const flows = insights?.flow_progress ?? [];
    let sum = 0;
    let n = 0;
    for (const f of flows) {
      const v = f.num_output_rows ?? f.num_upserted_rows;
      if (v != null) {
        sum += v;
        n += 1;
      }
    }
    return { sum, withMetrics: n };
  }, [insights?.flow_progress]);

  const silverFlowsReporting = useMemo(
    () =>
      silverInsights.filter(
        (x: { name: string; insight?: FlowInsight }) =>
          x.insight?.status ||
          x.insight?.num_output_rows != null ||
          x.insight?.num_upserted_rows != null,
      ).length,
    [silverInsights],
  );

  const goldFlowsReporting = useMemo(
    () =>
      goldInsights.filter(
        (x: { name: string; insight?: FlowInsight }) =>
          x.insight?.status ||
          x.insight?.num_output_rows != null ||
          x.insight?.num_upserted_rows != null,
      ).length,
    [goldInsights],
  );

  const triggerRun = async () => {
    if (!selectedId) return;
    setTriggering(true);
    setError(null);
    try {
      const res = await fetch(`/api/pipelines/${encodeURIComponent(selectedId)}/trigger`, {
        method: 'POST',
      });
      const j = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError((j as { error?: string }).error || `HTTP ${res.status}`);
        return;
      }
      await fetchPipelineStatusForId(selectedId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setTriggering(false);
    }
  };

  const onManualRefresh = useCallback(async () => {
    setManualRefreshing(true);
    try {
      await reloadConfigAndStatus();
    } finally {
      setManualRefreshing(false);
    }
  }, [reloadConfigAndStatus]);

  const selectedMeta = config?.pipelines.find((p) => p.id === selectedId);

  const ingestHealthLabel = (() => {
    if (!ingestStats) return { tone: 'muted' as const, text: 'Loading ingest stats…' };
    if (!ingestStats.zerobus_env_configured)
      return { tone: 'warn' as const, text: 'ZeroBus env not configured' };
    if (ingestStats.failed_ingests > 0) return { tone: 'bad' as const, text: 'Recent ingest failures' };
    if (ingestStats.successful_ingests > 0) return { tone: 'ok' as const, text: 'Ingest path exercised' };
    return { tone: 'idle' as const, text: 'No batches yet (since app boot)' };
  })();

  return (
    <div className="min-h-screen bg-[var(--background)] text-[var(--foreground)] antialiased">
      <section className="gradient-hero text-white border-b border-white/10 py-12 md:py-16 px-6 relative overflow-hidden">
        <div className="absolute inset-0 opacity-[0.04] pointer-events-none">
          <img
            src="/images/databricks-symbol-light.svg"
            alt=""
            aria-hidden
            className="absolute -top-20 -right-20 w-96 h-96 rotate-12"
          />
        </div>
        <div className="relative max-w-7xl mx-auto">
          <div className="flex flex-wrap items-start justify-between gap-8">
            <div className="max-w-3xl">
              <p className="text-xs font-bold uppercase tracking-widest text-[var(--dbx-lava-400)] mb-2">
                End-to-end status
              </p>
              <h1 className="text-3xl sm:text-4xl md:text-5xl font-bold tracking-tight leading-tight">
                ZeroBus → Lakehouse → DLT
              </h1>
              <p className="mt-4 text-base md:text-lg text-gray-300 leading-relaxed max-w-2xl">
                Live view of how payloads land in Unity Catalog through ZeroBus, how Lakeflow moves them through
                streaming bronze and silver, and how gold datasets refresh—using this app’s counters and your workspace
                Pipelines API.
              </p>
            </div>
            <div className="flex flex-col items-stretch sm:items-end gap-3 min-w-[220px]">
              <div className="flex flex-wrap gap-2 justify-end">
                <button
                  type="button"
                  aria-pressed={polling}
                  title={
                    polling
                      ? 'Auto-refresh: fetches pipeline + ingest stats every 3.5s. Turn off to freeze the view.'
                      : 'Turn on to poll the workspace Pipelines API every 3.5s while this tab is open.'
                  }
                  onClick={() => setPolling((p) => !p)}
                  className={`inline-flex items-center gap-2 rounded-lg border px-4 py-2.5 text-sm font-medium transition-colors ${
                    polling
                      ? 'border-sky-400/50 bg-sky-500/15 text-white ring-1 ring-sky-400/30'
                      : 'border-white/20 bg-white/10 text-white hover:bg-white/15'
                  }`}
                >
                  {polling ? <Radio className="h-4 w-4 shrink-0" /> : <Pause className="h-4 w-4 shrink-0" />}
                  <span className="text-left">
                    <span className="block leading-tight">{polling ? 'Live updates' : 'Live updates off'}</span>
                    <span className="block text-[10px] font-normal text-gray-400 leading-tight mt-0.5">
                      {polling ? 'Polling every 3.5s' : 'No auto-poll'}
                    </span>
                  </span>
                </button>
                <button
                  type="button"
                  title="Re-fetch pipeline config, then latest pipeline status and ingest counters"
                  disabled={manualRefreshing}
                  onClick={() => void onManualRefresh()}
                  className="inline-flex items-center gap-2 rounded-lg border border-white/20 bg-white/10 px-4 py-2.5 text-sm text-white hover:bg-white/15 disabled:opacity-50"
                >
                  <RefreshCw className={`h-4 w-4 shrink-0 ${manualRefreshing ? 'animate-spin' : ''}`} />
                  Refresh
                </button>
              </div>
              <p className="text-[11px] text-gray-400 text-right leading-relaxed max-w-xs sm:max-w-none">
                <span className="text-gray-300">Live</span> keeps this view aligned with long-running DLT updates.{' '}
                <span className="text-gray-300">Refresh</span> pulls immediately and re-resolves the pipeline id.
              </p>
              {lastSyncedAtMs ? (
                <p className="text-[10px] text-gray-500 text-right font-mono">
                  Last sync {new Date(lastSyncedAtMs).toLocaleTimeString()}
                  {config?.wearable_pipeline_name ? (
                    <>
                      {' '}
                      · <span className="text-gray-400">{config.wearable_pipeline_name}</span>
                    </>
                  ) : null}
                </p>
              ) : null}
            </div>
          </div>

          {/* KPI strip */}
          <div className="mt-10 grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
            <div className="rounded-xl border border-white/15 bg-white/[0.06] p-5 shadow-lg shadow-black/20 backdrop-blur-sm">
              <div className="flex items-center gap-2 text-gray-400 text-[11px] uppercase tracking-wider mb-2">
                <span className="flex h-7 w-7 items-center justify-center rounded-md bg-white/10">
                  <Zap className="h-3.5 w-3.5 text-sky-300" />
                </span>
                ZeroBus ingest
              </div>
              <p className="text-2xl font-bold text-white tabular-nums tracking-tight">
                {formatInt(ingestStats?.total_records_ingested)}
              </p>
              <p className="text-xs text-gray-400 mt-1">Rows committed (since app boot)</p>
              <p className="text-xs text-gray-400 mt-2">
                Last{' '}
                <span className="text-gray-200 tabular-nums">{formatRelativeTime(ingestStats?.last_ingest_at_ms)}</span>
              </p>
            </div>
            <div className="rounded-xl border border-white/15 bg-white/[0.06] p-5 shadow-lg shadow-black/20 backdrop-blur-sm">
              <div className="flex items-center gap-2 text-gray-400 text-[11px] uppercase tracking-wider mb-2">
                <span className="flex h-7 w-7 items-center justify-center rounded-md bg-white/10">
                  <DltStateIcon state={topState} className="h-3.5 w-3.5 shrink-0" />
                </span>
                DLT update
              </div>
              <p className={`text-base font-semibold inline-flex px-2.5 py-1 rounded-md border ${stateColor(topState)}`}>
                {topState}
              </p>
              <p className="text-xs text-gray-400 mt-3">
                API state{' '}
                <span className="text-gray-200 font-mono">{status?.insights?.update_progress_state ?? '—'}</span>
              </p>
            </div>
            <div className="rounded-xl border border-white/15 bg-white/[0.06] p-5 shadow-lg shadow-black/20 backdrop-blur-sm">
              <div className="flex items-center gap-2 text-gray-400 text-[11px] uppercase tracking-wider mb-2">
                <span className="flex h-7 w-7 items-center justify-center rounded-md bg-white/10">
                  <Layers className="h-3.5 w-3.5 text-gray-300" />
                </span>
                Flow metrics
              </div>
              <p className="text-2xl font-bold text-white tabular-nums tracking-tight">{formatInt(dltRowTotals.sum)}</p>
              <p className="text-xs text-gray-400 mt-1">
                Σ output rows ({dltRowTotals.withMetrics} datasets reporting)
              </p>
            </div>
            <div className="rounded-xl border border-white/15 bg-white/[0.06] p-5 shadow-lg shadow-black/20 backdrop-blur-sm">
              <div className="flex items-center gap-2 text-gray-400 text-[11px] uppercase tracking-wider mb-2">
                <span className="flex h-7 w-7 items-center justify-center rounded-md bg-white/10">
                  <Sparkles className="h-3.5 w-3.5 text-emerald-300" />
                </span>
                Gold tables
              </div>
              <p className="text-2xl font-bold text-white tabular-nums tracking-tight">{GOLD_MATERIALIZED_TABLES.length}</p>
              <p className="text-xs text-gray-400 mt-1">MV-style @dlt.table targets</p>
              <p className="text-xs text-gray-400 mt-2">+{GOLD_VIEWS.length} presentation views</p>
            </div>
          </div>

          {config?.workspace_api_configured ? (
            <div className="mt-10">
              <MedallionArchitectureBar
                zerobusReady={Boolean(ingestStats?.zerobus_env_configured)}
                ingestFailures={ingestStats?.failed_ingests ?? 0}
                hasIngestActivity={
                  (ingestStats?.successful_ingests ?? 0) > 0 || (ingestStats?.total_records_ingested ?? 0) > 0
                }
                totalRecordsIngested={ingestStats?.total_records_ingested ?? 0}
                bronzeInsight={bronzeStreamInsight}
                silverDone={silverFlowsReporting}
                silverTotal={SILVER_STREAMING_TABLES.length}
                goldDone={goldFlowsReporting}
                goldTotal={GOLD_MATERIALIZED_TABLES.length}
                dltUpdateState={topState}
              />
            </div>
          ) : null}
        </div>
      </section>

      <div className="max-w-7xl mx-auto px-6 py-12 md:py-14 space-y-16">
        {!config?.workspace_api_configured ? (
          <div className="rounded-lg border border-amber-500/25 bg-amber-950/20 px-5 py-4 flex gap-3 text-sm text-amber-100/95 ring-1 ring-amber-500/10">
            <AlertCircle className="h-5 w-5 shrink-0 mt-0.5 text-amber-400/90" />
            <div>
              <p className="font-medium text-amber-50">Workspace API not available in this runtime</p>
              <p className="mt-1 text-amber-100/75 leading-relaxed">
                Needs <span className="font-mono text-amber-200/95">ZEROBUS_WORKSPACE_URL</span> plus{' '}
                <span className="font-mono text-amber-200/95">DATABRICKS_CLIENT_ID</span> /{' '}
                <span className="font-mono text-amber-200/95">DATABRICKS_CLIENT_SECRET</span>. Grant the app service
                principal <span className="font-mono text-amber-200/95">CAN RUN</span> on the medallion pipeline.
              </p>
            </div>
          </div>
        ) : null}

        {/* Section 1 — ZeroBus */}
        <section className="space-y-6">
          <div className="flex items-end justify-between gap-4 flex-wrap">
            <div>
              <h2 className="text-xl md:text-2xl font-bold text-[var(--foreground)] flex items-center gap-3">
                <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-[var(--muted)] border border-[var(--border)] text-[var(--muted-foreground)] text-sm font-mono tabular-nums">
                  1
                </span>
                Ingest — ZeroBus to bronze
              </h2>
              <p className="text-sm text-[var(--muted-foreground)] mt-2 max-w-3xl leading-relaxed">
                NDJSON payloads hit the AppKit route, are normalized into bronze rows, then durable-inserted into your
                configured Unity Catalog Delta table via the ZeroBus REST API (OAuth client credentials).
              </p>
            </div>
            <div
              className={`text-xs font-medium px-3 py-1.5 rounded-md border ${
                ingestStats?.zerobus_env_configured
                  ? 'border-emerald-500/30 text-emerald-200/95 bg-emerald-950/30'
                  : 'border-amber-500/30 text-amber-100/95 bg-amber-950/25'
              }`}
            >
              {ingestStats?.zerobus_env_configured ? 'ZeroBus env ready' : 'ZeroBus env incomplete'}
            </div>
          </div>

          <div className="grid lg:grid-cols-2 gap-6 md:gap-8 items-stretch">
            <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6 md:p-8 ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[var(--muted-foreground)] mb-6">Request path</p>
              <div className="flex flex-col md:flex-row md:flex-wrap md:justify-center items-center gap-2 md:gap-0">
                <FlowNode icon={CircleDot} title="Sources" subtitle="HealthKit app, jobs, notebooks" />
                <FlowArrow />
                <FlowNode icon={GitBranch} title="REST gateway" subtitle="POST /api/v1/healthkit/ingest" accent />
                <FlowArrow />
                <FlowNode icon={Zap} title="ZeroBus insert" subtitle="Bearer token → table API" accent />
                <FlowArrow />
                <FlowNode icon={Database} title="UC Delta (bronze)" subtitle="VARIANT body + headers" />
              </div>
              <p className="text-[11px] text-[var(--muted-foreground)] text-center mt-8 font-mono">
                Headers such as X-Record-Type are stored for downstream DLT filtering.
              </p>
            </div>

            <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6 md:p-8 space-y-6 ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[var(--muted-foreground)]">Runtime metrics</p>
              <dl className="grid grid-cols-2 gap-3 md:gap-4 text-sm">
                <div className="rounded-lg bg-[var(--muted)] border border-[var(--border)] p-4">
                  <dt className="text-[var(--muted-foreground)] text-[11px] uppercase tracking-wide">Target table</dt>
                  <dd className="font-mono text-xs text-sky-300/90 mt-2 break-all leading-relaxed">
                    {ingestStats?.target_table ?? '—'}
                  </dd>
                </div>
                <div className="rounded-lg bg-[var(--muted)] border border-[var(--border)] p-4">
                  <dt className="text-[var(--muted-foreground)] text-[11px] uppercase tracking-wide">Successful batches</dt>
                  <dd className="text-2xl font-semibold text-[var(--foreground)] mt-1 tabular-nums tracking-tight">
                    {ingestStats?.successful_ingests ?? '—'}
                  </dd>
                </div>
                <div className="rounded-lg bg-[var(--muted)] border border-[var(--border)] p-4">
                  <dt className="text-[var(--muted-foreground)] text-[11px] uppercase tracking-wide">Failed batches</dt>
                  <dd className={`text-2xl font-semibold mt-1 tabular-nums tracking-tight ${(ingestStats?.failed_ingests ?? 0) > 0 ? 'text-red-300' : 'text-[var(--foreground)]'}`}>
                    {ingestStats?.failed_ingests ?? '—'}
                  </dd>
                </div>
                <div className="rounded-lg bg-[var(--muted)] border border-[var(--border)] p-4">
                  <dt className="text-[var(--muted-foreground)] text-[11px] uppercase tracking-wide">Health signal</dt>
                  <dd className="mt-2 flex items-center gap-2 text-sm">
                    {ingestHealthLabel.tone === 'ok' ? (
                      <>
                        <CheckCircle2 className="h-4 w-4 text-emerald-400" />
                        <span className="text-emerald-200/90">{ingestHealthLabel.text}</span>
                      </>
                    ) : ingestHealthLabel.tone === 'bad' ? (
                      <>
                        <AlertCircle className="h-4 w-4 text-red-400" />
                        <span className="text-red-200/90">{ingestHealthLabel.text}</span>
                      </>
                    ) : ingestHealthLabel.tone === 'muted' ? (
                      <span className="text-[var(--muted-foreground)]">{ingestHealthLabel.text}</span>
                    ) : ingestHealthLabel.tone === 'warn' ? (
                      <>
                        <AlertCircle className="h-4 w-4 text-amber-400" />
                        <span className="text-amber-100/90">{ingestHealthLabel.text}</span>
                      </>
                    ) : (
                      <>
                        <Info className="h-4 w-4 text-sky-400/80" />
                        <span className="text-[var(--muted-foreground)]">{ingestHealthLabel.text}</span>
                      </>
                    )}
                  </dd>
                </div>
              </dl>
              {ingestStats?.last_error_message ? (
                <div className="rounded-lg border border-red-500/25 bg-red-950/30 px-3 py-2 text-xs text-red-200 font-mono break-words">
                  Last error: {ingestStats.last_error_message}
                </div>
              ) : null}
              {ingestStats && ingestStats.by_record_type.length > 0 ? (
                <div>
                  <p className="text-[11px] text-[var(--muted-foreground)] uppercase tracking-wider mb-2">By X-Record-Type</p>
                  <div className="flex flex-wrap gap-2">
                    {ingestStats.by_record_type.slice(0, 8).map((t) => (
                      <span
                        key={t.record_type}
                        className="inline-flex items-center gap-1.5 rounded-md border border-[var(--border)] bg-[var(--muted)] px-2.5 py-1 text-[11px] text-[var(--muted-foreground)]"
                      >
                        <span className="font-mono text-sky-400/85">{t.record_type}</span>
                        <span className="text-[var(--muted-foreground)]">·</span>
                        <span className="tabular-nums">{formatInt(t.records)}</span>
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        </section>

        {/* Section 2 — DLT */}
        <section className="space-y-6">
          <div>
            <h2 className="text-xl md:text-2xl font-bold text-[var(--foreground)] flex items-center gap-3">
              <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-[var(--muted)] border border-[var(--border)] text-[var(--muted-foreground)] text-sm font-mono tabular-nums">
                2
              </span>
              Lakeflow DLT — streaming medallion
            </h2>
            <p className="text-sm text-[var(--muted-foreground)] mt-2 max-w-3xl leading-relaxed">
              The bundle pipeline reads your external bronze Delta table as a stream, materializes{' '}
              <span className="font-mono text-[var(--muted-foreground)]">{BRONZE_STREAM_TABLE}</span>, fans out into append-only silver
              streaming tables, then refreshes gold datasets. Event-derived metrics below reflect the latest{' '}
              <span className="font-mono text-[var(--muted-foreground)]">flow_progress</span> signals returned by the workspace API.
            </p>
          </div>

          {config && config.pipelines.length === 0 ? (
            <div className="rounded-lg border border-[var(--border)] bg-[var(--card)] p-6 text-sm text-[var(--muted-foreground)] ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
              <p className="font-medium text-[var(--foreground)] mb-2">No pipeline IDs configured</p>
              <p className="leading-relaxed text-[var(--muted-foreground)]">
                The bundle sets <span className="font-mono text-[var(--foreground)]">WEARABLE_PIPELINE_NAME</span> (for example{' '}
                <span className="font-mono text-[var(--foreground)]">dbxw-wearable-medallion-dev</span>) and the app resolves it to
                a pipeline UUID. You can instead set <span className="font-mono text-[var(--foreground)]">WEARABLE_PIPELINE_ID</span>{' '}
                under Workspace → Apps → Environment. Confirm the app service principal has{' '}
                <span className="font-mono text-[var(--foreground)]">CAN RUN</span> on the pipeline and can list pipelines.
              </p>
            </div>
          ) : null}

          {config && config.pipelines.length > 0 ? (
            <div className="flex flex-wrap gap-2 md:gap-3">
              {config.pipelines.map((p) => (
                <button
                  key={p.key}
                  type="button"
                  onClick={() => {
                    setSelectedId(p.id);
                    setLoading(true);
                    void fetchPipelineStatusForId(p.id);
                  }}
                  className={`rounded-lg border px-4 py-3 text-left transition-all ${
                    selectedId === p.id
                      ? 'border-sky-500/45 bg-sky-950/40 ring-1 ring-sky-500/20 shadow-md shadow-black/20'
                      : 'border-[var(--border)] bg-[var(--card)] hover:border-[var(--border)] hover:bg-[var(--muted)]'
                  }`}
                >
                  <p className="text-[11px] uppercase tracking-wider text-[var(--muted-foreground)]">{p.key}</p>
                  <p className="text-sm font-medium text-[var(--foreground)] mt-1">{p.label}</p>
                  <p className="text-[11px] font-mono text-[var(--muted-foreground)] mt-1 truncate max-w-[280px]">{p.id}</p>
                </button>
              ))}
            </div>
          ) : null}

          {error ? (
            <div className="rounded-lg border border-red-500/25 bg-red-950/20 px-4 py-3 text-sm text-red-200/95 ring-1 ring-red-500/10">
              {error}
            </div>
          ) : null}

          {loading && selectedId ? (
            <div className="flex items-center gap-2 text-sm text-[var(--muted-foreground)]">
              <Loader2 className="h-4 w-4 animate-spin text-sky-400/80" />
              Loading pipeline status…
            </div>
          ) : null}

          {status && selectedId ? (
            <div className="space-y-8">
              <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6 md:p-8 overflow-x-auto ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
                <p className="text-[11px] font-semibold uppercase tracking-wider text-[var(--muted-foreground)] mb-5">Logical graph</p>
                <div className="min-w-[720px] flex items-stretch justify-between gap-3 text-[11px]">
                  <div className="flex-1 rounded-lg border border-dashed border-[var(--border)] bg-[var(--muted)] p-4 border-l-2 border-l-[var(--muted-foreground)]/35">
                    <p className="text-[var(--muted-foreground)] uppercase tracking-wider mb-2">External</p>
                    <p className="font-mono text-[var(--foreground)] leading-relaxed">UC Delta bronze</p>
                    <p className="text-[var(--muted-foreground)] mt-2">ZeroBus target (readStream)</p>
                  </div>
                  <div className="flex items-center text-[var(--muted-foreground)] px-0.5 font-mono text-sm">→</div>
                  <div className="flex-1 rounded-lg border border-[var(--border)] bg-[var(--muted)] p-4 border-l-2 border-l-emerald-500/45">
                    <p className="text-emerald-400/85 uppercase tracking-wider mb-2">Streaming</p>
                    <p className="font-mono text-[var(--foreground)]">{BRONZE_STREAM_TABLE}</p>
                    <FlowMini insight={bronzeStreamInsight} />
                  </div>
                  <div className="flex items-center text-[var(--muted-foreground)] px-0.5 font-mono text-sm">→</div>
                  <div className="flex-[1.4] rounded-lg border border-[var(--border)] bg-[var(--muted)] p-4 border-l-2 border-l-sky-500/45">
                    <p className="text-sky-400/85 uppercase tracking-wider mb-2">Silver (×{SILVER_STREAMING_TABLES.length})</p>
                    <ul className="font-mono text-[var(--muted-foreground)] space-y-1 max-h-28 overflow-y-auto pr-1">
                      {SILVER_STREAMING_TABLES.map((n) => (
                        <li key={n} className="truncate text-[10px]">
                          {n}
                        </li>
                      ))}
                    </ul>
                  </div>
                  <div className="flex items-center text-[var(--muted-foreground)] px-0.5 font-mono text-sm">→</div>
                  <div className="flex-1 rounded-lg border border-[var(--border)] bg-[var(--muted)] p-4 border-l-2 border-l-amber-500/40">
                    <p className="text-amber-400/85 uppercase tracking-wider mb-2">Gold MV</p>
                    <p className="font-mono text-[var(--foreground)]">{GOLD_MATERIALIZED_TABLES.length} tables</p>
                    <p className="text-[var(--muted-foreground)] mt-2">+ {GOLD_VIEWS.length} views</p>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
                <div className="xl:col-span-1 space-y-4">
                  <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-6 ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
                    <div className="flex items-center justify-between gap-3 mb-4">
                      <h3 className="text-base font-semibold text-[var(--foreground)] flex items-center gap-2">
                        <Activity className="h-5 w-5 text-sky-400/90" />
                        Current run
                      </h3>
                      {workspaceLink ? (
                        <a
                          href={workspaceLink}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs text-sky-400 hover:text-sky-300 inline-flex items-center gap-1 font-medium"
                        >
                          Workspace <ExternalLink className="h-3 w-3" />
                        </a>
                      ) : null}
                    </div>
                    <p className="text-sm text-[var(--muted-foreground)] mb-1">{selectedMeta?.label ?? pipelineName}</p>
                    <p className="text-xs font-mono text-[var(--muted-foreground)] break-all mb-4">{selectedId}</p>
                    <div
                      className={`inline-flex items-center px-3 py-1.5 rounded-md border text-sm font-medium ${stateColor(topState)}`}
                    >
                      {topState}
                    </div>
                    <dl className="mt-6 space-y-2.5 text-sm">
                      <div className="flex justify-between gap-4">
                        <dt className="text-[var(--muted-foreground)]">Health</dt>
                        <dd className="text-[var(--foreground)] font-mono text-xs">{status.pipeline?.health ?? '—'}</dd>
                      </div>
                      <div className="flex justify-between gap-4">
                        <dt className="text-[var(--muted-foreground)]">Update started</dt>
                        <dd className="text-[var(--foreground)] text-right tabular-nums text-xs">{formatTs(status.latest_update?.creation_time)}</dd>
                      </div>
                      <div className="flex justify-between gap-4">
                        <dt className="text-[var(--muted-foreground)]">Update finished</dt>
                        <dd className="text-[var(--foreground)] text-right tabular-nums text-xs">{formatTs(status.latest_update?.complete_time)}</dd>
                      </div>
                    </dl>
                    <button
                      type="button"
                      disabled={triggering || !config?.workspace_api_configured}
                      onClick={() => void triggerRun()}
                      className="mt-6 w-full gradient-red inline-flex items-center justify-center gap-2 rounded-lg px-4 py-3 text-sm font-semibold text-white shadow-md shadow-[var(--dbx-lava-600)]/25 hover:opacity-95 disabled:opacity-50 disabled:hover:opacity-50 transition-opacity"
                    >
                      {triggering ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                      Trigger pipeline update
                    </button>
                  </div>

                  <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-5 ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
                    <h3 className="text-sm font-semibold text-[var(--foreground)] mb-2">Progress payload</h3>
                    <p className="text-xs text-[var(--muted-foreground)] mb-3">
                      <span className="font-mono text-[var(--muted-foreground)]">latest_update.progress</span> (shape varies by runtime).
                    </p>
                    <pre className="text-[11px] leading-relaxed font-mono text-emerald-200/90 bg-[var(--muted)] rounded-lg p-3 max-h-52 overflow-auto border border-[var(--border)]">
                      {status.latest_update?.progress != null
                        ? JSON.stringify(status.latest_update.progress, null, 2)
                        : '—'}
                    </pre>
                  </div>
                </div>

                <div className="xl:col-span-2 space-y-6">
                  <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] overflow-hidden ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
                    <div className="px-5 py-4 border-b border-[var(--border)] flex items-center justify-between bg-[var(--muted)]">
                      <h3 className="text-sm font-semibold text-[var(--foreground)]">Silver streaming tables — latest flow signals</h3>
                      <span className="text-xs text-[var(--muted-foreground)]">from pipeline events</span>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead className="bg-[var(--muted)] text-left text-xs uppercase tracking-wide text-[var(--muted-foreground)]">
                          <tr>
                            <th className="px-4 py-3 font-medium">Dataset</th>
                            <th className="px-4 py-3 font-medium">Status</th>
                            <th className="px-4 py-3 font-medium text-right">Output rows</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-[var(--border)]">
                          {silverInsights.map(({ name, insight }) => (
                            <tr key={name} className="hover:bg-[var(--card)] transition-colors">
                              <td className="px-4 py-2.5 font-mono text-[11px] text-[var(--muted-foreground)]">{name}</td>
                              <td className="px-4 py-2.5">
                                {insight?.status ? (
                                  <span
                                    className={`inline-flex items-center gap-1.5 text-xs font-medium px-2 py-0.5 rounded border ${statusBadgeClass(insight.status)}`}
                                  >
                                    <DltStateIcon state={insight.status} className="h-3.5 w-3.5 shrink-0" />
                                    {insight.status}
                                  </span>
                                ) : (
                                  <span className="text-[var(--muted-foreground)] text-xs">No event match</span>
                                )}
                              </td>
                              <td className="px-4 py-2.5 text-right tabular-nums text-[var(--muted-foreground)] text-xs">
                                {formatInt(insight?.num_output_rows ?? insight?.num_upserted_rows)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] overflow-hidden ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
                    <div className="px-5 py-4 border-b border-[var(--border)] flex items-center justify-between bg-[var(--muted)]">
                      <h3 className="text-sm font-semibold text-[var(--foreground)]">Recent updates</h3>
                      <span className="text-xs text-[var(--muted-foreground)]">{status.updates.length} loaded</span>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead className="bg-[var(--muted)] text-left text-xs uppercase tracking-wide text-[var(--muted-foreground)]">
                          <tr>
                            <th className="px-4 py-3 font-medium">Update ID</th>
                            <th className="px-4 py-3 font-medium">State</th>
                            <th className="px-4 py-3 font-medium text-right">Started</th>
                            <th className="px-4 py-3 font-medium text-right">Ended</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-[var(--border)]">
                          {status.updates.map((u, i) => (
                            <tr
                              key={u.update_id ?? i}
                              className={i % 2 === 0 ? 'bg-[var(--muted)] hover:bg-[var(--card)]' : 'hover:bg-[var(--card)]'}
                            >
                              <td className="px-4 py-2.5 font-mono text-[11px] text-[var(--muted-foreground)] truncate max-w-[200px]">
                                {u.update_id ?? '—'}
                              </td>
                              <td className="px-4 py-2.5">
                                <span
                                  className={`inline-flex items-center gap-1.5 text-xs font-medium px-2 py-0.5 rounded border ${stateColor(u.state)}`}
                                >
                                  <DltStateIcon state={u.state} className="h-3.5 w-3.5 shrink-0" />
                                  {u.state ?? '—'}
                                </span>
                              </td>
                              <td className="px-4 py-2.5 text-right text-[var(--muted-foreground)] tabular-nums text-xs">
                                {formatTs(u.creation_time)}
                              </td>
                              <td className="px-4 py-2.5 text-right text-[var(--muted-foreground)] tabular-nums text-xs">
                                {formatTs(u.complete_time)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div className="rounded-xl border border-[var(--border)] bg-[var(--muted)] overflow-hidden ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
                    <div className="px-5 py-3 border-b border-[var(--border)] flex items-center justify-between bg-[var(--card)]">
                      <h3 className="text-sm font-semibold text-[var(--foreground)]">Event log</h3>
                      <span className="text-xs text-[var(--muted-foreground)]">{status.events.length} events</span>
                    </div>
                    <div className="max-h-[320px] overflow-y-auto p-3 font-mono text-[11px] leading-relaxed space-y-1.5">
                      {status.events.length === 0 ? (
                        <p className="text-[var(--muted-foreground)] px-2 py-6 text-center">No events returned for this poll.</p>
                      ) : (
                        status.events.map((ev, i) => (
                          <div
                            key={`${ev.time_stamp ?? ''}-${i}`}
                            className="flex gap-2 text-[var(--muted-foreground)] rounded-md px-1.5 py-0.5 hover:bg-[var(--muted)]"
                          >
                            <span className="text-[var(--muted-foreground)] shrink-0 w-36">{ev.time_stamp ?? '—'}</span>
                            <span
                              className={`shrink-0 w-28 ${
                                (ev.level ?? '').toUpperCase() === 'ERROR' ? 'text-red-400' : 'text-sky-400/90'
                              }`}
                            >
                              {ev.event_type || ev.level || ''}
                            </span>
                            <span className="text-[var(--foreground)] break-words">{ev.message ?? ''}</span>
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          ) : null}
        </section>

        {/* Section 3 — Gold */}
        <section className="space-y-6 pb-8">
          <div>
            <h2 className="text-xl md:text-2xl font-bold text-[var(--foreground)] flex items-center gap-3">
              <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-amber-500/15 border border-amber-500/25 text-amber-200/95 text-sm font-mono tabular-nums">
                3
              </span>
              Gold layer — materialized analytics
            </h2>
            <p className="text-sm text-[var(--muted-foreground)] mt-2 max-w-3xl leading-relaxed">
              Each card is a declared gold table in the medallion module. When the workspace emits matching{' '}
              <span className="font-mono text-[var(--muted-foreground)]">flow_progress</span> events, status and row counts appear
              automatically. If a card shows “No event match”, the pipeline may still be healthy—event sampling is
              bounded to the latest poll window.
            </p>
          </div>

          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {goldInsights.map(({ name, insight }) => (
              <div
                key={name}
                className="rounded-xl border border-[var(--border)] bg-gradient-to-br from-[var(--card)] via-[var(--muted)] to-[var(--background)] p-5 flex flex-col gap-3 ring-1 ring-amber-500/15 shadow-lg shadow-black/10"
              >
                <div className="flex items-start justify-between gap-2">
                  <p className="font-mono text-[11px] text-amber-100/85 leading-snug break-all">{name}</p>
                  {insight?.status === 'COMPLETED' ? (
                    <CheckCircle2 className="h-4 w-4 text-emerald-400 shrink-0" />
                  ) : insight?.status ? (
                    <CircleDot className="h-4 w-4 text-sky-400 shrink-0" />
                  ) : null}
                </div>
                <div className="flex items-baseline justify-between gap-2">
                  <span className="text-xs text-[var(--muted-foreground)] uppercase tracking-wider">Output rows</span>
                  <span className="text-xl font-semibold text-[var(--foreground)] tabular-nums tracking-tight">
                    {formatInt(insight?.num_output_rows ?? insight?.num_upserted_rows)}
                  </span>
                </div>
                <div>
                  {insight?.status ? (
                    <span className={`text-xs font-medium px-2 py-1 rounded border ${statusBadgeClass(insight.status)}`}>
                      {insight.status}
                    </span>
                  ) : (
                    <span className="text-xs text-[var(--muted-foreground)]">No event match in current window</span>
                  )}
                </div>
                {insight?.dataset_name && tableSuffix(insight.dataset_name) !== name ? (
                  <p className="text-[10px] text-[var(--muted-foreground)] font-mono truncate" title={insight.dataset_name}>
                    Matched: {insight.dataset_name}
                  </p>
                ) : null}
              </div>
            ))}
          </div>

          <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] px-5 py-4 ring-1 ring-black/[0.04] dark:ring-white/[0.06]">
            <p className="text-xs font-semibold uppercase tracking-wider text-[var(--muted-foreground)] mb-2">Presentation views</p>
            <div className="flex flex-wrap gap-2">
              {GOLD_VIEWS.map((v) => (
                <span
                  key={v}
                  className="font-mono text-[11px] text-[var(--muted-foreground)] bg-[var(--muted)] border border-[var(--border)] px-2 py-1 rounded-md"
                >
                  {v}
                </span>
              ))}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}

function FlowMini({ insight }: { insight: FlowInsight | undefined }) {
  if (!insight?.status && insight?.num_output_rows == null && insight?.num_upserted_rows == null) {
    return <p className="text-[10px] text-[var(--muted-foreground)] mt-2">Awaiting metrics…</p>;
  }
  return (
    <div className="mt-2 space-y-1 text-[10px] text-[var(--muted-foreground)]">
      {insight.status ? (
        <p>
          <span className="text-[var(--muted-foreground)]">status</span>{' '}
          <span className="text-emerald-200/90">{insight.status}</span>
        </p>
      ) : null}
      {(insight.num_output_rows != null || insight.num_upserted_rows != null) && (
        <p className="tabular-nums">
          rows {formatInt(insight.num_output_rows ?? insight.num_upserted_rows)}
        </p>
      )}
    </div>
  );
}
