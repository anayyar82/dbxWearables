import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import {
  Copy,
  Check,
  ChevronDown,
  ChevronRight,
  ArrowUpRight,
  Tag,
  AlertCircle,
  Sparkles,
} from 'lucide-react';
import { BrandIcon } from '@/components/BrandIcon';
import { generateBatchNdjson, TEMPLATE_BY_TYPE, type DemoRecordType } from './demoNdjson';

/* ═══════════════════════════════════════════════════════════════════
   DocsPage — API Documentation (Swagger-style)
   Interactive docs for POST /api/v1/healthkit/ingest and
   GET /api/v1/healthkit/health
   ═══════════════════════════════════════════════════════════════════ */

export function DocsPage() {
  return (
    <div className="max-w-5xl mx-auto py-12 px-6">
      {/* Header */}
      <div className="mb-10">
        <div className="flex items-center gap-3 mb-2">
          <img src="/images/apps-lockup-no-db-full-color.svg" alt="Databricks Apps" className="h-8" />
          <h1 className="text-3xl font-bold text-[var(--foreground)]">API Documentation</h1>
        </div>
        <p className="text-[var(--muted-foreground)]">
          REST API reference for the dbxWearables ZeroBus Health Data Gateway.
        </p>
        <div className="flex items-center gap-4 mt-4 text-sm">
          <span className="bg-[var(--dbx-green-600)]/10 text-[var(--dbx-green-600)] px-3 py-1 rounded-full font-medium">
            v1.0
          </span>
          <span className="text-[var(--muted-foreground)]">
            Base URL: <code className="bg-[var(--muted)] px-2 py-0.5 rounded text-xs font-mono">/api/v1</code>
          </span>
        </div>
      </div>

      {/* Endpoints */}
      <div className="space-y-6">
        <IngestEndpoint />
        <HealthEndpoint />
      </div>

      {/* Record types reference */}
      <RecordTypesRef />

      {/* Error codes */}
      <ErrorCodesRef />
    </div>
  );
}

/* ── POST /api/v1/healthkit/ingest ────────────────────────────────── */
function IngestEndpoint() {
  const [expanded, setExpanded] = useState(false);
  const [tryItOpen, setTryItOpen] = useState(false);

  return (
    <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl overflow-hidden">
      {/* Endpoint header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-4 p-5 hover:bg-[var(--muted)]/50 transition-colors"
      >
        <span className="px-3 py-1 rounded-md text-xs font-bold uppercase tracking-wider bg-[var(--dbx-green-600)] text-white">
          POST
        </span>
        <code className="text-sm font-mono font-bold text-[var(--foreground)] flex-1 text-left">
          /api/v1/healthkit/ingest
        </code>
        <span className="text-xs text-[var(--muted-foreground)]">Ingest HealthKit data via NDJSON</span>
        {expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
      </button>

      {expanded && (
        <div className="border-t border-[var(--border)] p-6 space-y-6">
          <p className="text-sm text-[var(--muted-foreground)] leading-relaxed">
            Receives NDJSON (Newline Delimited JSON) payloads from the iOS HealthKit app
            and streams each record to the Unity Catalog bronze table via the ZeroBus REST API.
            Each line in the NDJSON body becomes a separate record in the bronze table.
          </p>

          {/* Headers */}
          <div>
            <h4 className="font-bold text-sm text-[var(--foreground)] mb-3 flex items-center gap-2">
              <Tag className="h-4 w-4 text-[var(--dbx-lava-600)]" />
              Request Headers
            </h4>
            <div className="border border-[var(--border)] rounded-lg overflow-hidden">
              <table className="w-full text-sm">
                <thead className="bg-[var(--muted)]">
                  <tr>
                    <th className="text-left py-2 px-4 font-medium text-[var(--muted-foreground)]">Header</th>
                    <th className="text-left py-2 px-4 font-medium text-[var(--muted-foreground)]">Required</th>
                    <th className="text-left py-2 px-4 font-medium text-[var(--muted-foreground)]">Description</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[var(--border)]">
                  <tr>
                    <td className="py-2.5 px-4 font-mono text-xs text-[var(--dbx-lava-500)]">Content-Type</td>
                    <td className="py-2.5 px-4"><RequiredBadge /></td>
                    <td className="py-2.5 px-4 text-xs text-[var(--muted-foreground)]">
                      <code>application/x-ndjson</code>, <code>application/ndjson</code>, or <code>text/plain</code>
                    </td>
                  </tr>
                  <tr>
                    <td className="py-2.5 px-4 font-mono text-xs text-[var(--dbx-lava-500)]">X-Record-Type</td>
                    <td className="py-2.5 px-4"><RequiredBadge /></td>
                    <td className="py-2.5 px-4 text-xs text-[var(--muted-foreground)]">
                      Any non-empty string identifying the payload type:
                      <code className="ml-1">samples</code>, <code>workouts</code>, <code>sleep</code>, <code>activity_summaries</code>, <code>deletes</code>
                    </td>
                  </tr>
                  <tr>
                    <td className="py-2.5 px-4 font-mono text-xs text-[var(--dbx-lava-500)]">X-Platform</td>
                    <td className="py-2.5 px-4"><OptionalBadge /></td>
                    <td className="py-2.5 px-4 text-xs text-[var(--muted-foreground)]">
                      Source platform identifier (e.g., <code>ios</code>, <code>android</code>). Defaults to <code>unknown</code>.
                    </td>
                  </tr>
                  <tr>
                    <td className="py-2.5 px-4 font-mono text-xs text-[var(--dbx-lava-500)]">Authorization</td>
                    <td className="py-2.5 px-4"><OptionalBadge /></td>
                    <td className="py-2.5 px-4 text-xs text-[var(--muted-foreground)]">
                      <code>Bearer &lt;JWT&gt;</code> — Direct client auth. Token&apos;s <code>sub</code> claim becomes user_id.
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {/* Request body */}
          <div>
            <h4 className="font-bold text-sm text-[var(--foreground)] mb-3">Request Body</h4>
            <p className="text-xs text-[var(--muted-foreground)] mb-3">
              NDJSON format — one JSON object per line. Each line is parsed independently.
              Maximum 10,000 lines per request. Maximum body size: 10MB.
            </p>
            <CodeBlock
              title="Example: samples (snake_case — matches iOS NDJSON and DLT parsers)"
              code={`{"uuid":"…","type":"HKQuantityTypeIdentifierStepCount","value":8432,"unit":"count","start_date":"2026-01-15T08:00:00Z","end_date":"2026-01-15T08:30:00Z","source_name":"com.apple.health","source_bundle_id":"com.apple.Health"}
{"uuid":"…","type":"HKQuantityTypeIdentifierHeartRate","value":72,"unit":"count/min","start_date":"2026-01-15T08:15:00Z","end_date":"2026-01-15T08:15:00Z","source_name":"com.apple.health"}`}
            />
          </div>

          {/* Response */}
          <div>
            <h4 className="font-bold text-sm text-[var(--foreground)] mb-3">Success Response</h4>
            <CodeBlock
              title="200 OK"
              code={`{
  "status": "success",
  "message": "2 record(s) ingested",
  "record_id": "a1b2c3d4-...",
  "records_ingested": 2,
  "record_ids": ["a1b2c3d4-...", "e5f6g7h8-..."],
  "duration_ms": 145,
  "pipeline_update": {
    "triggered": true,
    "update_id": "…",
    "pipeline_id": "…",
    "update": { "state": "RUNNING", "update_id": "…", "progress": { } }
  }
}`}
            />
          </div>

          {/* Try it */}
          <div>
            <button
              onClick={() => setTryItOpen(!tryItOpen)}
              className="flex items-center gap-2 text-sm font-medium text-[var(--dbx-lava-600)] hover:text-[var(--dbx-lava-500)] transition-colors"
            >
              <BrandIcon name="data-flow" className="h-4 w-4" />
              Try it out
              {tryItOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            </button>
            {tryItOpen && <TryItPanel />}
          </div>

          {/* cURL example */}
          <div>
            <h4 className="font-bold text-sm text-[var(--foreground)] mb-3 flex items-center gap-2">
              <ArrowUpRight className="h-4 w-4 text-[var(--dbx-lava-600)]" />
              cURL Example
            </h4>
            <CodeBlock
              title="bash"
              code={`curl -X POST /api/v1/healthkit/ingest \\
  -H "Content-Type: application/x-ndjson" \\
  -H "X-Record-Type: samples" \\
  -H "X-Platform: ios" \\
  -d '{"uuid":"00000000-0000-4000-8000-000000000001","type":"HKQuantityTypeIdentifierStepCount","value":8432,"unit":"count","start_date":"2026-01-15T08:00:00Z","end_date":"2026-01-15T08:30:00Z","source_name":"com.apple.health"}'`}
            />
          </div>
        </div>
      )}
    </div>
  );
}

/* ── GET /api/v1/healthkit/health ─────────────────────────────────── */
function HealthEndpoint() {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-4 p-5 hover:bg-[var(--muted)]/50 transition-colors"
      >
        <span className="px-3 py-1 rounded-md text-xs font-bold uppercase tracking-wider bg-blue-500 text-white">
          GET
        </span>
        <code className="text-sm font-mono font-bold text-[var(--foreground)] flex-1 text-left">
          /api/v1/healthkit/health
        </code>
        <span className="text-xs text-[var(--muted-foreground)]">Health / readiness check</span>
        {expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
      </button>

      {expanded && (
        <div className="border-t border-[var(--border)] p-6 space-y-6">
          <p className="text-sm text-[var(--muted-foreground)] leading-relaxed">
            Lightweight health and readiness check. Returns the ZeroBus configuration status,
            target table name, and lists any missing environment variables.
          </p>

          <div>
            <h4 className="font-bold text-sm text-[var(--foreground)] mb-3">Response</h4>
            <CodeBlock
              title="200 OK"
              code={`{
  "status": "ok",
  "service": "zerobus-healthkit-ingest",
  "env_configured": true,
  "target_table": "hls_fde.wearables.wearables_zerobus"
}`}
            />
          </div>

          <div>
            <h4 className="font-bold text-sm text-[var(--foreground)] mb-3">Response (missing config)</h4>
            <CodeBlock
              title="200 OK"
              code={`{
  "status": "ok",
  "service": "zerobus-healthkit-ingest",
  "env_configured": false,
  "target_table": "(not set)",
  "missing_env_vars": ["ZEROBUS_ENDPOINT", "ZEROBUS_TARGET_TABLE"]
}`}
            />
          </div>

          <div>
            <h4 className="font-bold text-sm text-[var(--foreground)] mb-3 flex items-center gap-2">
              <ArrowUpRight className="h-4 w-4 text-[var(--dbx-lava-600)]" />
              cURL Example
            </h4>
            <CodeBlock
              title="bash"
              code={`curl -s /api/v1/healthkit/health | python3 -m json.tool`}
            />
          </div>
        </div>
      )}
    </div>
  );
}

const DLT_TERMINAL_STATES = new Set([
  'COMPLETED',
  'FAILED',
  'CANCELED',
  'CANCELLED',
  'TIMEDOUT',
  'TIMED_OUT',
]);

function isDltTerminalState(state: string | undefined): boolean {
  if (!state) return false;
  return DLT_TERMINAL_STATES.has(state.toUpperCase());
}

/* ── Try It Panel ─────────────────────────────────────────────────── */
function TryItPanel() {
  const [recordType, setRecordType] = useState<DemoRecordType>('samples');
  const [lineCount, setLineCount] = useState(25);
  const [spanDays, setSpanDays] = useState(30);
  const [demoUserSlots, setDemoUserSlots] = useState(6);
  const [body, setBody] = useState(TEMPLATE_BY_TYPE.samples);
  const [response, setResponse] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<number | null>(null);
  const [progressLine, setProgressLine] = useState<string | null>(null);
  const [dltPoll, setDltPoll] = useState<{ pid: string; uid: string } | null>(null);
  const [dltUpdate, setDltUpdate] = useState<Record<string, unknown> | null>(null);
  const [dltPollEnded, setDltPollEnded] = useState(false);

  useEffect(() => {
    if (!dltPoll) return;
    let cancelled = false;
    let polls = 0;
    let intervalId: ReturnType<typeof setInterval> | undefined;

    const pollOnce = async (): Promise<boolean> => {
      try {
        const r = await fetch(
          `/api/pipelines/${encodeURIComponent(dltPoll.pid)}/updates/${encodeURIComponent(dltPoll.uid)}`,
        );
        const j = (await r.json()) as { update?: Record<string, unknown>; error?: string };
        if (cancelled) return true;
        if (!r.ok) {
          setDltUpdate({ error: j.error ?? r.statusText, http_status: r.status });
          return true;
        }
        if (j.update) setDltUpdate(j.update);
        else setDltUpdate({ error: j.error ?? `HTTP ${r.status}` });
        const st = j.update?.state as string | undefined;
        if (isDltTerminalState(st)) return true;
      } catch (e) {
        if (!cancelled) setDltUpdate({ error: String(e) });
        return true;
      }
      polls += 1;
      if (polls >= 90) {
        if (!cancelled) {
          setDltUpdate((u) => ({ ...(u ?? {}), poll_notice: 'Stopped polling after ~3 minutes.' }));
        }
        return true;
      }
      return false;
    };

    void (async () => {
      setDltPollEnded(false);
      const doneImmediately = await pollOnce();
      if (cancelled) return;
      if (doneImmediately) {
        setDltPollEnded(true);
        return;
      }
      intervalId = setInterval(() => {
        void (async () => {
          const done = await pollOnce();
          if (done && intervalId) clearInterval(intervalId);
          if (done && !cancelled) setDltPollEnded(true);
        })();
      }, 2000);
    })();

    return () => {
      cancelled = true;
      if (intervalId) clearInterval(intervalId);
    };
  }, [dltPoll]);

  const setTypeAndTemplate = (t: DemoRecordType) => {
    setRecordType(t);
    setBody(TEMPLATE_BY_TYPE[t]);
  };

  const fillGeneratedBatch = () => {
    setBody(generateBatchNdjson(recordType, lineCount, spanDays, demoUserSlots));
  };

  const lineTotal = body.trim() ? body.trim().split(/\r?\n/).filter((l) => l.length > 0).length : 0;

  const sendRequest = async () => {
    setLoading(true);
    setResponse(null);
    setStatus(null);
    setProgressLine('Sending NDJSON to ingest…');
    setDltPoll(null);
    setDltUpdate(null);
    setDltPollEnded(false);
    try {
      const res = await fetch('/api/v1/healthkit/ingest', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-ndjson',
          'X-Record-Type': recordType,
          'X-Platform': 'web-docs',
        },
        body,
      });
      setStatus(res.status);
      const raw = await res.text();
      let parsed: Record<string, unknown> | null = null;
      try {
        parsed = JSON.parse(raw) as Record<string, unknown>;
        setResponse(JSON.stringify(parsed, null, 2));
      } catch {
        setResponse(raw || '(empty response body)');
      }

      if (res.ok && parsed?.status === 'success') {
        const n = parsed.records_ingested;
        setProgressLine(
          typeof n === 'number'
            ? `Bronze: ${n} row(s) accepted. Checking medallion DLT…`
            : 'Ingest succeeded. Checking medallion DLT…',
        );
        const pu = parsed.pipeline_update as
          | {
              triggered?: boolean;
              update_id?: string;
              pipeline_id?: string;
              update?: Record<string, unknown>;
              error?: string;
              skipped?: string;
            }
          | undefined;
        if (pu?.triggered && pu.update_id && pu.pipeline_id) {
          if (pu.update) setDltUpdate(pu.update);
          setProgressLine((prev) =>
            `${prev ?? ''} Same REST action as workspace “Trigger update”. Polling update ${pu.update_id!.slice(0, 8)}…`,
          );
          setDltPoll({ pid: pu.pipeline_id, uid: pu.update_id });
        } else if (pu && pu.triggered === false) {
          const why = pu.error ?? pu.skipped ?? 'skipped';
          setProgressLine((prev) => `${prev ?? ''} DLT not started: ${why}.`);
        } else {
          setProgressLine((prev) => `${prev ?? ''} No pipeline_update in response (check bundle env).`);
        }
      } else {
        setProgressLine(res.ok ? 'Unexpected response shape.' : `Request finished with HTTP ${res.status}.`);
      }
    } catch (err) {
      setResponse(String(err));
      setStatus(0);
      setProgressLine(`Request failed: ${String(err)}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="mt-4 bg-[var(--muted)] rounded-xl p-5 space-y-4">
      <p className="text-xs text-[var(--muted-foreground)] leading-relaxed">
        Use a single-line template, or generate up to 10,000 NDJSON lines with UUIDs and timestamps spread across the
        last N days (same shape as the iOS app). Each line becomes one bronze row when ZeroBus accepts the batch.
        With <span className="font-mono text-[var(--foreground)]">Demo user slots</span> greater than 1, lines include{' '}
        <span className="font-mono">demo_user_id</span> (rotating demo personas); the server stores it as bronze{' '}
        <span className="font-mono">user_id</span> and removes it from the JSON body so DLT parsers stay unchanged.
      </p>

      <div className="grid sm:grid-cols-2 lg:grid-cols-5 gap-4">
        <div>
          <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1">X-Record-Type</label>
          <select
            value={recordType}
            onChange={(e) => setTypeAndTemplate(e.target.value as DemoRecordType)}
            className="w-full bg-[var(--card)] border border-[var(--border)] rounded-lg px-3 py-2 text-sm font-mono text-[var(--foreground)]"
          >
            {(['samples', 'workouts', 'sleep', 'activity_summaries', 'deletes'] as const).map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1">Lines to generate</label>
          <input
            type="number"
            min={1}
            max={10_000}
            value={lineCount}
            onChange={(e) => setLineCount(Math.max(1, Math.min(10_000, Number(e.target.value) || 1)))}
            className="w-full bg-[var(--card)] border border-[var(--border)] rounded-lg px-3 py-2 text-sm text-[var(--foreground)]"
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1">Date spread (days)</label>
          <input
            type="number"
            min={1}
            max={730}
            value={spanDays}
            onChange={(e) => setSpanDays(Math.max(1, Math.min(730, Number(e.target.value) || 1)))}
            className="w-full bg-[var(--card)] border border-[var(--border)] rounded-lg px-3 py-2 text-sm text-[var(--foreground)]"
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1">Demo user slots</label>
          <input
            type="number"
            min={1}
            max={48}
            value={demoUserSlots}
            onChange={(e) => setDemoUserSlots(Math.max(1, Math.min(48, Number(e.target.value) || 1)))}
            className="w-full bg-[var(--card)] border border-[var(--border)] rounded-lg px-3 py-2 text-sm text-[var(--foreground)]"
            title="1 = default app user for every line; 2+ = rotate demo_user_id per line (matches notebook cohort + synth emails)"
          />
        </div>
        <div className="flex items-end gap-2">
          <button
            type="button"
            onClick={fillGeneratedBatch}
            className="w-full inline-flex items-center justify-center gap-2 rounded-lg border border-[var(--border)] bg-[var(--card)] px-3 py-2 text-sm font-medium text-[var(--foreground)] hover:bg-[var(--muted)]"
          >
            <Sparkles className="h-4 w-4 text-[var(--dbx-lava-600)]" />
            Generate into editor
          </button>
        </div>
      </div>

      <div>
        <div className="flex flex-wrap items-center justify-between gap-2 mb-1">
          <label className="block text-xs font-medium text-[var(--muted-foreground)]">Request body (NDJSON)</label>
          <span className="text-xs text-[var(--muted-foreground)] tabular-nums">{lineTotal} non-empty line(s)</span>
        </div>
        <textarea
          value={body}
          onChange={(e) => setBody(e.target.value)}
          rows={14}
          className="w-full min-h-[140px] bg-[var(--card)] border border-[var(--border)] rounded-lg px-3 py-2 text-xs font-mono text-[var(--foreground)] resize-y"
          placeholder="One JSON object per line..."
        />
      </div>

      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          onClick={() => setBody(TEMPLATE_BY_TYPE[recordType])}
          className="text-xs font-medium text-[var(--dbx-lava-600)] hover:underline"
        >
          Reset to single-line template
        </button>
      </div>

      <button
        type="button"
        onClick={sendRequest}
        disabled={loading || lineTotal === 0}
        className="gradient-red text-white px-5 py-2 rounded-lg text-sm font-medium shadow-md hover:shadow-lg transition-all disabled:opacity-60 flex items-center gap-2"
      >
        <BrandIcon name="data-flow" className="h-4 w-4" />
        {loading ? 'Sending...' : 'Send request'}
      </button>

      {progressLine ? (
        <div
          className={`rounded-lg border px-4 py-3 text-sm ${
            loading
              ? 'border-amber-500/40 bg-amber-950/25 text-amber-100'
              : 'border-[var(--border)] bg-[var(--card)] text-[var(--foreground)]'
          }`}
        >
          <p className="font-medium text-xs uppercase tracking-wide text-[var(--muted-foreground)] mb-1">Progress</p>
          <p className="leading-relaxed">{progressLine}</p>
          {dltPoll ? (
            <div className="mt-3 flex flex-wrap items-center gap-3 text-xs">
              <Link to="/dlt" className="inline-flex items-center gap-1 font-medium text-[var(--dbx-lava-600)] hover:underline">
                Open pipeline status
                <ArrowUpRight className="h-3 w-3" />
              </Link>
              {!dltPollEnded ? (
                <span className="text-[var(--muted-foreground)] inline-flex items-center gap-1">
                  <span className="inline-block h-1.5 w-1.5 rounded-full bg-[var(--dbx-lava-500)] animate-pulse" />
                  Live polling (every 2s)…
                </span>
              ) : (
                <span className="text-[var(--muted-foreground)]">Polling finished.</span>
              )}
            </div>
          ) : null}
          {dltUpdate ? (
            <div className="mt-3 rounded-md border border-[var(--border)] bg-black/20 p-3 font-mono text-[11px] text-[var(--muted-foreground)] max-h-48 overflow-auto">
              <p className="text-[var(--foreground)] mb-1">
                State:{' '}
                <span className="text-[var(--dbx-lava-500)]">{String(dltUpdate.state ?? '—')}</span>
              </p>
              <pre className="whitespace-pre-wrap break-all">
                {JSON.stringify(
                  {
                    update_id: dltUpdate.update_id,
                    state: dltUpdate.state,
                    progress: dltUpdate.progress,
                    update_details: dltUpdate.update_details,
                    error: dltUpdate.error ?? dltUpdate.fetch_error,
                    poll_notice: dltUpdate.poll_notice,
                  },
                  null,
                  2,
                )}
              </pre>
            </div>
          ) : null}
        </div>
      ) : null}

      {response && (
        <div>
          <div className="flex items-center gap-2 mb-2">
            <span className="text-xs font-medium text-[var(--muted-foreground)]">Response</span>
            <span
              className={`text-xs font-mono px-2 py-0.5 rounded ${
                status && status >= 200 && status < 300
                  ? 'bg-emerald-50 text-[var(--dbx-green-600)]'
                  : 'bg-red-50 text-red-600'
              }`}
            >
              {status}
            </span>
          </div>
          <div className="code-block text-xs">
            <pre>{response}</pre>
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Record Types Reference ───────────────────────────────────────── */
function RecordTypesRef() {
  return (
    <div className="mt-12">
      <h2 className="text-xl font-bold text-[var(--foreground)] mb-4">Record Types Reference</h2>
      <div className="border border-[var(--border)] rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-[var(--muted)]">
            <tr>
              <th className="text-left py-3 px-4 font-medium text-[var(--muted-foreground)]">X-Record-Type</th>
              <th className="text-left py-3 px-4 font-medium text-[var(--muted-foreground)]">Payload</th>
              <th className="text-left py-3 px-4 font-medium text-[var(--muted-foreground)]">Description</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--border)]">
            {[
              ['samples', 'HK quantity/category samples', 'Step count, heart rate, distance, energy burned, VO2 max, SpO2, sleep analysis, stand hours'],
              ['workouts', 'Workout records', 'Activity type, duration, energy burned, distance — 70+ activity types'],
              ['sleep', 'Sleep sessions', 'Grouped from contiguous sleep stage samples (inBed, asleepCore, asleepDeep, asleepREM, awake)'],
              ['activity_summaries', 'Daily ring data', 'Active energy, exercise minutes, stand hours with goals'],
              ['deletes', 'Deletion records', 'UUID + sample_type for soft-delete matching on backend'],
            ].map(([type, payload, desc]) => (
              <tr key={type} className="hover:bg-[var(--muted)]/50">
                <td className="py-3 px-4 font-mono text-xs text-[var(--dbx-lava-500)] font-bold">{type}</td>
                <td className="py-3 px-4 text-xs text-[var(--foreground)]">{payload}</td>
                <td className="py-3 px-4 text-xs text-[var(--muted-foreground)]">{desc}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-[var(--muted-foreground)] mt-2 flex items-center gap-1">
        <AlertCircle className="h-3 w-3" />
        Unknown record types are accepted and ingested but logged at warn level for visibility.
      </p>
    </div>
  );
}

/* ── Error Codes Reference ────────────────────────────────────────── */
function ErrorCodesRef() {
  return (
    <div className="mt-10">
      <h2 className="text-xl font-bold text-[var(--foreground)] mb-4">Error Responses</h2>
      <div className="border border-[var(--border)] rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-[var(--muted)]">
            <tr>
              <th className="text-left py-3 px-4 font-medium text-[var(--muted-foreground)]">Status</th>
              <th className="text-left py-3 px-4 font-medium text-[var(--muted-foreground)]">Condition</th>
              <th className="text-left py-3 px-4 font-medium text-[var(--muted-foreground)]">Example Message</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--border)]">
            {[
              ['400', 'Missing X-Record-Type header', 'Missing X-Record-Type header. Provide any non-empty string...'],
              ['400', 'Empty request body', 'Request body is empty. Expected NDJSON...'],
              ['400', 'No valid JSON lines', 'No valid records found. Parse errors: Line 1: invalid JSON'],
              ['500', 'ZeroBus SDK failure', 'Ingestion failed: stream write error'],
            ].map(([code, condition, msg], i) => (
              <tr key={i} className="hover:bg-[var(--muted)]/50">
                <td className="py-3 px-4">
                  <span className={`font-mono text-xs font-bold px-2 py-0.5 rounded ${
                    code === '400' ? 'bg-amber-50 text-amber-600' : 'bg-red-50 text-red-600'
                  }`}>
                    {code}
                  </span>
                </td>
                <td className="py-3 px-4 text-xs text-[var(--foreground)]">{condition}</td>
                <td className="py-3 px-4 text-xs text-[var(--muted-foreground)] font-mono">{msg}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ── Shared components ────────────────────────────────────────────── */
function RequiredBadge() {
  return <span className="text-xs font-medium px-2 py-0.5 rounded bg-red-50 text-red-600">required</span>;
}

function OptionalBadge() {
  return <span className="text-xs font-medium px-2 py-0.5 rounded bg-gray-100 text-gray-500">optional</span>;
}

function CodeBlock({ title, code }: { title: string; code: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative">
      <div className="flex items-center justify-between bg-[#0D1117] rounded-t-lg px-4 py-2 border-b border-white/10">
        <span className="text-xs text-gray-400 font-mono">{title}</span>
        <button
          onClick={handleCopy}
          className="text-gray-400 hover:text-white transition-colors p-1"
          title="Copy to clipboard"
        >
          {copied ? <Check className="h-3.5 w-3.5 text-[var(--dbx-green-600)]" /> : <Copy className="h-3.5 w-3.5" />}
        </button>
      </div>
      <div className="code-block rounded-t-none">
        <pre className="whitespace-pre-wrap break-all">{code}</pre>
      </div>
    </div>
  );
}
