import { CheckCircle2, Loader2 } from 'lucide-react';
import { useId } from 'react';
import type { MedallionStep, MedallionStepKind } from './medallionSteps';

/** Scoped styles — keyframes for flowing edges + “running” arrow (respects reduced motion). */
const MDAG_STYLES = `
.mdag-scope .mdag-edge-path--live {
  stroke-dasharray: 5 11;
  stroke-dashoffset: 0;
  animation: mdag-dashflow 1.15s linear infinite;
}
.mdag-scope .mdag-edge-path--live.mdag-edge-path--rush {
  animation-duration: 0.62s;
}
.mdag-scope .mdag-edge-path--wait {
  stroke-dasharray: 3 12;
  stroke-dashoffset: 0;
  animation: mdag-dashflow 2.15s linear infinite;
  opacity: 0.82;
}
@keyframes mdag-dashflow {
  to { stroke-dashoffset: -32; }
}
.mdag-scope .mdag-arrow-rot--live {
  animation: mdag-arrowrun 0.9s ease-in-out infinite;
}
.mdag-scope .mdag-arrow-rot--rush {
  animation: mdag-arrowrun 0.55s ease-in-out infinite;
}
.mdag-scope .mdag-arrow-rot--wait {
  animation: mdag-arrowrun 1.7s ease-in-out infinite;
}
@keyframes mdag-arrowrun {
  0%, 100% { transform: translateY(0) scaleY(1); opacity: 0.88; }
  45% { transform: translateY(3px) scaleY(1.1); opacity: 1; }
}
.mdag-scope .mdag-pulse-dot {
  animation: mdag-dotpulse 1.1s ease-in-out infinite;
}
.mdag-scope .mdag-pulse-dot--rush {
  animation-duration: 0.6s;
}
@keyframes mdag-dotpulse {
  0%, 100% { opacity: 0.35; transform: translate(-50%, -50%) scale(0.85); }
  50% { opacity: 1; transform: translate(-50%, -50%) scale(1.15); }
}
@media (prefers-reduced-motion: reduce) {
  .mdag-scope .mdag-edge-path--live,
  .mdag-scope .mdag-edge-path--wait { animation: none; stroke-dasharray: none; }
  .mdag-scope .mdag-arrow-rot--live,
  .mdag-scope .mdag-arrow-rot--rush,
  .mdag-scope .mdag-arrow-rot--wait { animation: none; }
  .mdag-scope .mdag-pulse-dot,
  .mdag-scope .mdag-pulse-dot--rush { animation: none; opacity: 0.75; }
}
`;

function nodeShellClass(kind: MedallionStepKind): string {
  const base =
    'relative w-full max-w-[300px] rounded-2xl px-5 py-4 transition-all duration-500 ease-out overflow-hidden';
  switch (kind) {
    case 'pending':
      return `${base} border border-[var(--border)] bg-[var(--muted)] text-[var(--muted-foreground)] shadow-[inset_0_1px_0_rgba(255,255,255,0.04)] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]`;
    case 'active':
      return `${base} border border-sky-400/55 bg-gradient-to-br from-sky-950/40 via-[var(--card)] to-sky-950/25 text-[var(--foreground)] shadow-[0_0_28px_-10px_rgba(56,189,248,0.35)] dark:via-[#0a1620]/80`;
    case 'done':
      return `${base} border border-emerald-500/35 bg-emerald-500/[0.08] text-[var(--foreground)] shadow-[inset_0_0_0_1px_rgba(16,185,129,0.12)]`;
    case 'warn':
      return `${base} border border-amber-400/40 bg-amber-950/20 text-amber-100`;
    case 'error':
      return `${base} border border-red-400/45 bg-red-950/25 text-red-100`;
    default:
      return base;
  }
}

type EdgeMotion = 'off' | 'static' | 'wait' | 'rush';

function edgeMotionFor(
  prevKind: MedallionStepKind,
  nextKind: MedallionStepKind,
  dltPipelineActive: boolean,
): EdgeMotion {
  if (nextKind === 'active') return 'rush';
  if (dltPipelineActive && prevKind === 'active' && nextKind === 'pending') return 'wait';
  if (prevKind === 'done' || prevKind === 'warn' || prevKind === 'error' || prevKind === 'active') return 'static';
  return 'off';
}

function DagConnector({
  motion,
  gradId,
}: {
  motion: EdgeMotion;
  gradId: string;
}) {
  const live = motion !== 'off';
  const rush = motion === 'rush';
  const wait = motion === 'wait';

  return (
    <div className="relative flex h-[52px] w-10 shrink-0 items-stretch justify-center select-none" aria-hidden>
      <svg className="absolute inset-0 h-full w-full overflow-visible" viewBox="0 0 40 52" preserveAspectRatio="xMidYMid meet">
        <defs>
          <linearGradient id={gradId} x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="rgba(56, 189, 248, 0.95)" />
            <stop offset="55%" stopColor="rgba(14, 165, 233, 0.85)" />
            <stop offset="100%" stopColor="rgba(52, 211, 153, 0.98)" />
          </linearGradient>
        </defs>
        {!live ? (
          <path
            d="M 20 4 L 20 38"
            fill="none"
            stroke="rgba(63, 63, 70, 0.65)"
            strokeWidth="2"
            strokeLinecap="round"
          />
        ) : motion === 'static' ? (
          <path
            d="M 20 4 L 20 38"
            fill="none"
            stroke={`url(#${gradId})`}
            strokeWidth="2.25"
            strokeLinecap="round"
            opacity={0.88}
          />
        ) : (
          <path
            className={`${rush ? 'mdag-edge-path--live mdag-edge-path--rush' : 'mdag-edge-path--wait'}`}
            d="M 20 4 L 20 38"
            fill="none"
            stroke={`url(#${gradId})`}
            strokeWidth="2.5"
            strokeLinecap="round"
          />
        )}
        <g
          className={`mdag-arrow-rot${
            rush ? ' mdag-arrow-rot--live mdag-arrow-rot--rush' : wait ? ' mdag-arrow-rot--live mdag-arrow-rot--wait' : live ? '' : ''
          }`}
          style={{ transformOrigin: '20px 40px' }}
        >
          <path
            d="M 20 40 L 12 52 L 28 52 Z"
            fill={live ? `url(#${gradId})` : 'rgba(63, 63, 70, 0.45)'}
            opacity={live ? 0.95 : 0.5}
            stroke={live ? 'rgba(167, 243, 208, 0.35)' : 'none'}
            strokeWidth={live ? 0.6 : 0}
            strokeLinejoin="round"
          />
        </g>
      </svg>
      {live && rush ? (
        <>
          <div
            className="pointer-events-none absolute left-1/2 top-[6px] h-2 w-2 -translate-x-1/2 rounded-full bg-sky-300/90 blur-[1.5px] mdag-pulse-dot mdag-pulse-dot--rush"
            style={{ animationDelay: '0s' }}
          />
          <div
            className="pointer-events-none absolute left-1/2 top-[18px] h-1.5 w-1.5 -translate-x-1/2 rounded-full bg-emerald-300/80 blur-[1px] mdag-pulse-dot mdag-pulse-dot--rush"
            style={{ animationDelay: '0.35s' }}
          />
        </>
      ) : live && wait ? (
        <div
          className="pointer-events-none absolute left-1/2 top-[12px] h-1.5 w-1.5 -translate-x-1/2 rounded-full bg-amber-300/75 blur-[1px] mdag-pulse-dot"
          style={{ animationDuration: '1.4s' }}
        />
      ) : live ? (
        <div className="pointer-events-none absolute left-1/2 top-[14px] h-1.5 w-1.5 -translate-x-1/2 rounded-full bg-emerald-300/70 blur-[1px] mdag-pulse-dot" />
      ) : null}
    </div>
  );
}

function DagNode({ step }: { step: MedallionStep }) {
  const { kind, title, subtitle, Icon } = step;
  const active = kind === 'active';

  return (
    <div className="relative flex justify-center">
      <div className={nodeShellClass(kind)}>
        {active ? (
          <>
            <div
              className="pointer-events-none absolute right-0 top-[22%] bottom-[26%] w-[3px] rounded-full bg-gradient-to-b from-transparent via-emerald-400/90 to-emerald-300 shadow-[0_0_12px_rgba(52,211,153,0.65)] animate-pulse"
              aria-hidden
            />
            <span
              className="pointer-events-none absolute bottom-2.5 right-2.5 h-2 w-2 rounded-full bg-emerald-400 ring-2 ring-emerald-400/30 shadow-[0_0_10px_3px_rgba(52,211,153,0.45)]"
              aria-hidden
            />
          </>
        ) : null}

        <div className="flex items-start gap-3 pr-1">
          <div
            className={`mt-0.5 flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border transition-colors ${
              kind === 'pending'
                ? 'border-[var(--border)] bg-[var(--card)]'
                : kind === 'active'
                  ? 'border-sky-400/40 bg-sky-500/15 text-sky-200'
                  : kind === 'done'
                    ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200'
                    : kind === 'warn'
                      ? 'border-amber-400/35 bg-amber-500/10 text-amber-200'
                      : 'border-red-400/35 bg-red-500/10 text-red-200'
            }`}
          >
            <Icon className={`h-5 w-5 ${kind === 'pending' ? 'opacity-45' : ''}`} aria-hidden />
          </div>
          <div className="min-w-0 flex-1 text-left pt-0.5">
            <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-[var(--muted-foreground)]">{title}</p>
            <p className="text-xs text-[var(--muted-foreground)] mt-1 leading-snug font-mono break-words line-clamp-3">{subtitle}</p>
          </div>
          <div className="shrink-0 pt-1">
            {kind === 'active' ? (
              <Loader2 className="h-4 w-4 text-sky-300 animate-spin" aria-label="Running" />
            ) : kind === 'done' ? (
              <CheckCircle2 className="h-4 w-4 text-emerald-400/90" aria-label="Complete" />
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}

export type MedallionVerticalDagProps = {
  steps: MedallionStep[];
  /** When the workspace DLT update is in a running state — intensifies edge + arrow motion. */
  dltPipelineActive?: boolean;
};

/**
 * Databricks DLT–inspired vertical flow: grid canvas, animated “flow” on live edges,
 * running chevron, and green progress rail on the active stage.
 */
export function MedallionVerticalDag({ steps, dltPipelineActive = false }: MedallionVerticalDagProps) {
  const reactId = useId();
  const baseId = reactId.replace(/[^a-zA-Z0-9_-]/g, '');

  return (
    <div className="mdag-scope relative rounded-xl border border-[var(--border)] bg-[var(--card)] p-6 sm:p-8 ring-1 ring-black/[0.04] dark:ring-white/[0.06] shadow-lg shadow-black/10">
      <style>{MDAG_STYLES}</style>
      <div
        className="absolute inset-0 rounded-2xl opacity-90 pointer-events-none"
        style={{
          backgroundImage: `
          linear-gradient(to right, color-mix(in oklab, var(--border) 40%, transparent) 1px, transparent 1px),
          linear-gradient(to bottom, color-mix(in oklab, var(--border) 40%, transparent) 1px, transparent 1px),
          radial-gradient(ellipse 85% 55% at 50% 0%, color-mix(in oklab, var(--dbx-lava-500) 12%, transparent), transparent 58%)
        `,
          backgroundSize: '22px 22px, 22px 22px, auto',
        }}
      />
      <p className="relative text-[10px] font-semibold uppercase tracking-[0.25em] text-[var(--dbx-lava-400)] mb-5 text-center sm:text-left">
        Live graph
      </p>
      <div className="relative flex flex-col items-center gap-0">
        {steps.map((step, i) => (
          <div key={step.key} className="flex flex-col items-center w-full">
            <DagNode step={step} />
            {i < steps.length - 1 ? (
              <div className="relative flex w-full justify-center py-0.5">
                <DagConnector
                  motion={edgeMotionFor(step.kind, steps[i + 1]!.kind, dltPipelineActive)}
                  gradId={`mdag-grad-${baseId}-${i}`}
                />
              </div>
            ) : null}
          </div>
        ))}
      </div>
    </div>
  );
}
