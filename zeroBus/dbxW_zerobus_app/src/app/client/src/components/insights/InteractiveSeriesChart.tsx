import { useCallback, useId, useMemo, useRef, useState } from 'react';

export type SeriesPoint = {
  xLabel: string;
  y: number;
  tooltipExtra?: Array<{ label: string; value: string }>;
};

type InteractiveSeriesChartProps = {
  points: SeriesPoint[];
  accent: string;
  /** Primary series label in tooltip (e.g. "Avg steps"). */
  valueLabel: string;
  formatY: (n: number) => string;
  emptyMessage?: string;
};

const W = 720;
const H = 160;
const PAD_L = 52;
const PAD_R = 12;
const PAD_T = 14;
const PAD_B = 28;

function clamp(n: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, n));
}

export function InteractiveSeriesChart({
  points,
  accent,
  valueLabel,
  formatY,
  emptyMessage = 'No series data for this window',
}: InteractiveSeriesChartProps) {
  const gid = useId().replace(/:/g, '');
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  /** Tooltip anchor (px from wrapper left) and wrapper width for clamping. */
  const [layout, setLayout] = useState({ relX: 0, width: 400 });

  const innerW = W - PAD_L - PAD_R;
  const innerH = H - PAD_T - PAD_B;

  const { minY, maxY, ys, xs } = useMemo(() => {
    if (!points.length) {
      return { minY: 0, maxY: 1, ys: [] as number[], xs: [] as number[] };
    }
    const raw = points.map((p) => p.y).filter((n) => Number.isFinite(n));
    const min = Math.min(...raw);
    const max = Math.max(...raw);
    const pad = max === min ? Math.abs(min) * 0.05 || 1 : (max - min) * 0.08;
    const lo = min - pad;
    const hi = max + pad;
    const span = hi - lo || 1;
    const ys = points.map((p) => {
      const v = Number.isFinite(p.y) ? p.y : lo;
      return PAD_T + innerH - ((v - lo) / span) * innerH;
    });
    const xs = points.map((_, i) => {
      const n = points.length;
      const x = n <= 1 ? PAD_L + innerW / 2 : PAD_L + (i / (n - 1)) * innerW;
      return x;
    });
    return { minY: lo, maxY: hi, ys, xs };
  }, [points, innerH]);

  const onMove = useCallback(
    (clientX: number) => {
      const el = svgRef.current;
      const wrap = wrapRef.current;
      if (!el || !wrap || points.length === 0) return;
      const rect = el.getBoundingClientRect();
      const wrapRect = wrap.getBoundingClientRect();
      const frac = (clientX - rect.left) / rect.width;
      const xf = clamp(frac, 0, 1) * W;
      if (points.length === 1) {
        setHoverIdx(0);
        setLayout({ relX: wrapRect.width / 2, width: wrapRect.width });
        return;
      }
      const innerXF = clamp(xf - PAD_L, 0, innerW);
      const f = innerXF / innerW;
      const idx = Math.round(f * (points.length - 1));
      setHoverIdx(idx);
      const xi = PAD_L + (idx / (points.length - 1)) * innerW;
      const relX = (xi / W) * rect.width + (rect.left - wrapRect.left);
      setLayout({ relX, width: wrapRect.width });
    },
    [points, innerW],
  );

  const onLeave = useCallback(() => setHoverIdx(null), []);

  if (!points.length) {
    return (
      <div className="h-24 flex items-center text-sm text-muted-foreground">{emptyMessage}</div>
    );
  }

  const polyPts = ys.map((y, i) => `${xs[i]},${y}`).join(' ');
  const areaPts = `${PAD_L},${PAD_T + innerH} ${polyPts} ${PAD_L + innerW},${PAD_T + innerH}`;

  const tickCount = 4;
  const ticks = Array.from({ length: tickCount + 1 }, (_, i) => {
    const t = i / tickCount;
    const v = minY + (maxY - minY) * (1 - t);
    const y = PAD_T + t * innerH;
    return { y, label: formatY(v) };
  });

  const hi = hoverIdx != null ? points[hoverIdx] : null;
  const hx = hoverIdx != null ? xs[hoverIdx] : null;
  const hy = hoverIdx != null ? ys[hoverIdx] : null;

  return (
    <div ref={wrapRef} className="relative w-full">
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${H}`}
        className="w-full h-36 select-none touch-none"
        role="img"
        aria-label="Interactive time series"
        onMouseMove={(e) => onMove(e.clientX)}
        onMouseLeave={onLeave}
        onTouchMove={(e) => {
          const t = e.touches[0];
          if (t) onMove(t.clientX);
        }}
        onTouchEnd={onLeave}
      >
        <defs>
          <linearGradient id={gid} x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={accent} stopOpacity="0.38" />
            <stop offset="100%" stopColor={accent} stopOpacity="0" />
          </linearGradient>
        </defs>

        {ticks.map((t) => (
          <g key={t.y}>
            <line
              x1={PAD_L}
              x2={PAD_L + innerW}
              y1={t.y}
              y2={t.y}
              stroke="rgba(255,255,255,0.06)"
              strokeDasharray="4 4"
            />
            <text x={4} y={t.y + 4} fill="rgba(148,163,184,0.9)" fontSize="10">
              {t.label}
            </text>
          </g>
        ))}

        <polygon fill={`url(#${gid})`} points={areaPts} opacity={0.95} />
        <polyline
          fill="none"
          stroke={accent}
          strokeWidth="2.5"
          points={polyPts}
          strokeLinejoin="round"
          strokeLinecap="round"
        />

        {hx != null && hy != null ? (
          <g>
            <line x1={hx} x2={hx} y1={PAD_T} y2={PAD_T + innerH} stroke="rgba(255,255,255,0.25)" strokeWidth="1" />
            <circle cx={hx} cy={hy} r={5} fill={accent} stroke="rgba(0,0,0,0.35)" strokeWidth="1" />
          </g>
        ) : null}
      </svg>

      {hi && hoverIdx != null ? (
        <div
          className="pointer-events-none absolute z-10 min-w-[180px] max-w-[min(92vw,280px)] rounded-lg border border-white/15 bg-[rgba(15,23,42,0.96)] px-3 py-2 text-xs shadow-xl shadow-black/40"
          style={{
            left: clamp(layout.relX - 90, 8, Math.max(16, layout.width - 188)),
            top: 4,
          }}
        >
          <p className="font-medium text-white/95 tabular-nums">{hi.xLabel}</p>
          <p className="mt-1 text-gray-300">
            <span className="text-gray-500">{valueLabel}: </span>
            <span className="text-white tabular-nums">{formatY(hi.y)}</span>
          </p>
          {hi.tooltipExtra?.map((row) => (
            <p key={row.label} className="mt-0.5 text-gray-400">
              <span className="text-gray-500">{row.label}: </span>
              <span className="text-gray-200 tabular-nums">{row.value}</span>
            </p>
          ))}
        </div>
      ) : null}

      <p className="mt-1 text-[10px] text-gray-500 px-1">Hover or drag across the chart for values.</p>
    </div>
  );
}
