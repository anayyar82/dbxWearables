import type { LucideIcon } from 'lucide-react';
import { BarChart3, CloudUpload, Cpu, Database, Gem, Layers } from 'lucide-react';
import { BRONZE_STREAM_TABLE, formatInt, type FlowInsight } from './dltStatusModel';

export type MedallionStepKind = 'pending' | 'active' | 'done' | 'warn' | 'error';

export type MedallionStep = {
  key: string;
  title: string;
  subtitle: string;
  Icon: LucideIcon;
  kind: MedallionStepKind;
};

export function isDltUpdateActive(state: string | undefined): boolean {
  const u = (state ?? '').toUpperCase();
  if (!u || u === '—') return false;
  if (u.includes('COMPLET') || u.includes('FAILED') || u.includes('CANCEL') || u === 'IDLE') return false;
  return true;
}

function bronzeStreamSubtitle(insight: FlowInsight | undefined, ingestRows: number | undefined): string {
  const dltRows = insight?.num_output_rows ?? insight?.num_upserted_rows;
  const st = insight?.status?.trim();
  if (dltRows != null) {
    const tail = st ? ` · ${st}` : '';
    return `${BRONZE_STREAM_TABLE} · ${formatInt(dltRows)} rows (DLT)${tail}`;
  }
  if (ingestRows != null && ingestRows > 0) {
    return `${BRONZE_STREAM_TABLE} · ${formatInt(ingestRows)} rows in UC (ZeroBus)`;
  }
  if (st) return `${BRONZE_STREAM_TABLE} · ${st}`;
  return BRONZE_STREAM_TABLE;
}

export function buildMedallionSteps(params: {
  zerobusReady: boolean;
  ingestFailures: number;
  hasIngestActivity: boolean;
  /** Rows committed via this app’s ingest API (ZeroBus bronze); shown on graph + bronze subtitle when DLT metrics absent. */
  totalRecordsIngested?: number;
  bronzeInsight: FlowInsight | undefined;
  silverDone: number;
  silverTotal: number;
  goldDone: number;
  goldTotal: number;
  dltUpdateState: string | undefined;
}): MedallionStep[] {
  const {
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
  } = params;
  const dltActive = isDltUpdateActive(dltUpdateState);
  const ingestRows = totalRecordsIngested ?? 0;

  const zKind: MedallionStepKind = !zerobusReady
    ? 'warn'
    : ingestFailures > 0
      ? 'error'
      : hasIngestActivity
        ? 'done'
        : 'pending';
  const bronzeHasSignal = Boolean(
    bronzeInsight?.status || bronzeInsight?.num_output_rows != null || bronzeInsight?.num_upserted_rows != null,
  );
  /** If silver/gold flows reported, bronze streaming table must have advanced (even when bronze flow_progress aged off the event window). */
  const bronzeDoneByDownstream = silverDone > 0 || goldDone > 0;
  const bronzeEffectivelyComplete = bronzeHasSignal || bronzeDoneByDownstream;

  /**
   * Single “frontier” DLT stage while an update is running so the Live graph does not mark
   * silver + gold (+ external) active at once. Order: external read → bronze ST → silver → gold → BI.
   */
  let dltCursor: 'ext' | 'bst' | 'sv' | 'gd' | 'bi' | null = null;
  if (zerobusReady && dltActive) {
    if (!bronzeEffectivelyComplete) {
      dltCursor = bronzeHasSignal ? 'bst' : 'ext';
    } else if (silverDone < silverTotal) {
      dltCursor = 'sv';
    } else if (goldDone < goldTotal) {
      dltCursor = 'gd';
    } else {
      dltCursor = 'bi';
    }
  }

  const extDone =
    bronzeEffectivelyComplete ||
    silverDone > 0 ||
    goldDone > 0 ||
    (dltCursor != null && dltCursor !== 'ext') ||
    (!dltActive && (hasIngestActivity || bronzeHasSignal));

  const extKind: MedallionStepKind = !zerobusReady
    ? 'pending'
    : dltCursor === 'ext'
      ? 'active'
      : extDone
        ? 'done'
        : 'pending';

  const bronzeKind: MedallionStepKind = !zerobusReady
    ? 'pending'
    : dltCursor === 'bst'
      ? 'active'
      : bronzeEffectivelyComplete
        ? 'done'
        : 'pending';

  const silverKind: MedallionStepKind = !zerobusReady
    ? 'pending'
    : dltCursor === 'sv'
      ? 'active'
      : silverDone >= silverTotal
        ? 'done'
        : silverDone > 0 && !dltActive
          ? 'done'
          : 'pending';

  const goldKind: MedallionStepKind = !zerobusReady
    ? 'pending'
    : dltCursor === 'gd'
      ? 'active'
      : goldDone >= goldTotal
        ? 'done'
        : goldDone > 0 && !dltActive
          ? 'done'
          : 'pending';

  const biKind: MedallionStepKind = !zerobusReady
    ? 'pending'
    : dltCursor === 'bi'
      ? 'active'
      : goldTotal > 0 && goldDone >= goldTotal && !dltActive
        ? 'done'
        : goldDone > 0 && !dltActive
          ? 'done'
          : 'pending';

  const zbSubtitle =
    ingestRows > 0
      ? `REST → bronze VARIANT · ${formatInt(ingestRows)} row(s) ingested (this app)`
      : 'REST → bronze VARIANT';

  return [
    { key: 'zb', title: 'ZeroBus', subtitle: zbSubtitle, Icon: CloudUpload, kind: zKind },
    { key: 'ext', title: 'External bronze', subtitle: 'readStream Delta', Icon: Database, kind: extKind },
    { key: 'bst', title: 'Bronze ST', subtitle: bronzeStreamSubtitle(bronzeInsight, ingestRows), Icon: Layers, kind: bronzeKind },
    {
      key: 'sv',
      title: 'Silver ST',
      subtitle: `${silverDone}/${silverTotal} flows reporting`,
      Icon: Cpu,
      kind: silverKind,
    },
    {
      key: 'gd',
      title: 'Gold MV',
      subtitle: `${goldDone}/${goldTotal} tables reporting`,
      Icon: Gem,
      kind: goldKind,
    },
    { key: 'bi', title: 'Analytics', subtitle: 'SQL / Insights / Lakeview', Icon: BarChart3, kind: biKind },
  ];
}
