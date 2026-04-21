/**
 * In-process counters for ZeroBus REST ingest (HealthKit → bronze).
 * Resets on app restart — suitable for demo / live status, not billing.
 */

type TypeBucket = { requests: number; records: number };

const byRecordType = new Map<string, TypeBucket>();
let successfulIngests = 0;
let failedIngests = 0;
let totalRecords = 0;
let lastIngestAt: number | null = null;
let lastErrorAt: number | null = null;
let lastErrorMessage: string | null = null;
const startedAt = Date.now();

function bucket(rt: string): TypeBucket {
  const key = rt || 'unknown';
  let b = byRecordType.get(key);
  if (!b) {
    b = { requests: 0, records: 0 };
    byRecordType.set(key, b);
  }
  return b;
}

export function recordIngestSuccess(recordType: string, recordsIngested: number): void {
  successfulIngests += 1;
  totalRecords += recordsIngested;
  lastIngestAt = Date.now();
  const b = bucket(recordType);
  b.requests += 1;
  b.records += recordsIngested;
}

export function recordIngestFailure(message: string): void {
  failedIngests += 1;
  lastErrorAt = Date.now();
  lastErrorMessage = message;
}

export function getIngestMetricsSnapshot(): {
  started_at_ms: number;
  successful_ingests: number;
  failed_ingests: number;
  total_records_ingested: number;
  last_ingest_at_ms: number | null;
  last_error_at_ms: number | null;
  last_error_message: string | null;
  by_record_type: Array<{ record_type: string; requests: number; records: number }>;
} {
  const types = [...byRecordType.entries()]
    .map(([record_type, v]) => ({
      record_type,
      requests: v.requests,
      records: v.records,
    }))
    .sort((a, b) => b.records - a.records);
  return {
    started_at_ms: startedAt,
    successful_ingests: successfulIngests,
    failed_ingests: failedIngests,
    total_records_ingested: totalRecords,
    last_ingest_at_ms: lastIngestAt,
    last_error_at_ms: lastErrorAt,
    last_error_message: lastErrorMessage,
    by_record_type: types,
  };
}
