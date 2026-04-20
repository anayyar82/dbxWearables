// ZeroBus Ingest Service — Singleton (REST API)
//
// Ingests records into the wearables_zerobus bronze table via the ZeroBus
// REST API. Uses native Node.js fetch (no external SDK dependency) with
// OAuth M2M client-credentials token management.
//
// Previous approach used @databricks/zerobus-ingest-sdk (Rust/NAPI-RS),
// which failed in the Databricks Apps runtime due to missing native binaries.
// The REST API provides identical functionality without native dependencies.
//
// Environment variables (injected via app.yaml valueFrom directives):
//   ZEROBUS_ENDPOINT      — ZeroBus Ingest server endpoint
//   ZEROBUS_WORKSPACE_URL — Databricks workspace URL
//   ZEROBUS_TARGET_TABLE  — Fully qualified bronze table name
//   ZEROBUS_CLIENT_ID     — ZeroBus SPN application_id (OAuth M2M)
//   ZEROBUS_CLIENT_SECRET — ZeroBus SPN OAuth secret
//
// Table schema (users.ankur_nayyar.wearables_zerobus):
//   record_id       STRING  NOT NULL  — Server-generated GUID (PK)
//   ingested_at     TIMESTAMP         — Epoch microseconds
//   body            VARIANT           — Raw NDJSON line as JSON-encoded string
//   headers         VARIANT           — HTTP request headers as JSON-encoded string
//   record_type     STRING            — From X-Record-Type header
//   source_platform STRING            — From X-Platform header (e.g. "apple_healthkit")
//   user_id         STRING            — App-authenticated user ID from JWT claims
//
// REST API reference:
//   https://docs.databricks.com/aws/en/ingestion/zerobus-ingest/

import crypto from 'node:crypto';
import { spawn } from 'node:child_process';

// ── Types matching the bronze table schema ───────────────────────────────

export interface WearablesRecord {
  record_id: string;       // NOT NULL PK — crypto.randomUUID()
  ingested_at: number;     // TIMESTAMP   — epoch microseconds (Date.now() * 1000)
  body: string;            // VARIANT     — JSON.stringify(parsedNdjsonLine)
  headers: string;         // VARIANT     — JSON.stringify(httpHeaders)
  record_type: string;     // STRING      — e.g. "samples", "workouts", "sleep"
  source_platform: string; // STRING      — e.g. "apple_healthkit", "android_health_connect"
  user_id: string;         // STRING      — app-authenticated user ID (default 'anonymous')
}

// ── Required env var names ───────────────────────────────────────────────

const ENV_KEYS = [
  'ZEROBUS_ENDPOINT',
  'ZEROBUS_WORKSPACE_URL',
  'ZEROBUS_TARGET_TABLE',
  'ZEROBUS_CLIENT_ID',
  'ZEROBUS_CLIENT_SECRET',
] as const;

// ── OAuth token cache ────────────────────────────────────────────────────

interface TokenCache {
  accessToken: string;
  expiresAt: number; // Date.now() ms when the token expires
}

// ── Service class ────────────────────────────────────────────────────────

class ZeroBusService {
  private tokenCache: TokenCache | null = null;

  private static readonly INSERT_RETRY_ATTEMPTS = 3;
  private static readonly INSERT_RETRY_BACKOFF_MS = 1000;

  private async ingestWithCurl(
    insertUrl: string,
    token: string,
    recordsJson: string,
  ): Promise<void> {
    const args = [
      '-sS',
      '-X',
      'POST',
      insertUrl,
      '-H',
      `Authorization: Bearer ${token}`,
      '-H',
      'Content-Type: application/json',
      '--data',
      recordsJson,
      '-w',
      '\n%{http_code}',
    ];

    await new Promise<void>((resolve, reject) => {
      const child = spawn('curl', args, { stdio: ['ignore', 'pipe', 'pipe'] });
      let stdout = '';
      let stderr = '';

      child.stdout.on('data', (chunk) => {
        stdout += chunk.toString();
      });
      child.stderr.on('data', (chunk) => {
        stderr += chunk.toString();
      });

      child.on('error', (err) => {
        reject(new Error(`curl fallback process error: ${err.message}`));
      });

      child.on('close', (code) => {
        if (code !== 0) {
          reject(
            new Error(
              `curl fallback failed (exit ${code}): ${stderr || stdout || '(no output)'}`,
            ),
          );
          return;
        }

        const lines = stdout.trimEnd().split('\n');
        const statusText = lines[lines.length - 1] || '';
        const statusCode = Number.parseInt(statusText, 10);
        const body = lines.slice(0, -1).join('\n');

        if (!Number.isFinite(statusCode) || statusCode < 200 || statusCode >= 300) {
          reject(
            new Error(
              `curl fallback insert failed (${statusCode || 'unknown'}): ${body || stderr || '(empty body)'}`,
            ),
          );
          return;
        }

        resolve();
      });
    });
  }

  // ── Table name parsing ───────────────────────────────────────────────

  /** Split a fully qualified table name into catalog, schema, table. */
  private parseTableName(fqn: string): {
    catalog: string;
    schema: string;
    table: string;
  } {
    const parts = fqn.split('.');
    if (parts.length !== 3) {
      throw new Error(
        `Invalid fully qualified table name "${fqn}" — expected catalog.schema.table`,
      );
    }
    return { catalog: parts[0], schema: parts[1], table: parts[2] };
  }

  /**
   * Extract the workspace ID from the ZeroBus endpoint URL.
   * ZeroBus endpoints follow the pattern:
   *   https://<workspace-id>.zerobus.<region>.cloud.databricks.com
   */
  private extractWorkspaceId(endpoint: string): string {
    const url = new URL(endpoint);
    const workspaceId = url.hostname.split('.')[0];
    if (!workspaceId || !/^\d+$/.test(workspaceId)) {
      throw new Error(
        `Cannot extract numeric workspace ID from ZeroBus endpoint "${endpoint}"`,
      );
    }
    return workspaceId;
  }

  // ── OAuth token management ───────────────────────────────────────────

  /**
   * Fetch (or return cached) OAuth M2M access token for ZeroBus.
   *
   * Token is refreshed 5 minutes before expiry. Uses the standard
   * Databricks OIDC client-credentials grant with authorization_details
   * scoped to the target table's catalog, schema, and table privileges.
   *
   * Reference:
   *   https://docs.databricks.com/aws/en/ingestion/zerobus-ingest/
   */
  private async getAccessToken(): Promise<string> {
    // Return cached token if still valid (with 5-minute buffer)
    const REFRESH_BUFFER_MS = 5 * 60 * 1000;
    if (
      this.tokenCache &&
      this.tokenCache.expiresAt > Date.now() + REFRESH_BUFFER_MS
    ) {
      return this.tokenCache.accessToken;
    }

    const workspaceUrl = process.env.ZEROBUS_WORKSPACE_URL!;
    const clientId = process.env.ZEROBUS_CLIENT_ID!;
    const clientSecret = process.env.ZEROBUS_CLIENT_SECRET!;
    const targetTable = process.env.ZEROBUS_TARGET_TABLE!;
    const endpoint = process.env.ZEROBUS_ENDPOINT!;

    const { catalog, schema } = this.parseTableName(targetTable);
    const workspaceId = this.extractWorkspaceId(endpoint);

    // Scope the token to the minimum required UC privileges
    const authorizationDetails = JSON.stringify([
      {
        type: 'unity_catalog_privileges',
        privileges: ['USE CATALOG'],
        object_type: 'CATALOG',
        object_full_path: catalog,
      },
      {
        type: 'unity_catalog_privileges',
        privileges: ['USE SCHEMA'],
        object_type: 'SCHEMA',
        object_full_path: `${catalog}.${schema}`,
      },
      {
        type: 'unity_catalog_privileges',
        privileges: ['SELECT', 'MODIFY'],
        object_type: 'TABLE',
        object_full_path: targetTable,
      },
    ]);

    const body = new URLSearchParams({
      grant_type: 'client_credentials',
      scope: 'all-apis',
      resource: `api://databricks/workspaces/${workspaceId}/zerobusDirectWriteApi`,
      authorization_details: authorizationDetails,
    });

    const basicAuth = Buffer.from(`${clientId}:${clientSecret}`).toString(
      'base64',
    );

    console.log('[ZeroBus] Fetching OAuth token...');
    const response = await fetch(`${workspaceUrl}/oidc/v1/token`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: `Basic ${basicAuth}`,
      },
      body: body.toString(),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`OAuth token fetch failed (${response.status}): ${text}`);
    }

    const data = (await response.json()) as {
      access_token: string;
      expires_in: number;
      token_type: string;
    };

    this.tokenCache = {
      accessToken: data.access_token,
      expiresAt: Date.now() + data.expires_in * 1000,
    };

    console.log(
      `[ZeroBus] OAuth token acquired (expires in ${data.expires_in}s)`,
    );
    return data.access_token;
  }

  // ── Record builder ───────────────────────────────────────────────────

  /**
   * Build a WearablesRecord for one NDJSON line.
   *
   * VARIANT columns (body, headers) are stored as JSON-encoded strings per
   * ZeroBus Ingest requirements:
   *   https://docs.databricks.com/aws/en/ingestion/zerobus-limits/
   *
   * TIMESTAMP is epoch microseconds (int64).
   */
  buildRecord(
    body: unknown,
    headers: Record<string, string>,
    recordType: string,
    sourcePlatform: string,
    userId: string = 'anonymous',
  ): WearablesRecord {
    return {
      record_id: crypto.randomUUID(),
      ingested_at: Date.now() * 1000, // ms → μs
      body: JSON.stringify(body), // VARIANT — JSON-encoded string
      headers: JSON.stringify(headers), // VARIANT — JSON-encoded string
      record_type: recordType,
      source_platform: sourcePlatform,
      user_id: userId,
    };
  }

  // ── Batch ingest via REST API ────────────────────────────────────────

  /**
   * Ingest an array of pre-built records via the ZeroBus REST API.
   *
   * POST /zerobus/v1/tables/{catalog.schema.table}/insert
   * Body: JSON array of record objects
   * Auth: Bearer token (OAuth M2M client-credentials)
   *
   * A 200 response with empty body confirms durable commit.
   *
   * @returns The number of records ingested.
   */
  async ingestRecords(records: WearablesRecord[]): Promise<number> {
    if (records.length === 0) return 0;

    // Validate env vars before attempting ingestion
    const envCheck = this.checkEnv();
    if (!envCheck.configured) {
      throw new Error(
        `Missing required ZeroBus env vars: ${envCheck.missing.join(', ')}`,
      );
    }

    const token = await this.getAccessToken();
    const endpoint = process.env.ZEROBUS_ENDPOINT!;
    const targetTable = process.env.ZEROBUS_TARGET_TABLE!;

    const insertUrl = `${endpoint}/zerobus/v1/tables/${targetTable}/insert`;
    let lastError: unknown = null;
    const recordsJson = JSON.stringify(records);

    for (let attempt = 1; attempt <= ZeroBusService.INSERT_RETRY_ATTEMPTS; attempt++) {
      try {
        const response = await fetch(insertUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
          body: recordsJson,
        });

        if (!response.ok) {
          const text = await response.text();
          throw new Error(`ZeroBus insert failed (${response.status}): ${text}`);
        }

        return records.length;
      } catch (err) {
        lastError = err;

        const errorMessage = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
        const errorCause =
          err &&
          typeof err === 'object' &&
          'cause' in err &&
          (err as { cause?: unknown }).cause
            ? ` | cause: ${String((err as { cause: unknown }).cause)}`
            : '';

        if (attempt < ZeroBusService.INSERT_RETRY_ATTEMPTS) {
          console.warn(
            `[ZeroBus] Insert attempt ${attempt}/${ZeroBusService.INSERT_RETRY_ATTEMPTS} failed, retrying in ${ZeroBusService.INSERT_RETRY_BACKOFF_MS}ms (${errorMessage}${errorCause})`,
          );
          await new Promise((resolve) =>
            setTimeout(resolve, ZeroBusService.INSERT_RETRY_BACKOFF_MS * attempt),
          );
          continue;
        }
      }
    }

    const finalMessage =
      lastError instanceof Error ? `${lastError.name}: ${lastError.message}` : String(lastError);
    const finalCause =
      lastError &&
      typeof lastError === 'object' &&
      'cause' in lastError &&
      (lastError as { cause?: unknown }).cause
        ? ` | cause: ${String((lastError as { cause: unknown }).cause)}`
        : '';

    console.warn(
      `[ZeroBus] fetch-based insert failed after ${ZeroBusService.INSERT_RETRY_ATTEMPTS} attempts, trying curl fallback`,
    );

    try {
      await this.ingestWithCurl(insertUrl, token, recordsJson);
      console.log('[ZeroBus] curl fallback insert succeeded');
      return records.length;
    } catch (curlErr) {
      const curlMessage =
        curlErr instanceof Error ? `${curlErr.name}: ${curlErr.message}` : String(curlErr);
      throw new Error(
        `ZeroBus insert failed after fetch retries and curl fallback to ${insertUrl}: ${finalMessage}${finalCause} | curl: ${curlMessage}`,
      );
    }
  }

  // ── Health check ─────────────────────────────────────────────────────

  /** Check whether all required env vars are present. */
  checkEnv(): { configured: boolean; missing: string[] } {
    const missing = ENV_KEYS.filter((k) => !process.env[k]);
    return { configured: missing.length === 0, missing: [...missing] };
  }

  // ── Graceful shutdown ────────────────────────────────────────────────

  async close(): Promise<void> {
    // Clear cached token on shutdown
    this.tokenCache = null;
    console.log('[ZeroBus] Service shut down (token cache cleared)');
  }
}

// ── Singleton export ─────────────────────────────────────────────────────

/** Shared instance — used by all Express route handlers. */
export const zeroBusService = new ZeroBusService();
