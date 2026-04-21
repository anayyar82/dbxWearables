/**
 * OAuth M2M client for Databricks Workspace REST APIs (Pipelines, Jobs, …).
 *
 * Uses the **app's** service principal (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`)
 * with `scope=all-apis` — not the ZeroBus-scoped token from zerobus-service.
 *
 * Grant the app's SPN at least **CAN RUN** / **CAN VIEW** on the target pipelines.
 */

const REFRESH_BUFFER_MS = 5 * 60 * 1000;

let tokenCache: { accessToken: string; expiresAt: number } | null = null;

export function workspaceOrigin(): string | null {
  const raw = process.env.ZEROBUS_WORKSPACE_URL?.trim();
  if (!raw) return null;
  try {
    return new URL(raw).origin;
  } catch {
    return null;
  }
}

export function workspaceApiConfigured(): boolean {
  return Boolean(
    workspaceOrigin() &&
      process.env.DATABRICKS_CLIENT_ID?.trim() &&
      process.env.DATABRICKS_CLIENT_SECRET?.trim(),
  );
}

async function getAccessToken(): Promise<string> {
  if (
    tokenCache &&
    tokenCache.expiresAt > Date.now() + REFRESH_BUFFER_MS
  ) {
    return tokenCache.accessToken;
  }

  const origin = workspaceOrigin();
  const clientId = process.env.DATABRICKS_CLIENT_ID?.trim();
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET?.trim();
  if (!origin || !clientId || !clientSecret) {
    throw new Error(
      'Workspace API not configured: need ZEROBUS_WORKSPACE_URL, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET',
    );
  }

  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    scope: 'all-apis',
  });

  const basic = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const res = await fetch(`${origin}/oidc/v1/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: `Basic ${basic}`,
    },
    body: body.toString(),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Workspace token failed (${res.status}): ${text}`);
  }

  const data = (await res.json()) as {
    access_token: string;
    expires_in: number;
  };

  tokenCache = {
    accessToken: data.access_token,
    expiresAt: Date.now() + data.expires_in * 1000,
  };
  return data.access_token;
}

export async function workspaceFetchJson<T>(
  path: string,
  init?: RequestInit,
): Promise<{ ok: boolean; status: number; json: T | null; text: string }> {
  const origin = workspaceOrigin();
  if (!origin) {
    return { ok: false, status: 0, json: null, text: 'ZEROBUS_WORKSPACE_URL is not set' };
  }

  const token = await getAccessToken();
  const url = `${origin}${path.startsWith('/') ? path : `/${path}`}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(init?.headers as Record<string, string>),
    },
  });
  const text = await res.text();
  let json: T | null = null;
  try {
    json = text ? (JSON.parse(text) as T) : null;
  } catch {
    json = null;
  }
  return { ok: res.ok, status: res.status, json, text };
}
