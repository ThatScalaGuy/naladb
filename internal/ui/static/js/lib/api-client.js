/**
 * API client for NalaDB Web UI.
 */

const BASE = '';

export async function executeQuery(query) {
  const resp = await fetch(`${BASE}/api/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
  });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ error: resp.statusText }));
    throw new Error(err.error || resp.statusText);
  }
  return resp.json();
}

export async function fetchGraphNodes(opts = {}) {
  const params = new URLSearchParams();
  if (opts.type) params.set('type', opts.type);
  if (opts.limit) params.set('limit', String(opts.limit));
  if (opts.at) params.set('at', opts.at);
  const resp = await fetch(`${BASE}/api/graph/nodes?${params}`);
  if (!resp.ok) throw new Error((await resp.json()).error);
  return resp.json();
}

export async function fetchGraphEdges(opts = {}) {
  const params = new URLSearchParams();
  if (opts.relation) params.set('relation', opts.relation);
  if (opts.limit) params.set('limit', String(opts.limit));
  if (opts.at) params.set('at', opts.at);
  const resp = await fetch(`${BASE}/api/graph/edges?${params}`);
  if (!resp.ok) throw new Error((await resp.json()).error);
  return resp.json();
}

export async function fetchGraphTraverse(opts) {
  const params = new URLSearchParams({ start: opts.start });
  if (opts.depth) params.set('depth', String(opts.depth));
  if (opts.direction) params.set('direction', opts.direction);
  if (opts.at) params.set('at', opts.at);
  const resp = await fetch(`${BASE}/api/graph/traverse?${params}`);
  if (!resp.ok) throw new Error((await resp.json()).error);
  return resp.json();
}

export async function fetchStats() {
  const resp = await fetch(`${BASE}/api/stats`);
  if (!resp.ok) throw new Error((await resp.json()).error);
  return resp.json();
}

export async function fetchLatestValue(key) {
  const resp = await executeQuery(`GET history("${key}") LAST 1`);
  if (resp.rows && resp.rows.length > 0) {
    return resp.rows[0];
  }
  return null;
}

export async function fetchSensorHistory(key, limit = 50) {
  const resp = await executeQuery(`GET history("${key}") LAST ${limit}`);
  return resp.rows || [];
}

/**
 * Convert HLC uint64 (as string) to a JS Date.
 * HLC: upper 48 bits = wall clock microseconds since NalaDB epoch, lower 16 bits = node+logical.
 * NalaDB epoch: 2025-01-01T00:00:00Z = 1_735_689_600_000_000 µs since Unix epoch.
 */
const NALADB_EPOCH_MICROS = 1_735_689_600_000_000n;

export function hlcToDate(hlcStr) {
  const hlc = BigInt(hlcStr);
  const wallMicros = hlc >> 16n;
  const unixMicros = wallMicros + NALADB_EPOCH_MICROS;
  return new Date(Number(unixMicros / 1000n));
}

/**
 * Format an HLC timestamp for display.
 */
export function formatHLC(hlcStr) {
  if (!hlcStr || hlcStr === '0') return '—';
  // MaxHLC (all bits set) means open-ended / still active.
  if (hlcStr === '18446744073709551615') return '∞';
  try {
    return hlcToDate(hlcStr).toISOString().replace('T', ' ').replace('Z', '');
  } catch {
    return hlcStr;
  }
}
