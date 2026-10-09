// Only experiment executions use this host-local admission state.
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const hash = value => createHash('sha256').update(value).digest('hex');
const helper = fileURLToPath(new URL('../provider_backoff.py', import.meta.url));

function unavailable(message) {
  const marker = process.env.ORACLE_EXPERIMENT_ADMISSION_MARKER;
  if (marker) process.stderr.write(`${marker}:unavailable\n`);
  return new Error(message);
}

function serviceRoute(request) {
  const url = new URL(request.model.baseUrl);
  if (url.protocol !== 'http:' || !['127.0.0.1', '[::1]'].includes(url.hostname)
      || !url.port || url.username || url.password || url.search || url.hash) {
    throw new Error('Experiment admission requires the selected local credential service');
  }
  const subscription = /^\/([A-Za-z0-9_-]+)\/pi\/?$/.exec(url.pathname);
  const openrouter = /^\/openrouter\/api\/v1\/?$/.test(url.pathname);
  if (!subscription && !openrouter) throw new Error('Invalid experiment credential service route');
  return { origin: url.origin, account: subscription?.[1] ?? 'openrouter',
    client: subscription ? 'pi' : 'api' };
}

async function localJSON(fetch, url, apiKey, signal) {
  const response = await fetch(url, { method: 'GET', redirect: 'error',
    headers: { authorization: `Bearer ${apiKey}` },
    signal: AbortSignal.any([AbortSignal.timeout(2000), ...(signal ? [signal] : [])]) });
  if (!response.ok) { await response.body?.cancel(); return null; }
  const reader = response.body.getReader();
  let bytes = Buffer.alloc(0);
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (bytes.length + value.byteLength > 16384) return null;
      bytes = Buffer.concat([bytes, value]);
    }
    const object = JSON.parse(bytes.toString('utf8'));
    return object && typeof object === 'object' && !Array.isArray(object) ? object : null;
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}

function transact(request, signal) {
  return new Promise((resolve, reject) => {
    // The helper receives scope hashes and fixed receipt facts, never credentials
    // or model input/output. It uses the same Python runtime as the experiment.
    const environment = Object.fromEntries(['PATH', 'HOME', 'XDG_CACHE_HOME',
      'ORACLE_EXPERIMENT_BACKOFF_DIRECTORY'].filter(key => process.env[key] !== undefined)
      .map(key => [key, process.env[key]]));
    const child = spawn(process.env.ORACLE_EXPERIMENT_PYTHON || 'python3', [helper],
      { env: environment, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    const abort = () => child.kill('SIGTERM');
    if (signal?.aborted) abort();
    signal?.addEventListener('abort', abort, { once: true });
    child.stdout.on('data', bytes => {
      stdout += bytes;
      if (stdout.length > 4096) child.kill('SIGTERM');
    });
    child.stderr.resume();
    child.stdin.on('error', () => {});
    child.once('error', () => {
      signal?.removeEventListener('abort', abort);
      reject(new Error('Experiment admission state unavailable'));
    });
    child.once('close', code => {
      signal?.removeEventListener('abort', abort);
      if (signal?.aborted) { reject(signal.reason); return; }
      if (code !== 0) { reject(new Error('Experiment admission state unavailable')); return; }
      try { resolve(JSON.parse(stdout)); }
      catch { reject(new Error('Invalid experiment admission result')); }
    });
    child.stdin.end(JSON.stringify(request));
  });
}

const kinds = new Set(['context_canceled', 'deadline_exceeded', 'unexpected_eof',
  'connection_reset', 'connection_closed', 'broken_pipe', 'http2_stream_error',
  'http2_connection_error', 'http2_goaway', 'timeout', 'network_error', 'other']);
const codes = new Set(['no_error', 'protocol_error', 'internal_error', 'flow_control_error',
  'settings_timeout', 'stream_closed', 'frame_size_error', 'refused_stream', 'cancel',
  'compression_error', 'connect_error', 'enhance_your_calm', 'inadequate_security',
  'http_1_1_required', 'other']);

function safeStatus(value) {
  if (!value) return null;
  const result = {};
  for (const [key, allowed] of [['state', new Set(['in_flight', 'complete', 'failed', 'incomplete',
    'canceled', 'deadline_exceeded', 'rejected', 'unknown'])],
    ['read_error_kind', kinds], ['read_error_code', codes],
    ['request_context', new Set(['active', 'canceled', 'deadline_exceeded', 'other'])],
    ['error_phase', new Set(['headers', 'body'])],
    ['terminal_event', new Set(['completed', 'failed', 'incomplete'])]]) {
    if (allowed.has(value[key])) result[key] = value[key];
  }
  if (Number.isInteger(value.http_status) && value.http_status >= 0 && value.http_status <= 599) {
    result.http_status = value.http_status;
  }
  return result;
}

function policyFacts(result) {
  const metadata = result.response_metadata;
  const facts = {};
  if (Number.isInteger(metadata.http_status)) facts.http_status = metadata.http_status;
  if (metadata.retry_after !== undefined) facts.retry_after =
    typeof metadata.retry_after === 'string' && metadata.retry_after.length <= 128
      || typeof metadata.retry_after === 'number' && Number.isFinite(metadata.retry_after)
      ? metadata.retry_after : 'invalid';
  if (metadata.abort_reason) facts.abort_reason = true;
  if (metadata.terminal_event) facts.terminal_event = metadata.terminal_event;
  if (['headers', 'body'].includes(metadata.transport_error?.phase)) {
    facts.transport_error = { phase: metadata.transport_error.phase };
  }
  if (metadata.proxy_status) facts.proxy_status = safeStatus(metadata.proxy_status);
  if (metadata.error?.code !== undefined && metadata.error.code !== null) {
    const code = metadata.error.code;
    facts.error = { code: typeof code === 'number' && Number.isFinite(code)
      || typeof code === 'string' && code.length <= 128 ? code : 'other' };
  }
  if (typeof metadata.id === 'string' && metadata.id) facts.id = 'present';
  if (Number.isInteger(metadata.websocket_close?.code)) {
    facts.websocket_close = { code: metadata.websocket_close.code };
  }
  // The legacy policy needs one exact retry prefix, never arbitrary error prose.
  const prefix = 'Codex error: An error occurred while processing your request. '
    + 'You can retry your request, or contact us';
  return { provider: result.provider, stop_reason: result.stop_reason,
    error: typeof result.error === 'string' && result.error.startsWith(prefix) ? prefix : null,
    response_metadata: facts };
}

export async function experimentAdmission(request, { apiKey, fetch, signal, concurrency }) {
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 1024) {
    throw new Error('Invalid experiment concurrency');
  }
  const route = serviceRoute(request);
  const url = new URL('/request-status/scopes', route.origin);
  url.searchParams.set('account', route.account);
  url.searchParams.set('client', route.client);
  let prepared = null;
  try { prepared = await localJSON(fetch, url.href, apiKey, signal); }
  catch { signal?.throwIfAborted(); }
  if (prepared?.schema !== 1 || ['account_scope', 'transport_scope'].some(name =>
    typeof prepared[name] !== 'string' || !/^[A-Za-z0-9_-]{8,128}$/.test(prepared[name]))) {
    throw unavailable('Experiment account/transport scopes unavailable');
  }
  const scopes = { account: hash(`${route.origin}\0${prepared.account_scope}`),
    transport: hash(`${route.origin}\0${prepared.transport_scope}`) };
  const marker = process.env.ORACLE_EXPERIMENT_ADMISSION_MARKER;
  if (marker) process.stderr.write(`${marker}:waiting\n`);
  const started = performance.now();
  let admitted;
  try {
    admitted = await transact({ action: 'acquire', scopes, concurrency,
      timeout_seconds: request.timeout_seconds, owner_pid: process.pid }, signal);
  } catch (error) {
    if (signal?.aborted) throw error;
    throw unavailable('Experiment admission state unavailable');
  }
  if (marker) process.stderr.write(`${marker}:admitted\n`);
  return {
    wait_ms: Math.round(performance.now() - started),
    async finish(result) {
      const metadata = result.response_metadata;
      const reference = metadata.proxy_request_id;
      if (!metadata.abort_reason && metadata.terminal_event !== 'response.completed'
          && metadata.http_status !== 101
          && result.stop_reason === 'error' && typeof reference === 'string'
          && /^[A-Za-z0-9_-]{8,128}$/.test(reference)) {
        try {
          const value = await localJSON(fetch,
            new URL(`/request-status/${reference}`, route.origin).href, apiKey, signal);
          const safe = value?.schema === 1 && value.request_id === reference ? safeStatus(value) : null;
          if (safe) metadata.proxy_status = safe;
        } catch { /* Missing evidence keeps the cause unknown; no upstream fallback. */ }
      }
      await transact({ action: 'report', ticket: admitted.ticket,
        record: { exit_code: result.stop_reason === 'stop' ? 0 : 1, model_result: policyFacts(result) } });
    },
  };
}
