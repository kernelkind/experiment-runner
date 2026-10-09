#!/usr/bin/env node
// One caller-supplied prompt through pi-ai. No agent session, resources, tools, or retry loop.
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { spawn } from 'node:child_process';
import { constants, homedir } from 'node:os';
import { join } from 'node:path';
import { zstdDecompressSync } from 'node:zlib';
import Ajv from 'ajv';
import Ajv2019 from 'ajv/dist/2019.js';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { Agent } from 'undici';
import { getSupportedThinkingLevels } from '@earendil-works/pi-ai';
import { OPENAI_CODEX_MODELS } from '@earendil-works/pi-ai/providers/openai-codex.models';
import { OPENROUTER_MODELS } from '@earendil-works/pi-ai/providers/openrouter.models';
import { stream as codexStream } from '@earendil-works/pi-ai/api/openai-codex-responses';
import { stream as openrouterStream } from '@earendil-works/pi-ai/api/openai-completions';
import { websocketRequest, websocketResponse } from './websocket.mjs';
import { experimentAdmission } from './backoff.mjs';

export const VERSION = 'oracle-model-runner/1';
const PI_AI_VERSION = '0.85.1';
// SSE can pause while the provider reasons. Only the caller's overall deadline
// bounds that wait; do not impose Undici's separate five-minute body idle limit.
const streamDispatcher = new Agent({ bodyTimeout: 0 });
const sha256 = text => createHash('sha256').update(text).digest('hex');
const nonempty = value => typeof value === 'string' && value.trim().length > 0;
const integer = (value, min, max) => Number.isSafeInteger(value) && value >= min && value <= max;
// Jev is not a chat model; pi-ai has no System One transport. Use its documented
// endpoint explicitly, under the same credential, receipt and no-retry contract.
const jev = { id: 'typesafe/jev-1.13', provider: 'openrouter', api: 'systemone',
  baseUrl: 'https://openrouter.ai/api/v1', reasoning: false };
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const probability = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;

function prepareDecisions(config) {
  if (config.reasoning !== 'provider-default' || config.max_output_tokens != null || config.debug !== undefined) {
    throw new Error('System One requires provider-default reasoning and no output cap or chat debug settings');
  }
  const payload = JSON.parse(config.prompt);
  if (!object(payload) || Object.keys(payload).some(key => !['state', 'questions'].includes(key)) ||
      !Object.hasOwn(payload, 'state') || !object(payload.questions) || !Object.keys(payload.questions).length) {
    throw new Error('System One prompt must encode JSON with state and nonempty questions only');
  }
  for (const q of Object.values(payload.questions)) {
    if (!object(q) || Object.keys(q).some(key => !['type', 'instructions', 'criteria'].includes(key)) ||
        !['choice', 'noul', 'score'].includes(q.type) || q.instructions == null) throw new Error('Invalid System One question');
    if (q.type === 'choice' && (!object(q.criteria) || !integer(Object.keys(q.criteria).length, 1, 255))) {
      throw new Error('Choice requires 1–255 named criteria');
    }
    if (q.type === 'score' && (!Array.isArray(q.criteria) || !integer(q.criteria.length, 2, 10))) {
      throw new Error('Score requires 2–10 ordered criteria');
    }
    if (q.type === 'noul' && q.criteria !== undefined && (!object(q.criteria) ||
        Object.keys(q.criteria).some(key => !['true', 'false'].includes(key)))) throw new Error('Invalid Noul criteria');
  }
  return payload;
}

function validateDecisions(questions, answers) {
  if (!object(answers) || Object.keys(answers).length !== Object.keys(questions).length) return false;
  return Object.entries(questions).every(([id, q]) => {
    const a = answers[id];
    if (!object(a) || a.type !== q.type) return false;
    if (q.type === 'noul') return probability(a.noul);
    const keys = Object.keys(q.criteria);
    if (!probability(a.confidence) || !object(a.probabilities) ||
        Object.keys(a.probabilities).length !== keys.length || !keys.every(key => probability(a.probabilities[key])) ||
        Math.abs(Object.values(a.probabilities).reduce((sum, p) => sum + p, 0) - 1) > 0.02) return false;
    return q.type === 'choice' ? keys.includes(a.choice)
      : typeof a.score === 'number' && Number.isFinite(a.score) && a.score >= 0 && a.score <= keys.length - 1;
  });
}

// Exact Luna metadata: official model page and API pricing, checked 2026-09-24.
// https://developers.openai.com/api/docs/models/gpt-6-luna
// https://developers.openai.com/api/docs/pricing#text-tokens
// Reference API rates are not subscription usage or bills.
const luna = {
  id: 'gpt-6-luna', name: 'GPT-6 Luna',
  provider: 'openai-codex', api: 'openai-codex-responses', baseUrl: 'https://chatgpt.com/backend-api',
  input: ['text', 'image'], reasoning: true, contextWindow: 1050000, maxTokens: 128000,
  thinkingLevelMap: { off: 'none', minimal: null, low: 'low', medium: 'medium', high: 'high', xhigh: 'xhigh', max: 'max' },
  cost: { input: 0.10, output: 0.50, cacheRead: 0.01, cacheWrite: 0.125,
    tiers: [{ inputTokensAbove: 272000, input: 0.20, output: 0.75, cacheRead: 0.02, cacheWrite: 0.25 }] },
};

// The pinned catalog predates GPT-6 Sol. Exact metadata, not a GPT-5.6 alias.
// Verified 2026-09-24: https://developers.openai.com/api/docs/models/gpt-6-sol
// API reference rates: https://developers.openai.com/api/docs/changelog#sep-22
// Catalog estimates are not subscription charges.
const sol = {
  id: 'gpt-6-sol', name: 'GPT-6 Sol',
  provider: 'openai-codex', api: 'openai-codex-responses', baseUrl: 'https://chatgpt.com/backend-api',
  input: ['text', 'image'], reasoning: true, contextWindow: 1050000, maxTokens: 128000,
  thinkingLevelMap: { off: 'none', minimal: null, low: 'low', medium: 'medium', high: 'high', xhigh: 'xhigh', max: 'max' },
  cost: { input: 2, output: 10, cacheRead: 0.2, cacheWrite: 2.5,
    tiers: [{ inputTokensAbove: 272000, input: 4, output: 15, cacheRead: 0.4, cacheWrite: 5 }] },
};

// The pinned catalog predates GPT-6.1 Sol. Exact metadata, verified 2026-09-29:
// https://developers.openai.com/api/docs/models/gpt-6.1-sol
// https://developers.openai.com/api/docs/pricing
// API reference rates are not subscription charges or quota measurements.
const sol61 = {
  id: 'gpt-6.1-sol', name: 'GPT-6.1 Sol',
  provider: 'openai-codex', api: 'openai-codex-responses', baseUrl: 'https://chatgpt.com/backend-api',
  input: ['text', 'image'], reasoning: true, contextWindow: 1050000, maxTokens: 128000,
  thinkingLevelMap: { off: null, minimal: null, low: 'low', medium: 'medium', high: 'high', xhigh: 'xhigh', max: 'max' },
  cost: { input: 2, output: 10, cacheRead: 0.1, cacheWrite: 2.5,
    tiers: [{ inputTokensAbove: 272000, input: 4, output: 15, cacheRead: 0.2, cacheWrite: 5 }] },
};

// This pinned catalog predates V4.1. Exact model metadata, not a V4 alias.
// OpenRouter /api/v1/models inspected 2026-09-16; catalog costs are not invoices or caps.
const deepseek = {
  id: 'deepseek/deepseek-v4.1-flash', name: 'DeepSeek V4.1 Flash',
  provider: 'openrouter', api: 'openai-completions', baseUrl: 'https://openrouter.ai/api/v1',
  input: ['text', 'image'], reasoning: true, contextWindow: 1048576, maxTokens: 384000,
  thinkingLevelMap: { off: 'none', low: 'low', high: 'high', max: 'max', minimal: null, medium: null, xhigh: null },
  cost: { input: 0.15, output: 0.60, cacheRead: 0.003, cacheWrite: 0 },
  compat: { supportsDeveloperRole: false, thinkingFormat: 'openrouter', maxTokensField: 'max_tokens' },
};

// This pinned catalog predates V2.6. Exact model metadata, not an alias.
// OpenRouter /api/v1/models inspected 2026-09-21; catalog costs are not invoices or caps.
const mimo = {
  id: 'xiaomi/mimo-v2.6-pro', name: 'Xiaomi: MiMo-V2.6-Pro',
  provider: 'openrouter', api: 'openai-completions', baseUrl: 'https://openrouter.ai/api/v1',
  input: ['text', 'image'], reasoning: true, contextWindow: 1048576, maxTokens: 131072,
  cost: { input: 0.435, output: 0.87, cacheRead: 0.0036, cacheWrite: 0 },
  compat: { supportsDeveloperRole: false, thinkingFormat: 'openrouter', maxTokensField: 'max_tokens' },
};

// OpenRouter /api/v1/models inspected 2026-09-24. Flash advertises a thinking
// toggle, not graded efforts. Keep this separate from the existing Pro route.
const mimoFlash = {
  id: 'xiaomi/mimo-v2.6-flash', name: 'Xiaomi: MiMo-V2.6-Flash',
  provider: 'openrouter', api: 'openai-completions', baseUrl: 'https://openrouter.ai/api/v1',
  input: ['text', 'image'], reasoning: true, contextWindow: 1048576, maxTokens: 131072,
  thinkingLevelMap: { off: 'none', minimal: null, low: null, medium: null, high: null, xhigh: null, max: null },
  cost: { input: 0.14, output: 0.28, cacheRead: 0.0028, cacheWrite: 0 },
  compat: { supportsDeveloperRole: false, thinkingFormat: 'openrouter', maxTokensField: 'max_tokens' },
};

async function identity() {
  const hash = createHash('sha256');
  for (const path of ['model.mjs', 'websocket.mjs', 'backoff.mjs', '../provider_backoff.py',
    '../extraction_pipeline.py', 'package-lock.json', '../run-model.sh']) {
    const bytes = await readFile(new URL(path, import.meta.url));
    hash.update(`${path}\0${bytes.length}\0`).update(bytes);
  }
  return { runner_version: VERSION, pi_ai_version: PI_AI_VERSION, runner_sha256: hash.digest('hex') };
}

function compileSchema(schema) {
  if (typeof schema !== 'boolean' && (!schema || typeof schema !== 'object' || Array.isArray(schema))) {
    throw new Error('schema must be a JSON Schema object or boolean');
  }
  const dialect = schema.$schema;
  const Validator = dialect === 'https://json-schema.org/draft/2020-12/schema' ? Ajv2020
    : dialect === 'https://json-schema.org/draft/2019-09/schema' ? Ajv2019 : Ajv;
  // No coercion, defaults, property removal, remote schema loading, or unknown-keyword acceptance.
  const validator = new Validator({ strict: true, strictRequired: false, strictTypes: false, strictTuples: false, allowUnionTypes: true });
  addFormats(validator);
  try {
    const validate = validator.compile(schema);
    if (validate.$async) throw new Error('asynchronous schemas are unsupported');
    return validate;
  }
  catch (error) { throw new Error(`Invalid or unsupported JSON schema: ${error.message}`); }
}

/** Prepare the exact prompt and explicitly selected route, without reading any task inputs. */
export async function prepare(config) {
  if (!config || typeof config !== 'object' || Array.isArray(config)) throw new Error('Config must be an object');
  const allowed = new Set(['provider', 'model', 'reasoning', 'prompt', 'schema', 'max_output_tokens', 'timeout_seconds', 'debug', 'transport']);
  for (const key of Object.keys(config)) if (!allowed.has(key)) throw new Error(`Unknown config field: ${key}`);
  const transport = config.transport ?? 'sse';
  if (!['sse', 'websocket'].includes(transport)) throw new Error('transport must be sse or websocket');
  if (transport === 'websocket' && config.provider !== 'openai-codex') throw new Error('WebSocket is supported only for subscription Responses');
  if ('debug' in config) {
    if (config.provider !== 'openrouter') throw new Error('debug is supported only for OpenRouter');
    if (!config.debug || typeof config.debug !== 'object' || Array.isArray(config.debug) ||
        Object.keys(config.debug).length !== 1 || typeof config.debug.echo_upstream_body !== 'boolean') {
      throw new Error('debug must be an object containing only boolean echo_upstream_body');
    }
  }
  if (typeof config.prompt !== 'string') throw new Error('prompt must be a string');
  if (!nonempty(config.model)) throw new Error('model must be a nonempty string');
  // pi-ai removes unpaired surrogates. Refuse them instead of silently changing the prompt.
  if (!config.prompt.isWellFormed()) throw new Error('prompt must contain well-formed Unicode; unpaired surrogates are unsupported');
  let model;
  if (config.provider === 'openai-codex') {
    model = config.model === sol61.id ? sol61
      : config.model === sol.id ? sol
      : config.model === luna.id ? luna
      : Object.hasOwn(OPENAI_CODEX_MODELS, config.model) ? OPENAI_CODEX_MODELS[config.model] : undefined;
    if (!model) throw new Error('Unknown subscription model in the pinned pi-ai catalog; no model substitution');
    if (model.api !== 'openai-codex-responses') throw new Error(`Unsupported subscription transport: ${model.api}`);
    if ('max_output_tokens' in config) throw new Error('pi-ai subscription transport does not enforce an output token cap; use timeout_seconds');
  } else if (config.provider === 'openrouter') {
    model = config.model === jev.id ? jev
      : config.model === mimo.id ? mimo
      : config.model === mimoFlash.id ? mimoFlash
      : config.model === deepseek.id ? deepseek
      : Object.hasOwn(OPENROUTER_MODELS, config.model) ? OPENROUTER_MODELS[config.model] : undefined;
    if (!model) throw new Error('Unknown OpenRouter model in the pinned pi-ai catalog; no model substitution');
    if (!['openai-completions', 'systemone'].includes(model.api)) throw new Error(`Unsupported OpenRouter transport: ${model.api}; no model rerouting`);
    if (config.max_output_tokens != null && !integer(config.max_output_tokens, 1, model.maxTokens)) throw new Error('OpenRouter max_output_tokens must be within the model limit when supplied');
    model = { ...model, compat: { ...model.compat, maxTokensField: 'max_tokens' } };
  } else {
    throw new Error('provider must be openai-codex (subscription) or openrouter');
  }
  const decisions = model.api === 'systemone' ? prepareDecisions(config) : null;
  if (config.reasoning === 'on') {
    if (config.provider !== 'openrouter' || !model.reasoning) {
      throw new Error('Explicit reasoning on requires an OpenRouter reasoning model');
    }
  } else if (config.reasoning !== 'provider-default' && !getSupportedThinkingLevels(model).includes(config.reasoning)) {
    throw new Error('Unsupported reasoning setting; choose a supported effort or provider-default');
  }
  const timeout = config.timeout_seconds ?? 300;
  if (!integer(timeout, 1, 3600)) throw new Error('timeout_seconds must be an integer from 1 to 3600');
  const hasSchema = Object.hasOwn(config, 'schema');
  const schema = hasSchema ? structuredClone(config.schema) : null;
  const validate = hasSchema ? compileSchema(schema) : null;
  return {
    model, decisions, transport, reasoning: config.reasoning, prompt: config.prompt, debug: structuredClone(config.debug),
    max_output_tokens: config.max_output_tokens ?? null, timeout_seconds: timeout,
    prompt_sha256: sha256(config.prompt), schema, schema_name: 'output',
    schema_sha256: hasSchema ? sha256(JSON.stringify(schema)) : null, validate,
  };
}

function formatPayload(payload, request) {
  if (request.model.provider === 'openai-codex') {
    // The serializer otherwise injects a system instruction and output preferences.
    payload.instructions = '';
    delete payload.tool_choice;
    delete payload.parallel_tool_calls;
    delete payload.include;
    if (payload.reasoning) delete payload.reasoning.summary;
    if (request.schema !== null) payload.text = {
      format: { type: 'json_schema', name: request.schema_name, strict: true, schema: request.schema },
    };
    else delete payload.text;
  } else {
    if (request.max_output_tokens === null) {
      // The SDK otherwise supplies a catalog default even when the caller omitted the cap.
      delete payload.max_tokens;
      delete payload.max_completion_tokens;
    }
    if (request.reasoning === 'provider-default') delete payload.reasoning;
    if (request.reasoning === 'on') payload.reasoning = { enabled: true };
    if (request.schema !== null) payload.response_format = {
      type: 'json_schema', json_schema: { name: request.schema_name, strict: true, schema: request.schema },
    };
    payload.provider = { require_parameters: true, allow_fallbacks: true };
    if (request.debug) payload.debug = request.debug;
  }
  return payload;
}

function safeHeaders(headers) {
  return Object.fromEntries([...new Headers(headers)].map(([key, value]) =>
    [key, /authorization|api-key|cookie|account-id/i.test(key) ? '[REDACTED]' : value]));
}

async function wireRequest(input, options) {
  const request = new Request(input, options);
  let body = Buffer.from(await request.arrayBuffer());
  if (request.headers.get('content-encoding') === 'zstd') body = zstdDecompressSync(body);
  return { url: request.url, method: request.method, headers: safeHeaders(request.headers), body: JSON.parse(body.toString('utf8')) };
}

// Keep useful error fields, never arbitrary provider bodies, headers, or stacks.
function errorDetails(error, redact, depth = 0) {
  const details = {};
  for (const key of ['name', 'code', 'status', 'type', 'message']) {
    const value = error?.[key];
    if (typeof value === 'string') details[key] = redact(value).slice(0, 1024);
    else if (typeof value === 'number' && Number.isFinite(value)) details[key] = value;
  }
  Object.assign(details, scalarFields(error?.metadata, ['provider_name', 'error_type', 'provider_code'], redact));
  if (error?.cause && depth < 2) details.cause = errorDetails(error.cause, redact, depth + 1);
  return details;
}

function scalarFields(value, keys, redact) {
  return Object.fromEntries(keys.flatMap(key => {
    const item = value?.[key];
    return typeof item === 'string' ? [[key, redact(item).slice(0, 1024)]]
      : typeof item === 'boolean' || (typeof item === 'number' && Number.isFinite(item)) ? [[key, item]] : [];
  }));
}

// Echoes may contain provider-owned fields. Retain settings and fingerprints,
// never the arbitrary body, headers, input text, or encrypted reasoning.
function upstreamRequestSummary(body, redact) {
  const summary = scalarFields(body, ['model', 'stream', 'store', 'max_tokens', 'max_completion_tokens', 'max_output_tokens'], redact);
  summary.body_sha256 = sha256(JSON.stringify(body));
  if (body.reasoning) summary.reasoning = scalarFields(body.reasoning, ['effort', 'max_tokens', 'summary', 'enabled', 'exclude'], redact);
  const format = body.text?.format ?? body.response_format?.json_schema;
  if (format) {
    summary.format = scalarFields(format, ['type', 'name', 'strict'], redact);
    if (format.schema !== undefined) summary.schema_sha256 = sha256(JSON.stringify(format.schema));
  }
  const input = body.input ?? body.messages;
  if (input !== undefined) {
    summary.input_sha256 = sha256(JSON.stringify(input));
    const texts = typeof input === 'string' ? [input] : Array.isArray(input) ? input.flatMap(item =>
      typeof item.content === 'string' ? [item.content] : Array.isArray(item.content) ?
        item.content.flatMap(part => typeof part.text === 'string' ? [part.text] : []) : []) : [];
    summary.prompt_text_sha256 = sha256(texts.join(''));
  }
  if (typeof body.instructions === 'string') summary.instructions_sha256 = sha256(body.instructions);
  return summary;
}

// Observe metadata and timing without retaining another copy of the token stream.
function observeResponse(response, metadata, timing, elapsed, redact, captureDebug) {
  timing.headers_ms = elapsed();
  metadata.http_status = response.status;
  for (const [header, key] of [['x-request-id', 'request_id'], ['retry-after', 'retry_after'],
    ['x-agent-request-id', 'proxy_request_id']]) {
    const value = response.headers.get(header);
    if (value) metadata[key] = redact(value).slice(0, 1024);
  }
  // Both callers request SSE. A missing Content-Type does not change that
  // request contract; an explicitly different response type still takes priority.
  const contentType = response.headers.get('content-type');
  if (!response.ok || !response.body || (contentType && !contentType.includes('text/event-stream'))) return response;
  const decoder = new TextDecoder();
  let buffer = '';
  let data = [];
  function dispatch() {
    if (!data.length) return;
    try {
      const event = JSON.parse(data.join('\n'));
      if (['response.completed', 'response.failed', 'response.incomplete'].includes(event.type)) {
        metadata.terminal_event = event.type;
      }
      const now = elapsed();
      const gap = now - (timing.last_event_ms ?? timing.headers_ms);
      timing.max_event_gap_ms = Math.max(timing.max_event_gap_ms, gap);
      timing.first_event_ms ??= now;
      timing.last_event_ms = now;
      timing.event_count++;
      const value = event.response ?? event;
      for (const key of ['id', 'model', 'provider', 'usage', 'service_tier']) {
        if (value[key] !== undefined && value[key] !== null) metadata[key] = value[key];
      }
      const error = value.error ?? (event.type === 'error' ? event : null);
      const debug = event.debug?.echo_upstream_body;
      if (captureDebug && debug && typeof debug === 'object' && !Array.isArray(debug)) {
        metadata.upstream_requests ??= [];
        if (metadata.upstream_requests.length < 16) metadata.upstream_requests.push(upstreamRequestSummary(debug, redact));
        else metadata.upstream_requests_dropped = (metadata.upstream_requests_dropped ?? 0) + 1;
      }
      if (timing.event_trace) {
        const fields = [...new Set((event.choices ?? []).flatMap(choice => Object.keys(choice.delta ?? {})))];
        if (timing.event_trace.length < 256) timing.event_trace.push({ at_ms: now, gap_ms: gap,
          kind: error ? 'error' : debug ? 'debug' : fields.length ? 'delta' : 'metadata',
          delta_fields: fields.map(field => redact(field).slice(0, 128)).slice(0, 32),
        });
        else timing.event_trace_dropped++;
      }
      if (error) {
        metadata.error = errorDetails(error, redact);
        timing.error_ms = now;
      } else timing.last_non_error_event_ms = now;
    } catch { /* [DONE] or non-JSON event; pi-ai remains the response parser. */ }
    data = [];
  }
  function line(value) {
    if (!value) dispatch();
    else if (value.startsWith('data:')) data.push(value.slice(5).replace(/^ /, ''));
    else if (value.startsWith(':')) timing.comment_count++;
  }
  function consume() {
    let end;
    while ((end = buffer.indexOf('\n')) >= 0) {
      line(buffer.slice(0, end).replace(/\r$/, ''));
      buffer = buffer.slice(end + 1);
    }
  }
  const reader = response.body.getReader();
  const body = new ReadableStream({
    async pull(controller) {
      let next;
      try { next = await reader.read(); }
      catch (error) {
        metadata.transport_error = { phase: 'body', ...errorDetails(error, redact) };
        timing.error_ms = elapsed();
        reader.releaseLock();
        controller.error(error);
        return;
      }
      if (next.done) {
        buffer += decoder.decode();
        consume();
        if (buffer) line(buffer.replace(/\r$/, ''));
        dispatch();
        timing.stream_end_ms = elapsed();
        reader.releaseLock();
        controller.close();
        return;
      }
      const now = elapsed();
      timing.max_byte_gap_ms = Math.max(timing.max_byte_gap_ms, now - (timing.last_byte_ms ?? timing.headers_ms));
      timing.first_byte_ms ??= now;
      timing.last_byte_ms = now;
      timing.byte_count += next.value.byteLength;
      buffer += decoder.decode(next.value, { stream: true });
      consume();
      controller.enqueue(next.value);
    },
    async cancel(reason) {
      try { await reader.cancel(reason); }
      finally { reader.releaseLock(); }
    },
  }, { highWaterMark: 0 });
  return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
}

function checkOutput(request, raw, stopReason) {
  if (stopReason !== 'stop') return { valid: false, output: null, validation_error: `Generation ended with ${stopReason}` };
  if (request.schema === null) return { valid: true, output: raw, validation_error: null };
  let output;
  try { output = JSON.parse(raw); }
  catch { return { valid: false, output: null, validation_error: 'Output is not bare JSON' }; }
  const valid = request.validate(output);
  return { valid, output, validation_error: valid ? null : `Output does not match JSON schema: ${JSON.stringify(request.validate.errors)}` };
}

function redactor(apiKey) {
  const secrets = [apiKey];
  try {
    const account = JSON.parse(Buffer.from(apiKey.split('.')[1], 'base64url').toString('utf8'))['https://api.openai.com/auth']?.chatgpt_account_id;
    if (nonempty(account)) secrets.push(account);
  } catch { /* API keys need no JWT decoding. */ }
  function redact(value) {
    if (typeof value === 'string') return secrets.reduce((text, secret) => text.replaceAll(secret, '[REDACTED]'), value);
    if (Array.isArray(value)) return value.map(redact);
    if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, redact(item)]));
    return value;
  }
  return redact;
}

async function executeDecisions(request, { apiKey, fetch, signal }) {
  const provenance = await identity();
  const redact = redactor(apiKey);
  const abort = AbortSignal.any([AbortSignal.timeout(request.timeout_seconds * 1000), ...(signal ? [signal] : [])]);
  const started = performance.now();
  const url = `${request.model.baseUrl.replace(/\/$/, '')}/systemone`;
  const init = { method: 'POST', redirect: 'error', signal: abort,
    headers: { authorization: `Bearer ${apiKey}`, 'content-type': 'application/json' },
    body: JSON.stringify({ model: request.model.id, ...request.decisions }) };
  const wire = await wireRequest(url, init);
  const metadata = {};
  let rawResponse = '', raw = '', stop = 'error', error = null, usage = null, valid = false, validationError = null, output = null;
  let phase = 'headers';
  try {
    const response = await fetch(url, init);
    metadata.http_status = response.status;
    for (const [header, key] of [['retry-after', 'retry_after'], ['x-request-id', 'request_id'],
      ['x-agent-request-id', 'proxy_request_id']]) {
      if (response.headers.has(header)) metadata[key] = response.headers.get(header);
    }
    phase = 'body';
    rawResponse = await response.text();
    phase = 'parse';
    const body = JSON.parse(rawResponse);
    for (const key of ['id', 'model', 'provider', 'usage']) if (body[key] != null) metadata[key] = body[key];
    if (body.usage && Number.isSafeInteger(body.usage.input_tokens) && Number.isSafeInteger(body.usage.output_tokens)) {
      usage = { input: body.usage.input_tokens, output: body.usage.output_tokens,
        totalTokens: body.usage.input_tokens + body.usage.output_tokens };
    }
    if (!response.ok || body.error) {
      metadata.error = errorDetails(body.error, redact);
      error = body.error?.message ?? `System One HTTP ${response.status}`;
    } else {
      stop = 'stop';
      output = body.answers ?? null;
      raw = output === null ? '' : JSON.stringify(output);
      valid = validateDecisions(request.decisions.questions, output);
      validationError = valid ? null : 'Invalid or missing System One answers';
      if (valid && request.validate && !request.validate(output)) {
        valid = false;
        validationError = `Output does not match JSON schema: ${JSON.stringify(request.validate.errors)}`;
      }
    }
  } catch (failure) {
    error = failure.message;
    if (phase !== 'parse') metadata.transport_error = { phase, ...errorDetails(failure, redact) };
    else validationError = 'System One response is not valid JSON';
  }
  if (abort.aborted) metadata.abort_reason = errorDetails(abort.reason, redact);
  return redact({ ...provenance, provider: request.model.provider, model: request.model.id, reasoning: request.reasoning,
    prompt_sha256: request.prompt_sha256, schema_sha256: request.schema_sha256,
    elapsed_ms: Math.round(performance.now() - started), request: wire, response_metadata: metadata,
    response_id: metadata.id ?? null, response_model: metadata.model ?? null, provider_thinking_level: null,
    http_attempts: 1, stop_reason: stop, raw_stop_reason: null, diagnostics: [], error,
    raw_response: rawResponse, raw_text: raw, output, valid, validation_error: validationError, usage,
    usage_note: 'System One native token counts; provider-reported USD charge is response_metadata.usage.cost. raw_text serializes typed answers; it is not generated prose.',
  });
}

/** Invoke pi-ai once through an explicit credential and transport. No auth discovery or fallback. */
async function executeModel(request, { apiKey, fetch = globalThis.fetch, signal, offline = false } = {}) {
  if (!nonempty(apiKey)) throw new Error('Explicit credentials are required for execution');
  if (request.model.api === 'systemone') return executeDecisions(request, { apiKey, fetch, signal });
  const provenance = await identity();
  const redact = redactor(apiKey);
  const abort = AbortSignal.any([AbortSignal.timeout(request.timeout_seconds * 1000), ...(signal ? [signal] : [])]);
  let wire;
  let calls = 0;
  const responseMetadata = {};
  const timing = { headers_ms: null, first_byte_ms: null, last_byte_ms: null,
    first_event_ms: null, last_event_ms: null, stream_end_ms: null, error_ms: null,
    event_count: 0, byte_count: 0, comment_count: 0, max_byte_gap_ms: 0, max_event_gap_ms: 0,
    last_non_error_event_ms: null,
    ...(request.debug?.echo_upstream_body ? { event_trace: [], event_trace_dropped: 0 } : {}) };
  const started = performance.now();
  const elapsed = () => Math.round(performance.now() - started);
  const options = {
    // Use the existing SDK serializer/parser only. Its native WebSocket path
    // silently retries/falls back even with maxRetries:0, violating our contract.
    apiKey, transport: 'sse', cacheRetention: 'none', maxRetries: 0, timeoutMs: request.timeout_seconds * 1000,
    signal: abort,
    reasoningEffort: ['provider-default', 'on'].includes(request.reasoning) ? undefined : request.reasoning === 'off' ? 'none' : request.reasoning,
    ...(request.max_output_tokens === null ? {} : { maxTokens: request.max_output_tokens }),
    onPayload: payload => formatPayload(payload, request),
    fetch: async (input, init) => {
      if (calls !== 0) throw new Error('Only one HTTP attempt is permitted');
      calls++;
      wire = await wireRequest(input, init);
      if (request.transport === 'websocket') wire = websocketRequest(wire);
      let response;
      try {
        response = request.transport === 'websocket' && !offline
          ? await websocketResponse(wire, init, responseMetadata, abort)
          : await fetch(input, { ...init, dispatcher: streamDispatcher });
      }
      catch (error) {
        responseMetadata.transport_error = { phase: 'headers', ...errorDetails(error, redact) };
        timing.error_ms = elapsed();
        throw error;
      }
      const observed = observeResponse(response, responseMetadata, timing, elapsed, redact, request.debug?.echo_upstream_body);
      // observeResponse sees the in-process parser interface. Never report its
      // synthetic status as a successful HTTP request to the server.
      if (request.transport === 'websocket' && !offline) responseMetadata.http_status = 101;
      return observed;
    },
  };
  const context = { messages: [{ role: 'user', content: request.prompt, timestamp: 0 }] };
  const stream = request.model.provider === 'openai-codex' ? codexStream : openrouterStream;
  const events = stream(request.model, context, options);
  for await (const _event of events) { /* Drain incrementally; the final message retains the output. */ }
  const result = await events.result();
  if (abort.aborted) responseMetadata.abort_reason = errorDetails(abort.reason, redact);
  const raw = result.content.filter(item => item.type === 'text').map(item => item.text).join('');
  return {
    ...provenance, provider: request.model.provider, model: request.model.id, reasoning: request.reasoning, transport: request.transport,
    prompt_sha256: request.prompt_sha256, schema_sha256: request.schema_sha256,
    elapsed_ms: elapsed(), timing, request: wire ?? null, response_metadata: redact(responseMetadata),
    response_id: redact(result.responseId ?? null), response_model: redact(result.responseModel ?? null),
    provider_thinking_level: result.providerThinkingLevel ?? null,
    http_attempts: calls, stop_reason: result.stopReason, raw_stop_reason: result.rawStopReason ?? null,
    diagnostics: redact(result.diagnostics ?? []), error: redact(result.errorMessage ?? null),
    raw_text: raw, usage: result.usage,
    usage_note: 'pi-ai input excludes cacheRead/cacheWrite; output includes reasoning. SDK cost is a catalog estimate, not an invoice or subscription charge.',
    ...checkOutput(request, raw, result.stopReason),
  };
}

export async function execute(request, options = {}) {
  const { apiKey, fetch = globalThis.fetch, signal, offline = false, experimentConcurrency } = options;
  if (!nonempty(apiKey)) throw new Error('Explicit credentials are required for execution');
  const admission = experimentConcurrency !== undefined && !offline
    ? await experimentAdmission(request, { apiKey, fetch, signal, concurrency: experimentConcurrency }) : null;
  let result;
  try { result = await executeModel(request, options); }
  catch (error) {
    if (admission) await admission.finish({ provider: request.model.provider,
      stop_reason: 'aborted', error: null,
      response_metadata: { abort_reason: { name: 'LocalError' } } });
    throw error;
  }
  if (admission) {
    await admission.finish(result);
    result.admission_wait_ms = admission.wait_ms;
  }
  return result;
}

/** Use actual SDK serializers with fabricated credentials and a transport that cannot send. */
export async function preview(request) {
  const fakeToken = 'preview.' + Buffer.from(JSON.stringify({
    'https://api.openai.com/auth': { chatgpt_account_id: 'offline-preview' },
  })).toString('base64') + '.not-a-signature';
  const result = await execute(request, {
    offline: true,
    apiKey: request.model.provider === 'openai-codex' ? fakeToken : 'offline-preview',
    fetch: async () => new Response('{"error":{"message":"Offline preview: no request sent"}}', { status: 400 }),
  });
  if (!result.request || result.http_attempts !== 1) throw new Error('pi-ai could not serialize this request for preview');
  return {
    runner_version: result.runner_version, pi_ai_version: result.pi_ai_version, runner_sha256: result.runner_sha256,
    provider: result.provider, model: result.model, reasoning: result.reasoning,
    prompt_sha256: request.prompt_sha256, schema_sha256: request.schema_sha256, prompt: request.prompt,
    credentials_read: false, network_requests: 0, ...result.request,
    notice: 'Actual serialized request decoded for inspection; WebSocket body is the response.create frame. Authentication headers are placeholders. Server acceptance and hidden provider behavior are untested.',
  };
}

export async function launchWithSelectedPi(input) {
  // Keep the complete request inside the account launcher's selected view.
  // The child reads this exact input, not a second copy of a mutable config file.
  const child = spawn('agent-account', ['exec', 'pi', '--', process.execPath,
    process.argv[1], 'run', '--send', '--pi-config'], { stdio: ['pipe', 'inherit', 'inherit'] });
  const interrupt = () => child.kill('SIGINT');
  const terminate = () => child.kill('SIGTERM');
  process.on('SIGINT', interrupt);
  process.on('SIGTERM', terminate);
  child.stdin.on('error', () => { /* Early refusal is reported by the child's exit status. */ });
  try {
    const completed = new Promise((resolve, reject) => {
      child.once('error', () => reject(new Error('Cannot launch agent-account for model authentication')));
      child.once('close', (code, signal) => resolve(code ?? 128 + (constants.signals[signal] ?? 1)));
    });
    child.stdin.end(input);
    process.exitCode = await completed;
  } finally {
    process.off('SIGINT', interrupt);
    process.off('SIGTERM', terminate);
  }
}

export async function localPiConnection(providerName = 'openai-codex') {
  // agent-account owns selection. Use this process's native Pi configuration
  // and captured local capability; never reselect via a child Pi invocation.
  try {
    const directory = process.env.PI_CODING_AGENT_DIR || join(homedir(), '.pi', 'agent');
    const models = JSON.parse(await readFile(join(directory, 'models.json'), 'utf8'));
    if (!['openai-codex', 'openrouter'].includes(providerName)) throw new Error();
    const provider = models.providers?.[providerName];
    const endpoint = new URL(provider.baseUrl);
    const route = providerName === 'openrouter' ? /^\/openrouter\/api\/v1\/?$/ : /^\/[A-Za-z0-9_-]+\/pi\/?$/;
    if (endpoint.protocol !== 'http:' || !['127.0.0.1', '[::1]'].includes(endpoint.hostname) ||
        !endpoint.port || endpoint.username || endpoint.password || endpoint.search || endpoint.hash ||
        !route.test(endpoint.pathname)) throw new Error();
    let apiKey;
    if (providerName === 'openrouter' && provider.apiKey === '$AGENT_OPENROUTER_CAPABILITY') {
      // Stock Pi interpolation, captured by the launcher; never an upstream key.
      // This format neither reads auth.json nor falls back when the variable is absent.
      apiKey = process.env.AGENT_OPENROUTER_CAPABILITY;
      if (typeof apiKey !== 'string' || !/^[\x21-\x7e]+$/.test(apiKey)) throw new Error();
    } else {
      // Keep the installed literal endpoint/token handoff working during rollout.
      // Do not interpret arbitrary environment references or execute key commands.
      if (providerName === 'openrouter' && /^[!$]/.test(provider.apiKey)) throw new Error();
      const auth = JSON.parse(await readFile(join(directory, 'auth.json'), 'utf8'));
      const credential = auth[providerName];
      if (credential?.type !== 'api_key' || !nonempty(credential.key) || credential.key !== provider.apiKey) throw new Error();
      apiKey = credential.key;
    }
    return { baseUrl: endpoint.href.replace(/\/$/, ''), apiKey };
  } catch {
    // Do not export a real bearer or fall back to the upstream endpoint.
    throw new Error('Valid local Pi configuration is required; run through agent-account exec pi');
  }
}

const help = `Usage: run-model.sh preview|run [CONFIG.json] [--send]
Reads one JSON config from file or stdin; writes one JSON result to stdout.
Required: provider, model, reasoning, prompt (sent exactly as supplied).
Optional: schema, timeout_seconds (default 300), transport (sse default; websocket for subscription only).
No tools, history, retries, prompt repair, or WebSocket-to-SSE fallback.
Providers: openai-codex subscription; OpenRouter chat via pi-ai; Jev via System One.
Jev prompt: JSON encoding of {state, questions}; reasoning must be provider-default.
Jev schema validates answers locally; no output caps or chat debug settings.
Other catalog transports are rejected explicitly; no model substitution or rerouting.
OpenRouter max_output_tokens is optional; omitted or null sends no caller cap. Subscription output caps are unsupported.
OpenRouter-only debug: {"echo_upstream_body":true} records upstream settings and a bounded event timeline.
reasoning selects a supported pi-ai effort, provider-default (no preference), or on (OpenRouter thinking enabled without a requested effort).
schema is validated locally without coercion; omitted schema accepts plain text.
JSON Schema draft-07 (default), 2019-09, and 2020-12 are supported without remote refs.
Provider schema support may be narrower; unsupported parameters remain request failures.
preview cannot send and reads no credentials. run requires --send.
Subscription auth: the selected account's credential service, launched automatically.
No upstream credential export, auth files, or paid OpenAI API fallback.
OpenRouter auth: the same local credential service, with one shared provider key.
Never reads OPENROUTER_API_KEY or OPENAI_API_KEY; no direct upstream fallback.
--version prints versions; --identity prints versions and source/dependency SHA-256.
Before paid execution, obtain approved maximum spend and record the commitment in
budget-ledger.json. Provider-enforced spending caps are not required.
Timeouts/output limits are not spending approval or monetary hard caps.
`;

export async function main() {
  const { values, positionals } = parseArgs({ allowPositionals: true, options: {
    help: { type: 'boolean' }, version: { type: 'boolean' }, identity: { type: 'boolean' },
    send: { type: 'boolean' }, 'auth-file': { type: 'string' }, 'pi-config': { type: 'boolean' },
  } });
  if (values.help) { process.stdout.write(help); return; }
  if (values.version) { process.stdout.write(`${VERSION} pi-ai/${PI_AI_VERSION}\n`); return; }
  if (values.identity) { process.stdout.write(JSON.stringify(await identity()) + '\n'); return; }
  if (values['auth-file']) throw new Error('--auth-file is no longer supported; subscription credentials are service-owned. Remove the option or set batch auth_file to null.');
  const [command, path] = positionals;
  if (!['preview', 'run'].includes(command) || positionals.length > 2) throw new Error(help);
  if (command === 'run' && !values.send) throw new Error('Sending requires the explicit --send flag');
  if (command === 'preview' && values.send) throw new Error('preview does not accept --send');
  const input = path ? await readFile(path, 'utf8') : await new Response(process.stdin).text();
  const request = await prepare(JSON.parse(input));
  if (command === 'preview') { process.stdout.write(JSON.stringify(await preview(request), null, 2) + '\n'); return; }
  if (!values['pi-config']) return launchWithSelectedPi(input);
  const connection = await localPiConnection(request.model.provider);
  request.model = { ...request.model, baseUrl: connection.baseUrl };
  const result = await execute(request, { apiKey: connection.apiKey,
    ...(process.env.ORACLE_EXPERIMENT_CONCURRENCY === undefined ? {} : {
      experimentConcurrency: Number(process.env.ORACLE_EXPERIMENT_CONCURRENCY) }) });
  process.stdout.write(JSON.stringify(result, null, 2) + '\n');
  if (!result.valid) process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { process.stderr.write(`${error.message}\n`); process.exitCode = 1; });
}
