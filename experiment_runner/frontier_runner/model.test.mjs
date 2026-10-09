import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createServer } from 'node:http';
import { zstdDecompressSync } from 'node:zlib';
import { test } from 'node:test';
import { Agent, getGlobalDispatcher, setGlobalDispatcher } from 'undici';
import { prepare, preview, execute, VERSION } from './model.mjs';

test('native cache affinity changes only the key and matching routing headers', async () => {
  const base = { provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium',
    prompt: 'Fixed instructions. Different comparison data.', schema };
  const plain = await preview(await prepare(base));
  const key = 'affinity-trial-0123456789';
  for (const prompt of [base.prompt, base.prompt, base.prompt.replace('Different', 'Other')]) {
    const routed = await preview(await prepare({ ...base, prompt, prompt_cache_key: key }));
    assert.equal(routed.body.prompt_cache_key, key);
    assert.equal(routed.headers['session-id'], key);
    assert.equal(routed.headers['x-client-request-id'], key);
    assert.equal(routed.credentials_read, false);
    assert.equal(routed.network_requests, 0);
    const body = structuredClone(routed.body);
    delete body.prompt_cache_key;
    const expected = structuredClone(plain.body);
    expected.input[0].content[0].text = prompt;
    assert.deepEqual(body, expected);
    const headers = { ...routed.headers };
    delete headers['session-id'];
    delete headers['x-client-request-id'];
    assert.deepEqual(headers, plain.headers);
  }
  for (const key of ['', ' padded ', '\n', 'x'.repeat(65), '🌿', null, 123]) {
    await assert.rejects(prepare({ ...base, prompt_cache_key: key }), /prompt_cache_key/);
  }
  await assert.rejects(prepare({ ...base, provider: 'openrouter', prompt_cache_key: key }), /subscription/);
});

test('native affinity survives production transport serialization and accounting', async () => {
  const instructions = 'Fixed 🍵 instructions.\r\n';
  const local = instructions + '{"local": {"meaning": "Food"}';
  const prepared = await prepare({ provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium',
    prompt: local + ', "canonical": {"meaning": "Cooking"}}', schema,
    prompt_cache_key: 'native-affinity-success', prompt_cache_prefix: instructions });
  const expected = (await preview(prepared)).body;
  const item = { id: 'msg-cache', type: 'message', role: 'assistant', status: 'completed',
    content: [{ type: 'output_text', text: '{"count":2,"label":null}', annotations: [] }] };
  const token = 'test.' + Buffer.from(JSON.stringify({ 'https://api.openai.com/auth': {
    chatgpt_account_id: 'test-account' } })).toString('base64') + '.signature';
  const result = await execute(prepared, { apiKey: token, fetch: async (url, options) => {
    const sent = new Request(url, options);
    assert.equal(sent.headers.get('session-id'), 'native-affinity-success');
    assert.equal(sent.headers.get('x-client-request-id'), 'native-affinity-success');
    let body = Buffer.from(await sent.arrayBuffer());
    if (sent.headers.get('content-encoding') === 'zstd') body = zstdDecompressSync(body);
    assert.deepEqual(JSON.parse(body.toString('utf8')), expected);
    return sse([
      { type: 'response.created', response: { id: 'resp-cache', status: 'in_progress' } },
      { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
      { type: 'response.output_item.done', output_index: 0, item },
      { type: 'response.completed', response: { id: 'resp-cache', model: 'gpt-6.1-sol', status: 'completed',
        output: [item], usage: { input_tokens: 100, output_tokens: 10,
          input_tokens_details: { cached_tokens: 60, cache_write_tokens: 20 },
          output_tokens_details: { reasoning_tokens: 3 } } } },
    ]);
  } });
  assert.equal(result.valid, true, result.error);
  assert.equal(result.http_attempts, 1);
  assert.deepEqual(result.request.body, expected);
  assert.equal(result.usage.input, 20);
  assert.equal(result.usage.cacheRead, 60);
  assert.equal(result.usage.cacheWrite, 20);
  assert.equal(result.usage.output, 10);
  assert.deepEqual(result.response_metadata.usage.input_tokens_details, { cached_tokens: 60, cache_write_tokens: 20 });
  assert.equal(result.response_metadata.usage.output_tokens_details.reasoning_tokens, 3);
  assert.equal(result.usage.cost.total, (20 * 2 + 60 * 0.1 + 20 * 2.5 + 10 * 10) / 1e6);
  assert(Math.abs(result.uncached_catalog_cost_estimate_usd - 0.0003) < 1e-12);
});

test('uncached cost snapshot handles all-cached input and full-request pricing tiers', async () => {
  for (const [input, read, write, output, expected] of [
    [100, 100, 0, 10, 0.0003],
    [300000, 290000, 5000, 50, 1.20075],
  ]) {
    const request = await prepare({ provider: 'openai-codex', model: 'gpt-6.1-sol',
      reasoning: 'medium', prompt: 'Return the count and label.', schema });
    const item = { id: 'cost-msg', type: 'message', role: 'assistant', status: 'completed',
      content: [{ type: 'output_text', text: '{"count":2,"label":null}', annotations: [] }] };
    const token = 'test.' + Buffer.from(JSON.stringify({ 'https://api.openai.com/auth': {
      chatgpt_account_id: 'test-account' } })).toString('base64') + '.signature';
    const result = await execute(request, { apiKey: token, fetch: async () => sse([
      { type: 'response.created', response: { id: 'cost-response', status: 'in_progress' } },
      { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
      { type: 'response.output_item.done', output_index: 0, item },
      { type: 'response.completed', response: { id: 'cost-response', model: 'gpt-6.1-sol',
        status: 'completed', output: [item], usage: { input_tokens: input, output_tokens: output,
          input_tokens_details: { cached_tokens: read, cache_write_tokens: write },
          output_tokens_details: { reasoning_tokens: 3 } } } },
    ]) });
    assert.equal(result.valid, true, result.error);
    assert(Math.abs(result.uncached_catalog_cost_estimate_usd - expected) < 1e-12);
    assert.equal(result.usage.input, input - read - write);
    assert.equal(result.usage.cacheRead, read);
    assert.equal(result.usage.cacheWrite, write);
    const actual = input > 272000 ? 0.10375 : 0.00011;
    assert(Math.abs(result.usage.cost.total - actual) < 1e-12);
  }
});

test('native prefix format uses fixed developer instructions and independent comparison data', async () => {
  const prefix = 'Fixed 🍵 instructions.\r\n<input>\n';
  const base = { provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium',
    prompt: prefix + '{"local":"Food"}\n</input>\n', schema,
    prompt_cache_prefix: prefix, prompt_cache_key: 'native-prefix-test' };
  const prepared = await prepare(base);
  assert.equal(prepared.promptCachePrefix, prefix);
  const comparison = await preview(prepared);
  const seed = await preview(await prepare({ ...base, prompt: prefix }));
  const other = await preview(await prepare({ ...base, prompt: prefix + '{"local":"Cooking"}' }));
  const differentKey = await preview(await prepare({ ...base, prompt_cache_key: 'native-other-key' }));
  assert.equal(comparison.body.input.length, 3);
  assert.equal(seed.body.input.length, 2);
  assert.deepEqual(seed.body.input, comparison.body.input.slice(0, 2));
  assert.deepEqual(other.body.input.slice(0, 2), seed.body.input);
  assert.deepEqual(differentKey.body.input, comparison.body.input);
  assert.deepEqual(comparison.body.input[0].tools, []);
  assert.equal(comparison.body.input[0].type, 'additional_tools');
  assert.equal(comparison.body.input[0].role, 'developer');
  assert.equal(comparison.body.input[1].role, 'developer');
  assert.equal(comparison.body.input[1].content[0].text, prefix);
  assert.equal(comparison.body.input[2].role, 'user');
  assert.equal(comparison.body.input.slice(1).map(i => i.content[0].text).join(''), base.prompt);
  assert.notEqual(other.body.input[2].id, comparison.body.input[2].id);
  for (const item of comparison.body.input) assert.match(item.id, /^(at|msg)_[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.deepEqual(comparison.body.reasoning, { effort: 'medium', context: 'all_turns' });
  assert.equal(comparison.body.instructions, undefined);
  assert.equal(comparison.body.tools, undefined);
  assert.equal(comparison.body.tool_choice, 'auto');
  assert.equal(comparison.body.parallel_tool_calls, false);
  assert.deepEqual(comparison.body.include, ['reasoning.encrypted_content']);
  assert.equal(comparison.headers['x-openai-internal-codex-responses-lite'], 'true');
  assert.equal(comparison.headers['x-codex-routing-hint'], 'model=gpt-6.1-sol');
  assert.equal(comparison.headers['session-id'], base.prompt_cache_key);
  assert.equal(comparison.headers['thread-id'], base.prompt_cache_key);
  assert.equal(comparison.body.prompt_cache_key, base.prompt_cache_key);
  assert.deepEqual(comparison.body.client_metadata, { session_id: base.prompt_cache_key, thread_id: base.prompt_cache_key });
  assert.equal(comparison.body.prompt_cache_options, undefined);
  assert.equal(comparison.body.input[1].content[0].prompt_cache_breakpoint, undefined);
  const defaultConfig = { provider: base.provider, model: base.model, reasoning: base.reasoning, prompt: base.prompt, schema };
  assert.equal((await prepare(defaultConfig)).promptCachePrefix, null);
  const ordinary = await preview(await prepare(defaultConfig));
  assert.equal(ordinary.body.input.length, 1);
  assert.equal(ordinary.body.input[0].content[0].text, base.prompt);
  assert.equal(ordinary.body.instructions, '');
  assert.equal(ordinary.headers['x-openai-internal-codex-responses-lite'], undefined);
});

test('cache affinity and prefix reject unvalidated WebSocket use', async () => {
  const config = { provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium',
    prompt: 'Fixed instructions. Data.', prompt_cache_key: 'sse-only', transport: 'websocket' };
  await assert.rejects(prepare(config), /prompt_cache_key.*SSE/);
  await assert.rejects(prepare({ ...config, prompt_cache_prefix: 'Fixed instructions. ' }), /prompt_cache_key.*SSE/);
});

test('native instructions-only seed keeps one rejected SDK transport attempt', async () => {
  const prefix = 'Fixed instructions.\n';
  const prepared = await prepare({ provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium',
    prompt: prefix, prompt_cache_prefix: prefix, prompt_cache_key: 'seed-rejection', schema });
  const expected = (await preview(prepared)).body;
  const token = 'test.' + Buffer.from(JSON.stringify({ 'https://api.openai.com/auth': {
    chatgpt_account_id: 'test-account' } })).toString('base64') + '.signature';
  let calls = 0;
  const result = await execute(prepared, { apiKey: token, fetch: async (url, options) => {
    calls++;
    const sent = new Request(url, options);
    let body = Buffer.from(await sent.arrayBuffer());
    if (sent.headers.get('content-encoding') === 'zstd') body = zstdDecompressSync(body);
    assert.deepEqual(JSON.parse(body.toString('utf8')), expected);
    assert.equal(sent.headers.get('x-openai-internal-codex-responses-lite'), 'true');
    assert.equal(expected.input.length, 2);
    return new Response('{"error":{"message":"Unsupported input format","type":"invalid_request_error"}}', { status: 400 });
  } });
  assert.equal(calls, 1);
  assert.equal(result.http_attempts, 1);
  assert.equal(result.valid, false);
  assert.equal(result.response_metadata.http_status, 400);
  assert.match(result.error, /Unsupported input format/);
  assert.equal(result.request.body.input.length, 2);
});

test('native prefix rejects invalid settings before serialization', async () => {
  const base = { provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium',
    prompt: 'Fixed instructions. Data', prompt_cache_prefix: 'Fixed instructions. ', prompt_cache_key: 'prefix-test' };
  for (const change of [
    { prompt_cache_prefix: '' }, { prompt_cache_prefix: ' ' }, { prompt_cache_prefix: null },
    { prompt_cache_prefix: 2 }, { prompt_cache_prefix: 'Other' },
    { prompt_cache_prefix: '\ud800' }, { model: 'gpt-6-sol' },
    { provider: 'openrouter', model: 'deepseek/deepseek-v4.1-flash' },
    { prompt_cache_key: undefined },
  ]) await assert.rejects(prepare({ ...base, ...change }), /prompt_cache_(prefix|key)/);
  const withoutKey = { ...base };
  delete withoutKey.prompt_cache_key;
  await assert.rejects(prepare(withoutKey), /prompt_cache_prefix.*prompt_cache_key/);
});

test('retired explicit cache configuration is rejected before serialization', async () => {
  for (const mode of ['explicit', 'implicit']) {
    await assert.rejects(prepare({ provider: 'openai-codex', model: 'gpt-6.1-sol',
      reasoning: 'medium', prompt: 'Static instructions. Changing comparison.',
      prompt_cache: { mode, prefixes: ['Static instructions. '] } }),
    /Unknown config field: prompt_cache/);
  }
});

for (const script of ['model.mjs']) {
  test(`${script}: CLI help preserves spending approval without requiring provider caps`, () => {
    const result = spawnSync(process.execPath, [new URL(script, import.meta.url).pathname, '--help'],
      { encoding: 'utf8', env: {} });
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /approved maximum spend/);
    assert.match(result.stdout, /budget-ledger\.json/);
    assert.match(result.stdout, /Provider-enforced spending caps are not required/);
    assert.doesNotMatch(result.stdout, /configure the required external provider-side spending limit/);
  });
}

const codex = { provider: 'openai-codex', model: 'gpt-6-astra', reasoning: 'low' };
const openrouter = { provider: 'openrouter', model: 'meta/muse-spark-1.3-contributor', reasoning: 'low', max_output_tokens: 4096 };
const prompt = '  Arbitrary prompt.\r\nKeep {POST}, <tags>, nostr:npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqq and 🦉 exactly.\n';
const config = (route = openrouter, extra = {}) => ({ ...route, prompt, ...extra });
const schema = {
  type: 'object', properties: { count: { type: 'integer' }, label: { type: ['string', 'null'] } },
  required: ['count', 'label'], additionalProperties: false,
};
const sse = frames => new Response(frames.map(frame => `data: ${typeof frame === 'string' ? frame : JSON.stringify(frame)}\n\n`).join(''),
  { headers: { 'content-type': 'text/event-stream' } });
const reply = (text, finish = 'stop') => sse([
  { id: 'response-test', model: 'reported-model', provider: 'reported-provider', choices: [{ index: 0, delta: { content: text }, finish_reason: null }] },
  { id: 'response-test', choices: [{ index: 0, delta: {}, finish_reason: finish }],
    usage: { prompt_tokens: 10, completion_tokens: 5, prompt_tokens_details: { cached_tokens: 4 }, completion_tokens_details: { reasoning_tokens: 2 } } },
  '[DONE]',
]);
const run = async (text, extra = {}, finish = 'stop') => execute(await prepare(config(openrouter, extra)), {
  apiKey: 'test-only', fetch: async () => reply(text, finish),
});

test('GPT-6 Luna uses the exact subscription model and max effort without a caller cap', async () => {
  const route = { provider: 'openai-codex', model: 'gpt-6-luna', reasoning: 'max' };
  const result = await preview(await prepare(config(route, { schema })));
  assert.equal(result.body.model, 'gpt-6-luna');
  assert.deepEqual(result.body.reasoning, { effort: 'max' });
  assert.equal(result.body.max_output_tokens, undefined);
  assert.equal(result.credentials_read, false);
  assert.equal(result.network_requests, 0);
  assert.deepEqual(result.body.text.format.schema, schema);
  await assert.rejects(prepare(config(route, { reasoning: 'minimal' })), /reasoning/);
  await assert.rejects(prepare(config(route, { max_output_tokens: 100 })), /output.*cap/);
});

for (const route of [codex, openrouter]) {
  test(`${route.provider}: arbitrary prompt is the only message and needs no schema`, async () => {
    const request = await prepare(config(route));
    const result = await preview(request);
    assert.equal(request.prompt, prompt);
    assert.equal(result.prompt, prompt);
    assert.equal(result.network_requests, 0);
    assert.equal(result.credentials_read, false);
    assert.equal(result.body.model, route.model);
    const messages = result.body.input ?? result.body.messages;
    assert.equal(messages.length, 1);
    assert.equal(messages[0].role, 'user');
    assert.equal(typeof messages[0].content === 'string' ? messages[0].content : messages[0].content[0].text, prompt);
    assert.equal(result.body.instructions ?? '', '');
    for (const key of ['tools', 'tool_choice', 'previous_response_id', 'parallel_tool_calls', 'include', 'system']) assert.equal(result.body[key], undefined);
    assert.equal(result.body.response_format, undefined);
    assert.equal(result.body.text, undefined);
    assert.ok(!JSON.stringify(result).includes('helpful assistant'));
  });

  test(`${route.provider}: caller schema is preserved and provider-default adds no reasoning preference`, async () => {
    const result = await preview(await prepare(config(route, { schema, reasoning: 'provider-default' })));
    const format = result.body.text?.format ?? result.body.response_format?.json_schema;
    assert.deepEqual(format, { ...(route.provider === 'openai-codex' ? { type: 'json_schema' } : {}), name: 'output', strict: true, schema });
    assert.equal(result.body.reasoning, undefined);
  });
}

test('OpenRouter allows compatible provider fallback without changing model, effort or schema', async () => {
  const route = { provider: 'openrouter', model: 'z-ai/glm-5.3-flash', reasoning: 'high' };
  const result = await preview(await prepare(config(route, { schema })));
  assert.deepEqual(result.body.provider, { require_parameters: true, allow_fallbacks: true });
  assert.equal(result.body.model, route.model);
  assert.deepEqual(result.body.reasoning, { effort: 'high' });
  assert.deepEqual(result.body.response_format.json_schema, { name: 'output', strict: true, schema });
  assert.equal(result.body.max_tokens, undefined);
  assert.equal(result.body.max_completion_tokens, undefined);
});

test('pinned MiMo route uses exact metadata with standard graded reasoning', async () => {
  const route = { provider: 'openrouter', model: 'xiaomi/mimo-v2.6-pro', reasoning: 'high' };
  const result = await preview(await prepare(config(route, { schema })));
  assert.equal(result.body.model, route.model);
  assert.deepEqual(result.body.reasoning, { effort: 'high' });
  assert.deepEqual(result.body.provider, { require_parameters: true, allow_fallbacks: true });
  assert.deepEqual(result.body.response_format.json_schema, { name: 'output', strict: true, schema });
  assert.equal(result.body.max_tokens, undefined);
  await assert.doesNotReject(prepare(config(route, { reasoning: 'medium' })));
  await assert.rejects(prepare(config(route, { reasoning: 'max' })), /reasoning/);
});

for (const model of ['qwen/qwen3.8-flash', 'xiaomi/mimo-v2.6-flash']) {
  test(`${model}: explicit thinking on sends a toggle, not an effort or token cap`, async () => {
    const request = await prepare(config({ provider: 'openrouter', model, reasoning: 'on' }, { schema }));
    const result = await preview(request);
    assert.equal(result.credentials_read, false);
    assert.equal(result.network_requests, 0);
    assert.equal(result.body.model, model);
    assert.deepEqual(result.body.reasoning, { enabled: true });
    assert.deepEqual(result.body.provider, { require_parameters: true, allow_fallbacks: true });
    assert.deepEqual(result.body.response_format.json_schema.schema, schema);
    assert.equal(result.body.messages[0].content, prompt);
    assert.equal(result.body.max_tokens, undefined);
    assert.equal(result.body.max_completion_tokens, undefined);
    const receipt = await execute(request, { apiKey: 'test-only', fetch: async (_url, options) => {
      assert.deepEqual(JSON.parse(options.body), result.body);
      return reply('{"count":2,"label":null}');
    } });
    assert.equal(receipt.valid, true);
    assert.equal(receipt.reasoning, 'on');
    const defaults = await preview(await prepare(config({ provider: 'openrouter', model, reasoning: 'provider-default' })));
    assert.equal(defaults.body.reasoning, undefined);
  });
}

test('thinking on refuses subscription and non-reasoning routes; MiMo Flash refuses invented effort levels', async () => {
  await assert.rejects(prepare(config(codex, { reasoning: 'on' })), /reasoning/i);
  await assert.rejects(prepare(config({ provider: 'openrouter', model: 'openai/gpt-4o', reasoning: 'on' })), /reasoning/i);
  for (const reasoning of ['low', 'medium', 'high', 'max']) {
    await assert.rejects(prepare(config({ provider: 'openrouter', model: 'xiaomi/mimo-v2.6-flash', reasoning })), /reasoning/i);
  }
  const disabled = await preview(await prepare(config({ provider: 'openrouter', model: 'xiaomi/mimo-v2.6-flash', reasoning: 'off' })));
  assert.deepEqual(disabled.body.reasoning, { effort: 'none' });
});

test('OpenRouter omitted or null cap sends no SDK-default token limit', async () => {
  const capped = await preview(await prepare(config(openrouter, { reasoning: 'medium', schema })));
  assert.equal(capped.body.max_tokens, 4096);
  const expected = structuredClone(capped.body);
  delete expected.max_tokens;
  for (const cap of [undefined, null]) {
    const input = config(openrouter, { reasoning: 'medium', schema });
    if (cap === undefined) delete input.max_output_tokens;
    else input.max_output_tokens = cap;
    const prepared = await prepare(input);
    const result = await execute(prepared, { apiKey: 'test-only', fetch: async (_url, options) => {
      assert.deepEqual(JSON.parse(options.body), expected);
      return reply('{"count":2,"label":null}');
    } });
    assert.equal(result.valid, true);
    assert.equal(result.http_attempts, 1);
    assert.deepEqual((await preview(prepared)).body, expected);
  }
});

test('plain-text output succeeds without JSON parsing; response and usage provenance survive', async () => {
  const text = 'A normal answer, not JSON.\n';
  const result = await run(text);
  assert.equal(result.valid, true);
  assert.equal(result.raw_text, text);
  assert.equal(result.output, text);
  assert.equal(result.validation_error, null);
  assert.equal(result.runner_version, VERSION);
  assert.equal(result.response_id, 'response-test');
  assert.equal(result.response_metadata.model, 'reported-model');
  assert.equal(result.response_metadata.provider, 'reported-provider');
  assert.equal(result.response_metadata.http_status, 200);
  assert.equal(result.usage.input, 6);
  assert.equal(result.usage.cacheRead, 4);
  assert.equal(result.usage.output, 5);
  assert.equal(result.response_metadata.usage.completion_tokens_details.reasoning_tokens, 2);
  assert.equal(result.http_attempts, 1);
});

test('caller JSON schema validates without coercion, deletion, defaults, or repair', async () => {
  const good = await run('{"count":2,"label":null}', { schema });
  assert.equal(good.valid, true);
  assert.deepEqual(good.output, { count: 2, label: null });
  for (const text of ['{"count":"2","label":null}', '{"count":2}', '{"count":2,"label":null,"extra":1}', '```json\n{"count":2,"label":null}\n```']) {
    const result = await run(text, { schema });
    assert.equal(result.valid, false);
    assert.equal(result.raw_text, text);
    assert.equal(result.http_attempts, 1);
  }
  const numericString = await run('{"count":"2","label":null}', { schema });
  assert.deepEqual(numericString.output, { count: '2', label: null });
});

test('schemas may use arrays, local references, or boolean schemas', async () => {
  const arraySchema = { $schema: 'https://json-schema.org/draft/2020-12/schema', $defs: { item: { type: 'string', minLength: 1 } }, type: 'array', items: { $ref: '#/$defs/item' }, minItems: 1 };
  assert.equal((await run('["arbitrary"]', { schema: arraySchema })).valid, true);
  assert.equal((await run('[""]', { schema: arraySchema })).valid, false);
  assert.equal((await run('42', { schema: true })).valid, true);
  assert.equal((await run('42', { schema: false })).valid, false);
  const out = await preview(await prepare(config(openrouter, { schema: false })));
  assert.equal(out.body.response_format.json_schema.schema, false);
});

test('standard JSON Schema formats are validated instead of ignored', async () => {
  const dateSchema = { type: 'string', format: 'date-time' };
  assert.equal((await run('"2026-09-16T12:00:00Z"', { schema: dateSchema })).valid, true);
  assert.equal((await run('"not a date"', { schema: dateSchema })).valid, false);
});

test('catalog models and supported caller reasoning are not restricted to the extraction baseline', async () => {
  for (const [model, reasoning, effort] of [['gpt-5.6-terra', 'high', 'high'], ['gpt-6-astra', 'max', 'max']]) {
    const result = await preview(await prepare(config({ ...codex, model, reasoning })));
    assert.equal(result.body.model, model);
    assert.deepEqual(result.body.reasoning, { effort });
  }
  await assert.rejects(prepare(config({ ...codex, reasoning: 'off' })), /reasoning/);
  await assert.rejects(prepare(config({ ...codex, reasoning: 'invented' })), /reasoning/);
});

test('GPT-6 Sol uses its exact subscription model and requested effort without substitution', async () => {
  const route = { ...codex, model: 'gpt-6-sol' };
  const request = await prepare(config(route));
  const result = await preview(request);
  assert.equal(request.model.id, 'gpt-6-sol');
  assert.equal(request.model.api, 'openai-codex-responses');
  assert.equal(request.model.contextWindow, 1050000);
  assert.equal(request.model.maxTokens, 128000);
  assert.equal(result.network_requests, 0);
  assert.equal(result.credentials_read, false);
  assert.equal(result.body.model, 'gpt-6-sol');
  assert.deepEqual(result.body.reasoning, { effort: 'low' });
  assert.equal(result.body.input[0].content[0].text, prompt);
  const item = { id: 'msg-sol', type: 'message', role: 'assistant', status: 'completed',
    content: [{ type: 'output_text', text: 'Sol response', annotations: [] }] };
  const token = 'test.' + Buffer.from(JSON.stringify({ 'https://api.openai.com/auth': { chatgpt_account_id: 'test-account' } })).toString('base64') + '.signature';
  const receipt = await execute(request, { apiKey: token, fetch: async (_url, options) => {
    const sent = new Request(_url, options);
    let body = Buffer.from(await sent.arrayBuffer());
    if (sent.headers.get('content-encoding') === 'zstd') body = zstdDecompressSync(body);
    assert.deepEqual(JSON.parse(body.toString('utf8')), result.body);
    return sse([
      { type: 'response.created', response: { id: 'resp-sol', status: 'in_progress' } },
      { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
      { type: 'response.output_text.delta', output_index: 0, content_index: 0, delta: 'Sol response' },
      { type: 'response.output_item.done', output_index: 0, item },
      { type: 'response.completed', response: { id: 'resp-sol', model: 'gpt-6-sol', status: 'completed', output: [item],
        usage: { input_tokens: 10, output_tokens: 2 } } },
    ]);
  } });
  assert.equal(receipt.valid, true, receipt.error);
  assert.equal(receipt.output, 'Sol response');
  assert.equal(receipt.model, 'gpt-6-sol');
  assert.equal(receipt.reasoning, 'low');
  assert.equal(receipt.http_attempts, 1);
  for (const reasoning of ['off', 'medium', 'high', 'xhigh', 'max']) {
    const other = await preview(await prepare(config({ ...route, reasoning })));
    assert.deepEqual(other.body.reasoning, { effort: reasoning === 'off' ? 'none' : reasoning });
  }
  await assert.rejects(prepare(config({ ...route, reasoning: 'minimal' })), /reasoning/);
  await assert.rejects(prepare(config(route, { max_output_tokens: 100 })), /output.*cap|output.*limit/i);
});

test('GPT-6.1 Sol sends the exact subscription model with supported reasoning and pricing', async () => {
  const route = { ...codex, model: 'gpt-6.1-sol' };
  const request = await prepare(config(route, { schema }));
  const result = await preview(request);
  assert.equal(request.model.api, 'openai-codex-responses');
  assert.equal(request.model.contextWindow, 1050000);
  assert.equal(request.model.maxTokens, 128000);
  assert.deepEqual(request.model.cost, {
    input: 2, output: 10, cacheRead: 0.1, cacheWrite: 2.5,
    tiers: [{ inputTokensAbove: 272000, input: 4, output: 15, cacheRead: 0.2, cacheWrite: 5 }],
  });
  assert.equal(result.body.model, 'gpt-6.1-sol');
  assert.deepEqual(result.body.reasoning, { effort: 'low' });
  assert.equal(result.body.input[0].content[0].text, prompt);
  assert.deepEqual(result.body.text.format.schema, schema);
  assert.equal(result.body.max_output_tokens, undefined);
  assert.equal(result.credentials_read, false);
  assert.equal(result.network_requests, 0);
  for (const reasoning of ['medium', 'high', 'xhigh', 'max']) {
    const other = await preview(await prepare(config({ ...route, reasoning })));
    assert.deepEqual(other.body.reasoning, { effort: reasoning });
  }
  for (const reasoning of ['off', 'minimal']) {
    await assert.rejects(prepare(config({ ...route, reasoning })), /reasoning/);
  }
  await assert.rejects(prepare(config(route, { max_output_tokens: 100 })), /output.*cap|output.*limit/i);
});

test('invalid requests fail before transport without silently rewriting prompt, model, schema, or caps', async () => {
  for (const extra of [{ stage: 'subjects' }, { messages: [] }, { system: 'extra' }, { tools: [] }, { prompt: '\ud800' }, { prompt: 1 }, { schema: null }, { schema: [] }, { schema: { type: 'invented' } }, { schema: { $ref: 'https://example.com/remote-schema' } }]) {
    await assert.rejects(prepare(config(openrouter, extra)));
  }
  await assert.rejects(prepare(config({ ...codex, model: 'unknown-model' })), /model/i);
  await assert.rejects(prepare(config({ ...openrouter, model: 'anthropic/claude-3-haiku' })), /unsupported.*transport/i);
  await assert.rejects(prepare(config(codex, { max_output_tokens: 100 })), /output.*cap|output.*limit/i);
  for (const cap of [0, -1, true, '4096', 1.5, Number.MAX_SAFE_INTEGER]) {
    await assert.rejects(prepare(config({ ...openrouter, max_output_tokens: cap })), /max_output_tokens/);
  }
  await assert.rejects(prepare(config(openrouter, { timeout_seconds: 0 })), /timeout_seconds/);
  await assert.rejects(prepare(config({ ...codex, provider: 'openai' })), /provider/);
});

test('model IDs must be strings', async () => {
  await assert.rejects(prepare(config({ ...codex, model: ['gpt-6-astra'] })), /model/i);
});

test('asynchronous schema extensions cannot bypass validation', async () => {
  await assert.rejects(prepare(config(openrouter, { schema: { $async: true, type: 'string' } })), /schema/i);
});

test('failures preserve raw text and usage without retrying', async () => {
  const result = await run('partial answer', {}, 'length');
  assert.equal(result.valid, false);
  assert.equal(result.raw_text, 'partial answer');
  assert.equal(result.usage.output, 5);
  assert.equal(result.http_attempts, 1);
  let attempts = 0;
  const failed = await execute(await prepare(config()), { apiKey: 'secret-test-key', fetch: async () => {
    attempts++;
    return new Response('{"error":{"message":"Failure: secret-test-key"}}', { status: 429 });
  } });
  assert.equal(attempts, 1);
  assert.equal(failed.valid, false);
  assert.equal(failed.response_metadata.http_status, 429);
  assert.ok(!JSON.stringify(failed).includes('secret-test-key'));
});

test('chunked multiline SSE preserves native metadata and exact text', async () => {
  const frames = 'data: {"id":"multiline-response",\r\n' +
    'data: "model":"native-model", "provider":"native-provider", "choices":[{"index":0,"delta":{"content":"Hello 🦉"},"finish_reason":null}]}\r\n\r\n' +
    'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}\r\n\r\n' +
    'data: [DONE]\r\n\r\n';
  const bytes = Buffer.from(frames);
  const result = await execute(await prepare(config()), { apiKey: 'test-only', fetch: async () => new Response(new ReadableStream({
    start(controller) {
      for (let offset = 0; offset < bytes.length; offset += 3) controller.enqueue(bytes.subarray(offset, offset + 3));
      controller.close();
    },
  }), { headers: { 'content-type': 'text/event-stream' } }) });
  assert.equal(result.valid, true, result.error);
  assert.equal(result.raw_text, 'Hello 🦉');
  assert.equal(result.response_metadata.id, 'multiline-response');
  assert.equal(result.response_metadata.provider, 'native-provider');
  assert.equal(result.response_metadata.usage.completion_tokens, 2);
});

test('Codex SSE accepted by the SDK retains raw usage even under an application/json header', async () => {
  const request = await prepare(config({ ...codex, model: 'gpt-6.1-sol', reasoning: 'medium' }, { schema }));
  const expected = (await preview(request)).body;
  const text = '{"count":2,"label":"🦉"}';
  const item = { id: 'msg-mime', type: 'message', role: 'assistant', status: 'completed',
    content: [{ type: 'output_text', text, annotations: [] }] };
  const usage = { input_tokens: 100, output_tokens: 10, total_tokens: 110,
    input_tokens_details: { cached_tokens: 60, cache_write_tokens: 0 },
    output_tokens_details: { reasoning_tokens: 3 },
    attribution: { items: { 'msg-fixed': { input_tokens: 60, cached_tokens: 60, cache_write_tokens: 0 } } } };
  const events = [
    { type: 'response.created', response: { id: 'resp-mime', status: 'in_progress' } },
    { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
    { type: 'response.output_item.done', output_index: 0, item },
    { type: 'response.completed', response: { id: 'resp-mime', model: 'gpt-6.1-sol', status: 'completed', output: [item], usage } },
  ];
  const bytes = Buffer.from(events.map(event => `data: ${JSON.stringify(event)}\n\n`).join(''));
  const key = 'test.' + Buffer.from(JSON.stringify({ 'https://api.openai.com/auth': {
    chatgpt_account_id: 'test-account' } })).toString('base64url') + '.signature';
  let calls = 0;
  const result = await execute(request, { apiKey: key, fetch: async () => {
    calls++;
    return new Response(new ReadableStream({ start(controller) {
      for (let offset = 0; offset < bytes.length; offset += 3) controller.enqueue(bytes.subarray(offset, offset + 3));
      controller.close();
    } }), { headers: { 'content-type': 'application/json', 'x-agent-request-id': 'proxy-mime' } });
  } });
  assert.equal(result.valid, true, result.error);
  assert.equal(result.response_id, 'resp-mime');
  assert.equal(result.usage.cacheRead, 60);
  assert.deepEqual(result.response_metadata.usage, usage);
  assert.equal(result.response_metadata.terminal_event, 'response.completed');
  assert.equal(result.timing.byte_count, bytes.length);
  assert.equal(result.timing.event_count, events.length);
  assert.equal(result.raw_text, text);
  assert.deepEqual(result.request.body, expected);
  assert.equal(calls, 1);
  assert.equal(result.http_attempts, 1);
});

test('streamed errors retain distinguishing codes and safe fields before SDK normalization', async () => {
  for (const code of [429, 502]) {
    const response = sse([
      { id: 'error-response', model: 'reported-model', provider: 'Meta', choices: [] },
      { error: { code, message: 'Network connection lost. secret-test-key', type: 'upstream_error',
        metadata: { provider_name: 'Meta', error_type: 'provider_unavailable', provider_code: 'connection_closed',
          raw: 'private upstream body', headers: { authorization: 'private-header' } } } },
    ]);
    response.headers.set('retry-after', '60');
    response.headers.set('x-request-id', 'test-request-id');
    response.headers.set('set-cookie', 'private-cookie');
    const result = await execute(await prepare(config()), { apiKey: 'secret-test-key', fetch: async () => response });
    assert.equal(result.valid, false);
    assert.equal(result.http_attempts, 1);
    assert.equal(result.response_metadata.http_status, 200);
    assert.deepEqual(result.response_metadata.error, {
      code, message: 'Network connection lost. [REDACTED]', type: 'upstream_error', provider_name: 'Meta',
      error_type: 'provider_unavailable', provider_code: 'connection_closed',
    });
    assert.equal(result.response_metadata.retry_after, '60');
    assert.equal(result.response_metadata.request_id, 'test-request-id');
    assert.ok(result.timing.error_ms >= result.timing.headers_ms);
    assert.equal(result.timing.event_count, 2);
    const metadata = JSON.stringify(result.response_metadata);
    for (const secret of ['secret-test-key', 'private upstream body', 'private-header', 'private-cookie']) {
      assert.ok(!metadata.includes(secret));
    }
  }
});

test('OpenRouter upstream debugging is opt-in and changes no inference settings', async () => {
  const baseline = await preview(await prepare(config()));
  const debug = await preview(await prepare(config(openrouter, { debug: { echo_upstream_body: true } })));
  assert.deepEqual(debug.body, { ...baseline.body, debug: { echo_upstream_body: true } });
  assert.equal(baseline.body.debug, undefined);
  for (const value of [true, null, {}, { echo_upstream_body: 'yes' }, { echo_upstream_body: true, extra: true }]) {
    await assert.rejects(prepare(config(openrouter, { debug: value })), /debug/);
  }
  await assert.rejects(prepare(config(codex, { debug: { echo_upstream_body: true } })), /OpenRouter/);
});

test('debug receipts retain upstream settings and event gaps but not upstream bodies or reasoning', async () => {
  const request = await prepare(config(openrouter, { schema, debug: { echo_upstream_body: true } }));
  const frames = [
    { choices: [], debug: { echo_upstream_body: { model: 'upstream-model', stream: true, max_output_tokens: 4096,
      reasoning: { effort: 'low', private: 'private-field' }, text: { format: { type: 'json_schema', schema, strict: true } },
      input: [{ role: 'user', content: [{ type: 'input_text', text: prompt }] }], authorization: 'private-header' } } },
    { choices: [{ index: 0, delta: { reasoning: 'private-thought' }, finish_reason: null }] },
    { error: { code: 502, message: 'Network connection lost.', metadata: { error_type: 'provider_unavailable', provider_code: 123 } } },
  ];
  let index = 0;
  const result = await execute(request, { apiKey: 'test-only', fetch: async () => new Response(new ReadableStream({
    async pull(controller) {
      if (index === frames.length) { controller.close(); return; }
      await new Promise(resolve => setTimeout(resolve, 20));
      controller.enqueue(Buffer.from(`: OPENROUTER PROCESSING\n\ndata: ${JSON.stringify(frames[index++])}\n\n`));
    },
  }), { headers: { 'content-type': 'text/event-stream' } }) });
  const upstream = result.response_metadata.upstream_requests[0];
  assert.equal(upstream.model, 'upstream-model');
  assert.equal(upstream.stream, true);
  assert.equal(upstream.max_output_tokens, 4096);
  assert.deepEqual(upstream.reasoning, { effort: 'low' });
  assert.equal(upstream.schema_sha256, request.schema_sha256);
  assert.equal(upstream.prompt_text_sha256, request.prompt_sha256);
  assert.equal(result.response_metadata.error.provider_code, 123);
  assert.equal(result.timing.event_trace.length, 3);
  assert.equal(result.timing.event_trace[0].kind, 'debug');
  assert.deepEqual(result.timing.event_trace[1].delta_fields, ['reasoning']);
  assert.equal(result.timing.event_trace[2].kind, 'error');
  assert.ok(result.timing.max_event_gap_ms >= 10);
  assert.ok(result.timing.max_byte_gap_ms >= 10);
  assert.equal(result.timing.comment_count, 3);
  assert.ok(result.timing.last_non_error_event_ms < result.timing.error_ms);
  for (const secret of ['private-header', 'private-thought', 'private-field']) assert.ok(!JSON.stringify(result).includes(secret));
  assert.equal(result.http_attempts, 1);
});

test('event trace is bounded while aggregate timing covers the whole stream', async () => {
  const frames = Array.from({ length: 300 }, () => ({ choices: [] }));
  frames.push({ choices: [{ index: 0, delta: { content: 'ok' }, finish_reason: 'stop' }] });
  const result = await execute(await prepare(config(openrouter, { debug: { echo_upstream_body: true } })), {
    apiKey: 'test-only', fetch: async () => sse(frames),
  });
  assert.equal(result.valid, true);
  assert.equal(result.timing.event_count, 301);
  assert.equal(result.timing.event_trace.length, 256);
  assert.equal(result.timing.event_trace_dropped, 45);
  assert.ok(result.timing.last_event_ms >= result.timing.event_trace.at(-1).at_ms);
  assert.equal((await run('ok')).timing.event_trace, undefined);
});

test('stream timing distinguishes received bytes, parsed events, and normal completion', async () => {
  const result = await run('answer');
  const timing = result.timing;
  assert.ok(timing.headers_ms >= 0);
  assert.ok(timing.first_byte_ms >= timing.headers_ms);
  assert.ok(timing.last_byte_ms >= timing.first_byte_ms);
  assert.ok(timing.first_event_ms >= timing.first_byte_ms);
  assert.ok(timing.last_event_ms >= timing.first_event_ms);
  assert.ok(timing.stream_end_ms >= timing.last_event_ms);
  assert.ok(result.elapsed_ms >= timing.stream_end_ms);
  assert.equal(timing.event_count, 2);
  assert.ok(timing.byte_count > 0);
  assert.equal(result.response_metadata.error, undefined);
  assert.equal(result.response_metadata.transport_error, undefined);
});

for (const route of [openrouter, codex]) {
for (const declaredType of [true, false]) {
test(`${route.provider}: ${declaredType ? 'declared' : 'headerless'} SSE body failure preserves its cause without retrying`, async () => {
  const key = route === codex ? 'test.' + Buffer.from(JSON.stringify({
    'https://api.openai.com/auth': { chatgpt_account_id: 'test-account' },
  })).toString('base64url') + '.signature' : 'secret-test-key';
  const first = route === codex
    ? { type: 'response.created', response: { id: 'broken-stream', status: 'in_progress' } }
    : { id: 'broken-stream', choices: [] };
  let read = false;
  const result = await execute(await prepare(config(route)), { apiKey: key, fetch: async () =>
    new Response(new ReadableStream({ pull(controller) {
      if (!read) {
        read = true;
        controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(first)}\n\n`));
      } else {
        const cause = Object.assign(new Error(`socket closed ${key}`), { code: 'UND_ERR_SOCKET', private: 'must-not-record' });
        controller.error(new TypeError('terminated', { cause }));
      }
    } }), { headers: declaredType ? { 'content-type': 'text/event-stream' } : {} }) });
  assert.equal(result.http_attempts, 1);
  assert.equal(result.error, 'terminated');
  assert.equal(result.response_metadata.error, undefined);
  assert.deepEqual(result.response_metadata.transport_error, {
    phase: 'body', name: 'TypeError', message: 'terminated',
    cause: { name: 'Error', message: 'socket closed [REDACTED]', code: 'UND_ERR_SOCKET' },
  });
  assert.equal(result.response_metadata.id, 'broken-stream');
  assert.equal(result.timing.event_count, 1);
  assert.ok(result.timing.byte_count > 0);
  assert.ok(result.timing.error_ms >= result.timing.last_byte_ms);
  assert.equal(result.timing.stream_end_ms, null);
  assert.ok(!JSON.stringify(result).includes('must-not-record'));
  assert.ok(!JSON.stringify(result).includes(key));
});
}
}

test('an explicit JSON response is not observed as SSE', async () => {
  const result = await execute(await prepare(config()), { apiKey: 'test-only', fetch: async () =>
    new Response('{"id":"not-a-stream"}', { headers: { 'content-type': 'application/json' } }) });
  assert.equal(result.valid, false);
  assert.equal(result.response_metadata.id, undefined);
  assert.equal(result.timing.first_event_ms, null);
  assert.equal(result.timing.event_count, 0);
});

test('error text is redacted before bounding, and HTTP retry hints survive', async () => {
  const response = sse([{ error: { code: 502, message: 'x'.repeat(990) + 'secret-test-key' + 'y'.repeat(2000) } }]);
  const result = await execute(await prepare(config()), { apiKey: 'secret-test-key', fetch: async () => response });
  assert.ok(result.response_metadata.error.message.length <= 1024);
  assert.ok(!result.response_metadata.error.message.includes('secret-test'));
  const rejected = await execute(await prepare(config()), { apiKey: 'test-only', fetch: async () =>
    new Response('{"error":{"message":"Rate limited"}}', { status: 429, headers: { 'retry-after': '120' } }) });
  assert.equal(rejected.response_metadata.retry_after, '120');
  assert.equal(rejected.http_attempts, 1);
});

test('failure before headers records its cause and caller cancellation remains an abort', async () => {
  const request = await prepare(config());
  const failed = await execute(request, { apiKey: 'test-only', fetch: async () => {
    throw new TypeError('fetch failed', { cause: Object.assign(new Error('unreachable'), { code: 'ECONNREFUSED' }) });
  } });
  assert.equal(failed.response_metadata.transport_error.phase, 'headers');
  assert.equal(failed.response_metadata.transport_error.cause.code, 'ECONNREFUSED');
  assert.equal(failed.timing.headers_ms, null);
  assert.equal(failed.timing.first_byte_ms, null);
  assert.equal(failed.http_attempts, 1);
  const abort = new AbortController();
  const cancelled = await execute(request, { apiKey: 'test-only', signal: abort.signal, fetch: async () => {
    abort.abort();
    throw abort.signal.reason;
  } });
  assert.equal(cancelled.stop_reason, 'aborted');
  assert.equal(cancelled.response_metadata.abort_reason.name, 'AbortError');
  assert.equal(cancelled.http_attempts, 1);
});

for (const route of [openrouter, codex]) {
test(`${route.provider}: real HTTP streaming ignores body idle limits but keeps the overall deadline`, async t => {
  const previousDispatcher = getGlobalDispatcher();
  const shortIdleDispatcher = new Agent({ bodyTimeout: 50 });
  setGlobalDispatcher(shortIdleDispatcher);
  t.after(async () => {
    setGlobalDispatcher(previousDispatcher);
    await shortIdleDispatcher.destroy();
  });
  const item = { id: 'msg-idle-test', type: 'message', role: 'assistant', status: 'completed',
    content: [{ type: 'output_text', text: 'answer', annotations: [] }] };
  const frames = route === codex ? [
    { type: 'response.created', response: { id: 'resp-idle-test', status: 'in_progress' } },
    { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
    { type: 'response.output_text.delta', output_index: 0, content_index: 0, delta: 'answer' },
    { type: 'response.output_item.done', output_index: 0, item },
    { type: 'response.completed', response: { id: 'resp-idle-test', model: route.model, status: 'completed',
      output: [item], usage: { input_tokens: 2, output_tokens: 1 } } },
  ] : [
    { id: 'resp-idle-test', choices: [] },
    { id: 'resp-idle-test', choices: [{ index: 0, delta: { content: 'answer' }, finish_reason: null }] },
    { id: 'resp-idle-test', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
      usage: { prompt_tokens: 2, completion_tokens: 1 } },
    '[DONE]',
  ];
  const encode = frame => `data: ${typeof frame === 'string' ? frame : JSON.stringify(frame)}\n\n`;
  const calls = [];
  let stall = false;
  const server = createServer((request, response) => {
    request.resume();
    calls.push(request.url);
    response.writeHead(200); // Production subscription responses can omit Content-Type.
    response.write(encode(frames[0]));
    if (stall) return;
    // Undici's body timeout uses coarse timers even for a scaled 50-ms limit.
    const timer = setTimeout(() => response.end(frames.slice(1).map(encode).join('')), 1800);
    response.once('close', () => clearTimeout(timer));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  // Prove the scaled idle limit is active on the real HTTP path before calling the runner.
  await assert.rejects(async () => (await fetch(baseUrl + '/control')).text(),
    error => error.cause?.code === 'UND_ERR_BODY_TIMEOUT');
  const key = route === codex ? 'test.' + Buffer.from(JSON.stringify({
    'https://api.openai.com/auth': { chatgpt_account_id: 'test-account' },
  })).toString('base64url') + '.signature' : 'test-only';
  const request = await prepare(config(route, { timeout_seconds: 3 }));
  request.model = { ...request.model, baseUrl };
  const completed = await execute(request, { apiKey: key });
  assert.equal(completed.valid, true, JSON.stringify(completed.response_metadata));
  assert.equal(completed.raw_text, 'answer');
  assert.equal(completed.http_attempts, 1);
  assert.equal(completed.response_metadata.transport_error, undefined);
  assert.equal(getGlobalDispatcher(), shortIdleDispatcher); // No process-wide timeout override.

  stall = true;
  const timedOut = await execute({ ...request, timeout_seconds: 1 }, { apiKey: key });
  assert.equal(timedOut.valid, false);
  assert.equal(timedOut.http_attempts, 1);
  assert.equal(timedOut.stop_reason, 'aborted');
  assert.equal(timedOut.response_metadata.abort_reason.name, 'TimeoutError');
  assert.ok(timedOut.elapsed_ms >= 900 && timedOut.elapsed_ms < 3000);
  assert.equal(timedOut.response_metadata.transport_error?.cause?.code, undefined);
  assert.equal(calls.length, 3);
});
}

test('independent executions never supply earlier input or generated output to later calls', async () => {
  await run('previous response marker', { prompt: 'previous prompt marker' });
  const next = await preview(await prepare(config(openrouter, { prompt: 'next prompt' })));
  assert.deepEqual(next.body.messages, [{ role: 'user', content: 'next prompt' }]);
});

test('CLI version and offline preview do not require credentials; execution requires explicit sending', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'model-runner-test-'));
  const cli = new URL('model.mjs', import.meta.url).pathname;
  const env = { PATH: process.env.PATH, HOME: dir };
  try {
    const version = spawnSync(process.execPath, [cli, '--version'], { encoding: 'utf8', env });
    assert.equal(version.status, 0, version.stderr);
    assert.equal(version.stdout.trim(), `${VERSION} pi-ai/0.85.1`);
    const identity = spawnSync(process.execPath, [cli, '--identity'], { encoding: 'utf8', env });
    assert.equal(identity.status, 0, identity.stderr);
    assert.equal(JSON.parse(identity.stdout).runner_version, VERSION);
    assert.match(JSON.parse(identity.stdout).runner_sha256, /^[0-9a-f]{64}$/);
    const result = spawnSync(process.execPath, [cli, 'preview'], { input: JSON.stringify(config(codex)), encoding: 'utf8', env });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(JSON.parse(result.stdout).network_requests, 0);
    const denied = spawnSync(process.execPath, [cli, 'run'], { input: JSON.stringify(config(codex)), encoding: 'utf8', env });
    assert.equal(denied.status, 1);
    assert.match(denied.stderr, /--send/);
    await writeFile(join(dir, 'auth.json'), '{"OPENAI_API_KEY":"must-not-use"}');
    const auth = spawnSync(process.execPath, [cli, 'run', '--send', '--auth-file', join(dir, 'auth.json')], { input: JSON.stringify(config(codex)), encoding: 'utf8', env });
    assert.equal(auth.status, 1);
    assert.ok(!auth.stderr.includes('must-not-use'));
  } finally { await rm(dir, { recursive: true }); }
});
