import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { prepare, preview, execute } from './model.mjs';

const questions = {
  contains: { type: 'choice', instructions: 'Does A contain B?', criteria: { yes: 'Yes', no: 'No', uncertain: 'Cannot determine' } },
  related: { type: 'noul', instructions: 'Are these topics related?' },
  strength: { type: 'score', instructions: 'How related?', criteria: ['Unrelated', 'Related'] },
};
const config = (extra = {}) => ({ provider: 'openrouter', model: 'typesafe/jev-1.13',
  reasoning: 'provider-default', prompt: JSON.stringify({ state: { A: 'Food', B: 'Bread' }, questions }), ...extra });
const answers = {
  contains: { type: 'choice', choice: 'yes', probabilities: { yes: 0.9, no: 0.07, uncertain: 0.03 }, confidence: 0.8 },
  related: { type: 'noul', noul: 0.99 },
  strength: { type: 'score', score: 0.9, confidence: 0.8, probabilities: { 0: 0.1, 1: 0.9 }, legend: { 0: 'Unrelated', 1: 'Related' } },
};
const body = { id: 'gen-test', model: 'typesafe/jev-1.13-20260917', provider: 'TypeSafe', answers,
  usage: { input_tokens: 100, output_tokens: 30, cost: 0.0000042 } };

test('Jev preview uses System One with exact state/questions and no chat controls', async () => {
  const result = await preview(await prepare(config()));
  assert.equal(result.url, 'https://openrouter.ai/api/v1/systemone');
  assert.deepEqual(result.body, { model: 'typesafe/jev-1.13', ...JSON.parse(config().prompt) });
  assert.equal(result.network_requests, 0);
  assert.equal(result.credentials_read, false);
  assert.equal(result.headers.authorization, '[REDACTED]');
});

test('Jev preserves typed answers, probabilities, native costs and local-only schema', async () => {
  const request = await prepare(config({ schema: { type: 'object', required: ['contains', 'related', 'strength'] } }));
  request.model = { ...request.model, baseUrl: 'http://127.0.0.1:18743/openrouter/api/v1' };
  let calls = 0;
  const result = await execute(request, { apiKey: 'local-secret', fetch: async (url, init) => {
    calls++;
    assert.equal(url, 'http://127.0.0.1:18743/openrouter/api/v1/systemone');
    assert.equal(init.redirect, 'error');
    assert.equal(JSON.parse(init.body).schema, undefined);
    return Response.json(body);
  } });
  assert.equal(calls, 1);
  assert.equal(result.valid, true);
  assert.deepEqual(result.output, answers);
  assert.deepEqual(JSON.parse(result.raw_text), answers);
  assert.equal(result.response_metadata.usage.cost, 0.0000042);
  assert.equal(result.usage.input, 100);
  assert.equal(result.usage.output, 30);
  assert.equal(result.response_model, body.model);
  assert.ok(!JSON.stringify(result).includes('local-secret'));
});

test('Jev refuses invalid requests and unsupported settings before sending', async () => {
  for (const extra of [{ reasoning: 'max' }, { max_output_tokens: 10 }, { debug: { echo_upstream_body: true } },
    { prompt: 'plain text' }, { prompt: JSON.stringify({ state: '', questions: {} }) },
    { prompt: JSON.stringify({ model: 'different', state: '', questions }) },
    { prompt: JSON.stringify({ state: '', questions: { x: { type: 'choice', instructions: 'Pick', criteria: {} } } }) }]) {
    await assert.rejects(prepare(config(extra)));
  }
});

test('Jev errors preserve HTTP/retry metadata, redact secrets, and never retry', async () => {
  let calls = 0;
  const result = await execute(await prepare(config()), { apiKey: 'secret-value', fetch: async () => {
    calls++;
    return Response.json({ error: { code: 429, message: 'secret-value busy' } }, { status: 429,
      headers: { 'retry-after': '3', 'x-agent-request-id': 'proxy-systemone-fixture' } });
  } });
  assert.equal(calls, 1);
  assert.equal(result.valid, false);
  assert.equal(result.response_metadata.http_status, 429);
  assert.equal(result.response_metadata.retry_after, '3');
  assert.equal(result.response_metadata.proxy_request_id, 'proxy-systemone-fixture');
  assert.equal(result.usage, null);
  assert.ok(!JSON.stringify(result).includes('secret-value'));
});

test('Jev rejects missing answers, invalid choices/probabilities and malformed responses', async () => {
  for (const response of [() => new Response('not JSON'), () => Response.json({ ...body, answers: {} }),
    () => Response.json({ ...body, answers: { ...answers, contains: { ...answers.contains, choice: 'unknown' } } }),
    () => Response.json({ ...body, answers: { ...answers, related: { type: 'noul', noul: 2 } } })]) {
    const result = await execute(await prepare(config()), { apiKey: 'local', fetch: async () => response() });
    assert.equal(result.valid, false);
    assert.equal(result.http_attempts, 1);
  }
});

test('Jev preserves network failures and cancellation without a retry', async () => {
  const controller = new AbortController();
  controller.abort();
  const result = await execute(await prepare(config()), { apiKey: 'local', signal: controller.signal,
    fetch: async (_url, init) => { init.signal.throwIfAborted(); } });
  assert.equal(result.valid, false);
  assert.ok(result.response_metadata.abort_reason);
  assert.equal(result.http_attempts, 1);
});

test('Jev CLI uses the native local credential handoff and System One route', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'jev-local-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const requests = [];
  const server = createServer(async (req, res) => {
    let text = '';
    for await (const chunk of req) text += chunk;
    requests.push({ path: req.url, authorization: req.headers.authorization, body: JSON.parse(text) });
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const baseUrl = `http://127.0.0.1:${server.address().port}/openrouter/api/v1`;
  await writeFile(join(directory, 'models.json'), JSON.stringify({ providers: { openrouter: {
    baseUrl, apiKey: '$AGENT_OPENROUTER_CAPABILITY',
  } } }));
  const child = spawn(process.execPath, [new URL('model.mjs', import.meta.url).pathname, 'run', '--send', '--pi-config'], {
    env: { ...process.env, PI_CODING_AGENT_DIR: directory, AGENT_OPENROUTER_CAPABILITY: 'LOCAL_ONLY_TEST' },
    stdio: ['pipe', 'pipe', 'pipe'], timeout: 10000,
  });
  let stdout = '', stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  child.stdin.end(JSON.stringify(config()));
  const status = await new Promise((resolve, reject) => { child.on('error', reject); child.on('close', resolve); });
  assert.equal(status, 0, stderr);
  assert.equal(JSON.parse(stdout).valid, true);
  assert.deepEqual(requests, [{ path: '/openrouter/api/v1/systemone', authorization: 'Bearer LOCAL_ONLY_TEST',
    body: { model: 'typesafe/jev-1.13', ...JSON.parse(config().prompt) } }]);
  assert.ok(!stdout.includes('LOCAL_ONLY_TEST'));
});
