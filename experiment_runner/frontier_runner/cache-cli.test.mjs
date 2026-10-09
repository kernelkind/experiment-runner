import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { zstdDecompressSync } from 'node:zlib';
import { createHash } from 'node:crypto';

test('shared CLI warms the exact SSE prefix and accounts for real worker receipts', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'experiment-cache-cli-'));
  const cli = process.env.EXPERIMENT_RUNNER_TEST_CLI || fileURLToPath(new URL('../../bin/experiment-runner', import.meta.url));
  const worker = process.env.EXPERIMENT_RUNNER_TEST_WORKER || fileURLToPath(new URL('../run-model.sh', import.meta.url));
  const prefix = 'Compare the supplied data. Return the requested JSON.' + ' word'.repeat(1100) + '\r\n';
  const key = 'prefix-' + createHash('sha256').update(prefix).digest('hex').slice(0, 56);
  const credential = 'fixture.' + Buffer.from(JSON.stringify({ 'https://api.openai.com/auth': {
    chatgpt_account_id: 'offline-account' } })).toString('base64url') + '.invalid';
  const schema = { type: 'object', properties: { answer: { type: 'string' } }, required: ['answer'], additionalProperties: false };
  const observations = [];
  const failures = [];
  let scopeCalls = 0;
  const server = createServer(async (request, response) => {
    try {
      assert.equal(request.headers.authorization, 'Bearer ' + credential);
      const url = new URL(request.url, 'http://localhost');
      if (url.pathname === '/request-status/scopes') {
        assert.equal(request.method, 'GET');
        assert.equal(url.searchParams.get('account'), 'test');
        assert.equal(url.searchParams.get('client'), 'pi');
        scopeCalls++;
        response.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        response.end(JSON.stringify({ schema: 1, account_scope: 'fixture-account-scope', transport_scope: 'fixture-transport-scope' }));
        return;
      }
      assert.equal(request.method, 'POST');
      assert.equal(url.pathname, '/test/pi/codex/responses');
      let bytes = Buffer.concat(await Array.fromAsync(request));
      if (request.headers['content-encoding'] === 'zstd') bytes = zstdDecompressSync(bytes);
      const body = JSON.parse(bytes.toString());
      const native = body.input[0].type === 'additional_tools';
      if (native) {
        assert.equal(request.headers['session-id'], key);
        assert.equal(body.prompt_cache_key, key);
        assert.equal(body.input[1].content[0].text, prefix);
        assert.equal(request.headers['x-openai-internal-codex-responses-lite'], 'true');
      } else {
        assert.equal(request.headers['session-id'], undefined);
        assert.equal(body.input.length, 1);
      }
      assert.deepEqual(body.text.format.schema, schema);
      assert.equal(body.model, 'gpt-6.1-sol');
      assert.equal(body.reasoning.effort, 'medium');
      assert.equal(body.previous_response_id, undefined);
      const text = body.input.flatMap(item => (item.content || []).map(part => part.text)).join('');
      assert([prefix, prefix + 'Tea', prefix + 'Coffee'].includes(text));
      observations.push({ body, native, text });
      const id = 'fixture-response-' + observations.length;
      const item = { id: id + '-message', type: 'message', role: 'assistant', status: 'completed',
        content: [{ type: 'output_text', text: JSON.stringify({ answer: text === prefix ? 'warmup' : 'comparison' }), annotations: [] }] };
      const usage = { input_tokens: 100, output_tokens: 10,
        input_tokens_details: { cached_tokens: native && text !== prefix ? 60 : 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 3 } };
      // The SDK accepts successful SSE regardless of MIME; exercise the observer too.
      response.writeHead(200, { 'content-type': 'application/json' });
      for (const event of [
        { type: 'response.created', response: { id, status: 'in_progress' } },
        { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
        { type: 'response.output_item.done', output_index: 0, item },
        { type: 'response.completed', response: { id, model: 'gpt-6.1-sol', status: 'completed', output: [item], usage } },
      ]) response.write('data: ' + JSON.stringify(event) + '\n\n');
      response.end();
    } catch (error) {
      failures.push(error);
      response.writeHead(500); response.end();
    }
  });
  server.on('upgrade', (_, socket) => { failures.push(new Error('SSE cache workflow attempted a WebSocket')); socket.destroy(); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const write = (name, value) => fs.writeFileSync(path.join(root, name), JSON.stringify(value));
  const load = name => JSON.parse(fs.readFileSync(path.join(root, name), 'utf8'));
  const pi = path.join(root, 'pi'); fs.mkdirSync(pi);
  write('pi/models.json', { providers: { 'openai-codex': { baseUrl: `http://127.0.0.1:${server.address().port}/test/pi`, apiKey: credential } } });
  write('pi/auth.json', { 'openai-codex': { type: 'api_key', key: credential } });
  const launcher = path.join(root, 'worker.sh');
  fs.writeFileSync(launcher, '#!/usr/bin/env bash\nset -euo pipefail\nexec "$EXPERIMENT_RUNNER_TEST_WORKER" "$@" --pi-config\n', { mode: 0o755 });
  fs.writeFileSync(path.join(root, 'prompt.txt'), prefix + '{DATA}');
  fs.writeFileSync(path.join(root, 'prefix.txt'), prefix);
  write('cases.json', { cases: ['Tea', 'Coffee'].map((value, index) => ({ id: String(index), inputs: { DATA: value } })) });
  write('spec.json', { schema_version: 1, prompt: { id: 'cache-cli-v1', path: 'prompt.txt' }, dataset: { adapter: 'generic', path: 'cases.json' },
    runner: { provider: 'openai-codex', auth_file: null, model: 'gpt-6.1-sol', reasoning_effort: 'medium',
      concurrency: 2, runner_binary: launcher, output_schema: schema } });
  write('budget.json', { schema_version: 1, limit: 5, used: 0 });
  async function command(...args) {
    const child = spawn(cli, ['--project-root', root, ...args], { cwd: root,
      env: { ...process.env, PI_CODING_AGENT_DIR: pi, EXPERIMENT_RUNNER_TEST_WORKER: worker,
        ORACLE_EXPERIMENT_BACKOFF_DIRECTORY: path.join(root, 'backoff') }, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = ''; child.stdout.on('data', data => output += data); child.stderr.on('data', data => output += data);
    const code = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error('Native cache CLI timed out')); }, 30000);
      child.once('error', error => { clearTimeout(timer); reject(error); });
      child.once('exit', code => { clearTimeout(timer); resolve(code); });
    });
    assert.deepEqual(failures, []); assert.equal(code, 0, output);
  }
  try {
    await command('prepare', 'ordinary', '--spec', 'spec.json');
    await command('prepare', 'cached', '--from', 'ordinary', '--prompt-cache-prefix', path.join(root, 'prefix.txt'));
    assert.equal(observations.length, 0);
    for (const name of ['ordinary', 'cached']) await command('run', name, '--attempt-budget', path.join(root, 'budget.json'));
    assert.equal(observations.length, 5); assert.equal(load('budget.json').used, 5);
    assert.equal(observations[2].text, prefix);
    for (const comparison of observations.slice(3)) assert.deepEqual(comparison.body.input.slice(0, 2), observations[2].body.input);
    assert.notDeepEqual(observations[3].body.input[2], observations[4].body.input[2]);
    for (const name of ['ordinary', 'cached']) {
      const report = load(name + '/report.json'); assert.equal(report.summary.failed, 0); assert.equal(report.usage.attempts_without_usage, 0);
      for (const c of report.cases) assert.equal(c.parsed.answer, 'comparison');
      for (const line of fs.readFileSync(path.join(root, name, 'run/results.jsonl'), 'utf8').trim().split('\n')) {
        const r = JSON.parse(line); assert.equal(r.model_result.http_attempts, 1); assert(r.model_result.response_metadata.usage);
        assert(!line.includes(credential));
      }
    }
    assert.equal(load('cached/report.json').usage.cached_input_tokens, 120);
    assert.equal(load('cached/report.json').summary.attempts, 3);
    assert.equal(load('cached/report.json').cache_warmup.attempts, 1);
    const warmup = load('cached/cache-warmups/0001/report.json');
    assert.equal(warmup.cases[0].parsed.answer, 'warmup');
    const total = ['ordinary', 'cached'].reduce((sum, name) => sum + load(name + '/report.json').usage.catalog_cost_estimate_usd, 0);
    assert(Math.abs(total - 0.001272) < 1e-12);
    const receipts = fs.readFileSync(path.join(root, 'cached/run/results.jsonl'));
    await command('prepare', 'reused', '--from', 'cached');
    await command('run', 'reused', '--attempt-budget', path.join(root, 'budget.json'));
    assert.equal(load('reused/report.json').summary.reused, 2);
    assert.equal(observations.length, 5); assert.equal(load('budget.json').used, 5);
    assert.deepEqual(fs.readFileSync(path.join(root, 'cached/run/results.jsonl')), receipts);
    assert(scopeCalls >= 5);
  } finally {
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
    fs.rmSync(root, { recursive: true, force: true });
  }
});
