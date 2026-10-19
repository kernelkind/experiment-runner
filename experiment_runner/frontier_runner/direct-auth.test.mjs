import assert from 'node:assert/strict';
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { test } from 'node:test';

const cli = new URL('model.mjs', import.meta.url).pathname;
const request = { provider: 'openrouter', model: 'deepseek/deepseek-v4.1-flash',
  reasoning: 'low', prompt: 'Only this prompt.' };
const token = suffix => 'fixture.' + Buffer.from(JSON.stringify({
  'https://api.openai.com/auth': { chatgpt_account_id: 'fixture-account' }, suffix,
})).toString('base64url') + '.signature';

function childResult(executable, args, env, { input = '', onError } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, { env, stdio: ['pipe', 'pipe', 'pipe'], timeout: 45000 });
    let stdout = '', stderr = '';
    child.stdout.on('data', bytes => { stdout += bytes; });
    child.stderr.on('data', bytes => { stderr += bytes; onError?.(stderr, child); });
    child.once('error', reject);
    child.once('close', status => resolve({ status, stdout, stderr }));
    if (!onError) child.stdin.end(input);
  });
}
const call = (env, args = ['scopes'], payload = request) =>
  childResult(process.execPath, [cli, ...args], env, { input: JSON.stringify(payload) });

async function fixture(t, handler) {
  const directory = await mkdtemp(join(tmpdir(), 'runner-direct-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await writeFile(join(directory, 'agent-account'), `#!${process.execPath}\nprocess.exit(37);\n`, { mode: 0o700 });
  const env = { ...process.env, PATH: directory + ':' + process.env.PATH,
    EXPERIMENT_RUNNER_AUTH: 'direct', EXPERIMENT_RUNNER_AUTH_FILE: join(directory, 'auth.json'),
    ORACLE_EXPERIMENT_BACKOFF_DIRECTORY: join(directory, 'queue'), OPENROUTER_API_KEY: 'fixture-direct-key' };
  if (handler) {
    const server = createServer(handler);
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    t.after(() => { server.closeAllConnections(); server.close(); });
    const preload = join(directory, 'offline.mjs');
    await writeFile(preload, `
      const localFetch = globalThis.fetch;
      globalThis.fetch = async (input, init) => {
        const request = new Request(input, init), url = new URL(request.url);
        if (!['auth.openai.com', 'chatgpt.com', 'openrouter.ai'].includes(url.hostname)) {
          throw new Error('Unexpected external request blocked by test');
        }
        return localFetch(new Request('http://127.0.0.1:${server.address().port}' + url.pathname + url.search, request));
      };
    `);
    env.NODE_OPTIONS = `--import=${preload}`;
  }
  return { directory, env };
}

test('explicit direct authentication discovers private queue scopes without Anvil', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'runner-direct-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await writeFile(join(directory, 'agent-account'), `#!${process.execPath}\nprocess.exit(37);\n`, { mode: 0o700 });
  const result = spawnSync(process.execPath, [cli, 'scopes'], {
    input: JSON.stringify(request), encoding: 'utf8', timeout: 10000,
    env: { ...process.env, PATH: directory, EXPERIMENT_RUNNER_AUTH: 'direct',
      EXPERIMENT_RUNNER_AUTH_FILE: join(directory, 'auth.json'), OPENROUTER_API_KEY: 'fixture-direct-key' },
  });
  assert.equal(result.status, 0, result.stderr);
  const scopes = JSON.parse(result.stdout);
  assert.deepEqual(Object.keys(scopes).sort(), ['account', 'transport']);
  for (const value of Object.values(scopes)) assert.match(value, /^[a-f0-9]{64}$/);
  assert.ok(!result.stdout.includes('fixture-direct-key'));
});

test('direct subscription scopes survive token rotation and never expose credentials', async t => {
  const { env } = await fixture(t);
  const subscription = { ...request, provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium' };
  const results = [];
  for (const suffix of ['first', 'second']) {
    await writeFile(env.EXPERIMENT_RUNNER_AUTH_FILE, JSON.stringify({ 'openai-codex': {
      type: 'oauth', access: token(suffix), refresh: 'fixture-refresh', expires: Date.now() + 3600000,
    } }), { mode: 0o600 });
    const result = await call(env, ['scopes'], subscription);
    assert.equal(result.status, 0, result.stderr);
    assert.ok(!result.stdout.includes('fixture-account'));
    assert.ok(!result.stdout.includes(token(suffix)));
    results.push(JSON.parse(result.stdout));
  }
  assert.deepEqual(results[0], results[1]);
});

test('concurrent native processes refresh one expired credential once', async t => {
  let refreshes = 0;
  const { env } = await fixture(t, async (req, res) => {
    assert.equal(req.url, '/oauth/token');
    let body = ''; for await (const chunk of req) body += chunk;
    assert.equal(new URLSearchParams(body).get('refresh_token'), 'fixture-old-refresh');
    refreshes++;
    await new Promise(resolve => setTimeout(resolve, 100));
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ access_token: token('renewed'), refresh_token: 'fixture-new-refresh', expires_in: 3600 }));
  });
  await writeFile(env.EXPERIMENT_RUNNER_AUTH_FILE, JSON.stringify({
    'openai-codex': { type: 'oauth', access: token('expired'), refresh: 'fixture-old-refresh', expires: 1 },
    unrelated: { type: 'api_key', key: 'unrelated-fixture' },
  }), { mode: 0o600 });
  const subscription = { ...request, provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium' };
  const results = await Promise.all(Array.from({ length: 6 }, () => call(env, ['scopes'], subscription)));
  for (const result of results) assert.equal(result.status, 0, result.stderr);
  assert.equal(refreshes, 1);
  assert.equal(new Set(results.map(result => result.stdout)).size, 1);
  const saved = JSON.parse(await readFile(env.EXPERIMENT_RUNNER_AUTH_FILE, 'utf8'));
  assert.equal(saved['openai-codex'].refresh, 'fixture-new-refresh');
  assert.equal(saved.unrelated.key, 'unrelated-fixture');
  assert.equal((await stat(env.EXPERIMENT_RUNNER_AUTH_FILE)).mode & 0o777, 0o600);
});

test('stored credential failures, repository paths and invalid modes refuse without fallback', async t => {
  const { directory, env } = await fixture(t);
  await writeFile(env.EXPERIMENT_RUNNER_AUTH_FILE, '{"openrouter":{"type":"oauth","access":"BAD_SECRET"}}', { mode: 0o600 });
  for (const mode of ['direct', 'unknown']) {
    const result = await call({ ...env, EXPERIMENT_RUNNER_AUTH: mode });
    assert.notEqual(result.status, 0);
    assert.equal(result.stdout, '');
    assert.ok(!result.stderr.includes('BAD_SECRET'));
    assert.ok(!result.stderr.includes(env.OPENROUTER_API_KEY));
  }
  await writeFile(env.EXPERIMENT_RUNNER_AUTH_FILE, '{"openrouter":{"type":"api_key","key":"STORED_SECRET"}}');
  await chmod(env.EXPERIMENT_RUNNER_AUTH_FILE, 0o644);
  assert.notEqual((await call(env)).status, 0, 'world-readable credentials accepted');
  await chmod(env.EXPERIMENT_RUNNER_AUTH_FILE, 0o600);
  await mkdir(join(directory, '.git'));
  assert.notEqual((await call(env)).status, 0, 'credentials inside a repository accepted');
  assert.notEqual((await call(env, ['scopes', '--pi-config'])).status, 0);
});

test('native login uses pi-ai device flow and saves private credentials outside the project', async t => {
  const calls = [];
  const { env } = await fixture(t, (req, res) => {
    calls.push(req.url);
    res.setHeader('content-type', 'application/json');
    if (req.url === '/api/accounts/deviceauth/usercode') {
      res.end(JSON.stringify({ device_auth_id: 'fixture-device', user_code: 'PUBLIC_CODE', interval: 0 }));
    } else if (req.url === '/api/accounts/deviceauth/token') {
      res.end(JSON.stringify({ authorization_code: 'fixture-code', code_verifier: 'fixture-verifier' }));
    } else if (req.url === '/oauth/token') {
      res.end(JSON.stringify({ access_token: token('login'), refresh_token: 'fixture-login-refresh', expires_in: 3600 }));
    } else { res.writeHead(404); res.end(); }
  });
  let answered = false;
  const result = await childResult(process.execPath, [cli, 'login', 'openai-codex'], env, {
    onError: (text, child) => {
      if (!answered && text.includes('Select OpenAI Codex login method:')) {
        answered = true; child.stdin.write('2\n');
      }
    },
  });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(calls, ['/api/accounts/deviceauth/usercode', '/api/accounts/deviceauth/token', '/oauth/token']);
  const saved = JSON.parse(await readFile(env.EXPERIMENT_RUNNER_AUTH_FILE, 'utf8'));
  assert.equal(saved['openai-codex'].access, token('login'));
  assert.ok(!result.stdout.includes(token('login')));
  assert.ok(!result.stderr.includes('fixture-login-refresh'));
  assert.equal((await stat(env.EXPERIMENT_RUNNER_AUTH_FILE)).mode & 0o777, 0o600);
});

test('direct native batch retains queue admission, retries, receipts and exact answer reuse', async t => {
  let attempts = 0;
  const bodies = [];
  const { directory, env } = await fixture(t, async (req, res) => {
    assert.equal(req.url, '/api/v1/chat/completions', 'direct run queried the Anvil service');
    assert.equal(req.headers.authorization, 'Bearer fixture-direct-key');
    let body = ''; for await (const chunk of req) body += chunk;
    bodies.push(JSON.parse(body)); attempts++;
    if (attempts === 1) {
      res.writeHead(503, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { code: 'provider_error', message: 'Fixture unavailable' } }));
      return;
    }
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    for (const value of [
      { id: 'gen-fixture-direct', model: request.model, choices: [{ index: 0, delta: { role: 'assistant', content: 'answer' }, finish_reason: null }] },
      { id: 'gen-fixture-direct', model: request.model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 10, completion_tokens: 2, total_tokens: 12 } },
    ]) res.write('data: ' + JSON.stringify(value) + '\n\n');
    res.end('data: [DONE]\n\n');
  });
  await writeFile(join(directory, 'prompt.txt'), 'Answer about {VALUE}.');
  await writeFile(join(directory, 'cases.json'), JSON.stringify({ cases: [{ id: 'one', inputs: { VALUE: 'tea' } }] }));
  await writeFile(join(directory, 'spec.json'), JSON.stringify({ schema_version: 1,
    prompt: { id: 'direct-fixture-v1', path: 'prompt.txt' }, dataset: { adapter: 'generic', path: 'cases.json' },
    runner: { provider: request.provider, model: request.model, reasoning_effort: 'low', timeout_seconds: 10,
      runner_binary: new URL('../run-model.sh', import.meta.url).pathname } }));
  const runner = new URL('../../bin/experiment-runner', import.meta.url).pathname;
  const experiment = join(directory, 'experiment');
  for (const args of [
    ['prepare', experiment, '--spec', join(directory, 'spec.json'), '--history-root', directory],
    ['run', experiment], ['run', experiment],
  ]) {
    const result = await childResult('python3', [runner, '--project-root', directory, ...args], env);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.ok(!result.stdout.includes('fixture-direct-key'));
    assert.ok(!result.stderr.includes('fixture-direct-key'));
  }
  assert.equal(attempts, 2, 'retry or exact reuse failed');
  assert.deepEqual(bodies[0], bodies[1]);
  const report = JSON.parse(await readFile(join(experiment, 'report.json'), 'utf8'));
  assert.equal(report.summary.valid, 1);
  const receipts = await readFile(join(experiment, 'run/results.jsonl'), 'utf8');
  assert.ok(!receipts.includes('fixture-direct-key'));
  assert.equal(receipts.trim().split('\n').length, 2);
});

test('direct cost reconciliation uses the chosen OpenRouter key without Anvil', async t => {
  const { env } = await fixture(t, (req, res) => {
    assert.equal(req.method, 'GET');
    assert.equal(req.url, '/api/v1/generation?id=gen-direct-billing');
    assert.equal(req.headers.authorization, 'Bearer fixture-direct-key');
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ data: { id: 'gen-direct-billing', total_cost: 0.01, model: request.model } }));
  });
  const result = await childResult(process.execPath, [new URL('billing.mjs', import.meta.url).pathname], env,
    { input: JSON.stringify(['gen-direct-billing']) });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout)[0].total_cost, 0.01);
  assert.ok(!result.stdout.includes('fixture-direct-key'));
});
