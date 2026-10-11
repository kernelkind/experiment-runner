import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { execute, prepare } from './model.mjs';
import { experimentAdmission } from './backoff.mjs';

test('unavailable or invalid scope discovery stops before model dispatch', async t => {
  const home = await mkdtemp(join(tmpdir(), 'oracle-backoff-scopes-'));
  const previous = process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY;
  t.after(async () => {
    if (previous === undefined) delete process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY;
    else process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = previous;
    await rm(home, { recursive: true, force: true });
  });
  const apiKey = 'test.' + Buffer.from(JSON.stringify({
    'https://api.openai.com/auth': { chatgpt_account_id: 'fixture-account' },
  })).toString('base64url') + '.local';
  const request = await prepare({ provider: 'openai-codex', model: 'gpt-6.1-sol',
    reasoning: 'medium', prompt: 'answer', timeout_seconds: 1 });
  request.model = { ...request.model, baseUrl: 'http://127.0.0.1:9876/fixture/pi' };
  const valid = { schema: 1, account_scope: 'account-fixture', transport_scope: 'transport-fixture' };
  const cases = [
    () => { throw new TypeError('local lookup unavailable'); },
    () => new Response('', { status: 404 }),
    () => new Response('', { status: 503 }),
    () => new Response('{malformed'),
    () => new Response(JSON.stringify({ ...valid, schema: 2 })),
    () => new Response(JSON.stringify({ ...valid, account_scope: undefined })),
    () => new Response(JSON.stringify({ ...valid, transport_scope: 'invalid scope' })),
  ];
  let posts = 0;
  for (let index = 0; index < cases.length; index++) {
    process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = join(home, String(index));
    await assert.rejects(execute(request, { apiKey, experimentConcurrency: 2,
      fetch: async (url, init) => {
        if (init.method === 'GET') {
          assert.equal(new URL(url).pathname, '/request-status/scopes');
          return cases[index]();
        }
        posts++;
        throw new Error('fixture model request must not be admitted');
      } }), /Experiment account\/transport scopes unavailable/);
  }
  assert.equal(posts, 0, 'failed discovery bypassed admission through fallback scopes');
});

test('status lookup rejects a mismatched reference or schema and ignores upgrade status', async t => {
  const home = await mkdtemp(join(tmpdir(), 'oracle-backoff-status-contract-'));
  const previous = process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY;
  t.after(async () => {
    if (previous === undefined) delete process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY;
    else process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = previous;
    await rm(home, { recursive: true, force: true });
  });
  const apiKey = 'test.' + Buffer.from(JSON.stringify({
    'https://api.openai.com/auth': { chatgpt_account_id: 'fixture-account' },
  })).toString('base64url') + '.local';
  const request = await prepare({ provider: 'openai-codex', model: 'gpt-6.1-sol',
    reasoning: 'medium', prompt: 'answer', timeout_seconds: 1 });
  request.model = { ...request.model, baseUrl: 'http://127.0.0.1:9876/fixture/pi' };
  const reference = 'proxy-reference-fixture';
  for (const [name, changes] of [['wrong-schema', { schema: 2 }],
    ['missing-schema', { schema: undefined }], ['wrong-reference', { request_id: 'other-reference' }]]) {
    process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = join(home, name);
    const result = await execute(request, { apiKey, experimentConcurrency: 2,
      fetch: async (url, init) => {
        if (url.includes('/request-status/scopes?')) return new Response(JSON.stringify({
          schema: 1, account_scope: 'account-fixture', transport_scope: 'transport-fixture' }));
        if (init.method === 'GET') return new Response(JSON.stringify({ schema: 1, request_id: reference,
          state: 'unknown', error_phase: 'body', request_context: 'active',
          read_error_kind: 'http2_goaway', read_error_code: 'enhance_your_calm', ...changes }));
        return new Response(new ReadableStream({ start(controller) {
          controller.enqueue(new TextEncoder().encode('data: {"type":"response.created","response":{"id":"resp-fixture","status":"in_progress"}}\n\n'));
          setTimeout(() => controller.error(new TypeError('body interrupted')), 5);
        } }), { status: 200, headers: { 'content-type': 'text/event-stream', 'x-agent-request-id': reference } });
      } });
    assert.equal(result.stop_reason, 'error');
    assert.equal(result.response_metadata.proxy_status, undefined, name);
  }
  process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = join(home, 'upgrade');
  let statusReads = 0;
  const admission = await experimentAdmission(request, { apiKey, concurrency: 2,
    fetch: async url => {
      if (url.includes('/request-status/scopes?')) return new Response(JSON.stringify({
        schema: 1, account_scope: 'account-fixture', transport_scope: 'transport-fixture' }));
      statusReads++;
      return new Response(JSON.stringify({ schema: 1, request_id: reference, state: 'complete',
        http_status: 101, terminal_event: 'completed' }));
    } });
  const result = { provider: 'openai-codex', stop_reason: 'error', error: 'closed', response_metadata: {
    http_status: 101, proxy_request_id: reference, id: 'resp-fixture',
    transport_error: { phase: 'body' }, websocket_close: { code: 1011 } } };
  await admission.finish(result);
  assert.equal(statusReads, 0, 'upgrade status was treated as per-generation evidence');
  assert.equal(result.response_metadata.proxy_status, undefined);
});

test('large transport diagnostics cannot overflow the shared failure report', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'oracle-backoff-large-error-'));
  const previous = process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY;
  process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = directory;
  t.after(async () => {
    if (previous === undefined) delete process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY;
    else process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = previous;
    await rm(directory, { recursive: true, force: true });
  });
  const request = await prepare({ provider: 'openai-codex', model: 'gpt-6.1-sol',
    reasoning: 'medium', prompt: 'answer', timeout_seconds: 1 });
  request.model = { ...request.model, baseUrl: 'http://127.0.0.1:9876/fixture/pi' };
  const large = depth => ({ name: 'E'.repeat(2000), code: 'C'.repeat(2000),
    type: 'T'.repeat(2000), message: 'M'.repeat(2000), metadata: {
      provider_name: 'P'.repeat(2000), error_type: 'F'.repeat(2000), provider_code: 'R'.repeat(2000) },
    ...(depth ? { cause: large(depth - 1) } : {}) });
  const apiKey = 'test.' + Buffer.from(JSON.stringify({
    'https://api.openai.com/auth': { chatgpt_account_id: 'fixture-account' },
  })).toString('base64url') + '.local';
  const result = await execute(request, { apiKey, experimentConcurrency: 2,
    fetch: async (_url, init) => {
      if (init.method === 'GET') return new Response(JSON.stringify({
        schema: 1, account_scope: 'account-fixture', transport_scope: 'transport-fixture' }));
      throw large(2);
    } });
  assert.equal(result.valid, false);
  assert.equal(result.stop_reason, 'error');
  assert.equal(result.response_metadata.transport_error.phase, 'headers');
  const state = JSON.parse(await readFile(join(directory, 'state.json'), 'utf8'));
  assert.equal(Object.keys(state.tickets).length, 0);
  assert.ok(Object.values(state.scopes).every(scope => scope.seconds === 0),
    'an unknown request error triggered shared backoff');
});

for (const [name, kind, code] of [
  ['connection pressure pauses shared admission', 'http2_goaway', 'enhance_your_calm'],
  ['ordinary stream failure leaves shared admission open', 'http2_stream_error', 'internal_error'],
]) test(`native execution: ${name}`, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'oracle-backoff-native-'));
  const previous = process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY;
  process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = directory;
  t.after(async () => {
    if (previous === undefined) delete process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY;
    else process.env.ORACLE_EXPERIMENT_BACKOFF_DIRECTORY = previous;
    await rm(directory, { recursive: true, force: true });
  });
  const key = 'test.' + Buffer.from(JSON.stringify({
    'https://api.openai.com/auth': { chatgpt_account_id: 'fixture-account' },
  })).toString('base64url') + '.local';
  const reference = 'proxy-status-test-reference';
  const starts = [];
  let lookups = 0;
  const server = createServer((request, response) => {
    assert.equal(request.headers.authorization, `Bearer ${key}`);
    request.resume();
    if (request.url.startsWith('/request-status/scopes?')) {
      response.setHeader('Content-Type', 'application/json');
      response.end(JSON.stringify({ schema: 1, account_scope: 'account-fixture', transport_scope: 'transport-fixture' }));
      return;
    }
    if (request.url === `/request-status/${reference}`) {
      lookups++;
      response.setHeader('Content-Type', 'application/json');
      response.end(JSON.stringify({ schema: 1, request_id: reference, state: 'unknown',
        error_phase: 'body', http_status: 200,
        read_error_kind: kind, read_error_code: code,
        request_context: 'active', PRIVATE_FIELD: 'PRIVATE_SERVER_DATA' }));
      return;
    }
    assert.equal(request.url, '/fixture/pi/codex/responses');
    starts.push(performance.now());
    response.writeHead(200, { 'Content-Type': 'text/event-stream', 'X-Agent-Request-Id': reference });
    if (starts.length === 1) {
      response.write('data: {"type":"response.created","response":{"id":"resp-test","status":"in_progress"}}\n\n');
      setTimeout(() => response.destroy(), 30);
    } else {
      const item = { id: 'msg-test', type: 'message', role: 'assistant', status: 'completed',
        content: [{ type: 'output_text', text: 'answer', annotations: [] }] };
      response.end([
        { type: 'response.created', response: { id: 'resp-test', status: 'in_progress' } },
        { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
        { type: 'response.output_text.delta', output_index: 0, content_index: 0, delta: 'answer' },
        { type: 'response.output_item.done', output_index: 0, item },
        { type: 'response.completed', response: { id: 'resp-test', model: 'gpt-6.1-sol',
          status: 'completed', output: [item], usage: { input_tokens: 1, output_tokens: 1 } } },
      ].map(event => `data: ${JSON.stringify(event)}\n\n`).join(''));
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const request = await prepare({ provider: 'openai-codex', model: 'gpt-6.1-sol',
    reasoning: 'medium', prompt: 'answer', timeout_seconds: 1 });
  request.model = { ...request.model, baseUrl: `http://127.0.0.1:${server.address().port}/fixture/pi` };
  const failed = await execute(request, { apiKey: key, experimentConcurrency: 2 });
  assert.equal(failed.valid, false);
  assert.equal(failed.response_metadata.proxy_status?.read_error_code, code);
  assert.equal(failed.response_metadata.transport_error?.cause?.code, 'UND_ERR_SOCKET');
  assert.equal(failed.http_attempts, 1);
  assert.equal(lookups, 1);
  if (kind === 'http2_stream_error') {
    const state = JSON.parse(await readFile(join(directory, 'state.json'), 'utf8'));
    assert.ok(Object.values(state.scopes).every(scope => !scope.seconds && !scope.limit),
      'the sanitized stream error imposed account/transport recovery');
  }
  const recovered = await execute(request, { apiKey: key, experimentConcurrency: 2 });
  assert.equal(recovered.valid, true, JSON.stringify(recovered));
  assert.equal(recovered.response_metadata.terminal_event, 'response.completed');
  assert.equal(recovered.http_attempts, 1);
  assert.equal(lookups, 1, 'successful completion made an unnecessary status lookup');
  if (kind === 'http2_goaway') {
    assert.ok(starts[1] - starts[0] >= 1900, 'new request bypassed the shared cooldown');
  }
  assert.equal(recovered.response_metadata.abort_reason, undefined, 'queue wait consumed the request timeout');
  assert.ok(!JSON.stringify([failed, recovered]).includes('PRIVATE_SERVER_DATA'));
  assert.ok(!JSON.stringify([failed, recovered]).includes(key));
});

test('two native experiments share recovery and preserve the persistent attempt cap', async t => {
  const home = await mkdtemp(join(tmpdir(), 'oracle-backoff-experiments-'));
  t.after(() => rm(home, { recursive: true, force: true }));
  const key = 'test.' + Buffer.from(JSON.stringify({
    'https://api.openai.com/auth': { chatgpt_account_id: 'local-fixture' },
  })).toString('base64url') + '.local';
  await symlink(process.execPath, join(home, 'node'));
  await writeFile(join(home, 'agent-account'), `#!${process.execPath}\n` +
    `const a=process.argv.slice(2);require('node:assert/strict').deepEqual(a.slice(0,3),['exec','pi','--']);\n` +
    `process.execve(a[3],a.slice(3),{...process.env,PI_CODING_AGENT_DIR:${JSON.stringify(home)}});\n`,
    { mode: 0o700 });
  const starts = [], statuses = new Map(), firstResponses = [];
  let closedAt = 0, firstRecoveryFinished = 0;
  let scopesAvailable = true, scopeReads = 0, changedScopesAfter = Infinity;
  const frame = event => `data: ${JSON.stringify(event)}\n\n`;
  const server = createServer((request, response) => {
    assert.equal(request.headers.authorization, `Bearer ${key}`);
    request.resume();
    response.setHeader('Content-Type', 'application/json');
    if (request.url.startsWith('/request-status/scopes?')) {
      scopeReads++;
      if (!scopesAvailable) { response.writeHead(503); response.end('{}'); return; }
      response.end(JSON.stringify({ schema: 1,
        account_scope: scopeReads > changedScopesAfter ? 'changed-account-fixture' : 'account-fixture',
        transport_scope: 'transport-fixture' }));
      return;
    }
    if (request.url.startsWith('/request-status/')) {
      response.end(JSON.stringify(statuses.get(request.url.split('/').at(-1))));
      return;
    }
    assert.equal(request.url, '/selected/pi/codex/responses');
    const number = starts.length;
    starts.push(performance.now());
    const reference = `proxy-reference-fixture-${number}`;
    response.writeHead(200, { 'Content-Type': 'text/event-stream', 'X-Agent-Request-Id': reference });
    response.write(frame({ type: 'response.created', response: { id: `resp-${number}`, status: 'in_progress' } }));
    if (number < 2) {
      statuses.set(reference, { schema: 1, request_id: reference, state: 'unknown',
        error_phase: 'body', http_status: 200, read_error_kind: 'http2_goaway',
        read_error_code: 'enhance_your_calm', request_context: 'active' });
      firstResponses.push(response);
      if (firstResponses.length === 2) setTimeout(() => {
        closedAt = performance.now();
        for (const first of firstResponses) first.destroy();
      }, 30);
      return;
    }
    const wasRecovery = closedAt && starts[number] >= closedAt + 1900;
    const item = { id: `msg-${number}`, type: 'message', role: 'assistant', status: 'completed',
      content: [{ type: 'output_text', text: 'answer', annotations: [] }] };
    setTimeout(() => {
      if (wasRecovery && !firstRecoveryFinished) firstRecoveryFinished = performance.now();
      response.end([
        { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
        { type: 'response.output_text.delta', output_index: 0, content_index: 0, delta: 'answer' },
        { type: 'response.output_item.done', output_index: 0, item },
        { type: 'response.completed', response: { id: `resp-${number}`, model: 'gpt-6.1-sol',
          status: 'completed', output: [item], usage: { input_tokens: 1, output_tokens: 1 } } },
      ].map(frame).join(''));
    }, 150);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const baseUrl = `http://127.0.0.1:${server.address().port}/selected/pi`;
  await writeFile(join(home, 'models.json'), JSON.stringify({ providers: { 'openai-codex': { baseUrl, apiKey: key } } }));
  await writeFile(join(home, 'auth.json'), JSON.stringify({ 'openai-codex': { type: 'api_key', key } }));
  const budget = join(home, 'budget.json');
  await writeFile(budget, JSON.stringify({ schema_version: 1, limit: 8, used: 0 }));
  await writeFile(join(home, 'prompt.txt'), 'Fixture request {VALUE}');
  const env = { ...process.env, PATH: `${home}:${process.env.PATH}`,
    ORACLE_EXPERIMENT_BACKOFF_DIRECTORY: join(home, 'backoff') };
  async function cli(args, expected = 0) {
    const child = spawn('python3', ['-B', new URL('../experiment.py', import.meta.url).pathname, ...args],
      { env, stdio: ['ignore', 'pipe', 'pipe'], timeout: 30000 });
    let stdout = '', stderr = '';
    child.stdout.on('data', bytes => { stdout += bytes; });
    child.stderr.on('data', bytes => { stderr += bytes; });
    const status = await new Promise((resolve, reject) => {
      child.once('error', reject); child.once('close', resolve);
    });
    assert.equal(status, expected, stderr + stdout);
    assert.ok(!(stderr + stdout).includes(key));
  }
  const experiments = [];
  for (const name of ['one', 'two']) {
    const cases = join(home, `cases-${name}.json`), spec = join(home, `spec-${name}.json`);
    await writeFile(cases, JSON.stringify({ cases: [0, 1, 2].map(i => ({ id: `${name}-${i}`, inputs: { VALUE: `${name}-${i}` } })) }));
    await writeFile(spec, JSON.stringify({ schema_version: 1,
      prompt: { id: 'backoff-fixture-v1', path: 'prompt.txt' },
      dataset: { adapter: 'generic', path: cases }, runner: {
        runner_binary: new URL('../run-model.sh', import.meta.url).pathname, provider: 'openai-codex', auth_file: null,
        model: 'gpt-6.1-sol', reasoning_effort: 'medium', timeout_seconds: 5 } }));
    const experiment = join(home, name);
    await cli(['prepare', experiment, '--spec', spec, '--history-root', home]);
    experiments.push(experiment);
  }
  assert.equal(starts.length, 0, 'preparation sent a model request');
  await Promise.all(experiments.map(experiment => cli(['run', experiment, '--max-new-attempts', '5', '--attempt-budget', budget])));
  assert.equal(starts.length, 8, 'extra model probes or retries escaped the attempt budget');
  assert.equal(JSON.parse(await readFile(budget, 'utf8')).used, 8);
  for (const experiment of experiments) {
    const report = JSON.parse(await readFile(join(experiment, 'report.json'), 'utf8'));
    assert.equal(report.summary.valid, 3);
    assert.equal(report.summary.failed, 0);
    const receipts = (await readFile(join(experiment, 'run/results.jsonl'), 'utf8')).trim().split('\n').map(JSON.parse);
    assert.ok(receipts.every(row => row.attempt <= 2 && row.model_result.http_attempts === 1));
    assert.ok(receipts.every(row => row.stderr === ''));
  }
  const recovery = starts.filter(start => start >= closedAt + 1900 && start < firstRecoveryFinished);
  assert.equal(recovery.length, 1, 'separate experiments resumed concurrently before the first recovery succeeded');
  await cli(['run', experiments[0], '--max-new-attempts', '5', '--attempt-budget', budget]);
  assert.equal(starts.length, 8, 'resume ignored saved completion or the lifetime cap');
  // Admission infrastructure failure stops the batch before later cases launch.
  const unavailableCases = join(home, 'cases-unavailable.json');
  const unavailableSpec = join(home, 'spec-unavailable.json');
  const unavailableExperiment = join(home, 'unavailable');
  const unavailableBudget = join(home, 'unavailable-budget.json');
  await writeFile(unavailableCases, JSON.stringify({ cases: [0, 1, 2].map(i => ({
    id: `unavailable-${i}`, inputs: { VALUE: `unavailable-${i}` } })) }));
  const inherited = JSON.parse(await readFile(join(home, 'spec-one.json'), 'utf8'));
  inherited.dataset.path = unavailableCases;
  await writeFile(unavailableSpec, JSON.stringify(inherited));
  await writeFile(unavailableBudget, JSON.stringify({ schema_version: 1, limit: 2, used: 0 }));
  await cli(['prepare', unavailableExperiment, '--spec', unavailableSpec, '--history-root', home]);
  scopesAvailable = false;
  const readsBefore = scopeReads;
  await cli(['run', unavailableExperiment, '--max-new-attempts', '2', '--attempt-budget', unavailableBudget], 1);
  assert.equal(starts.length, 8, 'scope outage sent a model request');
  assert.equal(scopeReads - readsBefore, 1, 'scope outage launched another untouched case');
  const report = JSON.parse(await readFile(join(unavailableExperiment, 'report.json'), 'utf8'));
  assert.equal(report.summary.unrun, 3, 'an unadmitted case was saved as a terminal provider failure');
  assert.equal(report.summary.failed, 0);
  // Scope discovery precedes worker launch and budget reservation.
  assert.equal(JSON.parse(await readFile(unavailableBudget, 'utf8')).used, 0);
  // Selection changes after queue admission: the native worker refuses its
  // mismatched ticket instead of sending under the wrong account's capacity.
  scopesAvailable = true;
  changedScopesAfter = scopeReads + 1;
  await cli(['run', unavailableExperiment, '--max-new-attempts', '2', '--attempt-budget', unavailableBudget], 1);
  assert.equal(starts.length, 8, 'changed scopes sent a model request');
  const changed = JSON.parse(await readFile(join(unavailableExperiment, 'report.json'), 'utf8'));
  assert.equal(changed.summary.unrun, 3);
  assert.equal(changed.summary.failed, 0);
  assert.equal(JSON.parse(await readFile(unavailableBudget, 'utf8')).used, 2,
    'already reserved attempts must remain conservative on a pre-send refusal');
});
