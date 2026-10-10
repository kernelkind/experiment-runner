import assert from 'node:assert/strict';
import { appendFile, cp, mkdtemp, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';
import { execute, prepare } from './model.mjs';

test('actual native serializer keeps request identity across scheduling updates', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'runner-upgrade-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const source = new URL('../', import.meta.url).pathname;
  const target = join(directory, 'experiment_runner');
  await cp(source, target, { recursive: true,
    filter: path => !path.includes('/node_modules') && !path.includes('/__pycache__') });
  await symlink(new URL('node_modules', import.meta.url).pathname, join(target, 'frontier_runner/node_modules'));
  const cli = join(target, 'frontier_runner/model.mjs');
  const config = { provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'medium',
    prompt: 'Exact shared instructions.\nChanging data: 🌿', schema: { type: 'object' } };
  function run(args, input) {
    const result = spawnSync(process.execPath, [cli, ...args], { encoding: 'utf8', input });
    assert.equal(result.status, 0, result.stderr);
    return JSON.parse(result.stdout);
  }
  const before = run(['--identity']);
  const payload = run(['preview'], JSON.stringify(config));
  await appendFile(join(target, 'scheduling.py'), '\n# Operational scheduling update.\n');
  const after = run(['--identity']);
  const next = run(['preview'], JSON.stringify(config));
  assert.notEqual(after.runner_sha256, before.runner_sha256);
  assert.equal(after.request_code_sha256, before.request_code_sha256);
  assert.deepEqual(next.body, payload.body);
  assert.deepEqual(next.headers, payload.headers);
  assert.equal(next.network_requests, 0);
  await appendFile(cli, '\n// Request-producing code changed; conservative refusal.\n');
  assert.notEqual(run(['--identity']).request_code_sha256, before.request_code_sha256);
});

test('native worker refuses changed source before a provider request', async () => {
  const old = process.env.ORACLE_EXPERIMENT_EXPECTED_RUNNER_SHA256;
  process.env.ORACLE_EXPERIMENT_EXPECTED_RUNNER_SHA256 = '0'.repeat(64);
  let calls = 0;
  try {
    await assert.rejects(execute(await prepare({ provider: 'openai-codex',
      model: 'gpt-6.1-sol', reasoning: 'medium', prompt: 'No request allowed.' }), {
      apiKey: 'fabricated-local-test-token', fetch: async () => { calls++; throw new Error('unexpected'); },
    }), /changed before request dispatch/);
    assert.equal(calls, 0);
  } finally {
    if (old === undefined) delete process.env.ORACLE_EXPERIMENT_EXPECTED_RUNNER_SHA256;
    else process.env.ORACLE_EXPERIMENT_EXPECTED_RUNNER_SHA256 = old;
  }
});
