import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { test } from 'node:test';

const cli = new URL('model.mjs', import.meta.url).pathname;
const config = { provider: 'openai-codex', model: 'gpt-6-astra', reasoning: 'low', prompt: 'Only this prompt.' };

test('preview, identity and invalid requests never launch authentication', async t => {
  const home = await mkdtemp(join(tmpdir(), 'oracle-auth-free-'));
  t.after(() => rm(home, { recursive: true, force: true }));
  const calls = join(home, 'calls');
  for (const name of ['pi', 'agent-account']) {
    await writeFile(join(home, name), `#!${process.execPath}\nrequire('node:fs').writeFileSync(${JSON.stringify(calls)},'bad');\n`, { mode: 0o700 });
  }
  const env = { ...process.env, HOME: home, PATH: home };
  for (const args of [['preview'], ['--version'], ['--identity']]) {
    const result = spawnSync(process.execPath, [cli, ...args], { input: JSON.stringify(config), encoding: 'utf8', env });
    assert.equal(result.status, 0, result.stderr);
  }
  for (const [args, input] of [[['run'], config], [['run', '--send'], { ...config, model: 'unknown' }]]) {
    const result = spawnSync(process.execPath, [cli, ...args], { input: JSON.stringify(input), encoding: 'utf8', env });
    assert.equal(result.status, 1);
    assert.equal(result.stdout, '');
  }
  const auth = join(home, 'auth.json');
  await writeFile(auth, '{"tokens":{"access_token":"OLD_SECRET"}}', { mode: 0o600 });
  for (const option of ['--auth-file', '--auth-file=']) {
    const args = option.endsWith('=') ? [option + auth] : [option, auth];
    const result = spawnSync(process.execPath, [cli, 'run', '--send', ...args], { input: JSON.stringify(config), encoding: 'utf8', env });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /--auth-file is no longer supported/);
    assert.ok(!result.stderr.includes('OLD_SECRET'));
  }
  assert.equal(await readFile(auth, 'utf8'), '{"tokens":{"access_token":"OLD_SECRET"}}');
  await assert.rejects(readFile(calls), { code: 'ENOENT' });
});

test('missing or failed account launcher never falls back to bearer export', async t => {
  const home = await mkdtemp(join(tmpdir(), 'oracle-service-failure-'));
  t.after(() => rm(home, { recursive: true, force: true }));
  const calls = join(home, 'pi-called');
  await writeFile(join(home, 'pi'), `#!${process.execPath}\nrequire('node:fs').writeFileSync(${JSON.stringify(calls)},'bad');\n`, { mode: 0o700 });
  const env = { ...process.env, HOME: home, PATH: home };
  const missing = spawnSync(process.execPath, [cli, 'run', '--send'], { input: JSON.stringify(config), encoding: 'utf8', env });
  assert.equal(missing.status, 1);
  assert.match(missing.stderr, /Cannot launch agent-account/);
  for (const code of [17, 42]) {
    await writeFile(join(home, 'agent-account'), `#!${process.execPath}\nprocess.exit(${code});\n`, { mode: 0o700 });
    const result = spawnSync(process.execPath, [cli, 'run', '--send'], { input: JSON.stringify(config), encoding: 'utf8', env });
    assert.equal(result.status, code);
    assert.equal(result.stdout, '');
  }
  await assert.rejects(readFile(calls), { code: 'ENOENT' });
});

test('terminating the runner also terminates its selected-view child', async t => {
  const home = await mkdtemp(join(tmpdir(), 'oracle-service-cancel-'));
  t.after(() => rm(home, { recursive: true, force: true }));
  await writeFile(join(home, 'agent-account'), `#!${process.execPath}\n` +
    `process.on('SIGTERM',()=>process.exit(42));setInterval(()=>{},1000);process.stdout.write('READY\\n');\n`, { mode: 0o700 });
  const child = spawn(process.execPath, [cli, 'run', '--send'], {
    env: { ...process.env, HOME: home, PATH: home }, stdio: ['pipe', 'pipe', 'pipe'], timeout: 10000,
  });
  const completion = new Promise((resolve, reject) => { child.once('error', reject); child.once('close', resolve); });
  child.stdin.end(JSON.stringify(config));
  for await (const chunk of child.stdout) {
    assert.equal(chunk.toString(), 'READY\n');
    child.kill('SIGTERM');
  }
  assert.equal(await completion, 42);
});
