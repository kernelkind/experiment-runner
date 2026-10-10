"""Cross-process scheduling exercised through the released CLI and model worker."""
import json
import os
import signal
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin/experiment-runner'


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control = self.root / 'control'
        self.control.mkdir()
        self.env = {**os.environ, 'FAKE_RUNNER_LOG': str(self.root / 'calls.json'),
                    'FAKE_RUNNER_CONTROL': str(self.control),
                    'ORACLE_EXPERIMENT_BACKOFF_DIRECTORY': str(self.root / 'admission')}
        self.processes = []
        self.addCleanup(self.stop)

    def stop(self):
        for process in self.processes:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for entry in self.starts():
            (self.control / f"{entry['pid']}.release").touch()
        for process in self.processes:
            try:
                process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()

    def cli(self, *args):
        result = subprocess.run([str(CLI), *args], cwd=self.root, env=self.env,
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def prepare(self, name, count):
        (self.root / f'{name}.txt').write_text('GENERIC PROMPT\n{TASK}')
        (self.root / f'{name}-cases.json').write_text(json.dumps({'cases': [
            {'id': str(i), 'inputs': {'TASK': f'{name}-{i}'}} for i in range(count)]}))
        (self.root / f'{name}-spec.json').write_text(json.dumps({
            'schema_version': 1, 'prompt': {'id': 'P1', 'path': f'{name}.txt'},
            'dataset': {'adapter': 'generic', 'path': f'{name}-cases.json'},
            'runner': {'provider': 'openai-codex', 'auth_file': None,
                       'model': 'fixture/model', 'reasoning_effort': 'low',
                       'runner_binary': str(ROOT / 'tests/fixtures/fake-runner.py')}}))
        self.cli('prepare', name, '--spec', f'{name}-spec.json')

    def launch(self, name, *args):
        process = subprocess.Popen([str(CLI), 'run', name, *args], cwd=self.root, env=self.env,
                                   text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True)
        self.processes.append(process)
        return process

    def starts(self):
        return [json.loads(path.read_text()) for path in self.control.glob('*.json')]

    def until(self, predicate):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.03)
        starts = self.starts()
        groups = {}
        for row in starts:
            name = row['prompt'].splitlines()[-1].split('-')[0]
            groups[name] = groups.get(name, 0) + 1
        self.fail(f'expected scheduling transition did not occur; worker starts={groups}')

    def test_global_limit_fair_newcomer_and_live_queued_status(self):
        self.prepare('older', 60)
        self.prepare('newer', 30)
        older = self.launch('older')
        self.until(lambda: len(self.starts()) >= 30)
        time.sleep(0.25)
        self.assertEqual(len(self.starts()), 30, 'only 30 model workers may start')
        newer = self.launch('newer')
        self.until(lambda: any(case['state'] == 'queued' for case in
                   json.loads(self.cli('show', 'newer', '--json').stdout)
                   .get('scheduling', {}).get('cases', {}).values()))
        live = json.loads(self.cli('show', 'older', '--json').stdout)['scheduling']
        self.assertEqual((live['running'], live['queued'], live['global_running'], live['global_limit']),
                         (30, 30, 30, 30))
        self.assertEqual(live['wait_reason'], 'capacity')
        original = sorted(self.starts(), key=lambda row: row['started'])
        for entry in original[:15]:
            (self.control / f"{entry['pid']}.release").touch()
        self.until(lambda: len([r for r in self.starts() if 'newer-' in r['prompt']]) == 15)
        self.assertEqual(len([r for r in self.starts() if 'older-' in r['prompt']]), 30,
                         'older run must yield released slots to the newcomer')
        new_live = json.loads(self.cli('show', 'newer', '--json').stdout)['scheduling']
        self.assertEqual(new_live['running'], 15)
        self.assertGreaterEqual(new_live['queue_wait_seconds'], 0)
        # Release continuously: the completed smaller run lends its share back.
        deadline = time.monotonic() + 12
        while (older.poll() is None or newer.poll() is None) and time.monotonic() < deadline:
            for entry in self.starts():
                (self.control / f"{entry['pid']}.release").touch()
            time.sleep(0.03)
        for process in (older, newer):
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 0, stdout + stderr)
            self.assertIn('queued=', stderr)
        final = json.loads(self.cli('show', 'older', '--json').stdout)
        self.assertEqual(final['summary']['valid'], 60)
        self.assertEqual(final['scheduling']['running'], 0)
        self.assertEqual(final['scheduling']['queued'], 0)
        calls = json.loads((self.root / 'calls.json').read_text())
        self.assertEqual(len(calls), 90)
        self.cli('run', 'older')
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 90)

    def test_three_ready_experiments_share_ten_slots_each(self):
        for name in ('first', 'second', 'third'):
            self.prepare(name, 30)
        first = self.launch('first')
        self.until(lambda: len(self.starts()) == 30)
        for name in ('second', 'third'):
            self.launch(name)
            self.until(lambda: any(case['state'] == 'queued' for case in
                       json.loads(self.cli('show', name, '--json').stdout)
                       .get('scheduling', {}).get('cases', {}).values()))
        for entry in sorted(self.starts(), key=lambda row: row['started'])[:20]:
            (self.control / f"{entry['pid']}.release").touch()
        self.until(lambda: len(self.starts()) == 50)
        counts = {name: json.loads(self.cli('show', name, '--json').stdout)['scheduling']['running']
                  for name in ('first', 'second', 'third')}
        self.assertEqual(counts, {'first': 10, 'second': 10, 'third': 10})
        self.assertIsNone(first.poll())

    def test_external_concurrency_settings_are_rejected(self):
        self.prepare('sample', 1)
        spec = self.root / 'sample-spec.json'
        value = json.loads(spec.read_text())
        value['runner']['concurrency'] = 50
        spec.write_text(json.dumps(value))
        for arguments in (['prepare', 'other', '--spec', str(spec)],
                          ['prepare', 'other', '--from', 'sample', '--concurrency', '50'],
                          ['run', 'sample', '--max-concurrency', '50']):
            result = subprocess.run([str(CLI), *arguments], cwd=self.root, env=self.env,
                                    text=True, capture_output=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('concurrency', result.stderr)

    def test_queued_cancellation_never_reserves_an_attempt_and_resume_dispatches(self):
        self.prepare('busy', 30)
        self.prepare('queued', 1)
        budget = self.root / 'budget.json'
        budget.write_text(json.dumps({'schema_version': 1, 'limit': 1, 'used': 0}))
        busy = self.launch('busy')
        self.until(lambda: len(self.starts()) == 30)
        queued = self.launch('queued', '--attempt-budget', str(budget))
        self.until(lambda: json.loads(self.cli('show', 'queued', '--json').stdout)
                   .get('scheduling', {}).get('wait_reason') == 'capacity')
        self.assertEqual(json.loads(budget.read_text())['used'], 0)
        os.killpg(queued.pid, signal.SIGTERM)
        queued.communicate(timeout=3)
        interrupted = json.loads(self.cli('show', 'queued', '--json').stdout)
        self.assertEqual(interrupted['scheduling']['state'], 'interrupted')
        self.assertEqual(interrupted['summary']['attempts'], 0)
        self.assertEqual(interrupted['summary']['unrun'], 1)
        os.killpg(busy.pid, signal.SIGTERM)
        busy.communicate(timeout=3)
        self.env.pop('FAKE_RUNNER_CONTROL')
        self.cli('run', 'queued', '--attempt-budget', str(budget))
        self.assertEqual(json.loads(budget.read_text())['used'], 1)
        final = json.loads(self.cli('show', 'queued', '--json').stdout)
        self.assertEqual(final['summary']['valid'], 1)
        self.assertEqual(final['scheduling']['global_running'], 0)

    def test_historical_concurrency_remains_frozen_but_does_not_limit_dispatch(self):
        from experiment_runner import extraction_pipeline as engine
        path = self.root / 'historical-config.json'
        path.write_text(json.dumps({
            'schema_version': 1, 'runner_binary': str(ROOT / 'tests/fixtures/fake-runner.py'),
            'provider': 'openai-codex', 'auth_file': None, 'model': 'fixture/model',
            'reasoning_effort': 'low', 'run_directory': str(self.root / 'historical-run'),
            'concurrency': 1, 'prompts': [
                {'prompt_id': name, 'text': 'GENERIC PROMPT ' + name} for name in ('one', 'two')]}))
        original = path.read_bytes()
        barrier = self.root / 'barrier'
        barrier.mkdir()
        env = {key: value for key, value in self.env.items() if key != 'FAKE_RUNNER_CONTROL'}
        with patch.dict(os.environ, {**env, 'FAKE_RUNNER_BARRIER': str(barrier)}):
            config = engine.PromptRunnerConfig.load(path)
            manifest = engine._build_prompt_manifest(config, engine._runner_version(config))
            engine._request_history_options(config, manifest, repeat=False, history_root=self.root)
            output = engine._execute_prompt_batch(config, manifest, retry_failed=False,
                                                 concurrency=config.concurrency)
        self.assertEqual(len(output['responses']), 2)
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(config.concurrency, 1)

    def test_live_orphaned_worker_keeps_its_global_slot(self):
        self.prepare('orphan', 1)
        self.prepare('other', 30)
        orphan = self.launch('orphan')
        self.until(lambda: len(self.starts()) == 1)
        held = self.starts()[0]
        orphan.terminate()  # Only the CLI parent; its request is still alive.
        orphan.wait(timeout=3)
        live = json.loads(self.cli('show', 'orphan', '--json').stdout)['scheduling']
        self.assertEqual((live['state'], live['running'], live['global_running']), ('interrupted', 1, 1))
        other = self.launch('other')
        self.until(lambda: len(self.starts()) == 30)
        time.sleep(0.2)
        self.assertEqual(len(self.starts()), 30, 'orphan must count against the 30-request cap')
        (self.control / f"{held['pid']}.release").touch()
        self.until(lambda: len(self.starts()) == 31)
        self.assertIsNone(other.poll())

    def test_help_exposes_automatic_scheduling_and_live_status(self):
        for command in (['--help'], ['run', '--help'], ['show', '--help']):
            value = self.cli(*command).stdout
            self.assertIn('30', value)
            self.assertIn('queue', value)
            self.assertIn('concurrency', value)
            if command[0] in ('run', 'show'):
                for name in ('scheduling', 'global_running', 'fair_share', 'prefix_warmup',
                             'queue_wait_seconds', 'cooldown_until_utc'):
                    self.assertIn(name, value)


if __name__ == '__main__':
    unittest.main()
