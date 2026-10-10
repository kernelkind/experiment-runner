"""Resume and handover through the actual CLI, receipts and persistent budget."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin/experiment-runner'


class UpdateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.worker = self.root / 'worker.py'
        source = (ROOT / 'tests/fixtures/fake-runner.py').read_text()
        source = source.replace('"runner_sha256": "e" * 64',
                                '"runner_sha256": "e" * 64, "request_code_sha256": "b" * 64')
        self.worker.write_text(source)
        self.worker.chmod(0o755)
        self.env = {**os.environ, 'FAKE_RUNNER_LOG': str(self.root / 'calls.json'),
                    'ORACLE_EXPERIMENT_BACKOFF_DIRECTORY': str(self.root / 'queue')}
        self.processes = []
        self.addCleanup(self.stop)

    def stop(self):
        for process in self.processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
            process.communicate(timeout=10)

    def cli(self, *args, succeeds=True):
        result = subprocess.run([str(CLI), *args], cwd=self.root, env=self.env,
                                text=True, capture_output=True, timeout=30)
        if succeeds:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def prepare(self, name='E1', count=2):
        (self.root / 'prompt.txt').write_text('GENERIC PROMPT\n{TASK}')
        (self.root / 'cases.json').write_text(json.dumps({'cases': [
            {'id': str(i), 'inputs': {'TASK': f'{name}-{i}'}} for i in range(count)]}))
        (self.root / 'spec.json').write_text(json.dumps({
            'schema_version': 1, 'prompt': {'id': 'P1', 'path': 'prompt.txt'},
            'dataset': {'adapter': 'generic', 'path': 'cases.json'},
            'runner': {'provider': 'openai-codex', 'model': 'fixture/model',
                       'reasoning_effort': 'low', 'runner_binary': str(self.worker)}}))
        self.cli('prepare', name, '--spec', 'spec.json')
        self.budget = self.root / f'{name}-budget.json'
        self.budget.write_text(json.dumps({'schema_version': 1, 'limit': count, 'used': 0}))

    def calls(self):
        path = self.root / 'calls.json'
        return json.loads(path.read_text()) if path.exists() else []

    def test_compatible_worker_update_resumes_same_directory_and_reuses_answers(self):
        self.prepare()
        self.cli('run', 'E1', '--only', '0', '--attempt-budget', str(self.budget))
        receipt = self.root / 'E1/run/results.jsonl'
        old_receipts = receipt.read_bytes()
        frozen = (self.root / 'E1/experiment.json').read_bytes()
        self.worker.write_text(self.worker.read_text().replace('"e" * 64', '"a" * 64'))
        self.cli('run', 'E1', '--attempt-budget', str(self.budget))
        self.assertEqual(len(self.calls()), 2)
        self.assertEqual(json.loads(self.budget.read_text())['used'], 2)
        self.assertTrue(receipt.read_bytes().startswith(old_receipts))
        self.assertEqual((self.root / 'E1/experiment.json').read_bytes(), frozen)
        rows = [json.loads(line) for line in receipt.read_text().splitlines()]
        self.assertEqual({r['runner_sha256'] for r in rows}, {'e' * 64, 'a' * 64})
        self.assertEqual(json.loads(self.cli('show', 'E1', '--json').stdout)['summary']['valid'], 2)
        self.cli('prepare', 'E2', '--from', 'E1')
        self.cli('run', 'E2', '--attempt-budget', str(self.budget))
        self.assertEqual(len(self.calls()), 2, 'compatible updates preserve exact answer reuse')

    def test_request_producing_code_change_refuses_resume_without_spending(self):
        self.prepare()
        self.cli('run', 'E1', '--only', '0', '--attempt-budget', str(self.budget))
        self.worker.write_text(self.worker.read_text().replace('"e" * 64', '"a" * 64')
                               .replace('"b" * 64', '"c" * 64'))
        result = self.cli('run', 'E1', '--attempt-budget', str(self.budget), succeeds=False)
        self.assertIn('model runner', result.stderr)
        self.assertEqual(len(self.calls()), 1)
        self.assertEqual(json.loads(self.budget.read_text())['used'], 1)

    def test_legacy_receipts_and_reuse_survive_a_verified_worker_mapping(self):
        release = self.root / 'release'
        shutil.copytree(ROOT / 'experiment_runner', release / 'experiment_runner',
                        ignore=shutil.ignore_patterns('node_modules', '__pycache__'))
        (release / 'bin').mkdir()
        shutil.copy2(CLI, release / 'bin/experiment-runner')
        old_source = self.worker.read_text().replace(', "request_code_sha256": "b" * 64', '')
        self.worker.write_text(old_source)
        self.prepare()
        command = [str(release / 'bin/experiment-runner')]
        def invoke(*args):
            result = subprocess.run([*command, *args], cwd=self.root, env=self.env,
                                    text=True, capture_output=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return result
        invoke('run', 'E1', '--only', '0', '--attempt-budget', str(self.budget))
        invoke('prepare', 'E2', '--from', 'E1')
        invoke('run', 'E2', '--only', '0', '--attempt-budget', str(self.budget))
        receipts = (self.root / 'E1/run/results.jsonl').read_bytes()
        self.worker.write_text(old_source.replace('"e" * 64',
            '"a" * 64, "request_code_sha256": "b" * 64'))
        registry = release / 'experiment_runner/frontier_runner/request-compatibility.json'
        registry.write_text(json.dumps({'e' * 64: 'b' * 64}))
        self.assertEqual(json.loads(invoke('show', 'E2', '--json').stdout)['summary']['valid'], 1)
        invoke('run', 'E1', '--attempt-budget', str(self.budget))
        self.assertTrue((self.root / 'E1/run/results.jsonl').read_bytes().startswith(receipts))
        invoke('prepare', 'E3', '--from', 'E1')
        invoke('run', 'E3', '--attempt-budget', str(self.budget))
        self.assertEqual(len(self.calls()), 2)

    def test_corrupted_worker_provenance_is_rejected(self):
        self.prepare()
        self.cli('run', 'E1', '--only', '0', '--attempt-budget', str(self.budget))
        self.worker.write_text(self.worker.read_text().replace('"e" * 64', '"a" * 64'))
        self.cli('run', 'E1', '--attempt-budget', str(self.budget))
        path = self.root / 'E1/run/worker-upgrades.jsonl'
        row = json.loads(path.read_text())
        row['results_prefix_sha256'] = '0' * 64
        path.write_text(json.dumps(row) + '\n')
        result = self.cli('run', 'E1', '--attempt-budget', str(self.budget), succeeds=False)
        self.assertIn('invalid worker upgrade history', result.stderr)
        self.assertEqual(len(self.calls()), 2)

    def test_batch_scheduling_counts_each_completed_case_once(self):
        self.prepare(count=1)
        result = self.cli('run', 'E1', '--attempt-budget', str(self.budget))
        self.assertIn('valid=1 failed=0', result.stderr)
        self.assertNotIn('valid=2 ', result.stderr)
        report = json.loads(self.cli('show', 'E1', '--json').stdout)
        self.assertEqual(report['scheduling']['valid'], 1)

    def test_pause_drains_only_this_experiment_then_resumes_unfinished_cases(self):
        self.prepare(count=32)
        control = self.root / 'control'
        control.mkdir()
        self.env['FAKE_RUNNER_CONTROL'] = str(control)
        process = subprocess.Popen([str(CLI), 'run', 'E1', '--attempt-budget', str(self.budget)],
            cwd=self.root, env=self.env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True)
        self.processes.append(process)
        deadline = time.monotonic() + 15
        while len(list(control.glob('*.json'))) < 30 and time.monotonic() < deadline:
            time.sleep(0.03)
        self.assertEqual(len(list(control.glob('*.json'))), 30)
        self.cli('pause', 'E1')
        state = json.loads(self.cli('show', 'E1', '--json').stdout)['scheduling']
        self.assertEqual(state['state'], 'pausing')
        for path in control.glob('*.json'):
            (control / (path.stem + '.release')).touch()
        stdout, stderr = process.communicate(timeout=15)
        self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertEqual(len(self.calls()), 30)
        self.assertEqual(json.loads(self.budget.read_text())['used'], 30)
        state = json.loads(self.cli('show', 'E1', '--json').stdout)['scheduling']
        self.assertEqual(state['state'], 'paused')
        del self.env['FAKE_RUNNER_CONTROL']
        self.cli('run', 'E1', '--attempt-budget', str(self.budget))
        self.assertEqual(len(self.calls()), 32)
        self.assertEqual(json.loads(self.budget.read_text())['used'], 32)

    def test_incompatible_live_worker_update_drains_before_new_attempts(self):
        self.prepare(count=32)
        control = self.root / 'control'
        control.mkdir()
        self.env['FAKE_RUNNER_CONTROL'] = str(control)
        process = subprocess.Popen([str(CLI), 'run', 'E1', '--attempt-budget', str(self.budget)],
            cwd=self.root, env=self.env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True)
        self.processes.append(process)
        deadline = time.monotonic() + 15
        while len(list(control.glob('*.json'))) < 30 and time.monotonic() < deadline:
            time.sleep(0.03)
        self.assertEqual(len(list(control.glob('*.json'))), 30)
        old_source = self.worker.read_text()
        self.worker.write_text(old_source.replace('"e" * 64', '"a" * 64')
                               .replace('"b" * 64', '"c" * 64'))
        for path in control.glob('*.json'):
            (control / (path.stem + '.release')).touch()
        stdout, stderr = process.communicate(timeout=15)
        self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertEqual(len(self.calls()), 30)
        self.assertEqual(json.loads(self.budget.read_text())['used'], 30)
        status = json.loads(self.cli('show', 'E1', '--json').stdout)['scheduling']
        self.assertEqual((status['state'], status['pause_reason']), ('paused', 'runner_updated'))
        del self.env['FAKE_RUNNER_CONTROL']
        self.worker.write_text(old_source.replace('"e" * 64', '"a" * 64'))
        self.cli('run', 'E1', '--attempt-budget', str(self.budget))
        self.assertEqual(len(self.calls()), 32)

    def test_compatible_live_worker_update_continues_without_global_pause(self):
        self.prepare(count=32)
        control = self.root / 'control'
        control.mkdir()
        self.env['FAKE_RUNNER_CONTROL'] = str(control)
        process = subprocess.Popen([str(CLI), 'run', 'E1', '--attempt-budget', str(self.budget)],
            cwd=self.root, env=self.env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True)
        self.processes.append(process)
        deadline = time.monotonic() + 15
        while len(list(control.glob('*.json'))) < 30 and time.monotonic() < deadline:
            time.sleep(0.03)
        self.assertEqual(len(list(control.glob('*.json'))), 30)
        self.worker.write_text(self.worker.read_text().replace('"e" * 64', '"a" * 64'))
        deadline = time.monotonic() + 15
        while process.poll() is None and time.monotonic() < deadline:
            for path in control.glob('*.json'):
                (control / (path.stem + '.release')).touch()
            time.sleep(0.03)
        stdout, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertEqual(len(self.calls()), 32)
        self.assertEqual(json.loads(self.budget.read_text())['used'], 32)
        report = json.loads(self.cli('show', 'E1', '--json').stdout)
        self.assertEqual(report['summary']['valid'], 32)
        self.assertEqual(report['scheduling']['state'], 'finished')

    def test_pausing_one_experiment_does_not_stop_another(self):
        self.prepare('first', count=32)
        first_budget = self.budget
        self.prepare('second', count=1)
        control = self.root / 'control'
        control.mkdir()
        self.env['FAKE_RUNNER_CONTROL'] = str(control)
        def launch(name, budget):
            process = subprocess.Popen([str(CLI), 'run', name, '--attempt-budget', str(budget)],
                cwd=self.root, env=self.env, text=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, start_new_session=True)
            self.processes.append(process)
            return process
        first = launch('first', first_budget)
        deadline = time.monotonic() + 15
        while len(list(control.glob('*.json'))) < 30 and time.monotonic() < deadline:
            time.sleep(0.03)
        self.assertEqual(len(list(control.glob('*.json'))), 30)
        second = launch('second', self.budget)
        self.cli('pause', 'first')
        paths = list(control.glob('*.json'))
        (control / (paths[0].stem + '.release')).touch()
        deadline = time.monotonic() + 15
        while len(list(control.glob('*.json'))) < 31 and time.monotonic() < deadline:
            time.sleep(0.03)
        self.assertEqual(len(list(control.glob('*.json'))), 31)
        other = next(path for path in control.glob('*.json') if 'second-' in json.loads(path.read_text())['prompt'])
        (control / (other.stem + '.release')).touch()
        stdout, stderr = second.communicate(timeout=10)
        self.assertEqual(second.returncode, 0, stdout + stderr)
        self.assertIsNone(first.poll(), 'first is still draining; second already finished')
        for path in paths:
            (control / (path.stem + '.release')).touch()
        stdout, stderr = first.communicate(timeout=15)
        self.assertEqual(first.returncode, 0, stdout + stderr)
        self.assertEqual(len(self.calls()), 31)

    def test_scheduler_code_update_resumes_without_explicit_upgrade_reason(self):
        release = self.root / 'release'
        shutil.copytree(ROOT / 'experiment_runner', release / 'experiment_runner',
                        ignore=shutil.ignore_patterns('node_modules', '__pycache__'))
        (release / 'bin').mkdir()
        shutil.copy2(CLI, release / 'bin/experiment-runner')
        self.prepare()
        command = [str(release / 'bin/experiment-runner'), 'run', 'E1', '--attempt-budget', str(self.budget)]
        result = subprocess.run([*command, '--only', '0'], cwd=self.root, env=self.env,
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        scheduler = release / 'experiment_runner/scheduling.py'
        scheduler.write_text(scheduler.read_text() + '\n# Scheduling-only maintenance.\n')
        result = subprocess.run(command, cwd=self.root, env=self.env,
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self.calls()), 2)
        self.assertEqual(json.loads(self.budget.read_text())['used'], 2)
        self.assertTrue((self.root / 'E1/code-upgrades.jsonl').exists())

    def test_project_file_named_like_a_runner_file_still_requires_review(self):
        (self.root / 'experiment-runner.json').write_text(json.dumps({
            'schema_version': 1, 'inputs': 'project_inputs', 'extension': 'project_extension'}))
        (self.root / 'project_inputs.py').write_text(
            'from experiment_runner import inputs as shared\n'
            'def __getattr__(name): return getattr(shared, name)\n')
        extension = self.root / 'project_extension.py'
        extension.write_text('from pathlib import Path\n'
            'def add_commands(commands): pass\n'
            'def handle_command(args): return NotImplemented\n'
            'def enrich_report(result, plan, cases): pass\n'
            "def code_files(dataset): return {'experiment.py': Path(__file__)}\n")
        self.prepare(count=1)
        extension.write_text(extension.read_text() + '\n# Project evaluator changed.\n')
        result = self.cli('run', 'E1', '--attempt-budget', str(self.budget), succeeds=False)
        self.assertIn('--runner-upgrade-reason', result.stderr)
        self.assertEqual(len(self.calls()), 0)
