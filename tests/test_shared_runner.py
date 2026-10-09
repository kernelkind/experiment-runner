"""Independent projects use the actual shared CLI without provider traffic."""
import json
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin/experiment-runner'


class SharedRunnerTests(unittest.TestCase):
    def test_prepare_from_can_explicitly_select_the_shared_worker(self):
        with tempfile.TemporaryDirectory() as temporary:
            project = Path(temporary)
            previous_worker = project / 'previous-worker.py'
            shutil.copy2(ROOT / 'tests/fixtures/fake-runner.py', previous_worker)
            (project / 'prompt.txt').write_text('P1\n{TASK}')
            (project / 'cases.json').write_text(json.dumps({'cases': [
                {'id': 'one', 'inputs': {'TASK': 'No provider traffic.'}}]}))
            (project / 'spec.json').write_text(json.dumps({
                'schema_version': 1, 'prompt': {'id': 'P1', 'path': 'prompt.txt'},
                'dataset': {'adapter': 'generic', 'path': 'cases.json'},
                'runner': {'provider': 'openai-codex', 'auth_file': None,
                           'model': 'fixture/model', 'reasoning_effort': 'low',
                           'concurrency': 2, 'runner_binary': str(previous_worker)}}))
            def cli(*arguments):
                return subprocess.run([str(CLI), *arguments], cwd=project,
                                      text=True, capture_output=True, timeout=30)
            prepared = cli('prepare', 'previous', '--spec', 'spec.json')
            self.assertEqual(prepared.returncode, 0, prepared.stdout + prepared.stderr)
            saved = (project / 'previous/experiment.json').read_bytes()
            inherited = cli('prepare', 'inherited', '--from', 'previous')
            self.assertEqual(inherited.returncode, 0, inherited.stdout + inherited.stderr)
            self.assertEqual(json.loads((project / 'inherited/experiment.json').read_text())
                             ['plan']['runner']['runner_binary'], str(previous_worker))
            worker = ROOT / 'experiment_runner/run-model.sh'
            migrated = cli('prepare', 'shared', '--from', 'previous',
                           '--runner-binary', str(worker))
            self.assertEqual(migrated.returncode, 0, migrated.stdout + migrated.stderr)
            plan = json.loads((project / 'shared/experiment.json').read_text())['plan']
            self.assertEqual(plan['runner']['runner_binary'], str(worker))
            self.assertEqual(plan['cases'], json.loads(saved)['plan']['cases'])
            self.assertEqual((project / 'previous/experiment.json').read_bytes(), saved)
            self.assertFalse((project / 'shared/run/results.jsonl').exists())

    def test_offline_execution_and_explicit_upgrade_preserve_receipts_and_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            project = Path(temporary)
            release = project / 'release'
            shutil.copytree(ROOT / 'experiment_runner', release / 'experiment_runner',
                            ignore=shutil.ignore_patterns('node_modules', '__pycache__'))
            (release / 'bin').mkdir()
            shutil.copy2(CLI, release / 'bin/experiment-runner')
            (project / 'prompt.txt').write_text('GENERIC PROMPT\n{TASK}')
            (project / 'cases.json').write_text(json.dumps({'cases': [
                {'id': name, 'inputs': {'TASK': name}} for name in ('one', 'two')]}))
            (project / 'spec.json').write_text(json.dumps({
                'schema_version': 1, 'prompt': {'id': 'P1', 'path': 'prompt.txt'},
                'dataset': {'adapter': 'generic', 'path': 'cases.json'},
                'runner': {'provider': 'openai-codex', 'auth_file': None,
                           'model': 'fixture/model', 'reasoning_effort': 'low',
                           'concurrency': 2, 'runner_binary': str(ROOT / 'tests/fixtures/fake-runner.py'),
                           'output_schema': {'type': 'object'}}}))
            budget = project / 'budget.json'
            budget.write_text(json.dumps({'schema_version': 1, 'limit': 2, 'used': 0}))
            environment = {**os.environ, 'FAKE_RUNNER_LOG': str(project / 'calls.json'),
                           'FAKE_RUNNER_PROMPT_RESPONSE': '{"answer":"local fixture"}'}
            def cli(*arguments):
                return subprocess.run([str(release / 'bin/experiment-runner'), *arguments],
                                      cwd=project, env=environment, text=True, capture_output=True, timeout=30)
            prepared = cli('prepare', 'E1', '--spec', 'spec.json')
            self.assertEqual(prepared.returncode, 0, prepared.stderr)
            ran = cli('run', 'E1', '--attempt-budget', str(budget))
            self.assertEqual(ran.returncode, 0, ran.stdout + ran.stderr)
            receipt = project / 'E1/run/results.jsonl'
            original = receipt.read_bytes()
            envelope = (project / 'E1/experiment.json').read_bytes()
            self.assertEqual(json.loads(budget.read_text())['used'], 2)
            report = json.loads(cli('show', 'E1', '--json').stdout)
            self.assertEqual(report['summary']['valid'], 2)
            self.assertEqual(report['cases'][0]['parsed'], {'answer': 'local fixture'})
            changed = release / 'experiment_runner/inputs.py'
            changed.write_text(changed.read_text() + '\n# Deliberately different release.\n')
            refused = cli('run', 'E1', '--attempt-budget', str(budget))
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn('--runner-upgrade-reason', refused.stderr)
            resumed = cli('run', 'E1', '--attempt-budget', str(budget),
                          '--runner-upgrade-reason', 'Use the explicitly selected shared release')
            self.assertEqual(resumed.returncode, 0, resumed.stdout + resumed.stderr)
            self.assertEqual(receipt.read_bytes(), original)
            self.assertEqual((project / 'E1/experiment.json').read_bytes(), envelope)
            self.assertEqual(json.loads(budget.read_text())['used'], 2)
            upgrade = json.loads((project / 'E1/code-upgrades.jsonl').read_text())
            self.assertEqual(upgrade['results_prefix_sha256'], hashlib.sha256(original).hexdigest())

    def test_separate_projects_use_the_same_executor_and_pin_its_code(self):
        with tempfile.TemporaryDirectory() as temporary:
            reports = []
            for name in ('project-a', 'project-b'):
                project = Path(temporary) / name
                project.mkdir()
                (project / 'prompt.txt').write_text('P1\n{TASK}')
                (project / 'cases.json').write_text(json.dumps({'cases': [
                    {'id': 'case-1', 'inputs': {'TASK': 'Local preparation only.'}}]}))
                (project / 'spec.json').write_text(json.dumps({
                    'schema_version': 1, 'prompt': {'id': 'P1', 'path': 'prompt.txt'},
                    'dataset': {'adapter': 'generic', 'path': 'cases.json'},
                    'runner': {'provider': 'openai-codex', 'auth_file': None,
                               'model': 'gpt-6.1-sol', 'reasoning_effort': 'medium',
                               'concurrency': 4}}))
                result = subprocess.run([str(CLI), 'prepare', 'E1', '--spec', 'spec.json'],
                                        cwd=project, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                envelope = json.loads((project / 'E1/experiment.json').read_text())
                reports.append(envelope['plan'])
                self.assertEqual(envelope['plan']['runner']['runner_binary'],
                                 str(ROOT / 'experiment_runner/run-model.sh'))
                self.assertEqual(envelope['plan']['requests_at_prepare']['new'], 1)
                self.assertFalse((project / 'E1/run/results.jsonl').exists())
                self.assertIn('extraction_pipeline.py', envelope['plan']['code'])
                shown = subprocess.run([str(CLI), 'show', 'E1', '--json'], cwd=project,
                                       capture_output=True, text=True, timeout=30)
                self.assertEqual(shown.returncode, 0, shown.stdout + shown.stderr)
                self.assertEqual(json.loads(shown.stdout)['summary']['attempts'], 0)
            self.assertEqual(reports[0]['code'], reports[1]['code'])


if __name__ == '__main__':
    unittest.main()
