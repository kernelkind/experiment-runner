"""Mechanical adapter decisions use the native CLI without model traffic."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin/experiment-runner'


class MechanicalResultsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.launcher = CLI
        self.log = self.root / 'calls.json'
        self.output = json.dumps({'valid': True, 'reason': 'Exact trusted wording.'})
        self.evidence = {'rule': 'exact_teacher_subject', 'teacher_numbers': ['1.1']}
        self.cases = [
            {'id': 'one', 'inputs': {'TASK': 'one'},
             'mechanical': {'output': self.output, 'evidence': self.evidence}},
            {'id': 'two', 'inputs': {'TASK': 'two'},
             'mechanical': {'output': self.output, 'evidence': self.evidence}},
        ]
        (self.root / 'prompt.txt').write_text('GENERIC PROMPT {TASK}')
        self.write('cases.json', {'cases': self.cases})
        self.write('spec.json', {
            'schema_version': 1, 'prompt': {'id': 'P1', 'path': 'prompt.txt'},
            'dataset': {'adapter': 'generic', 'path': 'cases.json'},
            'runner': {'provider': 'openai-codex', 'auth_file': None,
                       'model': 'fixture/model', 'reasoning_effort': 'low',
                       'concurrency': 2,
                       'runner_binary': str(ROOT / 'tests/fixtures/fake-runner.py'),
                       'output_schema': {'type': 'object'}},
        })

    def write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value))
        return path

    def cli(self, *args, ok=True):
        result = subprocess.run([str(self.launcher), *map(str, args)], cwd=self.root,
                                env={**os.environ, 'FAKE_RUNNER_LOG': str(self.log),
                                     'FAKE_RUNNER_PROMPT_RESPONSE': self.output},
                                text=True, capture_output=True, timeout=30)
        if ok:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def show(self, name, *args):
        return json.loads(self.cli('show', name, '--json', *args).stdout)

    def test_all_mechanical_results_are_native_and_spend_no_attempts(self):
        budget = self.write('budget.json', {'schema_version': 1, 'limit': 1, 'used': 1})
        before = budget.read_bytes()
        self.cli('prepare', 'E1', '--spec', 'spec.json')
        prepared = self.show('E1')
        self.assertEqual(prepared['summary']['valid'], 2)
        self.assertEqual(prepared['summary']['mechanical'], 2)
        self.assertEqual(prepared['summary']['attempts'], 0)
        self.assertEqual(prepared['requests']['new'], 0)
        for case in prepared['cases']:
            self.assertEqual(case['origin'], 'mechanical')
            self.assertEqual(case['output'], self.output)
            self.assertEqual(case['parsed'], json.loads(self.output))
            self.assertEqual(case['mechanical_evidence'], self.evidence)
            self.assertNotIn('receipt', case)
        self.cli('run', 'E1', '--attempt-budget', budget, '--max-new-attempts', '1')
        report = self.show('E1', '--generation-costs')
        self.assertEqual(report['summary']['attempts'], 0)
        self.assertEqual(report['usage']['input_tokens'], 0)
        self.assertEqual(report['usage']['cost_statistics']['requests'], 0)
        self.assertEqual(budget.read_bytes(), before)
        self.assertFalse(self.log.exists())
        self.assertFalse((self.root / 'E1/run/results.jsonl').exists())
        self.assertFalse((self.root / 'E1/run/output.json').exists())

    def test_mixed_batch_dispatches_only_residual_jobs_and_preserves_prompt_identity(self):
        del self.cases[1]['mechanical']
        self.write('cases.json', {'cases': self.cases})
        budget = self.write('budget.json', {'schema_version': 1, 'limit': 1, 'used': 0})
        self.cli('prepare', 'mixed', '--spec', 'spec.json')
        prepared = self.show('mixed')
        self.assertEqual(prepared['summary']['mechanical'], 1)
        self.assertEqual(prepared['requests']['new'], 1)
        config = self.root / 'mixed/runner-config.json'
        manifest = self.root / 'mixed/run/manifest.json'
        original = (config.read_bytes(), manifest.read_bytes())
        self.cli('run', 'mixed', '--attempt-budget', budget, '--max-new-attempts', '1')
        report = self.show('mixed')
        self.assertEqual(report['summary']['valid'], 2)
        self.assertEqual(report['summary']['mechanical'], 1)
        self.assertEqual(report['summary']['attempts'], 1)
        calls = json.loads(self.log.read_text())
        self.assertEqual([c['prompt'] for c in calls], ['GENERIC PROMPT two'])
        receipts = [json.loads(line) for line in (self.root / 'mixed/run/results.jsonl').read_text().splitlines()]
        self.assertEqual([r['job_key'] for r in receipts], ['prompt:two'])
        self.assertEqual(json.loads(budget.read_text())['used'], 1)
        self.assertEqual((config.read_bytes(), manifest.read_bytes()), original)
        self.assertEqual(len(json.loads(config.read_text())['prompts']), 2)

    def test_mixed_identical_residuals_keep_native_answer_reuse(self):
        del self.cases[1]['mechanical']
        self.cases.append({'id': 'three', 'inputs': self.cases[1]['inputs']})
        self.write('cases.json', {'cases': self.cases})
        budget = self.write('budget.json', {'schema_version': 1, 'limit': 1, 'used': 0})
        self.cli('prepare', 'mixed', '--spec', 'spec.json')
        self.cli('run', 'mixed', '--attempt-budget', budget, '--max-new-attempts', '1')
        report = self.show('mixed', '--generation-costs')
        self.assertEqual(report['summary']['valid'], 3)
        self.assertEqual(report['summary']['attempts'], 1)
        self.assertEqual(report['summary']['reused'], 1)
        self.assertEqual(len(json.loads(self.log.read_text())), 1)
        self.assertEqual(report['generation_usage']['original_calls'], 1)

    def test_repeat_forces_real_fixture_execution_without_rewriting_baseline(self):
        self.cli('prepare', 'original', '--spec', 'spec.json')
        envelope = self.root / 'original/experiment.json'
        original = envelope.read_bytes()
        self.cli('prepare', 'repeat', '--from', 'original', '--repeat')
        prepared = self.show('repeat')
        self.assertEqual(prepared['summary']['unrun'], 2)
        self.assertEqual(prepared['summary']['mechanical'], 0)
        self.assertEqual(prepared['requests']['new'], 2)
        budget = self.write('budget.json', {'schema_version': 1, 'limit': 2, 'used': 0})
        self.cli('run', 'repeat', '--attempt-budget', budget)
        repeated = self.show('repeat')
        self.assertEqual(repeated['summary']['attempts'], 2)
        self.assertEqual(repeated['summary']['mechanical'], 0)
        self.assertTrue(all('receipt' in c for c in repeated['cases']))
        self.assertTrue(all(c.get('origin') != 'mechanical' for c in repeated['cases']))
        self.assertEqual(len(json.loads(self.log.read_text())), 2)
        self.assertEqual(envelope.read_bytes(), original)
        self.assertFalse((self.root / 'original/run/results.jsonl').exists())
        receipts = (self.root / 'repeat/run/results.jsonl').read_bytes()
        self.cli('prepare', 'mechanical-again', '--from', 'repeat')
        self.assertEqual(self.show('mechanical-again')['summary']['mechanical'], 2)
        self.cli('run', 'mechanical-again', '--attempt-budget', budget)
        self.assertEqual((self.root / 'repeat/run/results.jsonl').read_bytes(), receipts)
        self.assertEqual(len(json.loads(self.log.read_text())), 2)

    def test_all_mechanical_cached_batch_never_warms(self):
        prefix = 'GENERIC PROMPT' + ' word' * 1100
        (self.root / 'prefix.txt').write_text(prefix)
        (self.root / 'prompt.txt').write_text(prefix + '{TASK}')
        spec = json.loads((self.root / 'spec.json').read_text())
        spec['runner']['model'] = 'gpt-6.1-sol'
        self.write('spec.json', spec)
        self.cli('prepare', 'cached', '--spec', 'spec.json', '--prompt-cache-prefix', 'prefix.txt')
        self.assertTrue(self.show('cached')['cache_warmup']['eligible'])
        budget = self.write('budget.json', {'schema_version': 1, 'limit': 1, 'used': 1})
        self.cli('run', 'cached', '--attempt-budget', budget)
        shown = self.show('cached')
        self.assertEqual(shown['summary']['attempts'], 0)
        self.assertEqual(shown['cache_warmup']['attempts'], 0)
        self.assertFalse((self.root / 'cached/cache-warmups').exists())
        self.assertFalse((self.root / 'cached/cache-warmups.jsonl').exists())
        self.assertFalse(self.log.exists())

    def test_cached_mixed_batch_warms_and_charges_only_the_residual_request(self):
        del self.cases[1]['mechanical']
        self.write('cases.json', {'cases': self.cases})
        prefix = 'GENERIC PROMPT' + ' word' * 1100
        (self.root / 'prefix.txt').write_text(prefix)
        (self.root / 'prompt.txt').write_text(prefix + '{TASK}')
        spec = json.loads((self.root / 'spec.json').read_text())
        spec['runner']['model'] = 'gpt-6.1-sol'
        self.write('spec.json', spec)
        self.cli('prepare', 'cached', '--spec', 'spec.json', '--prompt-cache-prefix', 'prefix.txt')
        budget = self.write('budget.json', {'schema_version': 1, 'limit': 2, 'used': 0})
        self.cli('run', 'cached', '--attempt-budget', budget, '--max-new-attempts', '2')
        shown = self.show('cached')
        self.assertEqual(shown['summary']['valid'], 2)
        self.assertEqual(shown['summary']['mechanical'], 1)
        self.assertEqual(shown['summary']['attempts'], 2)
        self.assertEqual(shown['cache_warmup']['attempts'], 1)
        self.assertEqual(json.loads(budget.read_text())['used'], 2)
        self.assertEqual([c['prompt'] for c in json.loads(self.log.read_text())],
                         [prefix, prefix + 'two'])

    def test_invalid_mechanical_metadata_is_refused_before_preparing_or_dispatching(self):
        invalid = [
            {'output': '', 'evidence': self.evidence},
            {'output': '{', 'evidence': self.evidence},
            {'output': '{"valid":true,"valid":false}', 'evidence': self.evidence},
            {'output': 'NaN', 'evidence': self.evidence},
            {'output': self.output, 'evidence': {}},
            {'output': self.output, 'evidence': []},
            {'output': self.output, 'evidence': self.evidence, 'status': 'valid'},
            {'output': self.output, 'evidence': self.evidence, 'qualification': {'passed': True}},
        ]
        for i, decision in enumerate(invalid):
            with self.subTest(decision=decision):
                self.cases[0]['mechanical'] = decision
                self.write('cases.json', {'cases': self.cases})
                self.cli('prepare', f'invalid-{i}', '--spec', 'spec.json', ok=False)
                self.assertFalse((self.root / f'invalid-{i}').exists())
                self.assertFalse(self.log.exists())

    def test_mechanical_results_allow_attributed_reviews_and_reject_tampering(self):
        self.cli('prepare', 'E1', '--spec', 'spec.json')
        self.cli('review', 'E1', '--case', 'one', '--role', 'agent', '--verdict', 'accept',
                 '--comment', 'Verified exact teacher identity.')
        report = self.show('E1', '--case', 'one')
        self.assertTrue(report['reviews'][0]['current'])
        self.assertEqual(report['mechanical_evidence'], self.evidence)
        self.assertNotIn('receipt', report)
        path = self.root / 'E1/experiment.json'
        envelope = json.loads(path.read_text())
        envelope['plan']['cases'][0]['mechanical']['evidence']['teacher_numbers'] = ['9']
        path.write_text(json.dumps(envelope))
        refused = self.cli('show', 'E1', '--json', ok=False)
        self.assertIn('prepared experiment changed', refused.stderr)
        self.cli('run', 'E1', ok=False)
        self.assertFalse(self.log.exists())

    def test_mechanical_only_run_retains_execution_code_provenance_checks(self):
        release = self.root / 'release'
        shutil.copytree(ROOT / 'experiment_runner', release / 'experiment_runner',
                        ignore=shutil.ignore_patterns('node_modules', '__pycache__'))
        (release / 'bin').mkdir()
        shutil.copy2(CLI, release / 'bin/experiment-runner')
        self.launcher = release / 'bin/experiment-runner'
        self.cli('prepare', 'E1', '--spec', 'spec.json')
        path = release / 'experiment_runner/inputs.py'
        path.write_text(path.read_text() + '\n# Updated adapter implementation.\n')
        refused = self.cli('run', 'E1', ok=False)
        self.assertIn('--runner-upgrade-reason', refused.stderr)
        self.cli('run', 'E1', '--runner-upgrade-reason', 'Reviewed adapter update')
        self.assertEqual(self.show('E1')['summary']['attempts'], 0)
        self.assertFalse(self.log.exists())
        self.assertFalse((self.root / 'E1/run/results.jsonl').exists())

    def test_cases_without_mechanical_metadata_keep_the_old_from_and_receipt_contract(self):
        for case in self.cases:
            del case['mechanical']
        self.write('cases.json', {'cases': self.cases})
        self.cli('prepare', 'original', '--spec', 'spec.json')
        self.cli('run', 'original')
        receipt = self.root / 'original/run/results.jsonl'
        original = receipt.read_bytes()
        self.assertNotIn('mechanical', self.show('original')['summary'])
        self.cli('prepare', 'candidate', '--from', 'original')
        self.cli('run', 'candidate')
        report = self.show('candidate')
        self.assertEqual(report['summary']['reused'], 2)
        self.assertEqual(report['summary']['attempts'], 0)
        self.assertNotIn('mechanical', report['summary'])
        self.assertTrue(all('mechanical' not in c and 'origin' not in c for c in report['cases']))
        self.assertEqual(receipt.read_bytes(), original)

    def test_legacy_import_without_request_reuse_shows_mechanical_results(self):
        self.cli('prepare', 'historical', '--spec', 'spec.json')
        path = self.root / 'historical/run/manifest.json'
        manifest = json.loads(path.read_text())
        del manifest['request_reuse']
        path.write_text(json.dumps(manifest))
        original = path.read_bytes()
        self.cli('prepare', 'imported', '--spec', 'spec.json',
                 '--import-config', 'historical/runner-config.json')
        report = self.show('imported')
        self.assertTrue(report['imported'])
        self.assertEqual(report['summary']['mechanical'], 2)
        self.assertEqual(report['summary']['attempts'], 0)
        self.assertTrue(all(c['origin'] == 'mechanical' for c in report['cases']))
        self.assertTrue(all('receipt' not in c for c in report['cases']))
        self.assertEqual(path.read_bytes(), original)
        self.assertFalse(self.log.exists())


if __name__ == '__main__':
    unittest.main()
