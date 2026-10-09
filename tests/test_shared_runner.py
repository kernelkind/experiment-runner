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
    def test_help_explains_the_cache_workflow_without_creating_an_experiment(self):
        with tempfile.TemporaryDirectory() as temporary:
            project = Path(temporary)
            for arguments, guidance in (
                (('--help',), ('SSE prefix caching', 'experiment-runner prepare --help',
                               'Recommended for large experiments sharing fixed instructions')),
                (('prepare', '--help'), (
                    'exact beginning of every rendered prompt',
                    'derived from the exact prefix',
                    'experiment-runner prepare target/cached-v1',
                    'experiment-runner run target/cached-v1',
                    '1,024',
                    'automatically',
                    'Recommended for large experiments sharing fixed instructions',
                    'Preparation rejects shorter prefixes',
                    'existing attempt budget',
                    '--runner-binary',
                )),
                (('run', '--help'), ('warmup', 'cached input', 'reasoning', 'unknown')),
                (('show', '--help'), ('usage.cost_statistics', 'API-equivalent', 'percent', 'pricing')),
            ):
                with self.subTest(arguments=arguments):
                    result = subprocess.run([str(CLI), *arguments], cwd=project,
                                            text=True, capture_output=True, timeout=30)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    for text in guidance:
                        self.assertIn(text, result.stdout)
            self.assertEqual(list(project.iterdir()), [])

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


class NativePrefixTests(unittest.TestCase):
    def test_cost_statistics_use_saved_components_without_repricing_old_receipts(self):
        self.prepare('costs')
        usage = dict(input=20, cacheRead=60, cacheWrite=20, output=10, reasoning=3,
                     cost=dict(input=0.00004, cacheRead=0.000006, cacheWrite=0.00005,
                               output=0.0001, total=0.000196))
        ran = self.cli('run', 'costs', env={'FAKE_RUNNER_RESULT_OVERRIDE': json.dumps({'usage': usage})})
        self.assertIn('API-equivalent request cost (USD):', ran.stdout)
        self.assertIn('Estimated net cache savings:', ran.stdout)
        stats = self.show('costs')['usage']['cost_statistics']
        self.assertEqual(stats['requests'], 2)
        self.assertEqual(stats['cache_hit_rate_percent'], 100)
        self.assertAlmostEqual(stats['total_cost_usd'], 0.000392)
        self.assertAlmostEqual(stats['uncached_comparison_cost_usd'], 0.0006)
        self.assertAlmostEqual(stats['net_cache_savings_percent'], 100 * 0.000208 / 0.0006)
        self.cli('prepare', 'reuse-costs', '--from', 'costs')
        self.cli('run', 'reuse-costs')
        reused = self.show('reuse-costs', '--generation-costs')
        self.assertEqual(reused['usage']['cost_statistics']['requests'], 0)
        self.assertIsNone(reused['usage']['cost_statistics']['net_cache_savings_percent'])
        self.assertEqual(reused['generation_usage']['cost_statistics'], stats)
        self.assertEqual(len(self.calls()), 2)

    def test_unknown_historical_input_rate_keeps_savings_unknown(self):
        self.prepare('costs')
        usage = dict(input=0, cacheRead=100, cacheWrite=0, output=10, reasoning=3,
                     cost=dict(input=0, cacheRead=0.00001, cacheWrite=0,
                               output=0.0001, total=0.00011))
        self.cli('run', 'costs', env={'FAKE_RUNNER_RESULT_OVERRIDE': json.dumps({'usage': usage})})
        stats = self.show('costs')['usage']['cost_statistics']
        self.assertAlmostEqual(stats['total_cost_usd'], 0.00022)
        self.assertEqual(stats['cache_hit_rate_percent'], 100)
        self.assertIsNone(stats['net_cache_savings_percent'])
        self.assertIsNone(stats['uncached_comparison_cost_usd'])
        self.assertEqual(stats['comparison_requests_without_uncached_cost'], 2)

    def test_successful_sdk_output_without_reported_usage_is_not_free(self):
        self.prepare('missing-usage')
        override = dict(usage=dict(input=0, output=0, cacheRead=0, cacheWrite=0,
                                   reasoning=0, cost={'total': 0}),
                        usage_note='pi-ai input excludes cacheRead/cacheWrite; output includes reasoning.',
                        uncached_catalog_cost_estimate_usd=None)
        self.cli('run', 'missing-usage', env={'FAKE_RUNNER_RESULT_OVERRIDE': json.dumps(override)})
        report = self.show('missing-usage')
        self.assertEqual(report['summary']['valid'], 2)
        stats = report['usage']['cost_statistics']
        self.assertEqual(stats['requests_without_cost'], 2)
        self.assertIsNone(stats['total_cost_usd'])
        self.assertIsNone(stats['mean_request_cost_usd'])
        self.assertIsNone(stats['net_cache_savings_percent'])
        self.assertIsNone(stats['cache_hit_rate_percent'])

    def automatic_prefix(self, tokens=1100):
        # GENERIC PROMPT is four o200k_base tokens; every appended ' word' is one.
        text = 'GENERIC PROMPT' + ' word' * (tokens - 4)
        prefix = self.root / 'prefix.txt'
        prefix.write_text(text)
        self.prompt.write_text(text + '{POST}')
        return prefix

    def test_prepare_rejects_short_cache_prefix_before_creating_experiment(self):
        prefix = self.automatic_prefix(1023)
        # Long case data must not make the fixed prefix qualify.
        cases = json.loads(self.cases.read_text())
        for case in cases['cases']:
            case['inputs']['POST'] = ' dynamic' * 2000
        self.cases.write_text(json.dumps(cases))
        original_spec = self.spec.read_text()
        self.prepare('plain')
        for mode in ('cli', 'spec', 'manual-key', 'from-cli'):
            with self.subTest(mode=mode):
                spec = json.loads(original_spec)
                spec['runner']['model'] = 'gpt-6.1-sol'
                options = []
                if mode == 'spec':
                    spec['runner']['prompt_cache_prefix'] = prefix.read_text()
                else:
                    options = ['--prompt-cache-prefix', prefix]
                    if mode == 'manual-key':
                        options += ['--prompt-cache-key', 'fixed-key']
                self.spec.write_text(json.dumps(spec))
                source = ['--from', 'plain', '--model', 'gpt-6.1-sol'] if mode == 'from-cli' else ['--spec', self.spec]
                failed = self.cli('prepare', mode, *source, *options, ok=False)
                self.assertIn('prompt_cache_prefix', failed.stderr)
                self.assertIn('1,023', failed.stderr)
                self.assertIn('1,024', failed.stderr)
                self.assertFalse((self.root / mode).exists())
                self.assertEqual(self.calls(), [])
        # Preparing the same inputs without requesting caching made no calls.
        self.assertEqual(self.show('plain')['summary']['attempts'], 0)

    def test_minimum_cache_prefix_is_accepted_and_warms_before_comparisons(self):
        prefix = self.automatic_prefix(1024)
        self.prepare('minimum', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix)
        prepared = self.show('minimum')
        self.assertEqual(prepared['cache_warmup']['estimated_prefix_tokens'], 1024)
        self.cli('run', 'minimum', '--max-new-attempts', '3')
        self.assertEqual(len(self.calls()), 3)
        self.assertEqual(self.calls()[0]['prompt'], prefix.read_text())

    def test_warmup_never_consumes_the_only_slot_and_resume_rechecks_capacity(self):
        prefix = self.automatic_prefix()
        self.prepare('limited', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix)
        self.cli('run', 'limited', '--max-new-attempts', '1', ok=False)
        self.assertEqual(len(self.calls()), 1)
        self.assertNotEqual(self.calls()[0]['prompt'], prefix.read_text())
        self.assertEqual(self.show('limited')['cache_warmup']['attempts'], 0)
        self.cli('run', 'limited', '--max-new-attempts', '2')
        self.assertEqual(len(self.calls()), 3)
        self.assertEqual(self.calls()[1]['prompt'], prefix.read_text())
        self.assertEqual(self.show('limited')['summary']['attempts'], 3)

    def test_additional_budget_with_one_slot_skips_warmup(self):
        from unittest.mock import patch
        from experiment_runner import experiment, extraction_pipeline as engine, project
        prefix = self.automatic_prefix()
        self.prepare('limited', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix)
        stage = self.write('stage.json', dict(schema_version=1, limit=3, used=0))
        shared = self.write('shared.json', dict(schema_version=1, limit=1, used=0))
        project.configure(self.root)
        with patch.dict(os.environ, {'FAKE_RUNNER_LOG': str(self.log)}):
            with self.assertRaises(engine.RunnerError):
                experiment.run(self.root / 'limited', max_new_attempts=3,
                               attempt_budget_path=stage, additional_attempt_budget_paths=(shared,))
        self.assertEqual(len(self.calls()), 1)
        self.assertNotEqual(self.calls()[0]['prompt'], prefix.read_text())
        self.assertEqual(json.loads(stage.read_text())['used'], 1)
        self.assertEqual(json.loads(shared.read_text())['used'], 1)

    def test_command_cap_counts_warmup_and_both_persistent_counters(self):
        from unittest.mock import patch
        from experiment_runner import experiment, extraction_pipeline as engine, project
        prefix = self.automatic_prefix()
        self.prepare('limited', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix,
                     '--concurrency', '2')
        stage = self.write('stage.json', dict(schema_version=1, limit=3, used=0))
        shared = self.write('shared.json', dict(schema_version=1, limit=3, used=0))
        project.configure(self.root)
        with patch.dict(os.environ, {'FAKE_RUNNER_LOG': str(self.log)}):
            with self.assertRaises(engine.RunnerError):
                experiment.run(self.root / 'limited', max_new_attempts=2,
                               attempt_budget_path=stage, additional_attempt_budget_paths=(shared,))
        self.assertEqual(len(self.calls()), 2)
        self.assertEqual(self.calls()[0]['prompt'], prefix.read_text())
        self.assertEqual(json.loads(stage.read_text())['used'], 2)
        self.assertEqual(json.loads(shared.read_text())['used'], 2)
        self.assertEqual(self.show('limited')['summary']['unrun'], 1)

    def test_warmup_answer_cannot_satisfy_an_identical_real_comparison(self):
        prefix = self.automatic_prefix()
        cases = json.loads(self.cases.read_text())
        cases['cases'][0]['inputs']['POST'] = ''
        self.cases.write_text(json.dumps(cases))
        self.prepare('same', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix)
        self.cli('run', 'same', '--max-new-attempts', '3')
        self.assertEqual([c['prompt'] for c in self.calls()].count(prefix.read_text()), 2)
        self.assertEqual(self.show('same')['summary']['reused'], 0)

    def test_failed_warmup_blocks_concurrent_comparisons_and_keeps_unknown_cost(self):
        prefix = self.automatic_prefix()
        self.prepare('failed', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix,
                     '--concurrency', '2')
        budget = self.write('budget.json', dict(schema_version=1, limit=3, used=0))
        self.cli('run', 'failed', '--attempt-budget', budget, ok=False,
                 env={'FAKE_RUNNER_FAIL_STAGE': 'prompt'})
        self.assertEqual(len(self.calls()), 1)
        self.assertEqual(self.calls()[0]['prompt'], prefix.read_text())
        self.assertEqual(json.loads(budget.read_text())['used'], 1)
        saved = self.show('failed')
        self.assertEqual(saved['summary']['unrun'], 2)
        self.assertEqual(saved['summary']['attempts'], 1)
        self.assertEqual(saved['usage']['attempts_without_usage'], 1)
        self.assertIsNone(saved['usage']['catalog_cost_estimate_usd'])

    def test_key_is_stable_across_experiments_and_changes_with_prefix_whitespace(self):
        prefix = self.automatic_prefix()
        self.prepare('one', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix)
        self.prepare('two', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix)
        self.assertEqual(self.show('one')['settings']['prompt_cache_key'],
                         self.show('two')['settings']['prompt_cache_key'])
        prefix.write_text(prefix.read_text() + '\n')
        self.prompt.write_text(prefix.read_text() + '{POST}')
        self.prepare('three', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix)
        self.assertNotEqual(self.show('one')['settings']['prompt_cache_key'],
                            self.show('three')['settings']['prompt_cache_key'])

    def test_invalid_budget_settings_fail_before_warming(self):
        prefix = self.automatic_prefix()
        self.prepare('invalid', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix)
        failed = self.cli('run', 'invalid', '--max-cost-usd', '1',
                          '--max-request-cost-usd', '0.1', ok=False)
        self.assertIn('requires provider-reported OpenRouter billing', failed.stderr)
        self.assertEqual(self.calls(), [])

    def test_prefix_only_automatically_warms_and_counts_the_shared_allowance(self):
        prefix = self.root / 'prefix.txt'
        text = 'GENERIC PROMPT' + ' word' * 1100 + '\n'
        prefix.write_text(text)
        self.prompt.write_text(text + '{POST}')
        self.prepare('auto', '--model', 'gpt-6.1-sol', '--prompt-cache-prefix', prefix,
                     '--concurrency', '2')
        settings = self.show('auto')['settings']
        self.assertEqual(settings['prompt_cache_key'],
                         'prefix-' + hashlib.sha256(prefix.read_bytes()).hexdigest()[:56])
        budget = self.write('budget.json', dict(schema_version=1, limit=3, used=0))
        self.cli('run', 'auto', '--max-new-attempts', '3', '--attempt-budget', budget)
        self.assertEqual(self.calls()[0]['prompt'], text)
        self.assertEqual(len(self.calls()), 3)
        result = self.show('auto')
        self.assertEqual(result['summary']['attempts'], 3)
        self.assertEqual(result['cache_warmup']['attempts'], 1)
        self.assertEqual(result['usage']['input_tokens'], 300)
        self.assertEqual(json.loads(budget.read_text())['used'], 3)
        self.assertTrue(all(c['output'] != 'Response to: ' + text for c in result['cases']))
        self.cli('prepare', 'reused-auto', '--from', 'auto')
        self.cli('run', 'reused-auto', '--attempt-budget', budget)
        self.cli('run', 'auto', '--attempt-budget', budget)
        self.assertEqual(len(self.calls()), 3)
        self.assertEqual(self.show('reused-auto')['summary']['reused'], 2)
        traced = self.show('reused-auto', '--generation-costs')['generation_usage']
        self.assertEqual(traced['original_calls'], 3)
        self.assertEqual(traced['input_tokens'], 300)
        self.assertEqual(traced['source_calls_added'], 3)

    def test_warmup_cannot_change_the_comparison_key_or_model_settings(self):
        prefix = self.automatic_prefix()
        self.prepare('native', '--model', 'gpt-6.1-sol', '--prompt-cache-key', 'fixed-key',
                     '--prompt-cache-prefix', prefix)
        for index, overrides in enumerate((
            ('--prompt-cache-key', 'different-key'),
            ('--prompt-cache-prefix', prefix),
            ('--reasoning-effort', 'medium'),
            ('--concurrency', '2'),
        )):
            name = f'changed-warmup-{index}'
            failed = self.cli('prepare', name, '--from', 'native', '--cache-warmup',
                              *overrides, ok=False)
            self.assertIn('--cache-warmup', failed.stderr)
            self.assertFalse((self.root / name).exists())
        self.assertEqual(self.calls(), [])

    def test_native_prefix_rejects_whitespace_before_execution(self):
        from experiment_runner import extraction_pipeline as engine
        self.prepare('plain', '--model', 'gpt-6.1-sol')
        path = self.root / 'plain/runner-config.json'
        data = json.loads(path.read_text())
        data.update(prompt_cache_key='native-prefix-test', prompt_cache_prefix='\n\t')
        for prompt in data['prompts']:
            prompt['text'] = '\n\t' + prompt['text']
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(engine.RunnerError, 'prompt_cache_prefix'):
            engine.PromptRunnerConfig.load(path)
        self.assertEqual(self.calls(), [])

    def test_native_prefix_warmup_is_frozen_ordered_and_does_not_replace_case_answers(self):
        import copy
        from dataclasses import replace
        from experiment_runner import extraction_pipeline as engine
        key = '01234567-89ab-5cde-8012-0123456789ab'
        prefix = self.automatic_prefix()
        self.prepare('native', '--model', 'gpt-6.1-sol', '--prompt-cache-key', key,
                     '--prompt-cache-prefix', prefix)
        cfg = engine.PromptRunnerConfig.load(self.root/'native/runner-config.json')
        self.assertEqual(cfg.prompt_cache_prefix, prefix.read_text())
        plan = json.loads((self.root / 'native/experiment.json').read_text())['plan']
        self.assertEqual(plan['sources'][str(prefix)], hashlib.sha256(prefix.read_bytes()).hexdigest())
        self.cli('prepare', 'warm', '--from', 'native', '--cache-warmup')
        warm = self.show('warm')
        self.assertEqual(len(warm['cases']), 1)
        self.assertEqual(warm['requests']['new'], 1)
        warm_plan = json.loads((self.root / 'warm/experiment.json').read_text())['plan']
        self.assertIsNone(warm_plan['baseline'])
        self.assertFalse(warm_plan['preparation']['application_allowed'])
        self.assertEqual(warm_plan['runner']['concurrency'], 1)
        self.assertEqual(warm['cases'][0]['inputs'], {})
        self.cli('run', 'warm', '--max-new-attempts', '1')
        self.cli('run', 'native', '--max-new-attempts', '2')
        calls = self.calls()
        self.assertEqual([c['prompt'] for c in calls], [prefix.read_text(), *(p.text for p in cfg.prompts)])
        self.assertTrue(all(c['request']['prompt_cache_prefix'] == prefix.read_text() for c in calls))
        self.cli('prepare', 'copy', '--from', 'native')
        self.assertEqual(self.show('copy')['requests']['reusable'], 2)
        self.cli('prepare', 'warm-again', '--from', 'native', '--cache-warmup')
        self.assertEqual(self.show('warm-again')['requests']['new'], 1)
        manifest = json.loads((cfg.run_directory/'manifest.json').read_text())
        self.assertEqual(manifest['prompt_cache_prefix'], prefix.read_text())
        ordinary = {k: v for k, v in manifest.items() if k != 'prompt_cache_prefix'}
        self.assertNotEqual(engine._prompt_request_key(manifest, cfg.prompts[0].text),
                            engine._prompt_request_key(ordinary, cfg.prompts[0].text))
        receipt = json.loads((cfg.run_directory/'results.jsonl').read_text().splitlines()[0])
        cfg = replace(cfg, prompts=(cfg.prompts[0],))
        engine.validate_prompt_receipt(cfg, manifest, receipt)
        for mutation in ('prefix', 'extra', 'id', 'lite'):
            changed = copy.deepcopy(receipt)
            result = changed['model_result']
            if mutation == 'prefix': result['request']['body']['input'][1]['content'][0]['text'] += 'changed'
            elif mutation == 'extra': result['request']['body']['input'].append({'role': 'developer', 'content': 'hidden'})
            elif mutation == 'id': result['request']['body']['input'][1]['id'] = 'other'
            else: result['request']['headers']['x-openai-internal-codex-responses-lite'] = 'false'
            changed['stdout_json'] = json.dumps(result)
            changed['stdout_sha256'] = engine._sha256_bytes(changed['stdout_json'].encode())
            with self.assertRaisesRegex(engine.RunnerError, 'native|cache prefix'):
                engine.validate_prompt_receipt(cfg, manifest, changed)
        self.cli('prepare', 'plain-warmup', '--from', 'native', '--only', 'one', '--cache-warmup', ok=False)

    def test_native_affinity_is_frozen_inherited_repeated_and_bound_to_receipts(self):
        import copy
        from dataclasses import replace
        from experiment_runner import extraction_pipeline as engine
        key = 'affinity-native-trial'
        self.prepare('plain', '--model', 'gpt-6.1-sol')
        self.cli('prepare', 'A', '--from', 'plain', '--only', 'one', '--prompt-cache-key', key)
        config = engine.PromptRunnerConfig.load(self.root / 'A/runner-config.json')
        self.assertEqual(config.prompt_cache_key, key)
        self.cli('run', 'A')
        manifest = json.loads((config.run_directory / 'manifest.json').read_text())
        receipt = json.loads((config.run_directory / 'results.jsonl').read_text().splitlines()[0])
        engine.validate_prompt_receipt(config, manifest, receipt)
        self.assertEqual(manifest['prompt_cache_key'], key)
        ordinary = {k: v for k, v in manifest.items() if k != 'prompt_cache_key'}
        self.assertNotEqual(engine._prompt_request_key(manifest, config.prompts[0].text),
                            engine._prompt_request_key(ordinary, config.prompts[0].text))
        for field in ('body', 'session-id', 'x-client-request-id'):
            changed = copy.deepcopy(receipt)
            request = changed['model_result']['request']
            if field == 'body': request['body']['prompt_cache_key'] = 'other-key'
            else: request['headers'][field] = 'other-key'
            changed['stdout_json'] = json.dumps(changed['model_result'])
            changed['stdout_sha256'] = engine._sha256_bytes(changed['stdout_json'].encode())
            with self.assertRaisesRegex(engine.RunnerError, 'cache affinity'):
                engine.validate_prompt_receipt(config, manifest, changed)
        with self.assertRaisesRegex(engine.RunnerError, 'cache affinity'):
            engine.validate_prompt_receipt(replace(config, prompt_cache_key=None), manifest, receipt)
        self.cli('prepare', 'copy', '--from', 'A')
        self.assertEqual(self.show('copy')['requests']['reusable'], 1)
        self.cli('prepare', 'repeat', '--from', 'A', '--repeat')
        self.assertEqual(self.show('repeat')['requests']['new'], 1)
        self.cli('run', 'repeat')
        self.cli('prepare', 'B', '--from', 'plain', '--only', 'two', '--prompt-cache-key', key)
        self.cli('run', 'B')
        calls = self.calls()
        self.assertEqual(len(calls), 3)
        self.assertEqual([c['request']['prompt_cache_key'] for c in calls], [key] * 3)
        self.assertEqual(calls[0]['request'], calls[1]['request'])
        self.assertNotEqual(calls[1]['prompt'], calls[2]['prompt'])
        self.cli('prepare', 'other-key', '--from', 'A', '--prompt-cache-key', 'other-key')
        self.assertEqual(self.show('other-key')['requests']['new'], 1)
        for value in ('', ' padded ', 'x' * 65, '🌿', None, 123):
            path = self.root / 'A/runner-config.json'
            data = json.loads(path.read_text())
            path.write_text(json.dumps({**data, 'prompt_cache_key': value}))
            with self.assertRaisesRegex(engine.RunnerError, 'prompt_cache_key'):
                engine.PromptRunnerConfig.load(path)

    def test_cli_rejects_invalid_native_controls_without_execution(self):
        prefix = self.automatic_prefix()
        base = ['--model', 'gpt-6.1-sol', '--prompt-cache-key', 'native-test',
                '--prompt-cache-prefix', str(prefix)]
        for index, options in enumerate((
            ['--prompt-cache-key', ' padded '],
            ['--prompt-cache-key', 'x' * 65],
            ['--provider', 'openrouter'],
            ['--model', 'fixture/model'],
            ['--transport', 'websocket'],
        )):
            name = f'invalid-{index}'
            failed = self.cli('prepare', name, '--spec', self.spec, *base, *options, ok=False)
            self.assertIn('prompt_cache', failed.stderr)
            self.assertFalse((self.root / name).exists())
        prefix.write_text('different prefix' + ' word' * 1100)
        failed = self.cli('prepare', 'mismatch', '--spec', self.spec, *base, ok=False)
        self.assertIn('prompt_cache_prefix', failed.stderr)
        self.assertFalse((self.root / 'mismatch').exists())
        self.cli('prepare', 'warm-from-spec', '--spec', self.spec, '--cache-warmup', ok=False)
        self.assertFalse((self.root / 'warm-from-spec').exists())
        self.assertEqual(self.calls(), [])

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.runner = self.root / "runner"
        shutil.copy2(ROOT / 'tests/fixtures/fake-runner.py', self.runner)
        self.runner.chmod(0o755)
        self.log = self.root / "calls.json"
        self.prompt = self.root / "v1.txt"
        self.prompt.write_text('GENERIC PROMPT {POST}\n{"literal_json":true}\n')
        self.cases = self.write(
            "cases.json",
            {
                "cases": [
                    {
                        "id": "one",
                        "inputs": {"POST": "Tea {POST}"},
                        "evidence": {"private": "REVIEW ONLY"},
                    },
                    {"id": "two", "inputs": {"POST": "Coffee"}},
                ]
            },
        )
        self.spec = self.write(
            "spec.json",
            {
                "schema_version": 1,
                "prompt": {"id": "P1", "path": "v1.txt"},
                "dataset": {"adapter": "generic", "path": "cases.json"},
                "runner": {
                    "runner_binary": str(self.runner),
                    "auth_file": None,
                    "provider": "openai-codex",
                    "model": "fixture/model",
                    "reasoning_effort": "low",
                    "concurrency": 1,
                },
            },
        )

    def write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value))
        return path

    def cli(self, *args, ok=True, env=None):
        result = subprocess.run(
            [str(CLI), *map(str, args)],
            cwd=self.root,
            env={**os.environ, "FAKE_RUNNER_LOG": str(self.log), **(env or {})},
            text=True,
            capture_output=True,
            timeout=30,
        )
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result

    def prepare(self, name="E1", *args):
        return self.cli("prepare", name, "--spec", self.spec, *args)

    def show(self, name="E1", *args):
        return json.loads(self.cli("show", name, *args, "--json").stdout)

    def calls(self):
        return json.loads(self.log.read_text()) if self.log.exists() else []



if __name__ == '__main__':
    unittest.main()
