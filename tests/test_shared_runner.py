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


class NativePrefixTests(unittest.TestCase):
    def test_warmup_cannot_change_the_comparison_key_or_model_settings(self):
        prefix = self.root / 'prefix.txt'
        prefix.write_text('GENERIC PROMPT ')
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
        prefix = self.root / 'prefix.txt'
        prefix.write_text('GENERIC PROMPT ')
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
        prefix = self.root / 'prefix.txt'
        prefix.write_text('GENERIC PROMPT ')
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
        prefix.write_text('different prefix')
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
