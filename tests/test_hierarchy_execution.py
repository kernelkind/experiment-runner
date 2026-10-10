"""Hierarchy clients exercise the shared executor with local transport fixtures."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiment_runner import experiment, project
from experiment_runner.extraction_pipeline import (
    NewJobLimitReached, PersistentAttemptBudget, RunnerError,
)

ROOT = Path(__file__).resolve().parents[1]


class HierarchyExecutionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        project.configure(self.root)
        self.addCleanup(project.configure, ROOT)
        (self.root / 'prompt.txt').write_text('GENERIC PROMPT\n{TASK}')
        (self.root / 'cases.json').write_text(json.dumps({'cases': [
            {'id': name, 'inputs': {'TASK': name}} for name in ('one', 'two', 'three')]}))
        (self.root / 'spec.json').write_text(json.dumps({
            'schema_version': 1, 'prompt': {'id': 'P1', 'path': 'prompt.txt'},
            'dataset': {'adapter': 'generic', 'path': 'cases.json'},
            'runner': {'provider': 'openai-codex', 'auth_file': None,
                       'model': 'fixture/model', 'reasoning_effort': 'low',
                       'concurrency': 3, 'runner_binary': str(ROOT / 'tests/fixtures/fake-runner.py'),
                       'output_schema': {'type': 'object'}}}))
        self.environment = patch.dict(os.environ, {
            'FAKE_RUNNER_LOG': str(self.root / 'calls.json'),
            'FAKE_RUNNER_PROMPT_RESPONSE': '{"answer":"local fixture"}'})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def prepare(self, name='E1'):
        args = experiment.parser().parse_args([
            'prepare', str(self.root / name), '--spec', str(self.root / 'spec.json')])
        return experiment.prepare(args, include_request_preview=False)

    def test_prepare_report_and_run_can_skip_request_preview(self):
        # Preview is optional work, not a prerequisite for audited execution.
        with patch.object(experiment, 'request_preview', side_effect=AssertionError('preview invoked')):
            prepared = self.prepare()
            self.assertIsNone(prepared['requests'])
            envelope = json.loads((self.root / 'E1/experiment.json').read_text())
            self.assertIsNone(envelope['plan']['requests_at_prepare'])
            shown = experiment.report(self.root / 'E1', include_request_preview=False)
            self.assertEqual(shown['summary']['attempts'], 0)
            completed = experiment.run(self.root / 'E1', include_request_preview=False)
            self.assertEqual(completed['summary']['valid'], 3)
            self.assertIsNone(completed['requests'])
            inherited = experiment.parser().parse_args([
                'prepare', str(self.root / 'E2'), '--from', str(self.root / 'E1')])
            candidate = experiment.prepare(inherited, include_request_preview=False)
            self.assertEqual(candidate['summary']['cases'], 3)
            self.assertIsNone(candidate['requests'])

    def test_selected_run_keeps_lifetime_budgets_and_reuses_without_new_allowance(self):
        self.prepare()
        stage = PersistentAttemptBudget(self.root / 'stage.json')
        shared = PersistentAttemptBudget(self.root / 'shared.json')
        stage.initialize(3)
        shared.initialize(1)
        result = experiment.run(self.root / 'E1', only=['one'], max_new_attempts=1,
            attempt_budget_path=stage.path, additional_attempt_budget_paths=(shared.path,),
            include_request_preview=False)
        self.assertEqual(result['summary']['valid'], 1)
        self.assertEqual(result['summary']['unrun'], 2)
        before = (stage.path.read_bytes(), shared.path.read_bytes())
        result = experiment.run(self.root / 'E1', only=['one'], max_new_attempts=0,
            attempt_budget_path=stage.path, additional_attempt_budget_paths=(shared.path,),
            include_request_preview=False)
        self.assertEqual(result['summary']['attempts'], 1)
        self.assertEqual(before, (stage.path.read_bytes(), shared.path.read_bytes()))
        with self.assertRaisesRegex(RunnerError, 'allowance exhausted'):
            experiment.run(self.root / 'E1', only=['two'], max_new_attempts=1,
                attempt_budget_path=stage.path, additional_attempt_budget_paths=(shared.path,),
                include_request_preview=False)
        self.assertEqual(before, (stage.path.read_bytes(), shared.path.read_bytes()))
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 1)

    def test_selected_run_ignores_unselected_failure_and_retains_its_receipt(self):
        self.prepare()
        with patch.dict(os.environ, {'FAKE_RUNNER_FAIL_STAGE': 'prompt'}):
            with self.assertRaises(RunnerError):
                experiment.run(self.root / 'E1', only=['two'], include_request_preview=False)
        results = self.root / 'E1/run/results.jsonl'
        failed = results.read_bytes()
        completed = experiment.run(self.root / 'E1', only=['one'], include_request_preview=False)
        self.assertEqual([(row['id'], row['status']) for row in completed['cases']],
                         [('one', 'valid'), ('two', 'failed'), ('three', 'unrun')])
        self.assertTrue(results.read_bytes().startswith(failed))
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 2)

    def test_selected_run_rejects_unknown_and_duplicate_ids_before_dispatch(self):
        self.prepare()
        for only in (['missing'], ['one', 'one']):
            with self.subTest(only=only), self.assertRaisesRegex(ValueError, 'selected case ID'):
                experiment.run(self.root / 'E1', only=only, include_request_preview=False)
        self.assertFalse((self.root / 'calls.json').exists())
        self.assertFalse((self.root / 'E1/execution-settings.jsonl').exists())

    def test_empty_selected_run_is_a_noop_with_the_full_report(self):
        self.prepare()
        result = experiment.run(self.root / 'E1', only=[], include_request_preview=False)
        self.assertEqual(result['summary']['unrun'], 3)
        self.assertEqual(result['summary']['attempts'], 0)
        self.assertFalse((self.root / 'calls.json').exists())

    def test_selected_slices_can_share_one_native_cache_warmup(self):
        (self.root / 'prompt.txt').write_text('GENERIC PROMPT' + ' word' * 1100 + '{TASK}')
        args = experiment.parser().parse_args([
            'prepare', str(self.root / 'E1'), '--spec', str(self.root / 'spec.json'),
            '--model', 'gpt-6.1-sol', '--prompt-cache'])
        experiment.prepare(args, include_request_preview=False)
        frozen = (self.root / 'E1/run/manifest.json').read_bytes()
        budget = PersistentAttemptBudget(self.root / 'shared.json')
        budget.initialize(3)
        first = experiment.run(self.root / 'E1', only=['one'], warm_cache=True,
            max_new_attempts=2, attempt_budget_path=budget.path, include_request_preview=False)
        self.assertEqual(first['cache_warmup']['attempts'], 1)
        self.assertEqual(first['summary']['valid'], 1)
        second = experiment.run(self.root / 'E1', only=['two'], warm_cache=False,
            max_new_attempts=1, attempt_budget_path=budget.path, include_request_preview=False)
        self.assertEqual(second['cache_warmup']['attempts'], 1)
        self.assertEqual(second['summary']['valid'], 2)
        self.assertEqual(second['summary']['unrun'], 1)
        self.assertEqual(json.loads(budget.path.read_text())['used'], 3)
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 3)
        self.assertEqual((self.root / 'E1/run/manifest.json').read_bytes(), frozen)

    def test_programmatic_warm_cache_false_suppresses_only_the_warmup(self):
        (self.root / 'prompt.txt').write_text('GENERIC PROMPT' + ' word' * 1100 + '{TASK}')
        args = experiment.parser().parse_args([
            'prepare', str(self.root / 'E1'), '--spec', str(self.root / 'spec.json'),
            '--model', 'gpt-6.1-sol', '--prompt-cache'])
        experiment.prepare(args, include_request_preview=False)
        result = experiment.run(self.root / 'E1', warm_cache=False,
                                max_new_attempts=3, include_request_preview=False)
        self.assertEqual(result['cache_warmup']['attempts'], 0)
        self.assertEqual(result['summary']['valid'], 3)
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 3)

    def test_selected_run_preserves_provider_retries_and_two_lifetime_attempts(self):
        self.prepare()
        failures = {name: {'attempts': attempts, 'metadata': {'http_status': 503}}
                    for name, attempts in (('one', 1), ('two', 100))}
        with patch.dict(os.environ, {'FAKE_RUNNER_FAILURES': json.dumps(failures)}):
            first = experiment.run(self.root / 'E1', only=['one'], max_new_attempts=2,
                                   include_request_preview=False)
            self.assertEqual(first['summary']['valid'], 1)
            self.assertEqual(first['summary']['unrun'], 2)
            self.assertEqual(first['summary']['attempts'], 2)
            with self.assertRaises(RunnerError):
                experiment.run(self.root / 'E1', only=['two'], max_new_attempts=2,
                               include_request_preview=False)
            saved = (self.root / 'E1/run/results.jsonl').read_bytes()
            with self.assertRaises(RunnerError):
                experiment.run(self.root / 'E1', only=['two'], max_new_attempts=2,
                               include_request_preview=False)
            self.assertEqual((self.root / 'E1/run/results.jsonl').read_bytes(), saved)
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 4)

    def test_selected_run_keeps_native_parallel_dispatch_and_concurrency_cap(self):
        self.prepare()
        barrier = self.root / 'barrier'
        barrier.mkdir()
        with patch.dict(os.environ, {'FAKE_RUNNER_BARRIER': str(barrier)}):
            result = experiment.run(self.root / 'E1', only=['one', 'two'], max_concurrency=2,
                                    include_request_preview=False)
        self.assertEqual(result['summary']['valid'], 2)
        self.assertEqual(result['summary']['unrun'], 1)
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 2)
        execution = json.loads((self.root / 'E1/execution-settings.jsonl').read_text())
        self.assertEqual(execution['concurrency'], 2)
        self.assertEqual(execution['prepared_concurrency'], 3)
        self.assertEqual(execution['only'], ['one', 'two'])

    def test_preparation_context_reaches_the_selected_project_adapter(self):
        import sys
        module_name = 'hierarchy_fixture_adapter'
        module_path = self.root / (module_name + '.py')
        module_path.write_text("""from pathlib import Path
from experiment_runner.inputs import *
from experiment_runner.inputs import prepare_file as generic_prepare

def prepare_file(dataset, path, template, load_report=None, *, preparation_context=None):
    cases, sources, preparation = generic_prepare(dataset, path, template, load_report)
    return [{**case, 'evidence': {'context': preparation_context}} for case in cases], sources, preparation

def code_files(dataset):
    return {'hierarchy_fixture_adapter.py': Path(__file__)}

def add_commands(commands):
    pass

def enrich_report(result, plan, cases):
    pass
""")
        (self.root / 'experiment-runner.json').write_text(json.dumps({
            'schema_version': 1, 'inputs': module_name, 'extension': module_name}))
        project.configure(self.root)
        self.addCleanup(sys.modules.pop, module_name, None)
        args = experiment.parser().parse_args([
            'prepare', str(self.root / 'E1'), '--spec', str(self.root / 'spec.json')])
        result = experiment.prepare(args, preparation_context={'fixed_graph': 'before'},
                                    include_request_preview=False)
        self.assertEqual(result['cases'][0]['evidence']['context'], {'fixed_graph': 'before'})

    def test_native_run_reserves_both_stage_and_shared_allowances_before_dispatch(self):
        self.prepare()
        stage = PersistentAttemptBudget(self.root / 'stage.json')
        shared = PersistentAttemptBudget(self.root / 'shared.json')
        stage.initialize(3)
        shared.initialize(1)
        with self.assertRaisesRegex(RunnerError, 'allowance exhausted'):
            experiment.run(
                self.root / 'E1', attempt_budget_path=stage.path,
                additional_attempt_budget_paths=(shared.path,), include_request_preview=False)
        result = experiment.report(self.root / 'E1', include_request_preview=False)
        self.assertEqual(result['summary']['valid'], 1)
        self.assertEqual(result['summary']['unrun'], 2)
        self.assertEqual(json.loads(stage.path.read_text())['used'], 1)
        self.assertEqual(json.loads(shared.path.read_text())['used'], 1)
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 1)
        before = (stage.path.read_bytes(), shared.path.read_bytes())
        with self.assertRaisesRegex(RunnerError, 'allowance exhausted'):
            experiment.run(self.root / 'E1', attempt_budget_path=stage.path,
                additional_attempt_budget_paths=(shared.path,), include_request_preview=False)
        self.assertEqual(before, (stage.path.read_bytes(), shared.path.read_bytes()))
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 1)

    def test_native_run_with_expired_shared_deadline_sends_no_requests(self):
        self.prepare()
        stage = PersistentAttemptBudget(self.root / 'stage.json')
        shared = PersistentAttemptBudget(self.root / 'shared.json')
        stage.initialize(3)
        shared.initialize(3)
        value = json.loads(shared.path.read_text())
        value['deadline_utc'] = '2000-01-01T00:00:00Z'
        shared.path.write_text(json.dumps(value))
        with self.assertRaisesRegex(RunnerError, 'allowance exhausted'):
            experiment.run(self.root / 'E1', attempt_budget_path=stage.path,
                additional_attempt_budget_paths=(shared.path,), include_request_preview=False)
        self.assertFalse((self.root / 'calls.json').exists())
        self.assertEqual(json.loads(stage.path.read_text())['used'], 0)
        self.assertEqual(json.loads(shared.path.read_text())['used'], 0)

    def test_native_cli_inputs_only_returns_exact_inputs_without_answers_or_previews(self):
        from contextlib import redirect_stdout, redirect_stderr
        from io import StringIO
        self.prepare()
        experiment.run(self.root / 'E1', include_request_preview=False)
        before = (self.root / 'calls.json').read_bytes()
        plan = json.loads((self.root / 'E1/experiment.json').read_text())['plan']
        exact = [{key: case[key] for key in ('id', 'inputs', 'input_sha256')}
                 for case in plan['cases']]
        def show(*options):
            stdout, stderr = StringIO(), StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr), patch.object(
                    experiment, 'report', side_effect=AssertionError('answer report invoked')):
                status = experiment.main(['show', str(self.root / 'E1'), '--inputs-only', *options])
            return status, stdout.getvalue(), stderr.getvalue()
        status, output, error = show('--json')
        self.assertEqual(status, 0, error)
        self.assertEqual(json.loads(output), {'cases': exact})
        status, output, error = show('--case', 'two')
        self.assertEqual(status, 0, error)
        self.assertEqual(json.loads(output), exact[1])
        status, output, error = show('--only', 'three', '--only', 'one')
        self.assertEqual(status, 0, error)
        self.assertEqual(json.loads(output), {'cases': [exact[0], exact[2]]})
        status, output, error = show('--case', 'one', '--only', 'three')
        self.assertEqual(status, 0, error)
        self.assertEqual(json.loads(output), {'cases': [exact[0], exact[2]]})
        for options in (('--case', 'missing'), ('--note', 'one'),
                        ('--generation-costs',), ('--case', 'one', '--prompt-only')):
            status, output, error = show(*options)
            self.assertEqual(status, 1, (options, output, error))
            self.assertEqual(output, '')
        self.assertEqual((self.root / 'calls.json').read_bytes(), before)

    def test_native_accounting_reports_known_subtotals_and_deduplicated_reuse_costs(self):
        self.prepare()
        with patch.dict(os.environ, {'FAKE_RUNNER_RESULT_OVERRIDE': json.dumps({
                'usage': {'input': 100, 'output': 20, 'cost': {'total': 0.25}}})}):
            experiment.run(self.root / 'E1', include_request_preview=False)
        original = experiment.report(self.root / 'E1', generation_costs=True,
                                     include_request_preview=False)
        self.assertEqual(original['usage']['known_catalog_cost_estimate_usd'], 0.75)
        self.assertEqual(original['usage']['catalog_cost_estimate_usd'], 0.75)
        self.assertEqual(original['usage']['known_provider_reported_cost_usd'], 0)
        self.assertIsNone(original['usage']['provider_reported_cost_usd'])
        for name in ('reuse_one', 'reuse_two'):
            args = experiment.parser().parse_args([
                'prepare', str(self.root / name), '--from', str(self.root / 'E1')])
            experiment.prepare(args, include_request_preview=False)
            experiment.run(self.root / name, include_request_preview=False)
            reused = experiment.report(self.root / name, generation_costs=True,
                                       include_request_preview=False)
            self.assertEqual(reused['summary']['reused'], 3)
            self.assertEqual(reused['summary']['attempts'], 0)
            self.assertEqual(reused['usage']['known_catalog_cost_estimate_usd'], 0)
            self.assertEqual(reused['generation_usage']['original_calls'], 3)
            self.assertEqual(reused['generation_usage']['source_calls_added'], 3)
            self.assertEqual(reused['generation_usage']['known_catalog_cost_estimate_usd'], 0.75)
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 3)

    def test_native_accounting_keeps_failed_sdk_zero_placeholders_unknown(self):
        self.prepare()
        with patch.dict(os.environ, {
            'FAKE_RUNNER_RESULT_OVERRIDE': json.dumps({'usage': {
                'input': 0, 'output': 0, 'cacheRead': 0, 'cacheWrite': 0,
                'reasoning': 0, 'cost': {'total': 0}}}),
            'FAKE_RUNNER_FAILURES': json.dumps({'one': {
                'metadata': {'http_status': 400, 'error': {'code': 'invalid_request'}}}}),
        }), self.assertRaises(RunnerError):
            experiment.run(self.root / 'E1', include_request_preview=False)
        report = experiment.report(self.root / 'E1', generation_costs=True,
                                   include_request_preview=False)
        for usage in (report['usage'], report['generation_usage']):
            self.assertEqual(usage['attempts_without_usage'], 1)
            self.assertEqual(usage['attempts_without_cost_estimate'], 1)
            self.assertEqual(usage['known_catalog_cost_estimate_usd'], 0)
            self.assertIsNone(usage['catalog_cost_estimate_usd'])
        self.assertEqual(report['summary']['failed'], 1)
        self.assertEqual(report['summary']['valid'], 2)

    def test_secondary_budget_requires_primary_before_any_dispatch(self):
        self.prepare()
        budget = self.root / 'secondary.json'
        budget.write_text(json.dumps(dict(schema_version=1, limit=3, used=0)))
        with self.assertRaisesRegex(RunnerError, 'primary|attempt_budget_path'):
            experiment.run(self.root / 'E1', additional_attempt_budget_paths=(budget,),
                           include_request_preview=False)
        self.assertFalse((self.root / 'calls.json').exists())
        self.assertEqual(json.loads(budget.read_text())['used'], 0)


class PersistentHierarchyBudgetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_initialize_and_remaining_preserve_usage_deadline_and_claims(self):
        path = self.root / 'nested/budget.json'
        budget = PersistentAttemptBudget(path)
        budget.initialize(4)
        budget.claim('first')
        value = json.loads(path.read_text())
        value['deadline_utc'] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        path.write_text(json.dumps(value, indent=2))
        before = path.read_bytes()
        budget.initialize(4)
        self.assertEqual(before, path.read_bytes())
        self.assertEqual(budget.remaining(), 3)
        with self.assertRaisesRegex(RunnerError, 'limit'):
            budget.initialize(5)
        value['used'] = 0
        path.write_text(json.dumps(value))
        with self.assertRaisesRegex(RunnerError, 'rollback|claims'):
            budget.initialize(4)
        path.unlink()
        with self.assertRaisesRegex(RunnerError, 'claims'):
            budget.initialize(4)
        self.assertFalse(path.exists())

    def test_all_counters_validate_before_admission_and_earliest_deadline_blocks_launch(self):
        first = self.root / 'first.json'
        second = self.root / 'second.json'
        PersistentAttemptBudget(first).initialize(3)
        PersistentAttemptBudget(second).initialize(2)
        budget = PersistentAttemptBudget(first, additional_paths=(second, first))
        budget.claim('first')
        self.assertEqual(budget.remaining(), 1)
        value = json.loads(second.read_text())
        value['deadline_utc'] = '2000-01-01T00:00:00Z'
        second.write_text(json.dumps(value))
        before = (first.read_bytes(), second.read_bytes())
        self.assertEqual(budget.remaining(), 0)
        with self.assertRaises(NewJobLimitReached):
            budget.claim('late')
        self.assertEqual(before, (first.read_bytes(), second.read_bytes()))
        first.write_text(json.dumps(dict(schema_version=1, limit=3, used=3)))
        second.write_text(json.dumps(dict(schema_version=1, limit=2, used=True)))
        with self.assertRaises(RunnerError):
            budget.claim('invalid')

    def test_concurrent_initialization_and_multi_counter_claims_never_reset_caps(self):
        primary = self.root / 'primary.json'
        secondary = self.root / 'secondary.json'
        PersistentAttemptBudget(secondary).initialize(7)
        def claim(index):
            budget = PersistentAttemptBudget(primary, additional_paths=(secondary,))
            budget.initialize(12)
            try:
                budget.claim(str(index))
                return True
            except NewJobLimitReached:
                return False
        with ThreadPoolExecutor(max_workers=8) as pool:
            accepted = list(pool.map(claim, range(30)))
        self.assertEqual(sum(accepted), 7)
        for path in (primary, secondary):
            self.assertEqual(json.loads(path.read_text())['used'], 7)
            rows = [json.loads(line) for line in path.with_suffix('.json.claims.jsonl').read_text().splitlines()]
            self.assertEqual([row['ordinal'] for row in rows], list(range(1, 8)))


if __name__ == '__main__':
    unittest.main()
