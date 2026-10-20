"""Compact prompt results through saved snapshots and the real offline CLI."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

from experiment_runner import extraction_pipeline as engine
from experiment_runner.prompt_publication import export_results


ROOT = Path(__file__).resolve().parents[1]


class PromptPublicationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir='/dev/shm' if Path('/dev/shm').is_dir() else None)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def save(self, relative, value):
        path = self.root/relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False)+'\n')
        return path

    def saved(self, role='standalone'):
        variables = {'TASK': 'Tea 🌿'}
        input_hash = engine._sha256_json(variables)
        plan = dict(prompt=dict(id='Prompt-fixture-v1', text='Task: {TASK}', source_path='/private/prompt'),
                    dataset=dict(adapter='generic', path='/private/inputs'),
                    runner=dict(provider='fixture', model='fixture/model', auth_file='/private/auth',
                                api_key='SECRET', prompt_cache_prefix='PRIVATE PREFIX',
                                output_schema={'type': 'object'}),
                    sources={'/private/inputs': 'ab'*32}, code={'experiment.py': 'cd'*32},
                    manifest_sha256='ef'*32, request_reuse={'intentional_repeat': True},
                    cases=[dict(id=name, input_sha256=input_hash,
                                prompt='RENDERED REQUEST', inputs=variables)
                           for name in ('first', 'reused', 'failed')])
        if role is not None:
            plan['publication_role'] = role
        self.save('saved/experiment.json', dict(sha256=engine._sha256_json(plan), plan=plan))
        reviews = [dict(role='agent', verdict='reject', comment='Recorded assessment.', current=True,
                        input_sha256=input_hash, output_sha256=engine._sha256_json('{"ok":true}'),
                        imported_from={'experiment': '/private/source'}),
                   dict(role='user', verdict='comment', comment='My comment.', current=True,
                        source='/private/ui-comments.json')]
        cases = [dict(id='first', inputs=variables, input_sha256=input_hash, status='valid',
                      output='{"ok":true}', reused=False, reviews=reviews,
                      prompt='RENDERED REQUEST', evidence={'private_path': '/private/context'},
                      baseline={'output': 'BASELINE ANSWER'}, receipt={'record_sha256': '01'*32,
                                                                    'results_path': '/private/results'}),
                 dict(id='reused', inputs=variables, input_sha256=input_hash, status='valid',
                      output='{"ok":true}', reused=True, request_sha256='02'*32,
                      reused_from={'result_line': 7, 'result_sha256': '03'*32,
                                   'manifest_sha256': '04'*32, 'results_path': '/private/original'}),
                 dict(id='failed', inputs=variables, input_sha256=input_hash, status='failed',
                      output='partial answer', error='SECRET /private/provider', failure_kind='provider_fatal')]
        report = dict(prompt_id='Prompt-fixture-v1', adapter='generic', imported=False,
                      settings=plan['runner'], cases=cases, summary={'cases': 3})
        self.save('saved/report.json', report)
        return self.root/'saved', plan, report

    def published(self, directory):
        output = directory/'publication'
        return (json.loads((output/'experiment.json').read_text()),
                [json.loads(line) for line in (output/'inputs.jsonl').read_text().splitlines()],
                [json.loads(line) for line in (output/'results.jsonl').read_text().splitlines()])

    def test_saved_snapshot_preserves_answers_reviews_reuse_and_deduplicated_inputs(self):
        directory, plan, _ = self.saved()
        export_results(directory)
        metadata, inputs, rows = self.published(directory)
        self.assertEqual(metadata['source_id'], engine._sha256_json(plan))
        self.assertEqual(metadata['request_reuse']['intentional_repeat'], True)
        self.assertEqual(metadata['snapshot']['sidecar_freshness'], 'not_checked')
        self.assertEqual(metadata['snapshot']['receipt_validation'], 'not_performed')
        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0]['inputs'], {'TASK': 'Tea 🌿'})
        self.assertEqual([row['case_id'] for row in rows], ['first', 'reused', 'failed'])
        self.assertEqual(rows[2]['output'], 'partial answer')
        self.assertEqual(rows[2]['failure_kind'], 'provider_fatal')
        self.assertEqual([review['role'] for review in rows[0]['reviews']], ['agent', 'user'])
        self.assertEqual(rows[0]['reviews'][1]['verdict'], 'comment')
        self.assertTrue(rows[1]['reused'])
        self.assertEqual(rows[1]['reused_from']['result_sha256'], '03'*32)
        raw = b''.join(path.read_bytes() for path in (directory/'publication').iterdir())
        for omitted in (b'/private/', b'SECRET', b'RENDERED REQUEST', b'BASELINE ANSWER', b'PRIVATE PREFIX'):
            self.assertNotIn(omitted, raw)
        for name, binding in metadata['files'].items():
            self.assertEqual(hashlib.sha256((directory/'publication'/name).read_bytes()).hexdigest(),
                             binding['sha256'])

    def test_unchanged_snapshot_preserves_output_bytes_and_mtimes(self):
        directory, _, _ = self.saved()
        export_results(directory)
        before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in (directory/'publication').iterdir()}
        export_results(directory)
        self.assertEqual(before,
            {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in (directory/'publication').iterdir()})

    def test_output_schema_preserves_its_request_significant_property_order(self):
        directory, plan, report = self.saved()
        schema = {'type': 'object', 'properties': {'z': {'type': 'string'}, 'a': {'type': 'number'}},
                  'required': ['z', 'a']}
        plan['runner']['output_schema'] = schema
        report['settings']['output_schema'] = schema
        self.save('saved/experiment.json', dict(sha256=engine._sha256_json(plan), plan=plan))
        self.save('saved/report.json', report)

        export_results(directory)

        metadata, _, _ = self.published(directory)
        encoded = metadata['settings']['output_schema_json']
        self.assertEqual(list(json.loads(encoded)['properties']), ['z', 'a'])
        self.assertEqual(encoded.encode(), engine._ordered_json_bytes(schema))
        self.assertEqual(metadata['settings']['output_schema_sha256'], hashlib.sha256(encoded.encode()).hexdigest())

    def test_internal_and_unmarked_jobs_never_export_automatically(self):
        for role in ('internal', None):
            with self.subTest(role=role):
                directory, _, _ = self.saved(role)
                self.assertIsNone(export_results(directory, automatic=True))
                self.assertFalse((directory/'publication').exists())
                if role == 'internal':
                    with self.assertRaisesRegex(ValueError, 'internal'):
                        export_results(directory)
                else:
                    export_results(directory)

    def test_missing_report_is_explicit_and_does_not_invent_case_outcomes(self):
        directory, _, _ = self.saved()
        (directory/'report.json').unlink()
        with self.assertRaisesRegex(ValueError, 'report.json'):
            export_results(directory)
        self.assertFalse((directory/'publication').exists())

    def test_same_prompt_report_with_different_input_selection_is_refused(self):
        directory, _, report = self.saved()
        export_results(directory)
        before = {p.name:p.read_bytes() for p in (directory/'publication').iterdir()}
        report['cases'][0]['inputs'] = {'TASK': 'Different saved input'}
        report['cases'][0]['input_sha256'] = engine._sha256_json(report['cases'][0]['inputs'])
        self.save('saved/report.json', report)

        with self.assertRaisesRegex(ValueError, 'input selection'):
            export_results(directory)

        self.assertEqual(before, {p.name:p.read_bytes() for p in (directory/'publication').iterdir()})

    def test_legacy_repeat_and_worker_identity_come_from_local_manifest_only(self):
        directory, plan, _ = self.saved(None)
        plan.pop('request_reuse')
        self.save('saved/experiment.json', dict(sha256=engine._sha256_json(plan), plan=plan))
        manifest = dict(request_reuse={'intentional_repeat': True, 'history_root': '/private/history'},
                        runner_version='fixture-worker-v1', runner_sha256='05'*32,
                        prompts=[{'text': 'RENDERED REQUEST'}], auth_file='/private/auth')
        self.save('saved/run/manifest.json', manifest)

        export_results(directory)

        metadata, _, _ = self.published(directory)
        self.assertTrue(metadata['request_reuse']['intentional_repeat'])
        self.assertEqual(metadata['code']['model_runner']['runner_sha256'], '05'*32)
        self.assertNotIn('/private/', json.dumps(metadata))

    def test_offline_projection_skips_large_case_evidence_before_materialization(self):
        directory, plan, report = self.saved()
        plan['cases'] = plan['cases'][:1]
        self.save('saved/experiment.json', dict(sha256=engine._sha256_json(plan), plan=plan))
        case = report['cases'][0]
        case.pop('evidence')
        path = directory/'report.json'
        with path.open('wb') as stream:
            stream.write(b'{"prompt_id":"Prompt-fixture-v1","cases":[')
            stream.write(json.dumps(case).encode()[:-1]+b',"evidence":[')
            chunk = b'"'+b'x'*1024+b'",'
            for _ in range(16384):
                stream.write(chunk)
            stream.write(b'null]}]} \n\t')
        expected = hashlib.sha256(path.read_bytes()).hexdigest()
        tracemalloc.start()
        try:
            export_results(directory)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        metadata, _, rows = self.published(directory)
        self.assertEqual(metadata['snapshot']['report_sha256'], expected)
        self.assertEqual(len(rows), 1)
        self.assertLess(peak, 8*1024*1024, 'export materialized discarded case evidence')


class PromptPublicationLifecycleTests(unittest.TestCase):
    setUp = PromptPublicationTests.setUp
    save = PromptPublicationTests.save
    published = PromptPublicationTests.published

    def configure(self):
        (self.root/'prompt.txt').write_text('GENERIC PROMPT {TASK}')
        self.save('cases.json', {'cases': [dict(id='one', inputs={'TASK': 'Tea'})]})
        self.save('spec.json', dict(schema_version=1, prompt=dict(id='P1', path='prompt.txt'),
            dataset=dict(adapter='generic', path='cases.json'),
            runner=dict(provider='openai-codex', model='fixture/model', reasoning_effort='low',
                        runner_binary=str(ROOT/'tests/fixtures/fake-runner.py'),
                        output_schema={'type': 'object'})))

    def cli(self, *arguments, ok=True, env=None):
        environment = {**os.environ, 'EXPERIMENT_RUNNER_ROOT': str(ROOT),
            'ORACLE_EXPERIMENT_BACKOFF_DIRECTORY': str(self.root/'admission'),
            'FAKE_RUNNER_LOG': str(self.root/'calls.json'),
            'FAKE_RUNNER_PROMPT_RESPONSE': '{"answer":"fixture"}',
            'PATH': str(Path(sys.executable).parent)+os.pathsep+os.environ.get('PATH', '')}
        environment.update(env or {})
        result = subprocess.run([sys.executable, str(ROOT/'bin/experiment-runner'), *map(str, arguments)],
            cwd=self.root, env=environment, text=True, capture_output=True, timeout=30)
        self.assertEqual(result.returncode == 0, ok, result.stdout+result.stderr)
        return result

    def plan(self, name):
        return json.loads((self.root/name/'experiment.json').read_text())['plan']

    def test_prepare_run_and_review_refresh_public_results_and_repeat_identity(self):
        self.configure()
        self.cli('prepare', 'trial', '--spec', 'spec.json')
        self.assertEqual(self.plan('trial')['publication_role'], 'standalone')
        self.assertEqual(self.published(self.root/'trial')[2][0]['status'], 'unrun')
        self.cli('run', 'trial')
        self.assertEqual(self.published(self.root/'trial')[2][0]['status'], 'valid')
        self.cli('review', 'trial', '--case', 'one', '--role', 'user', '--verdict', 'accept',
                 '--comment', 'Recorded user decision.')
        self.assertEqual(self.published(self.root/'trial')[2][0]['reviews'][0]['role'], 'user')
        self.cli('prepare', 'repeat', '--from', 'trial', '--repeat')
        self.assertEqual(self.plan('repeat')['publication_role'], 'standalone')
        original = self.published(self.root/'trial')[0]
        repeated = self.published(self.root/'repeat')[0]
        self.assertNotEqual(original['source_id'], repeated['source_id'])
        self.assertTrue(repeated['request_reuse']['intentional_repeat'])
        self.cli('export-results', 'trial', '--output', 'offline')
        self.assertTrue((self.root/'offline/results.jsonl').exists())

    def test_internal_role_and_unmarked_legacy_role_are_inherited(self):
        self.configure()
        self.cli('prepare', 'internal', '--spec', 'spec.json', '--publication-role', 'internal')
        self.cli('run', 'internal')
        self.assertFalse((self.root/'internal/publication').exists())
        self.cli('prepare', 'child', '--from', 'internal')
        self.assertEqual(self.plan('child')['publication_role'], 'internal')
        self.assertFalse((self.root/'child/publication').exists())
        path = self.root/'internal/experiment.json'
        envelope = json.loads(path.read_text())
        envelope['plan'].pop('publication_role')
        envelope['sha256'] = engine._sha256_json(envelope['plan'])
        path.write_text(json.dumps(envelope))
        self.cli('prepare', 'legacy-child', '--from', 'internal')
        self.assertNotIn('publication_role', self.plan('legacy-child'))
        self.assertFalse((self.root/'legacy-child/publication').exists())
        self.cli('prepare', 'explicit', '--from', 'legacy-child', '--publication-role', 'standalone')
        self.assertTrue((self.root/'explicit/publication').exists())

    def test_failed_call_still_refreshes_final_public_case_status(self):
        self.configure()
        self.cli('prepare', 'failed', '--spec', 'spec.json')

        self.cli('run', 'failed', ok=False, env={'FAKE_RUNNER_FAIL_STAGE': 'prompt'})

        self.assertEqual(self.published(self.root/'failed')[2][0]['status'], 'failed')

    def test_publication_failure_preserves_the_primary_run_error_or_interruption(self):
        from experiment_runner import experiment, project
        self.configure()
        previous_root = project.root()
        self.addCleanup(project.configure, previous_root)
        project.configure(self.root)
        for index, failure in enumerate((engine.RunnerError('original worker failure'),
                                         KeyboardInterrupt(), RuntimeError('original failure'))):
            with self.subTest(failure=type(failure).__name__):
                name = f'interrupted-{index}'
                self.cli('prepare', name, '--spec', 'spec.json')
                directory = self.root/name
                (directory/'publication/unrelated.txt').write_text('Unrelated evidence')
                with patch.dict(os.environ, {
                        'ORACLE_EXPERIMENT_BACKOFF_DIRECTORY': str(self.root/'admission')}):
                    with patch.object(engine, '_execute_prompt_batch', side_effect=failure) as execute:
                        with self.assertRaises(type(failure)) as caught:
                            experiment.run(directory, warm_cache=False,
                                           include_request_preview=False)
                execute.assert_called_once()
                original = caught.exception.__cause__ if isinstance(failure, engine.RunnerError) else caught.exception
                self.assertIs(original, failure)
                self.assertTrue(any('ValueError' in note and 'publication' in note.lower()
                                    for note in getattr(original, '__notes__', ())))
                self.assertTrue((directory/'report.json').is_file())
                self.assertEqual((directory/'publication/unrelated.txt').read_text(), 'Unrelated evidence')

    def test_cache_warmup_is_internal_even_with_an_explicit_standalone_request(self):
        self.configure()
        (self.root/'prompt.txt').write_text('GENERIC PROMPT'+' word'*1100+' {TASK}')
        spec = json.loads((self.root/'spec.json').read_text())
        spec['runner']['model'] = 'gpt-6.1-sol'
        self.save('spec.json', spec)
        self.cli('prepare', 'cached', '--spec', 'spec.json', '--prompt-cache')

        self.cli('prepare', 'warmup', '--from', 'cached', '--cache-warmup',
                 '--publication-role', 'standalone')

        self.assertEqual(self.plan('warmup')['publication_role'], 'internal')
        self.assertFalse((self.root/'warmup/publication').exists())


if __name__ == '__main__':
    unittest.main()
