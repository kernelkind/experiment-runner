"""Verify moved evidence through the public CLI, without the original runtime."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin/experiment-runner'


class VerifyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.original = self.root / 'original'
        self.original.mkdir()
        self.extracted = self.root / 'extracted'
        self.environment = {**os.environ,
            'ORACLE_EXPERIMENT_BACKOFF_DIRECTORY': str(self.root / 'backoff'),
            'FAKE_RUNNER_LOG': str(self.root / 'calls.json'),
            'FAKE_RUNNER_PROMPT_RESPONSE': '{"answer":"fixture"}'}
        shutil.copy2(ROOT / 'tests/fixtures/fake-runner.py', self.original / 'runner')
        (self.original / 'prompt.txt').write_text('GENERIC PROMPT\n{TASK}')
        (self.original / 'cases.json').write_text(json.dumps({'cases': [
            {'id': 'one', 'inputs': {'TASK': 'Data'}}]}))
        (self.original / 'spec.json').write_text(json.dumps({
            'schema_version': 1, 'prompt': {'id': 'P1', 'path': 'prompt.txt'},
            'dataset': {'adapter': 'generic', 'path': 'cases.json'},
            'runner': {'provider': 'local', 'model': 'fixture/model',
                       'reasoning_effort': 'off', 'transport': 'http',
                       'runner_binary': str(self.original / 'runner'),
                       'output_schema': {'type': 'object'}}}))
        for arguments in (('prepare', 'source', '--spec', 'spec.json'), ('run', 'source'),
                          ('prepare', 'reuse', '--from', 'source'), ('run', 'reuse'),
                          ('prepare', 'unrun', '--from', 'source'),
                          ('prepare', 'failed', '--from', 'source', '--repeat')):
            result = self.cli(*arguments, cwd=self.original)
            self.assertEqual(result.returncode, 0, result.stderr)
        self.environment['FAKE_RUNNER_FAIL_STAGE'] = 'prompt'
        result = self.cli('run', 'failed', '--max-new-attempts', '1', cwd=self.original)
        self.assertEqual(result.returncode, 1, result.stderr)
        del self.environment['FAKE_RUNNER_FAIL_STAGE']
        shutil.move(self.original, self.extracted)
        # The verifier must not require an executable or credentials.
        (self.extracted / 'runner').unlink()
        self.locations = self.root / 'locations.json'
        self.locations.write_text(json.dumps({'schema_version': 1,
            'roots': {str(self.original): 'extracted'}, 'files': {}}))

    def cli(self, *arguments, cwd=None):
        return subprocess.run([str(CLI), *map(str, arguments)],
            cwd=cwd or self.root, env=self.environment,
            capture_output=True, text=True, timeout=30)

    def snapshot(self):
        return {str(p.relative_to(self.extracted)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.extracted.rglob('*') if p.is_file()}

    def test_verifies_moved_original_and_reused_receipts_without_writes_or_execution(self):
        before = self.snapshot()
        calls = (self.root / 'calls.json').read_bytes()
        for name in ('source', 'reuse'):
            result = self.cli('verify', self.extracted / name,
                              '--locations', self.locations, '--json')
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report['cases'], {'succeeded': 1, 'failed': 0, 'unrun': 0, 'mechanical': 0})
            self.assertEqual(report['verified_receipts'], 1)
            self.assertEqual(report['reused_receipts'], int(name == 'reuse'))
        self.assertEqual(self.snapshot(), before)
        self.assertEqual((self.root / 'calls.json').read_bytes(), calls)

    def verify(self, name='reuse'):
        return self.cli('verify', self.extracted / name, '--locations', self.locations, '--json')

    def test_failed_and_unrun_are_not_counted_as_verified_receipts(self):
        for name in ('failed', 'unrun'):
            result = self.verify(name)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report['cases'][name], 1)
            self.assertEqual(report['verified_receipts'], 0)
            self.assertEqual(report['failed_attempts'], int(name == 'failed'))

    def test_checks_sources_and_producer_and_prepared_file_hashes(self):
        for path, expected in (
            (self.extracted / 'cases.json', 'source hash differs'),
            (self.extracted / 'source/run/results.jsonl', 'source receipt hash differs'),
            (self.extracted / 'reuse/runner-config.json', 'prepared file hash differs'),
            (self.extracted / 'reuse/run/manifest.json', 'manifest hash differs'),
        ):
            with self.subTest(path=path):
                before = path.read_bytes()
                value = json.loads(before)
                value['changed'] = True
                path.write_text(json.dumps(value) + '\n')
                result = self.verify()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, result.stderr)
                path.write_bytes(before)

    def test_missing_extracted_file_never_falls_back_to_existing_original(self):
        shutil.copytree(self.extracted, self.original)
        (self.extracted / 'cases.json').unlink()
        result = self.verify()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('extracted/cases.json', result.stderr)

    def test_exact_file_override_supports_deduplicated_corpora(self):
        (self.root / 'datasets').mkdir()
        shutil.move(self.extracted / 'cases.json', self.root / 'datasets/input.json')
        locations = json.loads(self.locations.read_text())
        locations['files'][str(self.original / 'cases.json')] = 'datasets/input.json'
        self.locations.write_text(json.dumps(locations))
        result = self.verify()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_mapping_is_reported_even_when_original_exists(self):
        shutil.copytree(self.extracted, self.original)
        self.locations.write_text(json.dumps({'schema_version': 1, 'roots': {}, 'files': {}}))
        result = self.verify()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('no extracted location', result.stderr)

    def test_duplicate_mapping_keys_are_rejected(self):
        self.locations.write_text('{"schema_version":1,"roots":{},"files":{},"files":{}}')
        result = self.verify()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('duplicate location key', result.stderr)

    def test_no_mapping_is_needed_for_files_still_at_original_locations(self):
        shutil.move(self.extracted, self.original)
        result = self.cli('verify', self.original / 'reuse')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Successful receipts: 1 (reused: 1)', result.stdout)

    def test_relocated_wrapper_upgrade_checks_the_saved_results_prefix(self):
        from experiment_runner import extraction_pipeline as engine
        run = self.extracted / 'source/run'
        manifest = json.loads((run / 'manifest.json').read_text())
        manifest['wrapper_sha256'] = 'f' * 64
        prepared = engine._prepare_manifest_directory(run, manifest,
            runner_upgrade_reason='Fixture workflow update')
        engine._append_runner_upgrade(run, prepared)
        result = self.verify('source')
        self.assertEqual(result.returncode, 0, result.stderr)
        history = run / 'runner-upgrades.jsonl'
        row = json.loads(history.read_text())
        row['results_prefix_sha256'] = '0' * 64
        history.write_text(json.dumps(row) + '\n')
        result = self.verify('source')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('does not match its results prefix', result.stderr)

    def test_saved_auth_path_is_identity_and_does_not_require_credentials(self):
        worker = self.extracted / 'runner'
        shutil.copy2(ROOT / 'tests/fixtures/fake-runner.py', worker)
        auth = self.root / 'fixture-auth.json'
        auth.write_text('{}')
        spec = json.loads((self.extracted / 'spec.json').read_text())
        spec['runner'].update(provider='openai-codex', transport='sse',
                              auth_file=str(auth), runner_binary=str(worker))
        (self.extracted / 'subscription-spec.json').write_text(json.dumps(spec))
        for args in (('prepare', 'subscription', '--spec', 'subscription-spec.json'),
                     ('run', 'subscription')):
            result = self.cli(*args, cwd=self.extracted)
            self.assertEqual(result.returncode, 0, result.stderr)
        auth.unlink()
        worker.unlink()
        locations = json.loads(self.locations.read_text())
        locations['roots'][str(self.extracted)] = 'extracted'
        self.locations.write_text(json.dumps(locations))
        before = self.snapshot()
        calls = (self.root / 'calls.json').read_bytes()
        result = self.verify('subscription')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual((self.root / 'calls.json').read_bytes(), calls)


if __name__ == '__main__':
    unittest.main()
