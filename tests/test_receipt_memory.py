"""Memory bounds and immutable bytes on the production receipt paths."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

from experiment_runner import experiment
from experiment_runner import compatibility
from experiment_runner import extraction_pipeline as engine


class ReceiptMemoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_remember_streams_large_ledger_and_keeps_first_exact_line(self):
        path = self.root / 'results.jsonl'
        record = {'manifest_sha256': 'a' * 64, 'job_key': 'target'}
        expected = engine._canonical_json_bytes(record)
        with path.open('wb') as stream:
            for number in range(32):
                stream.write(engine._canonical_json_bytes({'padding': 'x' * (256 * 1024)}))
            stream.write(expected)
            stream.write(expected)
        history = engine.RequestHistory(self.root, discover=False)
        tracemalloc.start()
        try:
            with history.lock('target'):
                history.remember('target', path, record)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        reference = json.loads((history.directory / 'target.json').read_text())
        self.assertEqual(reference['result_line'], 33)
        self.assertEqual(reference['result_sha256'], engine._sha256_bytes(expected))
        self.assertEqual(engine.ReceiptFiles().line(path, 33), expected)
        self.assertLess(peak, 2 * 1024 * 1024, 'remember copied the whole ledger')

    def test_concurrent_appends_remember_exact_receipt_hash_and_line(self):
        path = self.root / 'results.jsonl'
        history = engine.RequestHistory(self.root, discover=False)

        def append(number):
            key = str(number)
            record = {'manifest_sha256': 'a' * 64, 'job_key': key, 'output': 'λ' * 4096}
            with history.lock(key):
                engine._append_json_line(path, record, receipt_files=history.files)
                history.remember(key, path, record)
            return key, record

        with ThreadPoolExecutor(max_workers=30) as pool:
            appended = list(pool.map(append, range(60)))
        lines = path.read_bytes().splitlines(keepends=True)
        for key, record in appended:
            reference = json.loads((history.directory / (key + '.json')).read_text())
            raw = lines[reference['result_line'] - 1]
            self.assertEqual(raw, engine._canonical_json_bytes(record))
            self.assertEqual(reference['result_sha256'], engine._sha256_bytes(raw))

    def test_first_match_index_rebuilds_after_unowned_file_changes(self):
        path = self.root / 'results.jsonl'
        history = engine.RequestHistory(self.root, discover=False)
        first = {'manifest_sha256': 'a' * 64, 'job_key': 'one'}
        second = {'manifest_sha256': 'a' * 64, 'job_key': 'two'}
        first_raw, second_raw = map(engine._canonical_json_bytes, (first, second))

        def remember(number):
            key = str(number)
            with history.lock(key):
                engine._append_json_line(path, second, receipt_files=history.files)
                history.remember(key, path, second)
            return json.loads((history.directory / (key + '.json')).read_text())

        engine._append_json_line(path, first, receipt_files=history.files)
        self.assertEqual(remember(0)['result_line'], 2)
        self.assertEqual(remember(1)['result_line'], 2, 'duplicate lost its first line')
        path.write_bytes(second_raw + first_raw)
        self.assertEqual(remember(2)['result_line'], 1, 'same-size rewrite reused stale offsets')
        replacement = self.root / 'replacement.jsonl'
        replacement.write_bytes(first_raw)
        replacement.replace(path)
        self.assertEqual(remember(3)['result_line'], 2, 'replacement reused stale offsets')
        with path.open('r+b') as stream:
            stream.truncate(0)
        self.assertEqual(remember(4)['result_line'], 1, 'truncation reused stale offsets')
        engine._append_json_line(path, first)
        self.assertEqual(remember(5)['result_line'], 1, 'unowned append reused stale offsets')
        with history.lock('first'):
            history.remember('first', path, first)
        self.assertEqual(json.loads((history.directory / 'first.json').read_text())['result_line'], 2)

    def test_large_file_digest_streams_once_without_parsing_json(self):
        path = self.root / 'large.json'
        path.write_text(json.dumps({'padding': 'x' * (17 * 1024 * 1024)}))
        expected = engine._sha256_file(path)
        files = engine.ReceiptFiles()
        tracemalloc.start()
        try:
            with patch.object(engine.json, 'loads', side_effect=AssertionError('digest parsed JSON')), \
                    patch.object(engine, '_sha256_file', wraps=engine._sha256_file) as hashed:
                for _ in range(5):
                    self.assertEqual(files.sha256(path), expected)
                self.assertEqual(hashed.call_count, 1)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 3 * 1024 * 1024, 'digest retained the whole source file')

    def test_digest_cache_invalidates_after_rewrite_and_replacement(self):
        path = self.root / 'source.json'
        path.write_bytes(b'{"value":"old"}\n')
        files = engine.ReceiptFiles()
        before = files.sha256(path)
        stamp = path.stat()
        path.write_bytes(b'{"value":"new"}\n')
        os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns + 1_000_000))
        self.assertNotEqual(files.sha256(path), before)
        self.assertEqual(files.sha256(path), engine._sha256_file(path))
        replacement = self.root / 'replacement.json'
        replacement.write_bytes(b'{"value":"old"}\n')
        replacement.replace(path)
        self.assertEqual(files.sha256(path), before)

    def test_digest_rejects_source_changed_during_hashing_and_recovers(self):
        path = self.root / 'source.json'
        path.write_bytes(b'{"value":"old"}\n')
        files = engine.ReceiptFiles()
        hash_file = engine._sha256_file

        def changed(source):
            digest = hash_file(source)
            source.write_bytes(b'{"value":"changed"}\n')
            return digest

        with patch.object(engine, '_sha256_file', side_effect=changed):
            with self.assertRaisesRegex(engine.RunnerError, 'source changed while hashing'):
                files.sha256(path)
        self.assertEqual(files.sha256(path), hash_file(path))

    def test_verified_json_read_populates_digest_even_when_large_json_is_evicted(self):
        path = self.root / 'large.json'
        path.write_text(json.dumps({'padding': 'x' * (17 * 1024 * 1024)}))
        files = engine.ReceiptFiles()
        self.assertEqual(len(files.json(path)['padding']), 17 * 1024 * 1024)
        expected = engine._sha256_file(path)
        with patch.object(engine, '_sha256_file', side_effect=AssertionError('verified JSON was hashed again')), \
                patch.object(engine.json, 'loads', side_effect=AssertionError('verified JSON was parsed again')):
            self.assertEqual(files.sha256(path), expected)

    def test_first_match_index_checks_exact_bytes_even_for_hash_collisions(self):
        path = self.root / 'results.jsonl'
        history = engine.RequestHistory(self.root, discover=False)
        first = {'manifest_sha256': 'a' * 64, 'job_key': 'one'}
        second = {'manifest_sha256': 'a' * 64, 'job_key': 'two'}
        with patch.object(engine, '_sha256_bytes', return_value='c' * 64):
            for record in (first, second, second):
                engine._append_json_line(path, record, receipt_files=history.files)
            with history.lock('second'):
                history.remember('second', path, second)
        self.assertEqual(json.loads((history.directory / 'second.json').read_text())['result_line'], 2)

    def test_unterminated_tail_does_not_create_a_false_appended_line(self):
        path = self.root / 'results.jsonl'
        path.write_bytes(b'partial')
        history = engine.RequestHistory(self.root, discover=False)
        record = {'manifest_sha256': 'a' * 64, 'job_key': 'one'}
        engine._append_json_line(path, record, receipt_files=history.files)
        with history.lock('missing'):
            with self.assertRaises(StopIteration):
                history.remember('missing', path, record)
        self.assertFalse((history.directory / 'missing.json').exists())
        engine._append_json_line(path, record, receipt_files=history.files)
        with history.lock('one'):
            history.remember('one', path, record)
        self.assertEqual(json.loads((history.directory / 'one.json').read_text())['result_line'], 2)

    def test_latest_results_load_streams_beyond_retained_receipt_memory(self):
        path = self.root / 'results.jsonl'
        with path.open('wb') as stream:
            for number in range(32):
                stream.write(engine._canonical_json_bytes({
                    'manifest_sha256': 'a' * 64, 'job_key': str(number),
                    'status': 'failed', 'attempt': 1, 'padding': 'x' * (256 * 1024)}))
        provenance = engine.RunnerProvenance('b' * 64, 'b' * 64, ())
        tracemalloc.start()
        try:
            latest = engine._load_latest_results(path, 'a' * 64, provenance)
            retained, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(len(latest), 32)
        self.assertLess(peak - retained, 2 * 1024 * 1024, 'resume copied the whole ledger')

    def test_ordered_atomic_write_preserves_bytes_and_bounds_serialization(self):
        path = self.root / 'report.json'
        value = {'z': [{'b': 'λ\\\n"', 'a': None}, True, 1.25], 'a': '最後'}
        experiment.write(path, value)
        self.assertEqual(path.read_bytes(), engine._ordered_json_bytes(value))
        engine._atomic_write_json(path, value, sort_keys=True)
        self.assertEqual(path.read_bytes(), engine._canonical_json_bytes(value))
        repeated = {'rows': [{'text': 'λ' * (128 * 1024)}] * 128}
        tracemalloc.start()
        try:
            experiment.write(path, repeated)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 2 * 1024 * 1024, 'atomic JSON write copied the whole report')
        self.assertEqual(path.stat().st_size, len(engine._ordered_json_bytes(repeated)))

    def test_atomic_json_encoding_failure_preserves_destination_and_cleans_temporary(self):
        path = self.root / 'report.json'
        path.write_bytes(b'previous report\n')
        for invalid, error in ((object(), TypeError), ('\ud800', UnicodeEncodeError)):
            with self.assertRaises(error):
                experiment.write(path, {'valid': 'x' * (256 * 1024), 'invalid': invalid})
            self.assertEqual(path.read_bytes(), b'previous report\n')
            self.assertEqual(list(self.root.glob('.report.json.*.tmp')), [])

    def test_worker_upgrade_prefix_validation_streams_and_rejects_changed_receipts(self):
        manifest = {'runner_version': 'fixture', 'runner_sha256': 'a' * 64,
                    'request_code_sha256': 'c' * 64}
        manifest_path = self.root / 'manifest.json'
        manifest_path.write_bytes(engine._ordered_json_bytes(manifest))
        path = self.root / 'results.jsonl'
        block = engine._canonical_json_bytes({'padding': 'x' * (256 * 1024)})
        with path.open('wb') as stream:
            for _ in range(32):
                stream.write(block)
        current = {**manifest, 'runner_sha256': 'b' * 64}
        tracemalloc.start()
        try:
            compatibility.upgrade_worker(self.root, manifest, current)
            self.assertEqual(compatibility.worker_history(self.root, manifest), [manifest, current])
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 3 * 1024 * 1024, 'worker provenance copied the whole ledger')
        row = json.loads((self.root / 'worker-upgrades.jsonl').read_text())
        self.assertEqual(row['results_prefix_bytes'], path.stat().st_size)
        self.assertEqual(row['results_prefix_sha256'], engine._sha256_file(path))
        with path.open('r+b') as stream:
            stream.write(b'!')
        with self.assertRaisesRegex(ValueError, 'invalid worker upgrade history'):
            compatibility.worker_history(self.root, manifest)

    def test_wrapper_upgrade_prefix_streams_and_retains_line_boundaries(self):
        path = self.root / 'results.jsonl'
        block = engine._canonical_json_bytes({'padding': 'x' * (256 * 1024)})
        with path.open('wb') as stream:
            for _ in range(32):
                stream.write(block)
        provenance = engine.RunnerProvenance('b' * 64, 'b' * 64, ())
        prepared = engine.PreparedManifest('a' * 64, provenance, 'c' * 64, 'reviewed update')
        tracemalloc.start()
        try:
            appended = engine._append_runner_upgrade(self.root, prepared)
            loaded = engine._load_runner_upgrades(self.root, manifest_sha256='a' * 64,
                                                   initial_wrapper_sha256='b' * 64)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(loaded, appended)
        self.assertEqual(loaded.boundaries, ((33, 'c' * 64),))
        self.assertLess(peak, 3 * 1024 * 1024, 'wrapper provenance copied the whole ledger')
        row = json.loads((self.root / 'runner-upgrades.jsonl').read_text())
        self.assertEqual(row['results_prefix_sha256'], engine._sha256_file(path))
        with path.open('r+b') as stream:
            stream.truncate(path.stat().st_size - 1)
        with self.assertRaisesRegex(engine.RunnerError, 'invalid first_result_line'):
            engine._load_runner_upgrades(self.root, manifest_sha256='a' * 64,
                                          initial_wrapper_sha256='b' * 64)

    def test_wrapper_upgrade_line_counts_match_legacy_bytes_at_chunk_boundaries(self):
        provenance = engine.RunnerProvenance('b' * 64, 'b' * 64, ())
        prepared = engine.PreparedManifest('a' * 64, provenance, 'c' * 64, 'reviewed update')
        for number, raw in enumerate((b'', b'a\r\nb\rc\n', b'incomplete',
                                      b'x' * (1024 * 1024 - 1) + b'\r\n')):
            directory = self.root / str(number)
            directory.mkdir()
            (directory / 'results.jsonl').write_bytes(raw)
            appended = engine._append_runner_upgrade(directory, prepared)
            row = json.loads((directory / 'runner-upgrades.jsonl').read_text())
            self.assertEqual(row['first_result_line'], len(raw.splitlines()) + 1)
            self.assertEqual(row['results_prefix_bytes'], len(raw))
            self.assertEqual(row['results_prefix_sha256'], engine._sha256_bytes(raw))
            if raw and not raw.endswith(b'\n'):
                with self.assertRaisesRegex(engine.RunnerError, 'does not match its results prefix'):
                    engine._load_runner_upgrades(directory, manifest_sha256='a' * 64,
                                                  initial_wrapper_sha256='b' * 64)
            else:
                self.assertEqual(engine._load_runner_upgrades(directory, manifest_sha256='a' * 64,
                                      initial_wrapper_sha256='b' * 64), appended)


if __name__ == '__main__':
    unittest.main()
