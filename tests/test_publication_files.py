"""Publication payloads use managed, repeatable, staged file updates."""
import hashlib
import json
from pathlib import Path
import tempfile
import tracemalloc
import unittest

from experiment_runner.publication_files import write_publication


class PublicationFilesTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.output = Path(temporary.name)/'publication'
        self.metadata = dict(schema_version=1, format='test-publication-v1', source_id='source-a')

    def write(self, rows=({'id': 'one'},), metadata=None):
        return write_publication(self.output, self.metadata if metadata is None else metadata,
                                 {'results.jsonl': iter(rows)})

    def test_streamed_payloads_bind_counts_bytes_and_hashes(self):
        def rows():
            yield {'id': 'one', 'answer': 'café'}
            self.metadata['outcome'] = {'records': 1}
        result = self.write(rows())
        payload = (self.output/'results.jsonl').read_bytes()
        record = json.loads((self.output/'experiment.json').read_bytes())
        self.assertEqual(record['outcome'], {'records': 1})
        self.assertEqual(json.loads(payload), {'id': 'one', 'answer': 'café'})
        self.assertEqual(record['files']['results.jsonl'], dict(
            records=1, bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest()))
        self.assertEqual(result['files'], record['files'])

    def test_unchanged_export_preserves_bytes_and_mtimes(self):
        self.write()
        before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.output.iterdir()}
        self.write()
        self.assertEqual(before, {p.name: (p.read_bytes(), p.stat().st_mtime_ns)
                                 for p in self.output.iterdir()})

    def test_same_owner_refreshes_payload_and_metadata(self):
        self.write()
        self.write([{'id': 'two'}, {'id': 'three'}])
        self.assertEqual(json.loads((self.output/'experiment.json').read_text())
                         ['files']['results.jsonl']['records'], 2)

    def test_refuses_different_owner_unrelated_files_and_symlinks(self):
        self.write()
        with self.assertRaisesRegex(ValueError, 'different'):
            self.write(metadata={**self.metadata, 'source_id': 'source-b'})
        extra = self.output/'unrelated.txt'
        extra.write_text('keep')
        with self.assertRaisesRegex(ValueError, 'unrelated'):
            self.write()
        extra.unlink()
        payload = self.output/'results.jsonl'
        other = self.output.parent/'other.jsonl'
        other.write_bytes(payload.read_bytes())
        payload.unlink()
        payload.symlink_to(other)
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.write()
        self.assertTrue(payload.is_symlink())

    def test_invalid_or_interrupted_payload_preserves_previous_export(self):
        self.write()
        before = {p.name: p.read_bytes() for p in self.output.iterdir()}
        def interrupted():
            yield {'id': 'two'}
            raise RuntimeError('stop')
        with self.assertRaisesRegex(RuntimeError, 'stop'):
            self.write(interrupted())
        with self.assertRaises(ValueError):
            self.write([{'value': float('nan')}])
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.output.iterdir()})
        self.assertEqual({p.name for p in self.output.parent.iterdir()}, {'publication'})

    def test_rejects_output_symlink_and_unsafe_payload_names(self):
        self.output.symlink_to(self.output.parent/'elsewhere', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.write()
        self.output.unlink()
        for name in ('../escape.jsonl', 'experiment.json', '/absolute.jsonl'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                write_publication(self.output, self.metadata, {name: []})
        self.assertFalse(self.output.exists())

    def test_invalid_metadata_is_not_published(self):
        with self.assertRaises(ValueError):
            self.write(metadata={'source_id': 'source-a'})
        self.assertFalse(self.output.exists())

    def test_streamed_results_do_not_accumulate_in_memory(self):
        rows = ({'id': index, 'answer': 'x' * 2048} for index in range(13000))
        tracemalloc.start()
        try:
            result = self.write(rows)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertGreater(result['files']['results.jsonl']['bytes'], 24 * 1024 * 1024)
        self.assertLess(peak, 4 * 1024 * 1024)


if __name__ == '__main__':
    unittest.main()
