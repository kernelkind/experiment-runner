"""Project saved JSON without constructing discarded research artifacts."""
import io
import json
from pathlib import Path
import tempfile
import tracemalloc
import unittest

from experiment_runner.json_projection import project


class JsonProjectionTests(unittest.TestCase):
    def test_only_selected_top_level_fields_are_built_with_native_json_values(self):
        document = dict(integer=19, real=1.25, flag=False, nothing=None,
                        object={}, array=[], mixed=[{'label': '茶 🌿', 'value': 2.5}, []],
                        discarded={'integer': 'nested values are not selected'})
        fields = {'integer', 'real', 'flag', 'nothing', 'object', 'array', 'mixed', 'missing'}

        actual = project(io.BytesIO(json.dumps(document, ensure_ascii=False).encode()), fields)

        self.assertEqual(actual, {name: value for name, value in document.items() if name in fields})
        self.assertIs(type(actual['real']), float)
        self.assertIs(type(actual['integer']), int)
        self.assertEqual(project(io.BytesIO(b'{}'), {'missing'}), {})

    def test_duplicate_keys_keep_the_last_value_like_json_load(self):
        encoded = b'{"kept":{"old":1},"nested":{"x":1,"x":2},"kept":[false,{},[],null]}'
        self.assertEqual(project(io.BytesIO(encoded), {'kept', 'nested'}), json.loads(encoded))

    def test_short_reads_preserve_utf8_and_escaped_unicode(self):
        class ShortReads:
            def __init__(self, encoded):
                self.stream = io.BytesIO(encoded)

            def read(self, size):
                if size < 0:
                    raise AssertionError('projection attempted to read the whole stream')
                return self.stream.read(min(size, 3))

        encoded = '{"omitted":["🌿"],"kept":"茶 \\ud83c\\udf3f"}'.encode()
        self.assertEqual(project(ShortReads(encoded), {'kept'}), {'kept': '茶 🌿'})

    def test_nonobjects_malformed_and_trailing_json_are_rejected_even_when_skipped(self):
        for encoded in [b'[]', b'null', b'1', b'"value"', b'', b'{',
                        b'{"kept":1', b'{"skip":[1,2,]}', b'{"skip":{"x":}}',
                        b'{"kept":1} {}', b'{"kept":1} trailing', b'{"skip":"\xff"}']:
            with self.subTest(encoded=encoded), self.assertRaises(ValueError):
                project(io.BytesIO(encoded), {'kept'})
        self.assertEqual(project(io.BytesIO(b'{"skip":[1,2]} \n\t'), set()), {})

    def test_input_is_consumed_through_eof_after_the_object_closes(self):
        encoded = b'{"kept":1}' + b' \n\t'*(64*1024)
        stream = io.BytesIO(encoded)

        self.assertEqual(project(stream, {'kept'}), {'kept': 1})

        self.assertEqual(stream.tell(), len(encoded))
        self.assertFalse(stream.closed, 'projection closed its caller-owned stream')
        with self.assertRaises(ValueError):
            project(io.BytesIO(encoded+b'{}'), {'kept'})

    def test_large_unknown_array_does_not_get_materialized(self):
        # Write before tracing; no complete encoded document exists in memory.
        location = '/dev/shm' if Path('/dev/shm').is_dir() else None
        with tempfile.TemporaryFile(dir=location) as stream:
            stream.write(b'{"kept":1,"discarded":[')
            chunk = b'"' + b'x'*1024 + b'",'
            for _ in range(16384):
                stream.write(chunk)
            stream.write(b'null],"tail":true}')
            self.assertGreaterEqual(stream.tell(), 16*1024*1024)
            stream.seek(0)
            tracemalloc.start()
            try:
                actual = project(stream, {'kept', 'tail'})
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
        self.assertEqual(actual, {'kept': 1, 'tail': True})
        self.assertLess(peak, 8*1024*1024, 'discarded JSON array was materialized')


if __name__ == '__main__':
    unittest.main()
