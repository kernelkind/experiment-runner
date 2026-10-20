"""Deliver complete model requests even when worker startup is slow."""
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

from experiment_runner import extraction_pipeline as engine


class PromptWorkerInputTests(unittest.TestCase):
    def test_delayed_worker_receives_request_larger_than_pipe_capacity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            worker = root / 'worker.py'
            worker.write_text("""import json, signal, sys, time

def stalled(signum, frame):
    print('Timed out waiting for the complete request on stdin', file=sys.stderr)
    raise SystemExit(13)

signal.signal(signal.SIGALRM, stalled)
signal.alarm(3)
time.sleep(0.5)
request = json.load(sys.stdin)
signal.alarm(0)
print(json.dumps(request, ensure_ascii=False))
""")
            request = {'prompt': 'Unicode input: 漢字\n' * 16384}
            result = engine._invoke_prompt_worker(
                SimpleNamespace(timeout_seconds=1), [sys.executable, str(worker)],
                request, root, worker_identity={'runner_sha256': '0' * 64})
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), request)


if __name__ == '__main__':
    unittest.main()
