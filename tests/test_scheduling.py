"""Cross-process scheduling exercised through the released CLI and model worker."""
import json
import io
import os
import signal
from pathlib import Path
from contextlib import contextmanager
import subprocess
import tempfile
import tarfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin/experiment-runner'


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control = self.root / 'control'
        self.control.mkdir()
        self.env = {**os.environ, 'FAKE_RUNNER_LOG': str(self.root / 'calls.json'),
                    'FAKE_RUNNER_CONTROL': str(self.control),
                    'ORACLE_EXPERIMENT_BACKOFF_DIRECTORY': str(self.root / 'admission')}
        self.processes = []
        self.addCleanup(self.stop)

    def stop(self):
        for process in self.processes:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for entry in self.starts():
            (self.control / f"{entry['pid']}.release").touch()
        for process in self.processes:
            try:
                process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()

    def cli(self, *args):
        result = subprocess.run([str(CLI), *args], cwd=self.root, env=self.env,
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def prepare(self, name, count, *, provider='openai-codex'):
        (self.root / f'{name}.txt').write_text('GENERIC PROMPT\n{TASK}')
        (self.root / f'{name}-cases.json').write_text(json.dumps({'cases': [
            {'id': str(i), 'inputs': {'TASK': f'{name}-{i}'}} for i in range(count)]}))
        (self.root / f'{name}-spec.json').write_text(json.dumps({
            'schema_version': 1, 'prompt': {'id': 'P1', 'path': f'{name}.txt'},
            'dataset': {'adapter': 'generic', 'path': f'{name}-cases.json'},
            'runner': {'provider': provider, 'auth_file': None,
                       'model': 'fixture/model', 'reasoning_effort': 'off' if provider == 'local' else 'low',
                       'runner_binary': str(ROOT / 'tests/fixtures/fake-runner.py')}}))
        self.cli('prepare', name, '--spec', f'{name}-spec.json')

    def launch(self, name, *args, endpoint=None):
        environment = {**self.env, **({'FAKE_RUNNER_ADMISSION': json.dumps(endpoint)} if endpoint else {})}
        process = subprocess.Popen([str(CLI), 'run', name, *args], cwd=self.root, env=environment,
                                   text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True)
        self.processes.append(process)
        return process

    def starts(self):
        return [json.loads(path.read_text()) for path in self.control.glob('*.json')]

    def until(self, predicate):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.03)
        starts = self.starts()
        groups = {}
        for row in starts:
            name = row['prompt'].splitlines()[-1].split('-')[0]
            groups[name] = groups.get(name, 0) + 1
        self.fail(f'expected scheduling transition did not occur; worker starts={groups}')

    @contextmanager
    def stalled_first_waiter(self):
        from experiment_runner import scheduling

        with patch.dict(os.environ, self.env):
            for name in ('first', 'second'):
                (self.root / name).mkdir()
            scopes = {'account': 'a' * 64, 'transport': 'b' * 64}
            first = scheduling.ExperimentQueue(self.root / 'first', 1, scopes=scopes)
            second = scheduling.ExperimentQueue(self.root / 'second', 1, scopes=scopes)
            stalled, resume, release = threading.Event(), threading.Event(), threading.Event()
            entered = [threading.Event(), threading.Event()]
            errors = []
            original = first.gate._state
            held = False

            @contextmanager
            def held_poll():
                nonlocal held
                registered = False
                with original() as value:
                    yield value
                    state, _ = value
                    registered = any(job['owner'] == first.owner
                                     for job in state.get('queue', {}).get('waiting', {}).values())
                if registered and not held:
                    # Stop after the real waiter registration is persisted,
                    # outside the host lock and before its admission poll.
                    held = True
                    stalled.set()
                    if not resume.wait(3):
                        raise AssertionError('test did not release the first coordinator')

            def run(queue, index):
                try:
                    with queue.slot(str(index), 30):
                        entered[index].set()
                        release.wait(3)
                except BaseException as error:
                    errors.append(error)

            threads = [threading.Thread(target=run, args=(queue, index))
                       for index, queue in enumerate((first, second))]
            try:
                with patch.object(first.gate, '_state', held_poll):
                    threads[0].start()
                    self.assertTrue(stalled.wait(1))
                    threads[1].start()
                    self.assertTrue(entered[1].wait(1), 'free slots waited for the preferred owner to poll')
                    self.assertFalse(entered[0].is_set())
                    history = [json.loads(line) for line in
                               first.path.with_suffix('.jsonl').read_text().splitlines()]
                    self.assertFalse(any(row['event'] == 'admitted' for row in history))
                    yield first, second, resume, entered, errors
            finally:
                resume.set()
                release.set()
                for thread in threads:
                    if thread.ident is not None:
                        thread.join(timeout=3)
                first.close()
                second.close()
            self.assertTrue(all(not thread.is_alive() for thread in threads))
            with first.gate._state() as (state, _):
                self.assertEqual(state['tickets'], {})
                self.assertEqual(state['queue']['running'], {})
                self.assertEqual(state['queue']['waiting'], {})

    def test_waiter_fills_free_slots_when_the_first_fair_owner_is_not_polling(self):
        from experiment_runner import scheduling

        with self.stalled_first_waiter() as (first, second, resume, entered, errors):
            status = scheduling.snapshot(self.root / 'second')
            self.assertEqual(status['global_running'], 2)
            self.assertEqual(status['experiments'][first.owner]['running'], 1)
            self.assertEqual(status['experiments'][first.owner]['ready'], 0)
            resume.set()
            self.assertTrue(entered[0].wait(1))
        self.assertEqual(errors, [])

    def test_pause_releases_a_peer_reservation_before_its_waiter_adopts_it(self):
        from experiment_runner import scheduling
        from experiment_runner.extraction_pipeline import ExperimentPaused

        with self.stalled_first_waiter() as (first, second, resume, entered, errors):
            scheduling.pause(first.path.parent, 'test pause')
            resume.set()
        self.assertFalse(entered[0].is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ExperimentPaused)

    def test_stopped_waiter_releases_a_peer_reservation_before_adopting_it(self):
        from experiment_runner.extraction_pipeline import NewJobLimitReached

        with self.stalled_first_waiter() as (first, second, resume, entered, errors):
            first.stopped.set()
            resume.set()
        self.assertFalse(entered[0].is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], NewJobLimitReached)

    def test_expired_peer_reservation_does_not_decrement_ready_again_on_stop(self):
        from experiment_runner import scheduling

        with self.stalled_first_waiter() as (first, second, resume, entered, errors):
            future = time.monotonic() + 61
            with patch.object(time, 'monotonic', return_value=future):
                first.stopped.set()
                resume.set()
                for _ in range(100):
                    if errors:
                        break
                    threading.Event().wait(0.01)
                self.assertEqual(len(errors), 1)
                self.assertEqual(scheduling.snapshot(first.path.parent)['ready'], 0)

    def test_expired_peer_reservation_is_readmitted_without_losing_its_waiter(self):
        from experiment_runner import scheduling

        with self.stalled_first_waiter() as (first, second, resume, entered, errors):
            future = time.monotonic() + 61
            with patch.object(time, 'monotonic', return_value=future):
                resume.set()
                self.assertTrue(entered[0].wait(1), errors)
                status = scheduling.snapshot(first.path.parent)
                self.assertEqual(status['ready'], 0)
                self.assertEqual(status['running'], 1)
                self.assertEqual(set(status['cases']), {'0'})
        self.assertEqual(errors, [])

    def test_same_local_endpoint_serializes_before_launch_across_experiments(self):
        endpoint = {'scope': 'a' * 64, 'capacity': 1}
        self.prepare('older', 3, provider='local')
        self.prepare('newer', 2, provider='local')
        budget = self.root / 'local-budget.json'
        budget.write_text(json.dumps({'schema_version': 1, 'limit': 2, 'used': 0}))
        older = self.launch('older', endpoint=endpoint)
        self.until(lambda: len(self.starts()) >= 1)
        time.sleep(0.2)
        self.assertEqual(len(self.starts()), 1, 'a serial endpoint must admit one worker')
        newer = self.launch('newer', '--attempt-budget', str(budget), endpoint=endpoint)
        self.until(lambda: json.loads(self.cli('show', 'newer', '--json').stdout)
                   ['scheduling'].get('ready') == 2)
        queued = json.loads(self.cli('show', 'newer', '--json').stdout)['scheduling']
        self.assertEqual((queued['running'], queued['global_running'], queued['eligible_ready']), (0, 1, 0))
        self.assertEqual(queued['wait_reason'], 'capacity')
        self.assertEqual(json.loads(budget.read_text())['used'], 0)
        first = self.starts()[0]
        (self.control / f"{first['pid']}.release").touch()
        self.until(lambda: len(self.starts()) == 2)
        self.assertIn('newer-', max(self.starts(), key=lambda row: row['started'])['prompt'])
        deadline = time.monotonic() + 10
        while (older.poll() is None or newer.poll() is None) and time.monotonic() < deadline:
            active = [row for row in self.starts()
                      if not (self.control / f"{row['pid']}.release").exists()]
            self.assertLessEqual(len(active), 1)
            for row in active:
                (self.control / f"{row['pid']}.release").touch()
            time.sleep(0.03)
        for process in (older, newer):
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 0, stdout + stderr)
            self.assertNotIn(endpoint['scope'], stdout + stderr)
        self.assertEqual(len(self.starts()), 5)
        self.assertEqual(json.loads(budget.read_text())['used'], 2)
        for name in ('older', 'newer'):
            history = (self.root / name / 'run/scheduling.jsonl').read_text()
            self.assertNotIn(endpoint['scope'], history)
            self.assertLessEqual(max(json.loads(line)['global_running'] for line in history.splitlines()), 1)

    def test_busy_local_endpoint_does_not_block_distinct_endpoint_or_provider(self):
        for name, count, provider in [('held', 2, 'local'), ('other', 1, 'local'),
                                      ('remote', 2, 'openai-codex')]:
            self.prepare(name, count, provider=provider)
        held = self.launch('held', endpoint={'scope': 'a' * 64, 'capacity': 1})
        self.until(lambda: len(self.starts()) >= 1)
        other = self.launch('other', endpoint={'scope': 'b' * 64, 'capacity': 1})
        remote = self.launch('remote')
        self.until(lambda: len(self.starts()) >= 4)
        time.sleep(0.2)
        starts = self.starts()
        self.assertEqual({name: sum(name + '-' in row['prompt'] for row in starts)
                          for name in ('held', 'other', 'remote')},
                         {'held': 1, 'other': 1, 'remote': 2})
        status = json.loads(self.cli('show', 'held', '--json').stdout)['scheduling']
        self.assertEqual((status['global_running'], status['global_limit']), (4, 30))
        self.assertEqual((status['wait_reason'], status['eligible_ready']), ('capacity', 0))
        deadline = time.monotonic() + 10
        while any(process.poll() is None for process in (held, other, remote)) and time.monotonic() < deadline:
            for row in self.starts():
                (self.control / f"{row['pid']}.release").touch()
            time.sleep(0.03)
        for process in (held, other, remote):
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 0, stdout + stderr)

    def test_invalid_endpoint_metadata_refuses_launch_without_spending(self):
        self.prepare('invalid', 2, provider='local')
        budget = self.root / 'budget.json'
        budget.write_text(json.dumps({'schema_version': 1, 'limit': 2, 'used': 0}))
        private_url = 'http://127.0.0.1:54321'
        process = self.launch('invalid', '--attempt-budget', str(budget),
                              endpoint={'scope': private_url, 'capacity': 1})
        stdout, stderr = process.communicate(timeout=8)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn('endpoint admission metadata unavailable', stderr)
        self.assertNotIn(private_url, stdout + stderr)
        self.assertEqual(self.starts(), [])
        self.assertEqual(json.loads(budget.read_text())['used'], 0)
        self.assertEqual(json.loads(self.cli('show', 'invalid', '--json').stdout)
                         ['summary']['unrun'], 2)

    def test_loaded_previous_coordinator_keeps_dispatching_beside_busy_endpoint(self):
        # Use the released coordinator before endpoint admission support, with
        # its real shared-state eligibility rules, not a modified test scheduler.
        archive = subprocess.run(['git', 'archive', 'e3dea03e2b4412465d5de40422ee907054a0e4b5',
                                  'experiment_runner', 'bin/experiment-runner'],
                                 cwd=ROOT, capture_output=True, check=True).stdout
        legacy = self.root / 'previous-release'
        legacy.mkdir()
        with tarfile.open(fileobj=io.BytesIO(archive)) as source:
            source.extractall(legacy, filter='data')
        self.prepare('remote', 40)
        self.prepare('local', 2, provider='local')
        # A previous coordinator must prepare its own experiment. Feeding it a
        # current-code plan tests provenance refusal instead of coexistence.
        prepared = subprocess.run([str(legacy / 'bin/experiment-runner'), 'prepare', 'remote-previous',
            '--spec', 'remote-spec.json'], cwd=self.root, env=self.env,
            text=True, capture_output=True, timeout=15)
        self.assertEqual(prepared.returncode, 0, prepared.stdout + prepared.stderr)
        remote = subprocess.Popen([str(legacy / 'bin/experiment-runner'), 'run', 'remote-previous'],
            cwd=self.root, env=self.env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True)
        self.processes.append(remote)
        self.until(lambda: len(self.starts()) == 30)
        local = self.launch('local', endpoint={'scope': 'a' * 64, 'capacity': 1})
        self.until(lambda: json.loads(self.cli('show', 'local', '--json').stdout)
                   ['scheduling'].get('ready') == 2)
        held = sorted(self.starts(), key=lambda row: row['started'])
        (self.control / f"{held[0]['pid']}.release").touch()
        self.until(lambda: any('local-' in row['prompt'] for row in self.starts()))
        (self.control / f"{held[1]['pid']}.release").touch()
        self.until(lambda: sum('remote-' in row['prompt'] for row in self.starts()) == 31)
        self.assertEqual(sum('local-' in row['prompt'] for row in self.starts()), 1)
        deadline = time.monotonic() + 12
        while (remote.poll() is None or local.poll() is None) and time.monotonic() < deadline:
            for row in self.starts():
                (self.control / f"{row['pid']}.release").touch()
            time.sleep(0.03)
        for process in (remote, local):
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 0, stdout + stderr)

    def test_global_limit_fair_newcomer_and_live_queued_status(self):
        self.prepare('older', 60)
        self.prepare('newer', 30)
        older = self.launch('older')
        self.until(lambda: len(self.starts()) >= 30)
        time.sleep(0.25)
        self.assertEqual(len(self.starts()), 30, 'only 30 model workers may start')
        newer = self.launch('newer')
        self.until(lambda: any(case['state'] == 'queued' for case in
                   json.loads(self.cli('show', 'newer', '--json').stdout)
                   .get('scheduling', {}).get('cases', {}).values()))
        live = json.loads(self.cli('show', 'older', '--json').stdout)['scheduling']
        self.assertEqual((live['running'], live['queued'], live['global_running'], live['global_limit']),
                         (30, 30, 30, 30))
        self.assertEqual(live['wait_reason'], 'capacity')
        self.until(lambda: json.loads(self.cli('show', 'newer', '--json').stdout)['scheduling']['ready'] == 30)
        new_status = json.loads(self.cli('show', 'newer', '--json').stdout)['scheduling']
        self.assertEqual(new_status['ready'], 30)
        self.assertEqual(new_status['global_ready'], 30)
        self.assertEqual(new_status['global_eligible_ready'], 30)
        self.assertEqual(new_status['experiments'][new_status['owner']]['wait_reason'], 'capacity')
        original = sorted(self.starts(), key=lambda row: row['started'])
        for entry in original[:15]:
            (self.control / f"{entry['pid']}.release").touch()
        self.until(lambda: len([r for r in self.starts() if 'newer-' in r['prompt']]) == 15)
        self.assertEqual(len([r for r in self.starts() if 'older-' in r['prompt']]), 30,
                         'older run must yield released slots to the newcomer')
        new_live = json.loads(self.cli('show', 'newer', '--json').stdout)['scheduling']
        self.assertEqual(new_live['running'], 15)
        self.assertGreaterEqual(new_live['queue_wait_seconds'], 0)
        # Release continuously: the completed smaller run lends its share back.
        deadline = time.monotonic() + 12
        while (older.poll() is None or newer.poll() is None) and time.monotonic() < deadline:
            for entry in self.starts():
                (self.control / f"{entry['pid']}.release").touch()
            time.sleep(0.03)
        for process in (older, newer):
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 0, stdout + stderr)
            self.assertIn('queued=', stderr)
        final = json.loads(self.cli('show', 'older', '--json').stdout)
        self.assertEqual(final['summary']['valid'], 60)
        self.assertEqual(final['scheduling']['running'], 0)
        self.assertEqual(final['scheduling']['queued'], 0)
        calls = json.loads((self.root / 'calls.json').read_text())
        self.assertEqual(len(calls), 90)
        self.cli('run', 'older')
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 90)

    def test_three_ready_experiments_share_ten_slots_each(self):
        for name in ('first', 'second', 'third'):
            self.prepare(name, 30)
        first = self.launch('first')
        self.until(lambda: len(self.starts()) == 30)
        for name in ('second', 'third'):
            self.launch(name)
            self.until(lambda: any(case['state'] == 'queued' for case in
                       json.loads(self.cli('show', name, '--json').stdout)
                       .get('scheduling', {}).get('cases', {}).values()))
        for entry in sorted(self.starts(), key=lambda row: row['started'])[:20]:
            (self.control / f"{entry['pid']}.release").touch()
        self.until(lambda: len(self.starts()) == 50)
        counts = {name: json.loads(self.cli('show', name, '--json').stdout)['scheduling']['running']
                  for name in ('first', 'second', 'third')}
        self.assertEqual(counts, {'first': 10, 'second': 10, 'third': 10})
        self.assertIsNone(first.poll())

    def test_timestamped_history_records_ready_admitted_and_released_requests(self):
        self.prepare('sample', 2)
        process = self.launch('sample')
        self.until(lambda: len(self.starts()) == 2)
        history = self.root / 'sample' / 'run' / 'scheduling.jsonl'
        self.assertTrue(history.exists(), 'admission observations must be retained')
        live = json.loads(self.cli('show', 'sample', '--json').stdout)['scheduling']
        self.assertEqual((live['ready'], live['global_ready']), (0, 0))
        for entry in self.starts():
            (self.control / f"{entry['pid']}.release").touch()
        stdout, stderr = process.communicate(timeout=8)
        self.assertEqual(process.returncode, 0, stdout + stderr)
        observations = [json.loads(line) for line in history.read_text().splitlines()]
        self.assertTrue({'started', 'ready', 'admitted', 'released', 'finished'} <=
                        {row['event'] for row in observations})
        ready = [row for row in observations if row['event'] == 'ready']
        self.assertTrue(any(row['ready'] > 0 and row['global_running'] < 30 for row in ready))
        admissions = [row for row in observations if row['event'] == 'admitted']
        self.assertEqual(sorted(row['running'] for row in admissions), [1, 2])
        self.assertTrue(any(row['event'] == 'released' and row['running'] == 0
                            for row in observations))
        for row in observations:
            self.assertEqual(row['schema_version'], 1)
            self.assertTrue(row['observed_at_utc'].endswith('+00:00'))
            self.assertGreater(row['monotonic_seconds'], 0)
            self.assertTrue(row['boot_id'])
            self.assertEqual(row['global_ready'],
                             sum(run['ready'] for run in row['experiments'].values()))
            self.assertEqual(row['global_eligible_ready'],
                             sum(run['eligible_ready'] for run in row['experiments'].values()))
            self.assertNotIn('cases', row, 'history records counts, not case input or outputs')
        self.assertIn('observed_at_utc=', stderr)
        self.assertIn('ready=', stderr)

    def test_stream_retry_waits_without_holding_fresh_or_active_cases(self):
        self.prepare('retry', 31)
        self.env['FAKE_RUNNER_FAILURES'] = json.dumps({'retry-0': {
            'attempts': 1, 'metadata': {'http_status': 200, 'proxy_status': {
                'state': 'unknown', 'request_context': 'active', 'error_phase': 'body',
                'read_error_kind': 'http2_stream_error', 'read_error_code': 'internal_error'},
                'transport_error': {'phase': 'body'}}}})
        stdout_path, stderr_path = self.root / 'retry.stdout', self.root / 'retry.stderr'
        stdout_file, stderr_file = stdout_path.open('w'), stderr_path.open('w')
        self.addCleanup(stdout_file.close)
        self.addCleanup(stderr_file.close)
        process = subprocess.Popen([str(CLI), 'run', 'retry', '--max-new-attempts', '32'],
            cwd=self.root, env=self.env, stdout=stdout_file, stderr=stderr_file, start_new_session=True)
        self.processes.append(process)
        self.until(lambda: len(self.starts()) == 30)
        failed = next(row for row in self.starts() if row['prompt'].endswith('retry-0'))
        (self.control / f"{failed['pid']}.release").touch()
        # The remaining case must use the freed slot before the two-second
        # retry deadline. The other 29 requests stay active at their barriers.
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline:
            if any(row['prompt'].endswith('retry-30') for row in self.starts()):
                break
            time.sleep(.01)
        self.assertTrue(any(row['prompt'].endswith('retry-30') for row in self.starts()),
                        'a request-local retry delay held the whole batch')
        self.assertEqual(sum(row['prompt'].endswith('retry-0') for row in self.starts()), 1,
                         'the failed request retried before its deadline')
        self.assertEqual(sum(not (self.control / f"{row['pid']}.release").exists()
                             for row in self.starts()), 30)
        deadline = time.monotonic() + 10
        while process.poll() is None and time.monotonic() < deadline:
            for row in self.starts():
                (self.control / f"{row['pid']}.release").touch()
            time.sleep(.03)
        process.wait(timeout=3)
        self.assertEqual(process.returncode, 0, stdout_path.read_text() + stderr_path.read_text())
        self.assertEqual(len(self.starts()), 32)
        report = json.loads(self.cli('show', 'retry', '--json').stdout)
        self.assertEqual(report['summary']['valid'], 31)

    def test_history_distinguishes_recovery_block_from_healthy_ready_work(self):
        from experiment_runner import scheduling
        account = {'account': 'a' * 64, 'transport': 'b' * 64}
        other = {'account': 'c' * 64, 'transport': 'b' * 64}
        for name in ('recovering', 'healthy'):
            (self.root / name).mkdir()
        with patch.dict(os.environ, {'ORACLE_EXPERIMENT_BACKOFF_DIRECTORY':
                                    str(self.root / 'admission')}):
            recovering = scheduling.ExperimentQueue(self.root / 'recovering', 2, scopes=account)
            healthy = scheduling.ExperimentQueue(self.root / 'healthy', 1, scopes=other)
            ticket = recovering.gate.try_acquire(account, concurrency=30, timeout_seconds=30,
                                                  owner_pid=os.getpid())['ticket']
            recovering.gate.report(ticket, {'status': 'failed', 'exit_code': 1, 'error': 'HTTP 429',
                'model_result': {'stop_reason': 'error', 'response_metadata': {'http_status': 429}}})
            thread_errors = []
            def second_request():
                try:
                    with recovering.slot('second', 30):
                        pass
                except Exception as error:
                    thread_errors.append(error)
            with recovering.slot('first', 30):
                # The first recovery request is admitted after the cooldown.
                # Its active ticket fills the temporary account limit of one.
                thread = threading.Thread(target=second_request)
                thread.start()
                try:
                    self.until(lambda: scheduling.snapshot(self.root / 'recovering')['ready'] == 1)
                    status = scheduling.snapshot(self.root / 'recovering')
                    self.assertEqual(status['global_running'], 1)
                    self.assertEqual(status['eligible_ready'], 0)
                    self.assertEqual(status['wait_reason'], 'recovery_limit')
                    with healthy.slot('other-account', 30):
                        history = [json.loads(line) for line in
                                   (self.root / 'healthy' / 'scheduling.jsonl').read_text().splitlines()]
                        admitted = next(row for row in history if row['event'] == 'admitted')
                        blocked = admitted['experiments'][recovering.owner]
                        self.assertEqual((blocked['ready'], blocked['eligible_ready']), (1, 0))
                        self.assertEqual(blocked['wait_reason'], 'recovery_limit')
                        self.assertEqual(admitted['global_running'], 2)
                        self.assertEqual(admitted['global_eligible_ready'], 0)
                finally:
                    if thread_errors:
                        recovering.stopped.set()
            thread.join(timeout=4)
            self.assertFalse(thread.is_alive())
            self.assertEqual(thread_errors, [])
            recovering.close()
            healthy.close()
            history_text = (self.root / 'recovering' / 'scheduling.jsonl').read_text()
            rows = [json.loads(line) for line in history_text.splitlines()]
            self.assertTrue(any(row['event'] == 'ready' and row['wait_reason'] == 'recovery_limit'
                                and row['ready'] == 1 and row['global_eligible_ready'] == 0 for row in rows))
            self.assertTrue(any(row['event'] == 'progress' and row['wait_reason'] == 'account_backoff'
                                and row['cooldown_until_utc'] is not None for row in rows))
            for value in account.values():
                self.assertNotIn(value, history_text)

    def test_resume_appends_history_without_rewriting_observations_or_receipts(self):
        self.prepare('sample', 1)
        self.env.pop('FAKE_RUNNER_CONTROL')
        self.cli('run', 'sample')
        history = self.root / 'sample' / 'run' / 'scheduling.jsonl'
        receipts = history.with_name('results.jsonl')
        original_history = history.read_bytes()
        original_receipts = receipts.read_bytes()
        self.cli('run', 'sample')
        self.assertTrue(history.read_bytes().startswith(original_history))
        self.assertGreater(history.stat().st_size, len(original_history))
        self.assertEqual(receipts.read_bytes(), original_receipts)
        rows = [json.loads(line) for line in history.read_text().splitlines()]
        self.assertEqual(len({row['owner'] for row in rows if row['event'] == 'started'}), 2)
        self.assertEqual(len(json.loads((self.root / 'calls.json').read_text())), 1)

    def test_external_concurrency_settings_are_rejected(self):
        self.prepare('sample', 1)
        spec = self.root / 'sample-spec.json'
        value = json.loads(spec.read_text())
        value['runner']['concurrency'] = 50
        spec.write_text(json.dumps(value))
        for arguments in (['prepare', 'other', '--spec', str(spec)],
                          ['prepare', 'other', '--from', 'sample', '--concurrency', '50'],
                          ['run', 'sample', '--max-concurrency', '50']):
            result = subprocess.run([str(CLI), *arguments], cwd=self.root, env=self.env,
                                    text=True, capture_output=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('concurrency', result.stderr)

    def test_queued_cancellation_never_reserves_an_attempt_and_resume_dispatches(self):
        self.prepare('busy', 30)
        self.prepare('queued', 1)
        budget = self.root / 'budget.json'
        budget.write_text(json.dumps({'schema_version': 1, 'limit': 1, 'used': 0}))
        busy = self.launch('busy')
        self.until(lambda: len(self.starts()) == 30)
        queued = self.launch('queued', '--attempt-budget', str(budget))
        self.until(lambda: json.loads(self.cli('show', 'queued', '--json').stdout)
                   .get('scheduling', {}).get('wait_reason') == 'capacity')
        self.assertEqual(json.loads(budget.read_text())['used'], 0)
        os.killpg(queued.pid, signal.SIGTERM)
        queued.communicate(timeout=3)
        interrupted = json.loads(self.cli('show', 'queued', '--json').stdout)
        self.assertEqual(interrupted['scheduling']['state'], 'interrupted')
        self.assertEqual(interrupted['summary']['attempts'], 0)
        self.assertEqual(interrupted['summary']['unrun'], 1)
        os.killpg(busy.pid, signal.SIGTERM)
        busy.communicate(timeout=3)
        self.env.pop('FAKE_RUNNER_CONTROL')
        self.cli('run', 'queued', '--attempt-budget', str(budget))
        self.assertEqual(json.loads(budget.read_text())['used'], 1)
        final = json.loads(self.cli('show', 'queued', '--json').stdout)
        self.assertEqual(final['summary']['valid'], 1)
        self.assertEqual(final['scheduling']['global_running'], 0)

    def test_historical_concurrency_remains_frozen_but_does_not_limit_dispatch(self):
        from experiment_runner import extraction_pipeline as engine
        path = self.root / 'historical-config.json'
        path.write_text(json.dumps({
            'schema_version': 1, 'runner_binary': str(ROOT / 'tests/fixtures/fake-runner.py'),
            'provider': 'openai-codex', 'auth_file': None, 'model': 'fixture/model',
            'reasoning_effort': 'low', 'run_directory': str(self.root / 'historical-run'),
            'concurrency': 1, 'prompts': [
                {'prompt_id': name, 'text': 'GENERIC PROMPT ' + name} for name in ('one', 'two')]}))
        original = path.read_bytes()
        barrier = self.root / 'barrier'
        barrier.mkdir()
        env = {key: value for key, value in self.env.items() if key != 'FAKE_RUNNER_CONTROL'}
        with patch.dict(os.environ, {**env, 'FAKE_RUNNER_BARRIER': str(barrier)}):
            config = engine.PromptRunnerConfig.load(path)
            manifest = engine._build_prompt_manifest(config, engine._runner_version(config))
            engine._request_history_options(config, manifest, repeat=False, history_root=self.root)
            output = engine._execute_prompt_batch(config, manifest, retry_failed=False,
                                                 concurrency=config.concurrency)
        self.assertEqual(len(output['responses']), 2)
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(config.concurrency, 1)

    def test_live_orphaned_worker_keeps_its_global_slot(self):
        self.prepare('orphan', 1)
        self.prepare('other', 30)
        orphan = self.launch('orphan')
        self.until(lambda: len(self.starts()) == 1)
        held = self.starts()[0]
        orphan.terminate()  # Only the CLI parent; its request is still alive.
        orphan.wait(timeout=3)
        live = json.loads(self.cli('show', 'orphan', '--json').stdout)['scheduling']
        self.assertEqual((live['state'], live['running'], live['global_running']), ('interrupted', 1, 1))
        other = self.launch('other')
        self.until(lambda: len(self.starts()) == 30)
        time.sleep(0.2)
        self.assertEqual(len(self.starts()), 30, 'orphan must count against the 30-request cap')
        (self.control / f"{held['pid']}.release").touch()
        self.until(lambda: len(self.starts()) == 31)
        self.assertIsNone(other.poll())

    def test_help_exposes_automatic_scheduling_and_live_status(self):
        for command in (['--help'], ['run', '--help'], ['show', '--help']):
            value = self.cli(*command).stdout
            self.assertIn('30', value)
            self.assertIn('queue', value)
            self.assertIn('concurrency', value)
            if command[0] in ('run', 'show'):
                for name in ('scheduling', 'global_running', 'fair_share', 'prefix_warmup',
                             'queue_wait_seconds', 'cooldown_until_utc', 'scheduling.jsonl',
                             'observed_at_utc', 'global_ready', 'global_eligible_ready', 'boot_id'):
                    self.assertIn(name, value)

    def test_help_tells_workflows_to_start_all_ready_batches_before_waiting(self):
        for command in (['--help'], ['run', '--help'], ['show', '--help']):
            with self.subTest(command=command):
                value = self.cli(*command).stdout
                self.assertIn('Launch all ready, authorized model batches', value)
                self.assertIn('not run processes', value)
                self.assertIn('outer batch caps', value)
                self.assertIn('before waiting', value)
                self.assertIn('input dependencies', value)
        self.assertFalse((self.root / 'calls.json').exists(), 'help must not launch work')


if __name__ == '__main__':
    unittest.main()
