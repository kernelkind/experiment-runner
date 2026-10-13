"""Failure scope and cohort behavior through the production admission gate."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiment_runner.extraction_pipeline import provider_retry_delay
from experiment_runner.provider_backoff import SharedProviderBackoff


class BackoffPolicyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.gate = SharedProviderBackoff(self.root)
        self.scopes = {'account': 'a' * 64, 'transport': 'b' * 64}
        self.now = 100.0
        clock = patch('experiment_runner.provider_backoff.time.monotonic', side_effect=lambda: self.now)
        clock.start()
        self.addCleanup(clock.stop)

    def acquire(self, scopes=None):
        return self.gate.try_acquire(scopes or self.scopes, concurrency=30,
                                    timeout_seconds=300, owner_pid=os.getpid())

    def failure(self, metadata):
        return {'status': 'failed', 'exit_code': 1, 'attempt': 1,
                'model_result': {'provider': 'openai-codex', 'stop_reason': 'error',
                                 'response_metadata': metadata}}

    def stream_failure(self, code='internal_error'):
        return self.failure({'http_status': 200, 'proxy_status': {
            'state': 'unknown', 'request_context': 'active', 'error_phase': 'body',
            'read_error_kind': 'http2_stream_error', 'read_error_code': code},
            'transport_error': {'phase': 'body'}})

    def test_stream_failure_retries_without_holding_other_account_requests(self):
        ticket = self.acquire()['ticket']
        failed = self.stream_failure()
        self.assertEqual(provider_retry_delay(failed), 2)
        self.gate.report(ticket, failed)
        self.assertIn('ticket', self.acquire(), 'an individual stream error blocked unrelated work')
        self.assertIn('ticket', self.acquire(), 'stream recovery serialized unrelated work')

    def test_provider_retry_opt_in_uses_saved_attempt_number_and_honors_server_delay(self):
        failed = self.failure({'http_status': 503})
        for attempt, delay in ((1, 2), (2, 4), (3, 8), (6, 60), (10000, 60)):
            with self.subTest(attempt=attempt):
                failed['attempt'] = attempt
                self.assertEqual(provider_retry_delay(failed, keep_retrying=True), delay)
                if attempt >= 2:
                    self.assertIsNone(provider_retry_delay(failed))
        failed['model_result']['response_metadata']['retry_after'] = '120'
        self.assertEqual(provider_retry_delay(failed, keep_retrying=True), 120)
        failed['model_result']['response_metadata']['retry_after'] = 'invalid'
        self.assertIsNone(provider_retry_delay(failed, keep_retrying=True))
        failed['model_result']['response_metadata'] = {'http_status': 401}
        self.assertIsNone(provider_retry_delay(failed, keep_retrying=True))

    def test_generic_server_error_is_retryable_without_account_recovery(self):
        for status in (408, 500, 502, 503, 504):
            with self.subTest(status=status):
                scopes = {**self.scopes, 'account': f'{status:064x}'}
                ticket = self.acquire(scopes)['ticket']
                failed = self.failure({'http_status': status})
                self.assertEqual(provider_retry_delay(failed), 2)
                self.gate.report(ticket, failed)
                self.assertIn('ticket', self.acquire(scopes))

    def test_explicit_overload_retries_beyond_two_attempts_with_exponential_delay(self):
        failed = self.failure({'http_status': 200, 'error': {'code': 'server_is_overloaded'}})
        for attempt, delay in ((1, 2), (2, 4), (3, 8), (6, 60), (10000, 60)):
            with self.subTest(attempt=attempt):
                failed['attempt'] = attempt
                self.assertEqual(provider_retry_delay(failed), delay)
        failed['model_result']['response_metadata']['retry_after'] = '120'
        self.assertEqual(provider_retry_delay(failed), 120)
        failed['model_result']['response_metadata']['retry_after'] = 'invalid'
        self.assertEqual(provider_retry_delay(failed), 60)

    def test_other_transient_failures_keep_the_existing_retry_bound(self):
        failed = self.stream_failure()
        failed['attempt'] = 2
        self.assertIsNone(provider_retry_delay(failed))

    def test_in_flight_failures_share_one_cooldown_and_recover_capacity(self):
        first = self.acquire()['ticket']
        second = self.acquire()['ticket']
        failed = self.failure({'http_status': 429})
        self.gate.report(first, failed)
        self.now += 1
        self.gate.report(second, failed)
        self.now += 1.1
        recovery = self.acquire()
        self.assertIn('ticket', recovery, 'an older in-flight failure extended the same cooldown')
        self.gate.report(recovery['ticket'], {'exit_code': 0, 'model_result': {'stop_reason': 'stop'}})
        self.assertIn('ticket', self.acquire())
        self.assertIn('ticket', self.acquire(), 'successful recovery did not restore two slots')

    def test_retry_after_still_applies_to_other_requests_on_the_same_account(self):
        first = self.acquire()['ticket']
        self.gate.report(first, self.failure({'http_status': 503, 'retry_after': '5'}))
        self.assertEqual(self.acquire()['wait_reason'], 'account_backoff')
        self.now += 4.9
        self.assertEqual(self.acquire()['wait_reason'], 'account_backoff')
        self.now += .2
        self.assertIn('ticket', self.acquire())

    def test_explicit_rate_limit_is_shared_but_other_accounts_can_continue(self):
        first = self.acquire()['ticket']
        self.gate.report(first, self.failure({'http_status': 429}))
        self.assertEqual(self.acquire()['wait_reason'], 'account_backoff')
        other = {'account': 'c' * 64, 'transport': self.scopes['transport']}
        self.assertIn('ticket', self.acquire(other))
        self.now += 2.1
        self.assertIn('ticket', self.acquire())
        self.assertEqual(self.acquire()['wait_reason'], 'recovery_limit')

    def test_connection_failure_holds_the_transport_including_other_accounts(self):
        failed = self.stream_failure('enhance_your_calm')
        failed['model_result']['response_metadata']['proxy_status']['read_error_kind'] = 'http2_goaway'
        ticket = self.acquire()['ticket']
        self.gate.report(ticket, failed)
        other = {'account': 'c' * 64, 'transport': self.scopes['transport']}
        self.assertEqual(self.acquire(other)['wait_reason'], 'transport_backoff')

    def test_explicit_stream_pressure_holds_its_account(self):
        ticket = self.acquire()['ticket']
        self.gate.report(ticket, self.stream_failure('enhance_your_calm'))
        self.assertEqual(self.acquire()['wait_reason'], 'account_backoff')
        self.assertIn('ticket', self.acquire({'account': 'c' * 64,
                                             'transport': self.scopes['transport']}))

    def test_stale_failure_still_honors_an_explicit_retry_after(self):
        first, old = self.acquire()['ticket'], self.acquire()['ticket']
        self.gate.report(first, self.failure({'http_status': 429}))
        self.now += 1
        self.gate.report(old, self.failure({'http_status': 429, 'retry_after': '5'}))
        self.now += 4.9
        self.assertEqual(self.acquire()['wait_reason'], 'account_backoff')
        self.now += .2
        self.assertIn('ticket', self.acquire())

    def test_new_rate_limit_during_recovery_increases_the_wait(self):
        first = self.acquire()['ticket']
        self.gate.report(first, self.failure({'http_status': 429}))
        self.now += 2.1
        recovery = self.acquire()['ticket']
        self.gate.report(recovery, self.failure({'http_status': 429}))
        self.now += 3.9
        self.assertEqual(self.acquire()['wait_reason'], 'account_backoff')
        self.now += .2
        self.assertIn('ticket', self.acquire())

    def test_local_abort_and_unknown_transport_error_do_not_throttle_other_work(self):
        for index, metadata in enumerate([
                {'http_status': 429, 'abort_reason': 'request_timeout'},
                {'http_status': 200, 'transport_error': {'phase': 'body'}}]):
            with self.subTest(metadata=metadata):
                scopes = {**self.scopes, 'account': f'{100 + index:064x}'}
                ticket = self.acquire(scopes)['ticket']
                failed = self.failure(metadata)
                self.assertIsNone(provider_retry_delay(failed))
                self.gate.report(ticket, failed)
                self.assertIn('ticket', self.acquire(scopes))


if __name__ == '__main__':
    unittest.main()
