"""Failure scope and cohort behavior through the production admission gate."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiment_runner.extraction_pipeline import provider_retry_delay, prompt_failure_kind
from experiment_runner.provider_backoff import SharedProviderBackoff


class BackoffPolicyTests(unittest.TestCase):
    def test_all_verified_transient_failures_keep_retrying_by_default(self):
        for status in (408, 409, 429, 500, 502, 503, 504, 529):
            for attempt, expected in ((1, 2), (2, 4), (3, 8), (6, 60), (10000, 60)):
                with self.subTest(status=status, attempt=attempt):
                    failed = self.failure({'http_status': status})
                    failed['attempt'] = attempt
                    self.assertEqual(provider_retry_delay(failed), expected)
        failed = self.stream_failure()
        failed['attempt'] = 3
        self.assertEqual(provider_retry_delay(failed), 8)
        for value, expected in (('120', 120), ('invalid', 8), ('NaN', 8), ('Infinity', 8)):
            with self.subTest(retry_after=value):
                failed['model_result']['response_metadata']['retry_after'] = value
                self.assertEqual(provider_retry_delay(failed), expected)

    def test_server_retry_headers_are_preserved_without_overriding_fatal_errors(self):
        failed = self.failure({'http_status': 503, 'retry_after_ms': '120000'})
        self.assertEqual(provider_retry_delay(failed), 120)
        failed['model_result']['response_metadata']['retry_after'] = '150'
        self.assertEqual(provider_retry_delay(failed), 150)
        failed['model_result']['response_metadata']['retry_after'] = 'invalid'
        self.assertEqual(provider_retry_delay(failed), 120)
        failed['model_result']['response_metadata']['should_retry'] = False
        self.assertIsNone(provider_retry_delay(failed))
        failed = self.failure({'http_status': 418, 'should_retry': True})
        self.assertEqual(provider_retry_delay(failed), 2)
        failed['model_result']['response_metadata']['http_status'] = 401
        self.assertIsNone(provider_retry_delay(failed))
        failed = self.failure({'http_status': 503, 'should_retry': True,
            'error': {'code': 'insufficient_quota'}})
        self.assertIsNone(provider_retry_delay(failed))

    def test_generic_error_envelopes_do_not_hide_rate_limits_or_server_status(self):
        for status, code in ((429, 'rate_limit_exceeded'), (503, 'provider_error')):
            failed = self.failure({'http_status': status,
                'error': {'type': 'invalid_request_error', 'code': code}})
            self.assertEqual(provider_retry_delay(failed), 2)
        failed = self.failure({'http_status': 503,
            'error': {'code': 'invalid_request_error'}})
        self.assertEqual(provider_retry_delay(failed), 2)
        failed['model_result']['response_metadata']['http_status'] = 400
        self.assertIsNone(provider_retry_delay(failed))
        failed = self.failure({'http_status': 503,
            'error': {'code': 'unsupported_parameter'}})
        self.assertIsNone(provider_retry_delay(failed))

    def test_transport_and_provider_failure_contracts(self):
        cases = []
        for provider in ('openai-codex', 'openrouter'):
            for status in (408, 409, 429, 500, 502, 503, 504, 529):
                # A symbolic body code must not hide the actual HTTP status.
                cases.append((provider, {'http_status': status,
                    'error': {'code': 'provider_error'}}, 'provider_transient'))
                cases.append((provider, {'http_status': 200,
                    'error': {'code': str(status)}}, 'provider_transient'))
            for status in (400, 401, 402, 403, 404, 413, 422):
                cases.append((provider, {'http_status': status,
                    'error': {'code': 'server_error'}},
                    'provider_fatal' if status in (401, 402, 403, 404) else 'unknown'))
            for code in ('server_error', 'server_is_overloaded', 'rate_limit_exceeded',
                         'usage_limit_reached', 'websocket_connection_limit_reached'):
                for status in (200, 101):
                    cases.append((provider, {'http_status': status,
                        'terminal_event': 'response.failed', 'error': {'code': code}},
                        'provider_transient'))
            for code in ('insufficient_quota', 'invalid_api_key', 'usage_not_included',
                         'billing_hard_limit_reached', 'project_spend_limit_exceeded',
                         'organization_usage_limit_exceeded'):
                cases.append((provider, {'http_status': 429, 'error': {'code': code}},
                              'provider_fatal'))
            for code in ('ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'EAI_AGAIN',
                         'UND_ERR_SOCKET', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT'):
                cases.append((provider, {'transport_error': {'phase': 'headers',
                    'cause': {'code': code}}}, 'provider_transient'))
            cases.append((provider, {'http_status': 200, 'transport_error': {
                'phase': 'body', 'cause': {'code': 'CERT_HAS_EXPIRED'}}}, 'unknown'))
        for kind in ('unexpected_eof', 'connection_reset', 'connection_closed',
                     'broken_pipe', 'timeout', 'network_error'):
            cases.append(('openai-codex', {'proxy_status': {'state': 'unknown',
                'request_context': 'active', 'error_phase': 'body',
                'read_error_kind': kind}}, 'provider_transient'))
        for kind in ('http2_stream_error', 'http2_goaway', 'http2_connection_error'):
            for code in ('no_error', 'internal_error', 'refused_stream', 'cancel',
                         'enhance_your_calm', 'settings_timeout', 'connect_error'):
                cases.append(('openai-codex', {'proxy_status': {'state': 'unknown',
                    'request_context': 'active', 'error_phase': 'body',
                    'read_error_kind': kind, 'read_error_code': code}}, 'provider_transient'))
        for code in (1000, 1001, 1006, 1011, 1012, 1013, 1014):
            cases.append(('openai-codex', {'http_status': 101, 'websocket_close': {'code': code},
                'transport_error': {'phase': 'body'}}, 'provider_transient'))
        for code in (1002, 1003, 1007, 1008, 1009, 1010):
            cases.append(('openai-codex', {'http_status': 101, 'websocket_close': {'code': code},
                'transport_error': {'phase': 'body'}}, 'unknown'))
        for error_type in ('provider_overloaded', 'provider_unavailable', 'server', 'timeout'):
            cases.append(('openrouter', {'http_status': 200, 'error': {
                'code': 'upstream', 'error_type': error_type}}, 'provider_transient'))
        for error_type in ('invalid_request', 'context_length_exceeded', 'max_tokens_exceeded',
                           'refusal', 'content_policy_violation', 'unmapped'):
            cases.append(('openrouter', {'http_status': 200, 'error': {
                'code': 500, 'error_type': error_type}}, 'unknown'))
        for provider, metadata, expected in cases:
            with self.subTest(provider=provider, metadata=metadata):
                failed = self.failure(metadata)
                failed['model_result']['provider'] = provider
                failed['attempt'] = 3
                self.assertEqual(prompt_failure_kind(failed), expected)
                self.assertEqual(provider_retry_delay(failed), 8 if expected == 'provider_transient' else None)

    def test_terminal_failures_and_invalid_protocols_are_not_retried_as_socket_errors(self):
        for terminal in ('response.failed', 'response.incomplete', 'error'):
            failed = self.failure({'http_status': 101, 'terminal_event': terminal,
                'websocket_close': {'code': 1000}, 'transport_error': {'phase': 'body'}})
            self.assertIsNone(provider_retry_delay(failed))
        for code in ('protocol_error', 'inadequate_security', 'compression_error', 'other'):
            failed = self.stream_failure(code)
            self.assertIsNone(provider_retry_delay(failed))
        for code in ('service_unavailable_error', 'rate_limit_error', 'overloaded_error'):
            failed = self.failure({'http_status': 101, 'error': {'type': code}})
            self.assertEqual(provider_retry_delay(failed), 2)

    def test_success_cancellation_and_local_deadlines_override_stale_provider_errors(self):
        for changes in ({'abort_reason': 'request_timeout'},
                        {'terminal_event': 'response.completed'},
                        {'proxy_status': {'state': 'complete'}},
                        {'proxy_status': {'request_context': 'canceled'}},
                        {'proxy_status': {'request_context': 'deadline_exceeded'}}):
            with self.subTest(changes=changes):
                failed = self.failure({'http_status': 503,
                    'error': {'code': 'server_is_overloaded'}, **changes})
                self.assertIsNone(provider_retry_delay(failed))
        for stop_reason in ('stop', 'length', 'aborted'):
            failed = self.failure({'http_status': 503})
            failed['model_result']['stop_reason'] = stop_reason
            self.assertIsNone(provider_retry_delay(failed))

    def test_openrouter_in_flight_payment_wait_is_distinct_from_exhausted_credit(self):
        failed = self.failure({'http_status': 402, 'retry_after': '120',
            'error': {'code': 402, 'limit_source': 'openrouter_in_flight_budget'}})
        failed['model_result']['provider'] = 'openrouter'
        self.assertEqual(provider_retry_delay(failed), 120)
        failed['model_result']['response_metadata']['should_retry'] = False
        self.assertIsNone(provider_retry_delay(failed))
        del failed['model_result']['response_metadata']['should_retry']
        failed['model_result']['response_metadata']['retry_after'] = 'invalid'
        self.assertIsNone(provider_retry_delay(failed))
        failed['model_result']['response_metadata']['retry_after'] = '120'
        del failed['model_result']['response_metadata']['error']['limit_source']
        self.assertIsNone(provider_retry_delay(failed))

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

    def test_typed_overload_has_shared_pressure_but_generic_unavailability_does_not(self):
        ticket = self.acquire()['ticket']
        failed = self.failure({'http_status': 200, 'error': {'type': 'overloaded_error'}})
        self.gate.report(ticket, failed)
        self.assertEqual(self.acquire()['wait_reason'], 'account_backoff')
        other = {'account': 'c' * 64, 'transport': self.scopes['transport']}
        ticket = self.acquire(other)['ticket']
        self.gate.report(ticket, self.failure({'http_status': 200, 'error': {'code': 'server_error'}}))
        self.assertIn('ticket', self.acquire(other))

    def test_legacy_retry_option_matches_the_automatic_policy(self):
        failed = self.failure({'http_status': 503})
        for attempt, delay in ((1, 2), (2, 4), (3, 8), (6, 60), (10000, 60)):
            with self.subTest(attempt=attempt):
                failed['attempt'] = attempt
                self.assertEqual(provider_retry_delay(failed, keep_retrying=True), delay)
                self.assertEqual(provider_retry_delay(failed), delay)
        failed['model_result']['response_metadata']['retry_after'] = '120'
        self.assertEqual(provider_retry_delay(failed, keep_retrying=True), 120)
        failed['model_result']['response_metadata']['retry_after'] = 'invalid'
        self.assertEqual(provider_retry_delay(failed, keep_retrying=True), 60)
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

    def test_stream_failure_keeps_retrying_after_second_attempt(self):
        failed = self.stream_failure()
        failed['attempt'] = 2
        self.assertEqual(provider_retry_delay(failed), 4)

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

    def test_stale_failure_honors_millisecond_deadline_without_resetting_recovery(self):
        first, old = self.acquire()['ticket'], self.acquire()['ticket']
        self.gate.report(first, self.failure({'http_status': 429}))
        self.now += 1
        self.gate.report(old, self.failure({'http_status': 503, 'retry_after_ms': '5000'}))
        self.now += 4.9
        self.assertEqual(self.acquire()['wait_reason'], 'account_backoff')
        self.now += .2
        self.assertIn('ticket', self.acquire())
        self.assertEqual(self.acquire()['wait_reason'], 'recovery_limit')

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
