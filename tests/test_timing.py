import json
import unittest
from unittest.mock import patch
from timing import timed, timing_context, timing_event
from app import Binance, UpstreamError
from copy_executor import FuturesClient, ExchangeError


class TimingTests(unittest.TestCase):
    def test_monotonic_duration_and_context_cleanup_on_failure(self):
        with self.assertLogs('monitor', level='INFO') as logs:
            with patch('timing.time.monotonic', side_effect=[10, 10.125]):
                with self.assertRaisesRegex(ValueError, 'private-secret'):
                    with timing_context(task=42), timed('test'):
                        raise ValueError('private-secret')
            timing_event('after')
        self.assertIn('task=42', logs.output[0])
        self.assertIn('duration_ms=125.0', logs.output[0])
        self.assertIn('outcome=error', logs.output[0])
        self.assertNotIn('private-secret', str(logs.output))
        self.assertNotIn('task=42', logs.output[1])

    def test_request_failures_are_timed_without_credentials(self):
        for client, operation, error in (
            (Binance(), lambda c: c.call('lead-portfolio/detail?portfolioId=private-secret'), UpstreamError),
            (FuturesClient('private-key', 'private-secret'), lambda c: c.place({'symbol':'BTCUSDT'}), ExchangeError),
        ):
            with self.subTest(client=type(client).__name__):
                with patch('app.pooled_open', side_effect=TimeoutError('private-secret')), patch('copy_executor.pooled_open', side_effect=TimeoutError('private-secret')):
                    with self.assertLogs('monitor', level='INFO') as logs, self.assertRaises(error):
                        with timing_context(task=42): operation(client)
                output='\n'.join(logs.output)
                self.assertIn('outcome=error', output)
                self.assertIn('duration_ms=', output)
                self.assertIn('task=42', output)
                for secret in ('private-secret', 'private-key', 'signature='):
                    self.assertNotIn(secret, output)

    def test_successful_request_preserves_response(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size): return b'{"status":"FILLED","updateTime":123}'
        with patch('copy_executor.pooled_open', return_value=Response()):
            with self.assertLogs('monitor', level='INFO') as logs:
                result=FuturesClient('key','secret').place({'symbol':'BTCUSDT'})
        self.assertEqual(result, {'status':'FILLED','updateTime':123})
        self.assertIn('method=POST outcome=ok', logs.output[0])
