import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import Monitor, Store, UpstreamError, direction, number

PID = '5075281354358777856'


def order(stamp, qty='1.25'):
    return dict(symbol='BTCUSDT', baseAsset='BTC', quoteAsset='USDT', side='BUY', positionSide='LONG',
                type='MARKET', orderTime=stamp, orderUpdateTime=stamp, executedQty=qty, avgPrice='0.1', totalPnl='0')


class FakeAPI:
    def __init__(self):
        self.balance = '100.00'
        self.orders = [order(100)]
        self.transfers = []
        self.fail = False
        self.calls = []

    def detail(self, pid):
        if self.fail:
            raise UpstreamError('test upstream error')
        return {'leadPortfolioId': pid, 'nickname': '熬鹰资本', 'marginBalance': self.balance, 'initInvestAsset': 'USDT'}

    def history(self, pid, kind, page, page_size=20):
        self.calls.append((kind, page))
        rows = self.orders if kind == 'order' else self.transfers
        return {'total': len(rows), 'list': copy.deepcopy(rows[(page - 1) * page_size:page * page_size])}


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / 'monitor.db')
        self.api = FakeAPI()
        self.monitor = Monitor(self.store, self.api)
        self.monitor.add(PID)
        self.store.save_settings({'telegram_enabled': True, 'telegram_token': 'test-token', 'telegram_chat': '123',
                                  'bark_enabled': True, 'bark_key': 'test-key'})

    def tearDown(self):
        self.temp.cleanup()

    def rows(self, table):
        with self.store.db() as db:
            return [dict(r) for r in db.execute('SELECT * FROM ' + table)]

    def baseline(self):
        self.monitor.poll(PID)

    def test_baseline_silent_and_decimal_exact(self):
        self.baseline()
        self.assertEqual(len(self.rows('events')), 1)
        self.assertEqual(self.rows('events')[0]['historical'], 1)
        self.assertEqual(json.loads(self.rows('events')[0]['body'])['notional'], '0.125')
        self.assertEqual(self.rows('outbox'), [])

    def test_first_boot_only_fetches_recent_page(self):
        self.api.orders = [order(x) for x in range(3000, 0, -1)]
        self.baseline()
        self.assertEqual(len(self.rows('records')), 20)
        self.assertEqual(self.api.calls, [('order', 1), ('transfer', 1)])
        self.assertEqual(self.rows('outbox'), [])
        self.api.calls.clear()
        self.api.orders.insert(0, order(3001))
        self.monitor.poll(PID)
        self.assertEqual(self.api.calls, [('order', 1), ('transfer', 1)])
        self.assertEqual(len([r for r in self.rows('events') if not r['historical']]), 1)

    def test_restart_deduplicates(self):
        self.baseline()
        Monitor(Store(self.store.path), self.api).poll(PID)
        self.assertEqual(len(self.rows('events')), 1)
        self.assertEqual(self.rows('outbox'), [])

    def test_new_order_only_once_and_both_channels(self):
        self.baseline()
        self.api.orders.insert(0, order(101))
        self.monitor.poll(PID)
        self.monitor.poll(PID)
        self.assertEqual(len(self.rows('events')), 2)
        self.assertEqual(len(self.rows('outbox')), 2)

    def test_updated_order_has_explicit_update_flag(self):
        self.baseline()
        self.api.orders[0]['executedQty'] = '2'
        self.monitor.poll(PID)
        self.assertTrue(json.loads(self.rows('events')[-1]['body'])['updated'])
        self.assertEqual(len(self.rows('records')), 1)
        self.assertEqual(len(self.rows('events')), 1)
        self.assertEqual(self.rows('outbox'), [])

    def test_page_size_five_catches_more_than_one_page(self):
        self.baseline()
        self.api.orders = [order(x) for x in range(113, 98, -1)]
        self.monitor.poll(PID)
        self.assertEqual(len([e for e in self.rows('events') if not e['historical']]), 13)
        self.assertIn(('order', 3), self.api.calls)
        self.assertEqual(len(self.rows('outbox')), 26)
        self.monitor.poll(PID)
        self.assertEqual(len(self.rows('outbox')), 26)

    def test_backfill_multiple_pages_no_historical_alert(self):
        self.baseline()
        self.api.orders = [order(x) for x in range(145, 80, -1)]
        self.monitor.poll(PID)
        new = [e for e in self.rows('events') if not e['historical']]
        self.assertEqual(len(new), 45)
        self.assertIn(('order', 3), self.api.calls)

    def test_failed_batch_does_not_advance_baseline(self):
        self.api.history = lambda *args: (_ for _ in ()).throw(UpstreamError('failed'))
        self.monitor.poll(PID)
        self.assertEqual(self.rows('targets')[0]['initialized'], 0)
        self.assertEqual(self.rows('events'), [])
        self.assertEqual(self.rows('targets')[0]['failures'], 1)

    def test_balance_snapshot_changes_do_not_create_events(self):
        self.baseline()
        self.api.balance = '110'
        self.monitor.poll(PID)
        self.assertEqual(self.rows('targets')[0]['balance'], '110')
        self.assertEqual(len(self.rows('events')), 1)
        self.assertEqual(self.rows('outbox'), [])

    def test_transfer_is_separate_from_balance(self):
        self.baseline()
        self.api.transfers = [dict(time=101, coin='USDT', amount='10', transType='LEAD_WITHDRAW', **{'from': 'Lead Trading Account', 'to': 'Fiat and Spot'})]
        self.api.balance = '90'
        self.monitor.poll(PID)
        self.assertEqual([r['kind'] for r in self.rows('events')], ['order', 'transfer'])

    def test_pause_cancels_pending_resume_catches_up(self):
        self.baseline()
        self.api.orders.insert(0, order(101))
        self.monitor.poll(PID)
        self.monitor.toggle(PID, False)
        self.api.orders.insert(0, order(102))
        self.monitor.poll(PID)
        self.assertEqual(len(self.rows('events')), 2)
        self.assertTrue(all(r['status'] == 'cancelled' for r in self.rows('outbox')))
        self.monitor.toggle(PID, True)
        self.monitor.poll(PID)
        self.assertEqual(len(self.rows('events')), 3)

    def test_notification_channels_retry_independently(self):
        self.baseline()
        self.api.orders.insert(0, order(101))
        self.monitor.poll(PID)
        def send(channel, *args):
            if channel == 'bark':
                raise ValueError('secret must not appear')
        with patch('app.send_notification', side_effect=send):
            self.monitor.deliver()
        box = {r['channel']: r for r in self.rows('outbox')}
        self.assertEqual(box['telegram']['status'], 'sent')
        self.assertEqual(box['bark']['status'], 'pending')
        self.assertEqual(box['bark']['attempts'], 1)
        self.assertNotIn('secret', box['bark']['error'])
        with self.store.db() as db:
            db.execute('UPDATE outbox SET next_try=0')
        with patch('app.send_notification') as send:
            self.monitor.deliver()
        self.assertEqual(send.call_count, 1)
        self.assertTrue(all(r['status'] == 'sent' for r in self.rows('outbox')))

    def test_settings_secrets_preserved_and_explicit_clear(self):
        self.store.save_settings({'telegram_token': '', 'interval': 15})
        self.assertEqual(self.store.settings()['telegram_token'], 'test-token')
        self.store.save_settings({'telegram_enabled': False, 'clear_telegram_token': True})
        self.assertEqual(self.store.settings()['telegram_token'], '')
        for patch_data in [{'page_size': 0}, {'page_size': 21}, {'interval': 0}, {'balance_threshold': '-1'}, {'balance_threshold': 'NaN'}, {'bark_url': 'http://example.com'}]:
            with self.assertRaises(ValueError):
                self.store.save_settings(patch_data)

    def test_error_retains_balance_and_retry_success(self):
        self.baseline()
        self.api.fail = True
        self.api.balance = '200'
        self.monitor.poll(PID)
        self.assertEqual(self.rows('targets')[0]['balance'], '100.00')
        self.api.fail = False
        self.monitor.poll(PID)
        self.assertEqual(self.rows('targets')[0]['error'], '')
        self.assertEqual(self.rows('targets')[0]['balance'], '200')

    def test_direction_combinations(self):
        for side, position, expected in [('BUY', 'LONG', '开多 / 加多'), ('SELL', 'LONG', '减多 / 平多'), ('SELL', 'SHORT', '开空 / 加空'), ('BUY', 'SHORT', '减空 / 平空')]:
            self.assertEqual(direction({'side': side, 'positionSide': position}), expected)


if __name__ == '__main__':
    unittest.main()
