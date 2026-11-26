import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import Monitor, Store


class FakeAccount:
    info = {'multiAssetsMargin': False, 'totalCrossWalletBalance': '100', 'totalCrossUnPnl': '0',
            'positions': [{'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'isolated': False},
                          {'symbol': 'ETHUSDT', 'positionSide': 'LONG', 'isolated': True}]}
    positions = [{'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'positionAmt': '1', 'marginAsset': 'USDT', 'maintMargin': '60'},
                 {'symbol': 'ETHUSDT', 'positionSide': 'LONG', 'positionAmt': '1', 'marginAsset': 'USDT', 'maintMargin': '6', 'isolatedMargin': '10'}]
    def __init__(self, *args): pass
    def get(self, path): return self.info if path == '/fapi/v2/account' else self.positions


class MarginAlertTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'db')
        self.monitor = Monitor(self.store)
        self.account = {'id': 1, 'name': '测试账户', 'api_key': 'key', 'api_secret': 'secret'}
        self.store.save_settings({'telegram_enabled': True, 'telegram_token': 'test', 'telegram_chat': '123', 'bark_enabled': True, 'bark_key': 'test'})
        self.positions = [dict(p) for p in FakeAccount.positions]
        self.info = {**FakeAccount.info, 'positions': [dict(p) for p in FakeAccount.info['positions']]}

    def tearDown(self): self.tmp.cleanup()

    def check(self):
        with patch('app.FuturesAccount', FakeAccount), patch.object(FakeAccount, 'positions', self.positions), patch.object(FakeAccount, 'info', self.info):
            self.monitor.check_margin(self.account)

    def rows(self, sql):
        with self.store.db() as db: return [dict(r) for r in db.execute(sql)]

    def test_cross_and_isolated_crossing_deduplicates_and_rearms(self):
        self.check(); self.check()
        self.assertEqual(len(self.rows('SELECT * FROM events WHERE kind="margin"')), 2)
        self.assertEqual(len(self.rows('SELECT * FROM outbox')), 4)
        self.positions[0]['maintMargin'] = '59.99'
        self.positions[1]['maintMargin'] = '5.99'
        self.check()
        self.positions[0]['maintMargin'] = '60'
        self.positions[1]['maintMargin'] = '6'
        self.check()
        self.assertEqual(len(self.rows('SELECT * FROM events WHERE kind="margin"')), 4)
        event = self.rows('SELECT * FROM events WHERE kind="margin"')[0]
        self.assertIn('60.00%', self.monitor.message(event)[1])

    def test_invalid_or_missing_data_does_not_clear_alert_state(self):
        self.check()
        self.positions[0].pop('maintMargin')
        with self.assertRaises(KeyError): self.check()
        self.assertEqual(len([r for r in self.rows('SELECT * FROM margin_alert_state') if r['active']]), 2)

    def test_multi_asset_cross_fails_closed(self):
        self.info['multiAssetsMargin'] = True
        with self.assertRaisesRegex(ValueError, '多资产'): self.check()
        self.assertEqual(self.rows('SELECT * FROM events'), [])

    def test_monitor_notifies_when_risk_check_fails(self):
        with self.store.db() as db:
            db.execute('INSERT INTO futures_accounts VALUES(?,?,?,?)', (1,'测试账户','key','secret'))
        def wait(seconds):
            if seconds == 30: self.monitor.stop.set()
            return False
        with patch.object(self.monitor, 'check_margin', side_effect=ValueError('暂不支持多资产模式')) as check, \
             patch.object(self.monitor.stop, 'wait', side_effect=wait) as wait_mock:
            self.monitor.margin_monitor()
        self.assertEqual(check.call_count, 2)
        self.assertEqual(wait_mock.call_args_list[0].args, (3,))
        events = self.rows("SELECT * FROM events WHERE kind='margin'")
        self.assertEqual(len(events), 1)
        self.assertIn('保证金风险检查失败', events[0]['body'])
        self.assertEqual(len(self.rows('SELECT * FROM outbox')), 2)

    def test_monitor_does_not_notify_if_retry_succeeds(self):
        with self.store.db() as db:
            db.execute('INSERT INTO futures_accounts VALUES(?,?,?,?)', (1,'测试账户','key','secret'))
        def wait(seconds):
            if seconds == 30: self.monitor.stop.set()
            return False
        with patch.object(self.monitor, 'check_margin', side_effect=[ValueError('查询失败'), None]) as check, \
             patch.object(self.monitor.stop, 'wait', side_effect=wait) as wait_mock:
            self.monitor.margin_monitor()
        self.assertEqual(check.call_count, 2)
        self.assertEqual(wait_mock.call_args_list[0].args, (3,))
        self.assertEqual(self.rows('SELECT * FROM events'), [])
