import copy
import json
import tempfile
import threading
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
        self.monitor.now_ms = lambda: 100
        self.monitor.add(PID)
        self.store.save_settings({'page_size':5,'telegram_enabled': True, 'telegram_token': 'test-token', 'telegram_chat': '123',
                                  'bark_enabled': True, 'bark_key': 'test-key'})

    def tearDown(self):
        self.temp.cleanup()

    def rows(self, table):
        with self.store.db() as db:
            return [dict(r) for r in db.execute('SELECT * FROM ' + table)]

    def baseline(self):
        self.monitor.poll(PID)

    def test_order_history_requests_for_different_leaders_are_serialized(self):
        from app import Binance
        client=Binance(); active=0; peak=0; gate=threading.Lock()
        with patch.object(client,'call'),patch('app.time.sleep',return_value=None):
            # Use Event.wait for overlap while the production spacing sleep is mocked.
            def slow_call(endpoint,body):
                nonlocal active,peak
                with gate:
                    active+=1;peak=max(peak,active)
                threading.Event().wait(0.02)
                with gate: active-=1
                return {'list':[],'total':0}
            client.call.side_effect=slow_call
            threads=[threading.Thread(target=client.history,args=(pid,'order',1)) for pid in ('1','2','3')]
            for thread in threads: thread.start()
            for thread in threads: thread.join()
        self.assertEqual(peak,1)

    def test_upgrade_sets_poll_page_size_to_five(self):
        with self.store.db() as db:
            settings=self.store.settings(db);settings['page_size']=100
            db.execute('UPDATE settings SET value=? WHERE id=1',(json.dumps(settings),))
            db.execute("DELETE FROM app_migrations WHERE name='poll_page_size_5'")
        self.assertEqual(Store(self.store.path).settings()['page_size'],5)

    def test_baseline_silent_and_decimal_exact(self):
        self.baseline()
        self.assertEqual(len(self.rows('events')), 1)
        self.assertEqual(self.rows('events')[0]['historical'], 1)
        self.assertEqual(json.loads(self.rows('events')[0]['body'])['notional'], '0.125')
        self.assertEqual(self.rows('outbox'), [])

    def test_first_boot_only_fetches_recent_page(self):
        self.api.orders = [order(x) for x in range(3000, 0, -1)]
        self.baseline()
        self.assertEqual(len(self.rows('records')), 5)
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

    def test_late_record_after_baseline_is_new_and_deduplicated(self):
        self.baseline()
        self.api.orders.insert(0, order(102)); self.monitor.poll(PID)
        self.api.orders.insert(1, order(101)); self.monitor.poll(PID); self.monitor.poll(PID)
        events = {r['stamp']:r for r in self.rows('events')}
        self.assertEqual(events[101]['historical'], 0)
        self.assertEqual(len(self.rows('outbox')), 4)

    def test_late_open_after_monitor_start_is_not_silenced_by_initial_watermark(self):
        with self.store.db() as db:
            db.execute('UPDATE monitor_start SET started_at=100 WHERE target=?',(PID,))
        self.api.orders=[order(105),order(99)]
        self.monitor.poll(PID)
        self.api.orders.insert(1,order(102))
        self.monitor.poll(PID);self.monitor.poll(PID)
        events={e['stamp']:e for e in self.rows('events')}
        self.assertEqual(events[99]['historical'],1)
        self.assertEqual(events[102]['historical'],0)
        self.assertEqual(events[105]['historical'],0)
        self.assertEqual(len(self.rows('outbox')),4)

    def test_transfer_failure_does_not_discard_new_order(self):
        self.baseline()
        self.api.orders.insert(0,order(101))
        history=self.api.history
        def failing_transfer(pid,kind,page,page_size=20):
            if kind == 'transfer': raise UpstreamError('划转接口失败')
            return history(pid,kind,page,page_size)
        self.api.history=failing_transfer
        self.monitor.poll(PID)
        self.assertTrue(any(row['stamp']==101 and not row['historical'] for row in self.rows('events')))
        self.assertEqual(len(self.rows('outbox')),2)
        self.assertIn('划转接口失败',self.rows('targets')[0]['error'])

    def test_slow_transfer_does_not_delay_saving_new_order(self):
        self.baseline();self.api.orders.insert(0,order(101))
        original=self.api.history;entered=threading.Event();release=threading.Event()
        def slow(pid,kind,page,page_size=20):
            if kind=='transfer': entered.set();release.wait(3)
            return original(pid,kind,page,page_size)
        self.api.history=slow
        worker=threading.Thread(target=self.monitor.poll,args=(PID,));worker.start()
        try:
            self.assertTrue(entered.wait(2))
            self.assertTrue(any(e['stamp']==101 and not e['historical'] for e in self.rows('events')))
            self.assertEqual(len(self.rows('outbox')),2)
        finally:
            release.set();worker.join(3)

    def test_background_transfer_delay_does_not_block_subsequent_order_poll(self):
        self.baseline(); original=self.api.history;entered=threading.Event();release=threading.Event()
        def slow(pid,kind,page,page_size=20):
            if kind=='transfer': entered.set();release.wait(3)
            return original(pid,kind,page,page_size)
        self.api.history=slow;self.monitor.transfer_background=True
        worker=threading.Thread(target=self.monitor.transfer_worker);worker.start()
        try:
            self.api.orders.insert(0,order(101));self.monitor.poll(PID)
            self.assertTrue(entered.wait(2))
            self.api.orders.insert(0,order(102));self.monitor.poll(PID)
            self.assertEqual({e['stamp'] for e in self.rows('events') if not e['historical']},{101,102})
        finally:
            self.monitor.stop.set();release.set();worker.join(3)

    def test_first_transfer_failure_recovers_without_replaying_old_transfers(self):
        original=self.api.history
        self.api.history=lambda pid,kind,page,page_size=20: (_ for _ in ()).throw(UpstreamError('划转接口失败')) if kind=='transfer' else original(pid,kind,page,page_size)
        self.monitor.poll(PID)
        self.assertEqual(self.rows('targets')[0]['initialized'],1)
        self.api.transfers=[dict(time=99,coin='USDT',amount='10',transType='LEAD_DEPOSIT')]
        self.api.history=original
        self.monitor=Monitor(Store(self.store.path),self.api)
        self.monitor.poll(PID)
        transfers=[e for e in self.rows('events') if e['kind']=='transfer']
        self.assertEqual(len(transfers),1)
        self.assertEqual(transfers[0]['historical'],1)
        self.assertEqual(self.rows('outbox'),[])

    def test_enabled_leaders_cycle_sequentially_without_minute_delay(self):
        with self.store.db() as db:
            db.execute("INSERT INTO targets(id,name) VALUES('999999','other')")
        seen=[]
        def polling(pid):
            seen.append(pid)
            if len(seen)==4: self.monitor.stop.set()
        with patch.object(self.monitor,'poll',side_effect=polling),patch.object(self.monitor,'execute_copy'),patch.object(self.monitor,'deliver'):
            worker=threading.Thread(target=self.monitor.run);worker.start();worker.join(5)
            if worker.is_alive(): self.monitor.stop.set();worker.join(3)
        self.assertEqual(seen,[PID,'999999',PID,'999999'])

    def test_slow_poll_does_not_block_copy_worker(self):
        entered, release, copied = threading.Event(), threading.Event(), threading.Event()
        def slow_poll(_pid):
            entered.set()
            release.wait(3)
        def execute():
            if entered.wait(2): copied.set()
        with patch.object(self.monitor, 'poll', side_effect=slow_poll), \
             patch.object(self.monitor, 'execute_copy', side_effect=execute), \
             patch.object(self.monitor, 'deliver'):
            worker = threading.Thread(target=self.monitor.run)
            worker.start()
            try:
                self.assertTrue(entered.wait(2))
                self.assertTrue(copied.wait(2))
            finally:
                self.monitor.stop.set(); release.set(); worker.join(3)
        self.assertFalse(worker.is_alive())

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

    def test_order_poll_continues_when_page_has_no_saved_records(self):
        self.baseline()
        self.api.orders = [order(x) for x in range(113, 98, -1)]
        self.monitor.poll(PID)
        self.assertEqual(len([e for e in self.rows('events') if not e['historical']]), 13)
        self.assertIn(('order', 2), self.api.calls)
        self.assertEqual(len(self.rows('outbox')), 26)
        self.monitor.poll(PID)
        self.assertEqual(len(self.rows('outbox')), 26)

    def test_order_poll_continues_until_known_page(self):
        self.baseline()
        self.api.orders = [order(x) for x in range(145, 80, -1)]
        self.monitor.poll(PID)
        new = [e for e in self.rows('events') if not e['historical']]
        self.assertEqual(len(new), 45)
        self.assertIn(('order', 2), self.api.calls)

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

    def test_old_order_is_saved_without_notification_and_pending_order_expires(self):
        self.baseline()
        self.api.orders.insert(0,order(101));self.monitor.poll(PID)
        self.monitor.now_ms=lambda:180103
        with patch('app.send_notification') as send: self.monitor.deliver()
        send.assert_not_called()
        self.assertTrue(all(x['status']=='cancelled' for x in self.rows('outbox')))
        self.api.orders.insert(0,order(102));self.monitor.poll(PID)
        old=next(x for x in self.rows('events') if x['stamp']==102)
        self.assertEqual(old['historical'],1)
        self.assertEqual(len(self.rows('outbox')),2)

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
        for side, position, expected in [('BUY', 'LONG', '开多'), ('SELL', 'LONG', '平多'), ('SELL', 'SHORT', '开空'), ('BUY', 'SHORT', '平空')]:
            self.assertEqual(direction({'side': side, 'positionSide': position}), expected)


if __name__ == '__main__':
    unittest.main()
