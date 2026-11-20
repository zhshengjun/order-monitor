import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from app import Monitor, Store
from copy_executor import ExchangeError
from test_monitor import FakeAPI, PID, order


class CloseWatchTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.store=Store(Path(self.temp.name)/'db')
        self.api=FakeAPI(); self.monitor=Monitor(self.store,self.api); self.monitor.now_ms=lambda:100
        self.monitor.add(PID);self.monitor.poll(PID)
        with self.store.db() as db:
            db.execute("INSERT INTO futures_accounts VALUES(1,'账户1','key','secret')")
            db.execute('INSERT INTO copy_bindings VALUES(1,?)',(PID,))
            db.execute('INSERT INTO copy_live VALUES(1,?,1,100)',(PID,))
            db.execute('INSERT INTO virtual_sessions VALUES(1,?,100,1)',(PID,))
            db.execute("INSERT INTO virtual_positions VALUES(1,?,'BTCUSDT','LONG','2')",(PID,))
            db.execute("INSERT INTO copied_positions VALUES(1,?,'BTCUSDT','LONG','0.19')",(PID,))
    def tearDown(self): self.temp.cleanup()
    def close(self,qty='2'):
        row=order(101,qty); row.update(side='SELL',avgPrice='100')
        self.api.orders.insert(0,row); self.monitor.poll(PID)
    def test_full_close_waits_for_task_then_reports_residual_once(self):
        self.close()
        with patch('app.FuturesClient.positions',return_value=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'0.04'}]) as positions:
            self.monitor.check_close_watch();positions.assert_not_called()
            with self.store.db() as db:
                db.execute("UPDATE copy_tasks SET state='filled'")
                db.execute('UPDATE close_watch SET next_check=0')
            self.monitor.check_close_watch();self.monitor.check_close_watch()
        with self.store.db() as db:
            events=[json.loads(r['body']) for r in db.execute("SELECT body FROM events WHERE kind='copy'")]
            self.assertEqual(db.execute('SELECT state FROM close_watch').fetchone()[0],'checked')
        self.assertEqual(len(events),1)
        self.assertIn('疑似已清仓',events[0]['message']);self.assertIn('0.04',events[0]['message'])
    def test_actual_flat_does_not_alert_and_partial_does_not_trigger(self):
        self.close('1')
        with self.store.db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM close_watch').fetchone()[0],0)
        self.close('1')
        with self.store.db() as db:db.execute("UPDATE copy_tasks SET state='filled'")
        with patch('app.FuturesClient.positions',return_value=[]):self.monitor.check_close_watch()
        with self.store.db() as db:self.assertEqual(db.execute("SELECT COUNT(*) FROM events WHERE kind='copy'").fetchone()[0],0)
    def test_failed_query_alerts_once_and_retries(self):
        self.close()
        with self.store.db() as db:db.execute("UPDATE copy_tasks SET state='filled'")
        with patch('app.FuturesClient.positions',side_effect=ExchangeError('secret')):
            self.monitor.check_close_watch()
            with self.store.db() as db:db.execute('UPDATE close_watch SET next_check=0')
            self.monitor.check_close_watch()
        with self.store.db() as db:
            events=[json.loads(r['body'])['message'] for r in db.execute("SELECT body FROM events WHERE kind='copy'")]
            self.assertEqual(db.execute('SELECT state FROM close_watch').fetchone()[0],'pending')
        self.assertEqual(len(events),1);self.assertIn('无法查询',events[0]);self.assertNotIn('secret',events[0])
    def test_restarts_preserve_pending_watch_and_unknown_result_waits(self):
        self.close()
        with self.store.db() as db:db.execute("UPDATE copy_tasks SET state='submitted'")
        restarted=Monitor(Store(Path(self.temp.name)/'db'),self.api)
        with patch('app.FuturesClient.positions') as positions:
            restarted.check_close_watch();positions.assert_not_called()
        with self.store.db() as db:
            db.execute("UPDATE copy_tasks SET state='finished'")
            db.execute('UPDATE close_watch SET next_check=0')
        with patch('app.FuturesClient.positions',return_value=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'0.19'}]):
            restarted.check_close_watch()
        with self.store.db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM events WHERE kind='copy' AND body LIKE '%仍有 0.19%'").fetchone()[0],1)

    def test_unknown_order_after_thirty_seconds_still_alerts_residual(self):
        import time
        self.close()
        with self.store.db() as db:
            db.execute("UPDATE copy_tasks SET state='submitted',created=?",(int(time.time()*1000)-31000,))
        with patch('app.FuturesClient.positions',return_value=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'0.19'}]):
            self.monitor.check_close_watch()
        with self.store.db() as db:
            messages=[json.loads(r['body'])['message'] for r in db.execute("SELECT body FROM events WHERE kind='copy'")]
        self.assertTrue(any('仍有 0.19' in m and '待执行或确认' in m for m in messages))

    def test_tiny_residual_is_not_rounded_to_zero(self):
        self.close()
        with self.store.db() as db:db.execute("UPDATE copy_tasks SET state='filled'")
        with patch('app.FuturesClient.positions',return_value=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'0.00000001'}]):
            self.monitor.check_close_watch()
        with self.store.db() as db:
            message=json.loads(db.execute("SELECT body FROM events WHERE kind='copy'").fetchone()[0])['message']
        self.assertIn('仍有 0.00000001',message)

    def test_invalid_position_response_is_not_treated_as_flat(self):
        self.close()
        with self.store.db() as db:db.execute("UPDATE copy_tasks SET state='filled'")
        with patch('app.FuturesClient.positions',return_value=[{'symbol':'BTCUSDT','positionAmt':'0'}]):self.monitor.check_close_watch()
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT state FROM close_watch').fetchone()[0],'pending')
            self.assertEqual(db.execute("SELECT COUNT(*) FROM events WHERE kind='copy' AND body LIKE '%无法查询%'").fetchone()[0],1)

    def test_stale_close_alert_checks_positions_without_order(self):
        self.monitor.now_ms=lambda:400000
        self.close()
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM copy_tasks').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM close_watch').fetchone()[0],1)
        with patch('app.FuturesClient.positions',return_value=[{'symbol':'BTCUSDT','positionSide':'LONG','positionAmt':'0.19'}]):self.monitor.check_close_watch()
        with self.store.db() as db:
            events=[json.loads(r['body'])['message'] for r in db.execute("SELECT body FROM events WHERE kind='copy'")]
        self.assertTrue(any('仍有 0.19' in msg for msg in events))
