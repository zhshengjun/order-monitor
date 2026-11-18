import tempfile
import unittest
from pathlib import Path

from app import Monitor, Store
from leader_replay import replay
from test_monitor import FakeAPI, PID, order


class LeaderHistoryTests(unittest.TestCase):
    def test_replay_weighted_average_and_partial_close(self):
        a=order(1,'2'); a['avgPrice']='100'
        b=order(2,'1'); b['avgPrice']='130'
        c=order(3,'1'); c['side']='SELL'; c['avgPrice']='150'
        result=replay([c,b,a])
        self.assertEqual(result[0]['quantity'],'2')
        self.assertEqual(result[0]['avg_price'],'110')
        self.assertFalse(result[0]['uncertain'])

    def test_missing_open_and_same_timestamp_are_unknown(self):
        close=order(1,'2'); close['side']='SELL'
        later=order(2,'1')
        result=replay([later,close])
        self.assertTrue(result[0]['uncertain'])
        self.assertIsNone(result[0]['quantity'])
        a=order(3,'1'); b=order(3,'2'); b['side']='SELL'
        self.assertTrue(replay([a,b])[0]['uncertain'])

    def test_background_backfill_persists_without_notifications_or_copy_tasks(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db'); api=FakeAPI(); monitor=Monitor(store,api)
            api.orders=[order(x) for x in range(50,0,-1)]
            monitor.add(PID)
            for _ in range(3):
                with store.db() as db: db.execute('UPDATE leader_history_sync SET next_check=0')
                monitor.sync_history(PID)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],50)
                self.assertEqual(db.execute('SELECT complete FROM leader_history_sync').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM copy_tasks').fetchone()[0],0)
            monitor.sync_history(PID)
            with store.db() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],50)

