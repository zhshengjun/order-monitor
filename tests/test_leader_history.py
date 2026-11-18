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

    def test_excess_close_resets_position_and_later_open_restarts(self):
        close=order(1,'2'); close['side']='SELL'
        later=order(2,'1')
        result=replay([later,close])
        self.assertFalse(result[0]['uncertain'])
        self.assertEqual(result[0]['quantity'],'1')
        self.assertEqual(result[0]['avg_price'],'0.1')
        self.assertEqual(replay([close]),[])

    def test_same_timestamp_is_unknown(self):
        a=order(3,'1'); b=order(3,'2'); b['side']='SELL'
        self.assertTrue(replay([a,b])[0]['uncertain'])

    def test_same_timestamp_same_action_is_deterministic(self):
        a=order(3,'1');a['avgPrice']='100'
        b=order(3,'2');b['avgPrice']='130'
        result=replay([a,b])
        self.assertFalse(result[0]['uncertain'])
        self.assertEqual(result[0]['quantity'],'3')
        self.assertEqual(result[0]['avg_price'],'120')

    def test_background_backfill_persists_without_notifications_or_copy_tasks(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db'); api=FakeAPI(); monitor=Monitor(store,api)
            api.orders=[order(x) for x in range(50,0,-1)]
            monitor.add(PID)
            for _ in range(4):
                with store.db() as db: db.execute('UPDATE leader_history_sync SET next_check=0')
                monitor.sync_history(PID)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],50)
                self.assertEqual(db.execute('SELECT complete FROM leader_history_sync').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM copy_tasks').fetchone()[0],0)
            monitor.sync_history(PID)
            with store.db() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],50)

    def test_sync_continues_until_empty_even_if_total_is_inaccurate(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db'); api=FakeAPI(); monitor=Monitor(store,api)
            api.orders=[order(x) for x in range(25,0,-1)]
            original=api.history
            def understated(pid,kind,page,page_size=20):
                data=original(pid,kind,page,page_size);data['total']=1;return data
            api.history=understated;monitor.add(PID)
            for _ in range(3):
                with store.db() as db: db.execute('UPDATE leader_history_sync SET next_check=0')
                monitor.sync_history(PID)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],25)
                self.assertEqual(db.execute('SELECT complete FROM leader_history_sync').fetchone()[0],1)
            self.assertEqual([x[1] for x in api.calls], [1,2,3])

