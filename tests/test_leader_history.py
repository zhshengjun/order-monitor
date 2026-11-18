import tempfile
import unittest
from pathlib import Path

from app import Monitor, Store, history_identity, identity
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
            api.orders=[order(x) for x in range(250,0,-1)]
            monitor.add(PID)
            for _ in range(4):
                with store.db() as db: db.execute('UPDATE leader_history_sync SET next_check=0')
                monitor.sync_history(PID)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],250)
                self.assertEqual(db.execute('SELECT complete FROM leader_history_sync').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM copy_tasks').fetchone()[0],0)
            monitor.sync_history(PID)
            with store.db() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],250)

    def test_sync_continues_until_empty_even_if_total_is_inaccurate(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db'); api=FakeAPI(); monitor=Monitor(store,api)
            api.orders=[order(x) for x in range(125,0,-1)]
            original=api.history
            def understated(pid,kind,page,page_size=20):
                data=original(pid,kind,page,page_size);data['total']=1;return data
            api.history=understated;monitor.add(PID)
            for _ in range(3):
                with store.db() as db: db.execute('UPDATE leader_history_sync SET next_check=0')
                monitor.sync_history(PID)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],125)
                self.assertEqual(db.execute('SELECT complete FROM leader_history_sync').fetchone()[0],1)
            self.assertEqual([x[1] for x in api.calls], [1,2,3])

    def test_history_distinguishes_same_time_different_fills(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db');api=FakeAPI();monitor=Monitor(store,api);monitor.add(PID)
            a=order(1,'1');b=order(1,'2')
            self.assertEqual(identity('order',a),identity('order',b))
            self.assertNotEqual(history_identity(a),history_identity(b))
            self.assertEqual(history_identity(a),history_identity(dict(a,executedQty='1.00000000',avgPrice='0.10000000')))
            variant=dict(a,symbol=' btcusdt ',side=' buy ',positionSide=' long ',orderTime='1')
            self.assertEqual(history_identity(a),history_identity(variant))
            self.assertNotEqual(history_identity(a),history_identity(dict(a,side='SELL')))
            self.assertNotEqual(history_identity(a),history_identity(dict(a,positionSide='SHORT')))
            self.assertNotEqual(history_identity(a),history_identity(dict(a,orderTime=2)))
            api.orders=[a,b];monitor.sync_history(PID)
            with store.db() as db: db.execute('UPDATE leader_history_sync SET next_check=0')
            monitor.sync_history(PID)
            store=Store(store.path)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],2)

    def test_migration_recovers_distinct_fills_from_older_backup(self):
        import json
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db');a=order(10,'1');b=order(10,'2')
            with store.db() as db:
                db.execute("DELETE FROM app_migrations WHERE name='history_identity_v4'")
                db.execute('DROP TABLE leader_history_before_v4')
                db.execute('INSERT INTO leader_history VALUES(?,?,?,?)',(PID,'old',10,json.dumps(a)))
                db.execute('INSERT INTO leader_history_before_v3 VALUES(?,?,?,?)',(PID,'other',10,json.dumps(b)))
            store=Store(store.path)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history').fetchone()[0],2)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leader_history_before_v4').fetchone()[0],1)

    def test_failed_rebuild_keeps_previous_history_until_empty_page(self):
        import json
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db'); api=FakeAPI(); monitor=Monitor(store,api);monitor.add(PID)
            old=order(1,'1'); fresh=order(2,'2')
            with store.db() as db:
                db.execute('INSERT INTO leader_history VALUES(?,?,?,?)',(PID,history_identity(old),1,json.dumps(old)))
            api.orders=[fresh];monitor.sync_history(PID)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT stamp FROM leader_history').fetchone()[0],1)
                db.execute('UPDATE leader_history_sync SET next_check=0')
            original=api.history
            api.history=lambda *args: (_ for _ in ()).throw(ValueError('network'))
            monitor.sync_history(PID)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT stamp FROM leader_history').fetchone()[0],1)
                db.execute('UPDATE leader_history_sync SET next_check=0')
            api.history=original;monitor.sync_history(PID)
            with store.db() as db:
                self.assertEqual(db.execute('SELECT stamp FROM leader_history').fetchone()[0],2)

    def test_live_order_immediately_changes_replayed_position(self):
        import json
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db');api=FakeAPI();monitor=Monitor(store,api);monitor.add(PID)
            initial=order(100,'2');initial['avgPrice']='100';api.orders=[initial];monitor.poll(PID)
            newer=order(101,'1');newer['avgPrice']='130';api.orders.insert(0,newer);monitor.poll(PID)
            with store.db() as db:
                rows=[json.loads(r[0]) for r in db.execute('SELECT body FROM leader_history WHERE target=? ORDER BY stamp',(PID,))]
            result=replay(rows)
            self.assertEqual(result[0]['quantity'],'3')
            self.assertEqual(result[0]['avg_price'],'110')

    def test_rebuild_preserves_new_live_order_arriving_during_sync(self):
        import json, time
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db');api=FakeAPI();monitor=Monitor(store,api);monitor.add(PID)
            api.orders=[order(100,'1')];monitor.sync_history(PID)
            stamp=int(time.time()*1000)+1; fresh=order(stamp,'2')
            with store.db() as db:
                db.execute('INSERT OR REPLACE INTO records VALUES(?,?,?,?,?)',(PID,'order',identity('order',fresh),json.dumps(fresh),stamp))
                db.execute('UPDATE leader_history_sync SET next_check=0')
            monitor.sync_history(PID)
            with store.db() as db:
                stamps=[r[0] for r in db.execute('SELECT stamp FROM leader_history ORDER BY stamp')]
            self.assertEqual(stamps,[100,stamp])

    def test_repeated_page_reports_error_instead_of_infinite_sync(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'db');api=FakeAPI();monitor=Monitor(store,api);monitor.add(PID)
            api.history=lambda *args: {'total':100,'list':[order(10)]}
            monitor.sync_history(PID)
            with store.db() as db: db.execute('UPDATE leader_history_sync SET next_check=0')
            monitor.sync_history(PID)
            with store.db() as db:
                sync=db.execute('SELECT * FROM leader_history_sync').fetchone()
                self.assertEqual(sync['page'],2)
                self.assertEqual(sync['complete'],0)
                self.assertIn('相同',sync['error'])

