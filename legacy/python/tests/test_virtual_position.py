import unittest
from virtual_position import transition

class VirtualPositionTests(unittest.TestCase):
    def test_long_and_short(self):
        self.assertEqual(transition('0','BUY','LONG','0.1')['after'], '0.1')
        self.assertEqual(transition('0.1','SELL','LONG','0.03')['after'], '0.07')
        self.assertEqual(transition('0','SELL','SHORT','2')['after'], '2')
        self.assertEqual(transition('2','BUY','SHORT','2')['after'], '0')
    def test_unknown_close_is_not_assumed(self):
        result = transition('0','SELL','LONG','1')
        self.assertEqual(result['action'],'unmatched_close')
        self.assertEqual(result['after'],'0')
    def test_large_exact_amount(self):
        self.assertEqual(transition('9999999999999999999999999999.9','BUY','LONG','0.01')['after'],'9999999999999999999999999999.91')

class VirtualPersistenceTests(unittest.TestCase):
    def test_replay_and_unmatched_close(self):
        import tempfile
        from pathlib import Path
        from app import Store, Monitor
        from test_monitor import FakeAPI, PID, order
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'data.db'); api=FakeAPI(); monitor=Monitor(store,api); monitor.add(PID); monitor.poll(PID)
            monitor.now_ms=lambda:100
            with store.db() as db:
                db.execute('INSERT INTO virtual_sessions VALUES(?,?,?,1)', (1,PID,100))
            api.orders.insert(0,order(101,'2'))
            monitor.poll(PID)
            restarted=Monitor(Store(store.path),api); restarted.now_ms=lambda:100; restarted.poll(PID)
            close=order(102,'1'); close['side']='SELL'; api.orders.insert(0,close); monitor.poll(PID)
            excess=order(103,'3'); excess['side']='SELL'; api.orders.insert(0,excess); monitor.poll(PID)
            with store.db() as db:
                positions=db.execute('SELECT quantity FROM virtual_positions').fetchall()
                actions=[r[0] for r in db.execute('SELECT action FROM virtual_events ORDER BY stamp')]
            self.assertEqual(positions[0][0],'1')
            self.assertEqual(actions,['open','close','unmatched_close'])
