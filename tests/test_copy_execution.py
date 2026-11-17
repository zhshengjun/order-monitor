import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from app import Store, Monitor
from copy_executor import plan, ExchangeError
from test_monitor import FakeAPI, PID, order

RULES={'symbols':[{'symbol':'BTCUSDT','status':'TRADING','filters':[{'filterType':'MARKET_LOT_SIZE','stepSize':'0.001','minQty':'0.001','maxQty':'1000'},{'filterType':'MIN_NOTIONAL','notional':'5'}]}]}
BALANCES=[{'asset':'USDT','availableBalance':'100'}]
QUOTE={'bidPrice':'99.9','askPrice':'100.1'}
class FakeExchange:
    calls=[]
    result={'status':'FILLED','executedQty':'0.999','orderId':1}
    def __init__(self,*args): pass
    def balance(self): return BALANCES
    def positions(self): return [{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'1'}]
    def mode(self): return {'dualSidePosition':False}
    def quote(self,s): return QUOTE
    def rules(self): return RULES
    def place(self,p): self.calls.append(('place',p)); return dict(self.result)
    def order(self,s,c): self.calls.append(('query',c)); return dict(self.result)

class CopyExecutionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.store=Store(Path(self.tmp.name)/'db'); self.api=FakeAPI();self.monitor=Monitor(self.store,self.api);self.monitor.add(PID);self.monitor.poll(PID);FakeExchange.calls=[]
        with self.store.db() as db:
            db.execute('INSERT INTO futures_accounts VALUES(1,?,?,?)',('test','key','secret'))
            db.execute('INSERT INTO virtual_sessions VALUES(?,?,?,1)',(1,PID,100))
            db.execute('INSERT INTO copy_live VALUES(?,?,1,?)',(1,PID,100))
    def tearDown(self): self.tmp.cleanup()
    def rows(self,sql):
        with self.store.db() as db: return [dict(r) for r in db.execute(sql)]
    def test_new_signal_persisted_and_filled_once(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row)
        self.monitor.poll(PID);self.monitor.poll(PID)
        self.assertEqual(len(self.rows('SELECT * FROM copy_tasks')),1)
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy();self.monitor.execute_copy()
        self.assertEqual([x[0] for x in FakeExchange.calls],['place'])
        self.assertEqual(self.rows('SELECT * FROM copy_tasks')[0]['state'],'filled')
        self.assertEqual(self.rows('SELECT * FROM copied_positions')[0]['quantity'],'0.999')
        audit=self.rows('SELECT * FROM copy_task_audit')[0]
        self.assertEqual(audit['available_balance'],'100')
        self.assertEqual(audit['lead_balance'],'100.00')
        self.assertEqual(audit['reference_price'],'100.1')
        self.assertEqual(audit['planned_quantity'],'0.999')
    def test_open_then_proportional_close_uses_actual_position(self):
        row=order(101,'2');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange):
            FakeExchange.result={'status':'FILLED','executedQty':'1','orderId':1}
            self.monitor.execute_copy()
            close=order(102,'1');close['avgPrice']='100';close['side']='SELL';self.api.orders.insert(0,close);self.monitor.poll(PID)
            FakeExchange.result={'status':'FILLED','executedQty':'0.5','orderId':2}
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls[-1][1]['quantity'],'0.500')
        self.assertEqual(FakeExchange.calls[-1][1]['reduceOnly'],'true')
        self.assertEqual(self.rows('SELECT * FROM copied_positions')[0]['quantity'],'0.5')
        FakeExchange.result={'status':'FILLED','executedQty':'0.999','orderId':1}

    def test_timeout_is_queried_never_resent(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'place',side_effect=ExchangeError('timeout')) as place:
            self.monitor.execute_copy();self.assertEqual(self.rows('SELECT * FROM copy_tasks')[0]['state'],'submitted');self.monitor.execute_copy();self.assertEqual(place.call_count,1)
            with self.store.db() as db: db.execute('UPDATE copy_tasks SET next_check=0')
            self.monitor.execute_copy()
        self.assertEqual([x[0] for x in FakeExchange.calls],['query'])
        self.assertEqual(self.rows('SELECT * FROM copy_tasks')[0]['state'],'filled')
    def test_close_no_position_is_not_placed(self):
        row=order(101,'1');row['side']='SELL';row['avgPrice']='100'
        with self.assertRaisesRegex(ValueError,'没有对应方向持仓'):
            plan(row,'close','2','1','100',BALANCES,[],{'dualSidePosition':False},QUOTE,RULES)
    def test_reduce_only_not_blocked_by_opening_minimum_notional(self):
        row=order(101,'0.01');row['avgPrice']='100';row['side']='SELL'
        p=plan(row,'close','0.01','0.01','100',BALANCES,[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'0.01'}],{'dualSidePosition':False},QUOTE,RULES)
        self.assertEqual(p['quantity'],'0.010')
        self.assertEqual(p['reduceOnly'],'true')

    def test_price_guard_and_hedge(self):
        row=order(101,'1');row['avgPrice']='100'
        with self.assertRaisesRegex(ValueError,'0.5%'):
            plan(row,'open','0','0','100',BALANCES,[],{'dualSidePosition':False},{'bidPrice':'100','askPrice':'101'},RULES)
        row['side']='SELL'
        p=plan(row,'close','1','1','100',BALANCES,[{'symbol':'BTCUSDT','positionSide':'LONG','positionAmt':'1'}],{'dualSidePosition':True},QUOTE,RULES)
        self.assertEqual(p['positionSide'],'LONG');self.assertNotIn('reduceOnly',p)

    def test_conflicting_signal_stops_copy_before_dispatch(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        self.api.orders[0]['executedQty']='2';self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],0)
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'cancelled')
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls,[])

class FuturesTransportTests(unittest.TestCase):
    def test_market_order_is_signed_post(self):
        import hashlib,hmac,urllib.parse
        from copy_executor import FuturesClient
        class Response:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def read(self,n): return b'{"status":"FILLED","executedQty":"0.001"}'
        with patch('copy_executor.urllib.request.urlopen',return_value=Response()) as call, patch('copy_executor.time.time',return_value=1234):
            result=FuturesClient('key','secret').place({'symbol':'BTCUSDT','side':'BUY','type':'MARKET','quantity':'0.001','newClientOrderId':'cp_test'})
        request=call.call_args.args[0]; parts=urllib.parse.urlsplit(request.full_url);unsigned,sig=parts.query.rsplit('&signature=',1)
        self.assertEqual(request.get_method(),'POST')
        self.assertEqual(parts.path,'/fapi/v1/order')
        self.assertEqual(sig,hmac.new(b'secret',unsigned.encode(),hashlib.sha256).hexdigest())
        self.assertEqual(result['status'],'FILLED')
