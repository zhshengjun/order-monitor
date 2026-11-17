import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from app import Store, Monitor
from copy_executor import plan, ExchangeError
from test_monitor import FakeAPI, PID, order

RULES={'symbols':[{'symbol':'BTCUSDT','status':'TRADING','filters':[{'filterType':'MARKET_LOT_SIZE','stepSize':'0.001','minQty':'0.001','maxQty':'1000'},{'filterType':'MIN_NOTIONAL','notional':'5'}]}]}
BALANCES=[{'asset':'USDT','balance':'100','availableBalance':'100'}]
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
    def mark_price(self,s): return {'symbol':s,'markPrice':'100'}
    def symbol_config(self,s): return [{'symbol':s,'leverage':10}]
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
        self.assertEqual(audit['wallet_balance'],'100')
        self.assertEqual(audit['available_balance'],'100')
        self.assertEqual(audit['lead_balance'],'100.00')
        self.assertEqual(audit['reference_price'],'100.1')
        self.assertEqual(audit['planned_quantity'],'0.999')
    def test_notification_uses_actual_exchange_notional(self):
        import json
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        FakeExchange.result={'status':'FILLED','executedQty':'0.999','orderId':1,'cumQuote':'99.92500000','avgPrice':'100.0000'}
        try:
            with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
            result=json.loads(self.rows('SELECT result FROM copy_tasks')[0]['result'])
            self.assertEqual(result['cumQuote'], '99.925')
            notice=json.loads(self.rows("SELECT body FROM events WHERE kind='copy'")[0]['body'])['message']
            self.assertIn('实际成交数量：0.999 BTC',notice)
            self.assertIn('跟单金额：99.925 USDT',notice)
        finally:
            FakeExchange.result={'status':'FILLED','executedQty':'0.999','orderId':1}

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
    def test_http_451_precheck_reports_failed_interface_without_placing(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        reason='HTTP 451; code 0; 接口 /fapi/v3/balance；请检查容器代理与合约 API 出口'
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',side_effect=ExchangeError(reason)):
            self.monitor.execute_copy()
        task=self.rows('SELECT state,error FROM copy_tasks')[0]
        self.assertEqual(task['state'],'skipped');self.assertIn('/fapi/v3/balance',task['error'])
        self.assertEqual(FakeExchange.calls,[])

    def test_http_451_on_submit_keeps_pending_reconciliation(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'place',side_effect=ExchangeError('HTTP 451; 接口 /fapi/v1/order')) as place:
            self.monitor.execute_copy(); self.monitor.execute_copy()
        task=self.rows('SELECT state,error FROM copy_tasks')[0]
        self.assertEqual(task['state'],'submitted');self.assertIn('/fapi/v1/order',task['error'])
        self.assertEqual(place.call_count,1)

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
        with self.assertRaisesRegex(ValueError,'3%'):
            plan(row,'open','0','0','100',BALANCES,[],{'dualSidePosition':False},{'bidPrice':'100','askPrice':'103.01'},RULES)
        row['side']='SELL'
        p=plan(row,'close','1','1','100',BALANCES,[{'symbol':'BTCUSDT','positionSide':'LONG','positionAmt':'1'}],{'dualSidePosition':True},QUOTE,RULES)
        self.assertEqual(p['positionSide'],'LONG');self.assertNotIn('reduceOnly',p)

    def test_wallet_based_order_and_audit_use_distinct_available_balance(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        balances=[{'asset':'USDT','balance':'100','availableBalance':'20'}]
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',return_value=balances):
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls[0][1]['quantity'],'0.999')
        audit=self.rows('SELECT wallet_balance,available_balance,planned_quantity FROM copy_task_audit')[0]
        self.assertEqual((audit['wallet_balance'],audit['available_balance'],audit['planned_quantity']),('100','20','0.999'))

    def test_wallet_based_order_is_not_submitted_with_insufficient_available_balance(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        balances=[{'asset':'USDT','balance':'100','availableBalance':'5'}]
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',return_value=balances):
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls,[])
        task=self.rows('SELECT state,error FROM copy_tasks')[0]
        self.assertEqual(task['state'],'skipped');self.assertIn('可用余额',task['error'])

    def test_open_size_uses_wallet_balance_not_available_balance(self):
        row=order(101,'1');row['avgPrice']='100'
        balances=[{'asset':'USDT','balance':'100','availableBalance':'20'}]
        params=plan(row,'open','0','0','100',balances,[],{'dualSidePosition':False},QUOTE,RULES,mark_price='100',leverage='10')
        self.assertEqual(params['quantity'],'0.999')

    def test_open_checks_available_balance_even_above_minimum(self):
        row=order(101,'1');row['avgPrice']='100'
        balances=[{'asset':'USDT','balance':'100','availableBalance':'5'}]
        with self.assertRaisesRegex(ValueError,'可用余额'):
            plan(row,'open','0','0','100',balances,[],{'dualSidePosition':False},QUOTE,RULES,mark_price='100',leverage='10')

    def test_open_fails_when_wallet_balance_missing_or_invalid(self):
        row=order(101,'1');row['avgPrice']='100'
        for balances in ([{'asset':'USDT','availableBalance':'20'}],[{'asset':'USDT','balance':'NaN','availableBalance':'20'}]):
            with self.assertRaises(ValueError):
                plan(row,'open','0','0','100',balances,[],{'dualSidePosition':False},QUOTE,RULES,mark_price='100',leverage='10')

    def test_minimum_order_flows_through_executor_without_changing_leverage(self):
        row=order(101,'0.001');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'result',{'status':'FILLED','executedQty':'0.05','orderId':1}):
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls[0][0],'place')
        self.assertEqual(FakeExchange.calls[0][1]['quantity'],'0.050')
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'filled')
        self.assertEqual(self.rows('SELECT quantity FROM copied_positions')[0]['quantity'],'0.05')

    def test_small_open_uses_minimum_valid_market_order(self):
        row=order(101,'0.001');row['avgPrice']='100'
        result=plan(row,'open','0','0','1000',[{'asset':'USDT','balance':'10','availableBalance':'10'}],[],{'dualSidePosition':False},QUOTE,RULES,mark_price='100',leverage='10')
        self.assertEqual(result['quantity'],'0.050')

    def test_small_close_never_exceeds_owned_position(self):
        row=order(101,'0.01');row['avgPrice']='100';row['side']='SELL'
        positions=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'0.002'}]
        result=plan(row,'close','1','0.002','100',BALANCES,positions,{'dualSidePosition':False},QUOTE,RULES)
        self.assertEqual(result['quantity'],'0.001')
        with self.assertRaisesRegex(ValueError,'可平仓'):
            plan(row,'close','1','0.0005','100',BALANCES,positions,{'dualSidePosition':False},QUOTE,RULES)

    def test_minimum_order_checks_actual_leverage_and_mark_price(self):
        row=order(101,'0.001');row['avgPrice']='100'
        with self.assertRaisesRegex(ValueError,'可用余额'):
            plan(row,'open','0','0','1000',[{'asset':'USDT','balance':'0.4','availableBalance':'0.4'}],[],{'dualSidePosition':False},QUOTE,RULES,mark_price='99',leverage='10')
        result=plan(row,'open','0','0','1000',[{'asset':'USDT','balance':'1','availableBalance':'1'}],[],{'dualSidePosition':False},QUOTE,RULES,mark_price='99',leverage='10')
        self.assertEqual(result['quantity'],'0.051')

    def test_minimum_open_is_skipped_when_it_exceeds_maximum(self):
        import copy
        rules=copy.deepcopy(RULES)
        rules['symbols'][0]['filters'][0]['maxQty']='0.01'
        row=order(101,'0.001');row['avgPrice']='100'
        with self.assertRaisesRegex(ValueError,'超过最大数量'):
            plan(row,'open','0','0','1000',BALANCES,[],{'dualSidePosition':False},QUOTE,rules,mark_price='100',leverage='10')

    def test_missing_current_leverage_skips_without_order(self):
        row=order(101,'0.001');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'symbol_config',return_value=[]):
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls,[])
        self.assertIn('杠杆配置',self.rows('SELECT state,error FROM copy_tasks')[0]['error'])

    def test_conflicting_signal_stops_copy_before_dispatch(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        self.api.orders[0]['executedQty']='2';self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],0)
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'cancelled')
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls,[])

class FuturesTransportTests(unittest.TestCase):
    def test_http_451_identifies_endpoint_without_response_body(self):
        import io, urllib.error
        from copy_executor import FuturesClient
        error=urllib.error.HTTPError('https://example.invalid/secret',451,'denied',{},io.BytesIO(b'{"code":0,"msg":"private-secret"}'))
        with patch('copy_executor.urllib.request.urlopen',side_effect=error):
            with self.assertRaises(ExchangeError) as caught:
                FuturesClient('key','secret').balance()
        self.assertIn('HTTP 451',str(caught.exception))
        self.assertIn('/fapi/v3/balance',str(caught.exception))
        self.assertNotIn('private-secret',str(caught.exception))
        self.assertNotIn('example.invalid',str(caught.exception))

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
