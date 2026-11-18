import tempfile
import time
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
        self.monitor.now_ms=lambda:100
        with self.store.db() as db:
            db.execute('INSERT INTO futures_accounts VALUES(1,?,?,?)',('test','key','secret'))
            db.execute('INSERT INTO copy_bindings VALUES(?,?)',(1,PID))
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

    def test_timing_links_signal_execution_and_notification(self):
        self.store.save_settings({'bark_enabled':True, 'bark_key':'private-device-key'})
        row=order(101,'1'); row['avgPrice']='100'; self.api.orders.insert(0,row)
        with self.assertLogs('monitor',level='INFO') as logs:
            self.monitor.poll(PID)
            with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
            with patch('app.send_notification'): self.monitor.deliver()
        output='\n'.join(logs.output)
        for stage in ('leader_detail','order_collect','poll_apply','copy_enqueued','copy_dequeued',
                      'copy_preflight','copy_submit','copy_response','copy_finalize','copy_attempt','notification_send'):
            self.assertIn('stage='+stage,output)
        for stage in ('copy_enqueued','copy_preflight','copy_submit','copy_response','notification_send'):
            self.assertTrue(any('stage='+stage+' ' in line and 'task=1 ' in line for line in logs.output),stage)
        self.assertIn('source_to_enqueue_ms=',output)
        self.assertIn('exchange_update_ms=unknown',output)
        self.assertNotIn('private-device-key',output)
        self.assertEqual([call[0] for call in FakeExchange.calls],['place'])

    def test_first_execution_skips_stale_open_without_placing_order(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        self.monitor.now_ms=lambda:180102
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        task=self.rows('SELECT state,error FROM copy_tasks')[0]
        self.assertEqual(task['state'],'skipped')
        self.assertIn('超过 3 分钟',task['error'])
        self.assertEqual(FakeExchange.calls,[])

    def test_pending_stale_close_is_not_placed(self):
        row=order(101,'1');row.update(side='SELL',avgPrice='100');self.api.orders.insert(0,row);self.monitor.poll(PID)
        self.monitor.now_ms=lambda:180102
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        task=self.rows('SELECT state,error FROM copy_tasks')[0]
        self.assertEqual(task['state'],'skipped')
        self.assertIn('超过 3 分钟',task['error'])
        self.assertEqual(FakeExchange.calls,[])

    def test_same_order_in_page_creates_one_aggregated_task(self):
        self.store.save_settings({'page_size':20})
        fills=[]
        for i in range(1,8):
            row=order(101,str(i));row['avgPrice']=str(100+i);fills.append(row)
        self.api.orders=fills+[order(100)]
        self.monitor.poll(PID);self.monitor.poll(PID)
        tasks=self.rows('SELECT * FROM copy_tasks')
        self.assertEqual(len(tasks),1)
        import json
        row=json.loads(tasks[0]['body'])
        self.assertEqual(row['executedQty'],'28')
        self.assertEqual(row['avgPrice'],'105')
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],1)
        self.assertEqual(len(self.rows('SELECT * FROM leader_history WHERE stamp=101')),7)

    def test_order_type_change_does_not_replay_previous_fill(self):
        row=order(101,'1');row['avgPrice']='100';row['type']='MARKET';self.api.orders.insert(0,row);self.monitor.poll(PID)
        later=order(101,'2');later['avgPrice']='100';later['type']='LIMIT';self.api.orders.insert(0,later);self.monitor.poll(PID)
        import json
        tasks=[json.loads(x['body']) for x in self.rows('SELECT body FROM copy_tasks ORDER BY id')]
        self.assertEqual([x['executedQty'] for x in tasks],['1','2'])

    def test_equal_quantity_different_amount_is_followed_as_increment(self):
        import json
        first=order(101,'1');first['avgPrice']='100';self.api.orders.insert(0,first);self.monitor.poll(PID)
        second=order(101,'1');second['avgPrice']='101';self.api.orders.insert(0,second);self.monitor.poll(PID)
        tasks=[json.loads(x['body']) for x in self.rows('SELECT body FROM copy_tasks ORDER BY id')]
        self.assertEqual([x['executedQty'] for x in tasks],['1','1'])
        self.assertEqual(tasks[1]['notional'],'101')

    def test_existing_legacy_order_key_is_reused(self):
        import hashlib, json
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        old_key=hashlib.sha256(json.dumps([row['symbol'],row['side'],row['positionSide'],row['type'],row['orderTime']],separators=(',',':')).encode()).hexdigest()
        with self.store.db() as db:
            db.execute('UPDATE records SET key=? WHERE target=? AND stamp=101',(old_key,PID))
            db.execute('UPDATE virtual_events SET order_key=? WHERE target_id=? AND stamp=101',(old_key,PID))
            db.execute('UPDATE copy_tasks SET order_key=? WHERE target_id=?',(old_key,PID))
        later=order(101,'2');later['avgPrice']='100';later['type']='LIMIT';self.api.orders.insert(0,later);self.monitor.poll(PID)
        tasks=self.rows('SELECT order_key,body FROM copy_tasks ORDER BY id')
        self.assertEqual([json.loads(x['body'])['executedQty'] for x in tasks],['1','2'])
        self.assertEqual(tasks[1]['order_key'],old_key+':3')

    def test_increment_does_not_repeat_minimum_order(self):
        first=order(101,'0.001');first['avgPrice']='100';self.api.orders.insert(0,first);self.monitor.poll(PID)
        FakeExchange.result={'status':'FILLED','executedQty':'0.05','orderId':1}
        try:
            with patch('app.FuturesClient',FakeExchange):
                self.monitor.execute_copy()
                later=order(101,'0.002');later['avgPrice']='100';self.api.orders.insert(0,later);self.monitor.poll(PID)
                self.monitor.execute_copy()
            tasks=self.rows('SELECT state,error FROM copy_tasks ORDER BY id')
            self.assertEqual([t['state'] for t in tasks],['filled','skipped'])
            self.assertIn('累计跟单数量已达到目标',tasks[1]['error'])
            self.assertEqual(len(FakeExchange.calls),1)
        finally:
            FakeExchange.result={'status':'FILLED','executedQty':'0.999','orderId':1}

    def test_detail_failure_keeps_monitoring_and_recovers_balance(self):
        self.api.fail=True
        stamp=int(time.time()*1000);self.monitor.now_ms=lambda:stamp
        row=order(stamp,'1');row['avgPrice']='100';self.api.orders.insert(0,row)
        self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT lead_balance FROM copy_tasks')[0]['lead_balance'],'')
        self.assertEqual(len(self.rows('SELECT * FROM events WHERE stamp=%s' % stamp)),1)
        with patch('app.FuturesClient',FakeExchange):
            self.monitor.execute_copy()
            self.assertEqual(FakeExchange.calls,[])
            self.api.fail=False
            with self.store.db() as db: db.execute('UPDATE copy_tasks SET next_check=0')
            self.monitor.execute_copy()
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'filled')

    def test_first_poll_follows_only_trades_after_enable(self):
        with self.store.db() as db:
            db.execute('UPDATE targets SET initialized=0 WHERE id=?',(PID,))
            db.execute('DELETE FROM records WHERE target=?',(PID,))
            db.execute('UPDATE copy_live SET started_at=100 WHERE target_id=?',(PID,))
            db.execute('UPDATE virtual_sessions SET started_at=100 WHERE target_id=?',(PID,))
        self.api.orders=[order(x,'1') for x in range(125,98,-1)]
        self.monitor.poll(PID)
        tasks=self.rows('SELECT body FROM copy_tasks')
        import json
        self.assertEqual(len(tasks),25)
        self.assertEqual(min(json.loads(x['body'])['orderTime'] for x in tasks),101)
        self.assertIn(('order',2),self.api.calls)

    def test_enabled_copy_repairs_inactive_session_and_queues_order(self):
        with self.store.db() as db: db.execute('UPDATE virtual_sessions SET active=0 WHERE account_id=1 AND target_id=?',(PID,))
        row=order(101,'5.46');row.update(symbol='ZECUSDT',side='SELL',positionSide='SHORT',avgPrice='952.20873')
        self.api.orders.insert(0,row)
        with self.assertLogs('monitor',level='INFO') as captured: self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT active FROM virtual_sessions')[0]['active'],1)
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'pending')
        self.assertTrue(any('copy state repaired' in line for line in captured.output))
        self.assertTrue(any('result=queued' in line for line in captured.output))

    def test_copy_switch_survives_store_restart(self):
        monitor=Monitor(Store(self.store.path),self.api)
        monitor.now_ms=lambda:100
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row)
        with self.assertLogs('monitor',level='INFO') as captured: monitor.poll(PID)
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'pending')
        self.assertTrue(any('result=queued' in line for line in captured.output))

    def test_missing_binding_marks_task_skipped_and_reports_reason(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with self.store.db() as db: db.execute('DELETE FROM copy_bindings WHERE account_id=1 AND target_id=?',(PID,))
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        task=self.rows('SELECT state,error FROM copy_tasks')[0]
        self.assertEqual(task['state'],'skipped')
        self.assertIn('关联或开关状态不一致',task['error'])
        self.assertEqual(FakeExchange.calls,[])
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

    def test_definitive_rejection_does_not_block_account(self):
        row=order(101,'1'); row['avgPrice']='100'; self.api.orders.insert(0,row); self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'place',side_effect=ExchangeError('insufficient margin',400,-2019)):
            self.monitor.execute_copy()
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'skipped')

    def test_cancelled_task_is_not_submitted(self):
        row=order(101,'1'); row['avgPrice']='100'; self.api.orders.insert(0,row); self.monitor.poll(PID)
        def cancel(_symbol):
            with self.store.db() as db:
                db.execute("UPDATE copy_tasks SET state='cancelled' WHERE state='pending'")
            return QUOTE
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'quote',side_effect=cancel):
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls,[])
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'cancelled')

    def test_price_is_fetched_after_rules(self):
        row=order(101,'1'); row['avgPrice']='100'; self.api.orders.insert(0,row); self.monitor.poll(PID)
        calls=[]
        def rules(): calls.append('rules'); return RULES
        def quote(_): calls.append('quote'); return {'bidPrice':'119.9','askPrice':'120'}
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'rules',side_effect=rules), patch.object(FakeExchange,'quote',side_effect=quote):
            self.monitor.execute_copy()
        self.assertEqual(calls,['rules','quote'])
        self.assertEqual(FakeExchange.calls,[])
        self.assertIn('3%',self.rows('SELECT error FROM copy_tasks')[0]['error'])

    def test_attributed_position_exceeding_actual_is_not_closed(self):
        row=order(101,'1'); row['avgPrice']='100'; self.api.orders.insert(0,row); self.monitor.poll(PID)
        with self.store.db() as db:
            db.execute('INSERT INTO copied_positions VALUES(?,?,?,?,?)',(1,PID,'BTCUSDT','LONG','2'))
            db.execute("UPDATE copy_tasks SET action='close',before_qty='2'")
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls,[])
        self.assertIn('超过账户实际持仓',self.rows('SELECT error FROM copy_tasks')[0]['error'])

    def test_late_same_symbol_signal_is_not_applied_out_of_order(self):
        newer=order(102,'1'); newer['avgPrice']='100'; self.api.orders.insert(0,newer); self.monitor.poll(PID)
        late=order(101,'1'); late['avgPrice']='100'; self.api.orders.insert(1,late); self.monitor.poll(PID)
        self.assertEqual(len(self.rows('SELECT * FROM copy_tasks')),1)
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'cancelled')
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],0)
        self.assertIn('延迟到达',self.rows("SELECT body FROM events WHERE kind='copy'")[0]['body'])
    def test_http_451_precheck_reports_failed_interface_without_placing(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        reason='HTTP 451; code 0; 接口 /fapi/v3/balance；请检查容器代理与合约 API 出口'
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',side_effect=ExchangeError(reason)):
            self.monitor.execute_copy()
        task=self.rows('SELECT state,error,retry_count FROM copy_tasks')[0]
        self.assertEqual(task['state'],'pending');self.assertEqual(task['retry_count'],1);self.assertIn('/fapi/v3/balance',task['error'])
        self.assertEqual(FakeExchange.calls,[])

    def test_precheck_retries_three_times_then_stops(self):
        row=order(int(time.time()*1000),'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',side_effect=ExchangeError('HTTP 451',451,0)) as balance:
            for _ in range(4):
                with self.store.db() as db: db.execute('UPDATE copy_tasks SET next_check=0')
                self.monitor.execute_copy()
        task=self.rows('SELECT state,error,retry_count FROM copy_tasks')[0]
        self.assertEqual(balance.call_count,4)
        self.assertEqual((task['state'],task['retry_count']),('skipped',3))
        self.assertIn('重试 3 次',task['error'])
        self.assertEqual(FakeExchange.calls,[])

    def test_precheck_network_recovery_retries_original_task(self):
        row=order(int(time.time()*1000),'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',side_effect=[ExchangeError('timeout'),BALANCES]):
            self.monitor.execute_copy()
            with self.store.db() as db: db.execute('UPDATE copy_tasks SET next_check=0')
            self.monitor.execute_copy()
        self.assertEqual(self.rows('SELECT state,retry_count FROM copy_tasks')[0]['state'],'filled')
        self.assertEqual([x[0] for x in FakeExchange.calls],['place'])

    def test_retry_open_skips_if_leader_subsequently_closes(self):
        stamp=int(time.time()*1000);row=order(stamp,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',side_effect=ExchangeError('HTTP 451',451,0)):
            self.monitor.execute_copy()
        close=order(stamp+1,'0.5');close['avgPrice']='100';close['side']='SELL';self.api.orders.insert(0,close);self.monitor.poll(PID)
        with self.store.db() as db: db.execute('UPDATE copy_tasks SET next_check=0')
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        first=self.rows('SELECT state,error FROM copy_tasks ORDER BY id')[0]
        self.assertEqual(first['state'],'skipped')
        self.assertIn('平仓或减仓',first['error'])
        self.assertEqual(FakeExchange.calls,[])

    def test_retry_open_skips_when_source_order_older_than_three_minutes(self):
        row=order(int(time.time()*1000)-181000,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',side_effect=ExchangeError('HTTP 451',451,0)):
            self.monitor.execute_copy()
        with self.store.db() as db: db.execute('UPDATE copy_tasks SET next_check=0')
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        task=self.rows('SELECT state,error FROM copy_tasks')[0]
        self.assertEqual(task['state'],'skipped')
        self.assertIn('超过 3 分钟',task['error'])
        self.assertEqual(FakeExchange.calls,[])

    def test_precheck_retry_waits_and_blocks_later_same_account(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        row2=order(102,'1');row2['avgPrice']='100';self.api.orders.insert(0,row2);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'balance',side_effect=ExchangeError('timeout')) as balance:
            self.monitor.execute_copy();self.monitor.execute_copy()
        self.assertEqual(balance.call_count,1)
        self.assertEqual([x['state'] for x in self.rows('SELECT state FROM copy_tasks ORDER BY id')],['pending','pending'])

    def test_http_451_on_submit_keeps_pending_reconciliation(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'place',side_effect=ExchangeError('HTTP 451; 接口 /fapi/v1/order')) as place:
            self.monitor.execute_copy(); self.monitor.execute_copy()
        task=self.rows('SELECT state,error FROM copy_tasks')[0]
        self.assertEqual(task['state'],'submitted');self.assertIn('/fapi/v1/order',task['error'])
        self.assertEqual(place.call_count,1)

    def test_close_without_position_skips_without_retry_or_balance_query(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with self.store.db() as db: db.execute("UPDATE copy_tasks SET action='close'")
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'positions',return_value=[]), patch.object(FakeExchange,'balance') as balance:
            self.monitor.execute_copy()
        balance.assert_not_called()
        task=self.rows('SELECT state,error,retry_count FROM copy_tasks')[0]
        self.assertEqual((task['state'],task['retry_count']),('skipped',0))
        self.assertIn('没有对应方向持仓',task['error'])
        self.assertEqual(FakeExchange.calls,[])

    def test_unmatched_close_uses_actual_position_and_balance_ratio(self):
        row=order(101,'1');row['avgPrice']='100';row['side']='SELL';self.api.orders=[row];self.monitor.poll(PID)
        positions=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'2'}]
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'positions',return_value=positions), patch.object(FakeExchange,'result',{'status':'FILLED','executedQty':'1.001','orderId':1}):
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls[0][1]['quantity'],'1.001')
        self.assertEqual(FakeExchange.calls[0][1]['reduceOnly'],'true')
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'filled')

    def test_unmatched_close_never_exceeds_real_position(self):
        row=order(101,'100');row['avgPrice']='100';row['side']='SELL'
        positions=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'2'}]
        result=plan(row,'unmatched_close','0','0','100',BALANCES,positions,{'dualSidePosition':False},QUOTE,RULES)
        self.assertEqual(result['quantity'],'2.000')
        self.assertEqual(result['reduceOnly'],'true')

    def test_unmatched_close_uses_replayed_leader_reduction_ratio(self):
        import json
        from app import identity
        leader_open=order(99,'4');leader_open['avgPrice']='100'
        with self.store.db() as db:
            db.execute('DELETE FROM leader_history WHERE target=?',(PID,))
            db.execute('INSERT INTO leader_history VALUES(?,?,?,?)',(PID,identity('order',leader_open),99,json.dumps(leader_open)))
            db.execute('INSERT INTO leader_history_sync(target,complete) VALUES(?,1)',(PID,))
        row=order(101,'1');row['avgPrice']='100';row['side']='SELL';self.api.orders=[row];self.monitor.poll(PID)
        positions=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'2'}]
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'positions',return_value=positions), patch.object(FakeExchange,'result',{'status':'FILLED','executedQty':'0.5','orderId':1}):
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls[0][1]['quantity'],'0.500')

    def test_unmatched_close_without_position_is_skipped(self):
        row=order(101,'1');row['avgPrice']='100';row['side']='SELL';self.api.orders.insert(0,row);self.monitor.poll(PID)
        with patch('app.FuturesClient',FakeExchange), patch.object(FakeExchange,'positions',return_value=[]):
            self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls,[])
        self.assertIn('没有对应方向持仓',self.rows('SELECT error FROM copy_tasks')[0]['error'])
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

    def test_margin_check_uses_ten_or_actual_lower_leverage(self):
        row=order(101,'1');row['avgPrice']='100'
        balances=[{'asset':'USDT','balance':'100','availableBalance':'7'}]
        with self.assertRaisesRegex(ValueError,'可用余额'):
            plan(row,'open','0','0','100',balances,[],{'dualSidePosition':False},QUOTE,RULES,mark_price='100',leverage='20')
        balances[0]['availableBalance']='12'
        self.assertEqual(plan(row,'open','0','0','100',balances,[],{'dualSidePosition':False},QUOTE,RULES,mark_price='100',leverage='20')['quantity'],'0.999')
        with self.assertRaisesRegex(ValueError,'可用余额'):
            plan(row,'open','0','0','100',balances,[],{'dualSidePosition':False},QUOTE,RULES,mark_price='100',leverage='5')

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

    def test_split_fills_across_polls_follow_increment(self):
        import json
        row=order(101,'0.4');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],1)
        self.assertEqual(self.rows('SELECT quantity FROM virtual_positions')[0]['quantity'],'0.4')
        self.api.orders[0]['executedQty']='1';self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],1)
        tasks=self.rows('SELECT * FROM copy_tasks ORDER BY id')
        self.assertEqual([json.loads(t['body'])['executedQty'] for t in tasks],['0.4','0.6'])
        self.assertEqual([json.loads(t['body'])['avgPrice'] for t in tasks],['100','100'])
        self.assertEqual(self.rows('SELECT quantity FROM virtual_positions')[0]['quantity'],'1')
        self.assertEqual(self.rows("SELECT COUNT(*) AS n FROM copy_tasks WHERE state!='pending'")[0]['n'],0)
        self.assertEqual(self.rows("SELECT COUNT(*) AS n FROM events WHERE kind='copy'")[0]['n'],0)
        self.monitor.poll(PID)
        self.assertEqual(len(self.rows('SELECT * FROM copy_tasks')),2)

    def test_split_close_across_polls_keeps_tracked_quantity(self):
        import json
        open_row=order(101,'1');open_row['avgPrice']='100';self.api.orders.insert(0,open_row);self.monitor.poll(PID)
        close_row=order(102,'0.4');close_row.update(side='SELL',avgPrice='100');self.api.orders.insert(0,close_row);self.monitor.poll(PID)
        close_row['executedQty']='1';self.monitor.poll(PID)
        tasks=self.rows('SELECT * FROM copy_tasks ORDER BY id')
        self.assertEqual([t['action'] for t in tasks],['open','close','close'])
        self.assertEqual([t['before_qty'] for t in tasks],['0','1','0.6'])
        self.assertEqual([json.loads(t['body'])['executedQty'] for t in tasks],['1','0.4','0.6'])
        self.assertEqual(self.rows('SELECT quantity FROM virtual_positions')[0]['quantity'],'0')
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],1)

    def test_incremental_close_skips_whole_order_replay(self):
        import json
        open_row=order(101,'1');open_row['avgPrice']='100';self.api.orders.insert(0,open_row);self.monitor.poll(PID)
        close_row=order(102,'0.4');close_row.update(side='SELL',avgPrice='100');self.api.orders.insert(0,close_row);self.monitor.poll(PID)
        close_row['executedQty']='1';self.monitor.poll(PID)
        with self.store.db() as db:
            db.execute("INSERT OR REPLACE INTO leader_history_sync(target,complete) VALUES(?,1)",(PID,))
            db.execute('INSERT OR REPLACE INTO leader_history VALUES(?,?,?,?)',(PID,'h1',101,json.dumps(open_row)))
            db.execute('INSERT OR REPLACE INTO leader_history VALUES(?,?,?,?)',(PID,'h2',102,json.dumps(close_row)))
            db.execute("UPDATE copy_tasks SET state='filled' WHERE action='open' OR before_qty='1'")
            db.execute('INSERT OR REPLACE INTO copied_positions VALUES(?,?,?,?,?)',(1,PID,'BTCUSDT','LONG','0.6'))
        task=self.rows("SELECT * FROM copy_tasks WHERE state='pending'")[0]
        self.assertTrue(json.loads(task['body'])['incremental'])
        self.assertEqual(task['before_qty'],'0.6')
        FakeExchange.result={'status':'FILLED','executedQty':'0.6','orderId':1}
        try:
            with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        finally:
            FakeExchange.result={'status':'FILLED','executedQty':'0.999','orderId':1}
        places=[c[1] for c in FakeExchange.calls if c[0]=='place']
        self.assertEqual(len(places),1)
        self.assertEqual(places[0]['quantity'],'0.600')
        self.assertEqual(self.rows("SELECT state FROM copy_tasks WHERE id=%d" % task['id'])[0]['state'],'filled')

    def test_quantity_regression_stops_copy_before_dispatch(self):
        row=order(101,'2');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        self.api.orders[0]['executedQty']='1';self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],0)
        self.assertEqual(self.rows('SELECT state FROM copy_tasks')[0]['state'],'cancelled')
        with patch('app.FuturesClient',FakeExchange): self.monitor.execute_copy()
        self.assertEqual(FakeExchange.calls,[])

    def test_price_revision_without_quantity_change_keeps_copy(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        self.api.orders[0]['avgPrice']='101';self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],1)
        self.assertEqual(len(self.rows('SELECT * FROM copy_tasks')),1)
        import json
        self.assertEqual(json.loads(self.rows('SELECT body FROM records ORDER BY stamp DESC LIMIT 1')[0]['body'])['avgPrice'],'101')

    def test_decimal_formatting_change_is_not_a_regression(self):
        row=order(101,'1');row['avgPrice']='100';self.api.orders.insert(0,row);self.monitor.poll(PID)
        self.api.orders[0]['executedQty']='1.00000000';self.monitor.poll(PID)
        self.assertEqual(self.rows('SELECT enabled FROM copy_live')[0]['enabled'],1)
        self.assertEqual(len(self.rows('SELECT * FROM copy_tasks')),1)

class FuturesTransportTests(unittest.TestCase):
    def test_exchange_rules_cache_expires(self):
        from copy_executor import FuturesClient
        client=FuturesClient('key','secret')
        with patch.object(FuturesClient,'_rules_value',None), patch.object(FuturesClient,'_rules_until',0), \
             patch('copy_executor.time.monotonic',side_effect=[1,1,2,302,302]), \
             patch.object(client,'request',side_effect=[RULES,RULES]) as request:
            self.assertEqual(client.rules(),RULES)
            self.assertEqual(client.rules(),RULES)
            self.assertEqual(client.rules(),RULES)
            self.assertEqual(request.call_count,2)

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
