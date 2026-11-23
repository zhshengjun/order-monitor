import unittest
from copy import deepcopy
from decimal import Decimal

from manual_close import plan_manual_close

RULES = {'symbols': [{'symbol': 'BTCUSDT', 'status': 'TRADING', 'filters': [
    {'filterType': 'MARKET_LOT_SIZE', 'stepSize': '0.001', 'minQty': '0.001', 'maxQty': '100'}]}]}


class ManualClosePlanningTests(unittest.TestCase):
    def plan(self, percent=20, amount='1.234', side='LONG', hedge=False, rules=RULES):
        ps = side if hedge else 'BOTH'
        return plan_manual_close('BTCUSDT', side, percent,
            [{'symbol': 'BTCUSDT', 'positionSide': ps, 'positionAmt': amount}],
            {'dualSidePosition': hedge}, rules, amount.lstrip('-'))

    def test_one_way_long_and_short_use_reduce_only_and_correct_side(self):
        long = self.plan(20)
        self.assertEqual((long['quantity'], long['order']['side'], long['order']['reduceOnly']), ('0.246', 'SELL', 'true'))
        short = self.plan(50, '-1.234', 'SHORT')
        self.assertEqual((short['quantity'], short['order']['side'], short['order']['positionSide']), ('0.617', 'BUY', 'BOTH'))

    def test_hedge_uses_matching_position_side_without_reduce_only(self):
        long = self.plan(100, '1.234', 'LONG', True)
        short = self.plan(20, '-1.234', 'SHORT', True)
        self.assertEqual((long['order']['side'], long['order']['positionSide'], long['quantity']), ('SELL', 'LONG', '1.234'))
        self.assertEqual((short['order']['side'], short['order']['positionSide']), ('BUY', 'SHORT'))
        self.assertNotIn('reduceOnly', short['order'])

    def test_never_rounds_above_requested_fraction(self):
        p = self.plan(100, '1.2345')
        self.assertEqual((p['quantity'], p['remaining_if_filled']), ('1.234', '0.0005'))
        with self.assertRaisesRegex(ValueError, '最小下单量'):
            self.plan(20, '0.001')

    def test_rejects_wrong_mode_side_duplicate_and_invalid_fraction(self):
        with self.assertRaises(ValueError): self.plan(20, '-1', 'LONG')
        with self.assertRaises(ValueError): self.plan(30)
        with self.assertRaises(ValueError): self.plan(20, 'NaN')
        positions = [{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'1'}] * 2
        with self.assertRaises(ValueError): plan_manual_close('BTCUSDT','LONG',20,positions,{'dualSidePosition':False},RULES,'1')
        with self.assertRaises(ValueError): plan_manual_close('BTCUSDT','LONG',20,positions[:1],{'dualSidePosition':True},RULES,'1')

    def test_rejects_exchange_maximum_or_missing_market_rule(self):
        rules = deepcopy(RULES); rules['symbols'][0]['filters'][0]['maxQty'] = '0.1'
        with self.assertRaisesRegex(ValueError, '上限'): self.plan(100, '1', rules=rules)
        with self.assertRaises(ValueError): self.plan(20, rules={'symbols': []})

    def test_sizes_only_selected_leader_position(self):
        positions=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'3'}]
        p=plan_manual_close('BTCUSDT','LONG',50,positions,{'dualSidePosition':False},RULES,'0.8')
        self.assertEqual((p['position_qty'],p['attributed_qty'],p['quantity'],p['remaining_if_filled']),('3','0.8','0.400','0.4'))
        with self.assertRaisesRegex(ValueError,'超过账户持仓'):
            plan_manual_close('BTCUSDT','LONG',100,positions,{'dualSidePosition':False},RULES,'4')


class FakeManualExchange:
    def __init__(self):
        self.amount = Decimal('3')
        self.requests = []
        self.response = 'filled'
        self.queries = 0
        self.order_result = None

    def positions(self):
        return [{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':str(self.amount)}] if self.amount else []
    def mode(self): return {'dualSidePosition':False}
    def rules(self): return RULES
    def place(self, params):
        from copy_executor import ExchangeError
        self.requests.append(dict(params))
        quantity = Decimal(params['quantity'])
        self.amount -= quantity
        self.order_result = {'symbol':'BTCUSDT','side':'SELL','positionSide':'BOTH','clientOrderId':params['newClientOrderId'],'status':'FILLED','executedQty':str(quantity),'orderId':42}
        if self.response == 'unknown': raise ExchangeError('timeout')
        return dict(self.order_result)
    def order(self, symbol, client_id):
        self.queries += 1
        return dict(self.order_result)


class ManualCloseServiceTests(unittest.TestCase):
    def setUp(self):
        import tempfile, threading
        from pathlib import Path
        from app import Store
        from manual_close import ManualCloseService
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name)/'db')
        self.exchange = FakeManualExchange()
        self.service = ManualCloseService(self.store, threading.RLock(), lambda key, secret:self.exchange)
        with self.store.db() as db:
            db.execute("INSERT INTO futures_accounts(id,name,api_key,api_secret) VALUES(1,'test','key','secret')")
            db.executemany("INSERT INTO targets(id,name) VALUES(?,?)", [('a','Alice'),('b','Bob')])
            db.executemany("INSERT INTO copied_positions VALUES(?,?,?,?,?)", [(1,'a','BTCUSDT','LONG','0.8'),(1,'b','BTCUSDT','LONG','1.2')])
    def tearDown(self): self.tmp.cleanup()
    def quantities(self):
        with self.store.db() as db: return {r['target_id']:r['quantity'] for r in db.execute('SELECT target_id,quantity FROM copied_positions')}

    def test_targeted_close_changes_only_selected_leader_and_is_idempotent(self):
        options = self.service.candidates(1,'BTCUSDT','LONG')
        self.assertEqual([(r['target_id'],r['quantity']) for r in options['leaders']],[('a','0.8'),('b','1.2')])
        self.assertEqual(options['manual_qty'],'1')
        preview = self.service.preview(1,'BTCUSDT','LONG',50,'a')
        self.assertEqual(preview['plan']['quantity'],'0.400')
        result = self.service.submit(1,preview['id'])
        self.assertEqual((result['state'],result['executed_qty']),('finished','0.400'))
        self.assertEqual(self.quantities(),{'a':'0.4','b':'1.2'})
        self.assertEqual(self.service.submit(1,preview['id'])['state'],'finished')
        self.assertEqual(len(self.exchange.requests),1)

    def test_unattributed_close_leaves_leader_records_unchanged(self):
        preview=self.service.preview(1,'BTCUSDT','LONG',100,'')
        self.assertEqual(preview['plan']['quantity'],'1.000')
        self.assertEqual(self.service.submit(1,preview['id'])['state'],'finished')
        self.assertEqual(self.quantities(),{'a':'0.8','b':'1.2'})

    def test_stale_preview_does_not_submit(self):
        preview=self.service.preview(1,'BTCUSDT','LONG',20,'a')
        self.exchange.amount=Decimal('2.5')
        with self.assertRaisesRegex(ValueError,'变化'): self.service.submit(1,preview['id'])
        self.assertEqual(self.exchange.requests,[])

    def test_unknown_result_is_queried_by_original_id_without_resubmission(self):
        self.store.save_settings({'bark_enabled':True,'bark_key':'test-key'})
        self.exchange.response='unknown'
        preview=self.service.preview(1,'BTCUSDT','LONG',20,'a')
        result=self.service.submit(1,preview['id'])
        self.assertEqual(result['state'],'submitted')
        self.assertEqual(self.quantities(),{'a':'0.8','b':'1.2'})
        with self.store.db() as db:
            notices=[r['body'] for r in db.execute("SELECT body FROM events WHERE kind='copy'")]
            self.assertEqual(len(notices),1)
            self.assertIn('手动平仓结果待确认',notices[0])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM outbox WHERE channel='bark'").fetchone()[0],1)
        with self.store.db() as db: db.execute('UPDATE manual_closes SET next_check=0')
        self.service.check_pending()
        self.assertEqual(self.service.status(1,preview['id'])['state'],'finished')
        self.assertEqual(self.quantities(),{'a':'0.64','b':'1.2'})
        self.assertEqual((len(self.exchange.requests),self.exchange.queries),(1,1))
        with self.store.db() as db:
            notices=[r['body'] for r in db.execute("SELECT body FROM events WHERE kind='copy' ORDER BY id")]
            self.assertEqual(len(notices),2)
            self.assertIn('手动平仓结果已确认',notices[1])

    def test_partial_terminal_fill_deducts_only_actual_quantity(self):
        def partial(params):
            self.exchange.requests.append(dict(params))
            self.exchange.amount-=Decimal('0.1')
            return {'status':'EXPIRED','executedQty':'0.1','orderId':99,'symbol':'BTCUSDT'}
        self.exchange.place=partial
        preview=self.service.preview(1,'BTCUSDT','LONG',50,'a')
        result=self.service.submit(1,preview['id'])
        self.assertEqual((result['state'],result['executed_qty']),('finished','0.1'))
        self.assertEqual(self.quantities(),{'a':'0.7','b':'1.2'})

    def test_concurrent_confirmations_do_not_submit_twice(self):
        from concurrent.futures import ThreadPoolExecutor
        import time
        preview=self.service.preview(1,'BTCUSDT','LONG',20,'a')
        original=self.exchange.place
        def delayed(params):
            time.sleep(0.03)
            return original(params)
        self.exchange.place=delayed
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:self.service.submit(1,preview['id']),range(2)))
        self.assertTrue(all(x['state'] in ('submitted','finished') for x in results))
        self.assertEqual(self.service.status(1,preview['id'])['state'],'finished')
        self.assertEqual(len(self.exchange.requests),1)

    def test_slow_manual_order_does_not_hold_follow_lock(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        entered,release=threading.Event(),threading.Event()
        original=self.exchange.place
        def delayed(params):
            entered.set()
            if not release.wait(3): raise RuntimeError('test timed out')
            return original(params)
        self.exchange.place=delayed
        preview=self.service.preview(1,'BTCUSDT','LONG',20,'a')
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(self.service.submit,1,preview['id'])
            try:
                self.assertTrue(entered.wait(2))
                self.assertTrue(self.service.lock.acquire(timeout=1))
                try:
                    with self.store.db() as db:
                        self.assertEqual(db.execute('SELECT state FROM manual_closes WHERE id=?',(preview['id'],)).fetchone()['state'],'submitted')
                        db.execute("UPDATE copied_positions SET quantity='0.7' WHERE target_id='a'")
                        self.exchange.amount-=Decimal('0.1')
                finally:
                    self.service.lock.release()
            finally:
                release.set()
            self.assertEqual(future.result(timeout=3)['state'],'finished')
        self.assertEqual(self.quantities(),{'a':'0.54','b':'1.2'})

    def test_invalid_execution_response_blocks_without_modifying_tracking(self):
        self.exchange.place=lambda params:{'status':'FILLED','executedQty':'2','orderId':99}
        preview=self.service.preview(1,'BTCUSDT','LONG',20,'a')
        result=self.service.submit(1,preview['id'])
        self.assertEqual(result['state'],'needs_review')
        self.assertEqual(self.quantities(),{'a':'0.8','b':'1.2'})

    def test_conflicting_positions_or_other_unsettled_order_blocks(self):
        self.exchange.amount=Decimal('1')
        with self.assertRaisesRegex(ValueError,'超过账户实际持仓'): self.service.candidates(1,'BTCUSDT','LONG')
        self.exchange.amount=Decimal('3')
        with self.store.db() as db:
            db.execute("INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,created,state) VALUES(1,'a','x','{}','open','0','1','busy',1,'submitted')")
        with self.assertRaisesRegex(ValueError,'待确认'): self.service.candidates(1,'BTCUSDT','LONG')

    def test_unexpected_exchange_position_does_not_change_ledger(self):
        original=self.exchange.place
        def place_and_external_change(params):
            result=original(params);self.exchange.amount-=Decimal('1.1');return result
        self.exchange.place=place_and_external_change
        preview=self.service.preview(1,'BTCUSDT','LONG',20,'a')
        result=self.service.submit(1,preview['id'])
        self.assertEqual(result['state'],'submitted')
        self.assertEqual(self.quantities(),{'a':'0.8','b':'1.2'})
        self.assertIn('核对',result['error'])
