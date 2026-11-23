import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from app import Store, Monitor, handler_factory
from test_monitor import FakeAPI


class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.store = Store(Path(cls.temp.name) / 'http.db')
        cls.store.save_settings({'telegram_token': 'not-for-response', 'bark_key': 'private-key'})
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), handler_factory(cls.store, Monitor(cls.store, FakeAPI()), 'test-access-token-123456'))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = 'http://127.0.0.1:' + str(cls.server.server_port)
        request = urllib.request.Request(cls.url + '/api/login', data=json.dumps({'token': 'test-access-token-123456'}).encode(), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request) as response:
            cls.cookie = response.headers['Set-Cookie'].split(';')[0]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()
        cls.temp.cleanup()

    def request(self, path, data=None, authenticated=True, headers=None):
        hs = {'Content-Type': 'application/json'}
        if authenticated:
            hs['Cookie'] = self.cookie
        hs.update(headers or {})
        req = urllib.request.Request(self.url + path, headers=hs, data=json.dumps(data).encode() if data is not None else None)
        try:
            response = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            response = e
        with response:
            return response.status, json.loads(response.read())

    def test_copy_switch_is_explicit_and_starts_zero_baseline(self):
        from app import FuturesClient
        with self.store.db() as db:
            db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('copy-test','key','secret')")
            account_id=db.execute("SELECT id FROM futures_accounts WHERE name='copy-test'").fetchone()[0]
        self.assertEqual(self.request('/api/targets', {'id':'4321'})[0],200)
        self.assertEqual(self.request('/api/copy-binding',{'account_id':account_id,'target_id':'4321','selected':True})[0],200)
        self.assertEqual(self.request('/api/copy-live',{'account_id':account_id,'target_id':'4321','enabled':True})[0],200)
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT enabled FROM copy_live WHERE account_id=?',(account_id,)).fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM virtual_positions WHERE account_id=?',(account_id,)).fetchone()[0],0)
            before=db.execute('SELECT started_at FROM copy_live WHERE account_id=?',(account_id,)).fetchone()[0]
        self.assertEqual(self.request('/api/copy-live',{'account_id':account_id,'target_id':'4321','enabled':True})[0],200)
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT started_at FROM copy_live WHERE account_id=?',(account_id,)).fetchone()[0],before)
        self.assertEqual(self.request('/api/copy-live',{'account_id':account_id,'target_id':'4321','enabled':False})[0],200)
        with self.store.db() as db:
            db.execute("INSERT INTO copied_positions VALUES(?,?,?,?,?)",(account_id,'4321','BTCUSDT','LONG','1'))
            db.execute("INSERT INTO virtual_positions VALUES(?,?,?,?,?)",(account_id,'4321','BTCUSDT','LONG','10'))
        self.assertEqual(self.request('/api/copy-live',{'account_id':account_id,'target_id':'4321','enabled':True})[0],200)
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT quantity FROM virtual_positions WHERE account_id=?',(account_id,)).fetchone()[0],'10')
            self.assertEqual(db.execute('SELECT quantity FROM copied_positions WHERE account_id=?',(account_id,)).fetchone()[0],'1')

    def test_account_copy_records_are_scoped_and_paginated(self):
        with self.store.db() as db:
            db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('records-a','private-a','private-secret-a')")
            a=db.execute("SELECT id FROM futures_accounts WHERE name='records-a'").fetchone()[0]
            db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('records-b','private-b','private-secret-b')")
            b=db.execute("SELECT id FROM futures_accounts WHERE name='records-b'").fetchone()[0]
            for account,count in ((a,52),(b,1)):
                for i in range(count):
                    db.execute("INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,created) VALUES(?,?,?,?,?,?,?,?,?)",(account,'123',str(i),json.dumps({'symbol':'BTCUSDT'}),'open','0','100',str(account)+'-'+str(i),i))
        with self.store.db() as db:
            db.execute("UPDATE copy_tasks SET result=? WHERE account_id=? AND order_key='51'", (json.dumps({'orderId':1234567890123456789}),a))
            task_id=db.execute("SELECT id FROM copy_tasks WHERE account_id=? AND order_key='51'", (a,)).fetchone()[0]
            db.execute('INSERT INTO copy_task_audit VALUES(?,?,?,?,?,?,?,?)',(task_id,123,'USDT','100','20','1000','100','0.1'))
        status,first=self.request('/api/copy-tasks?account_id=%s&page=1'%a)
        self.assertEqual(status,200);self.assertEqual(first['total'],52);self.assertEqual(len(first['list']),50)
        self.assertEqual(first['list'][0]['result']['orderId'],'1234567890123456789')
        self.assertEqual((first['list'][0]['wallet_balance'],first['list'][0]['available_balance']),('100','20'))
        status,second=self.request('/api/copy-tasks?account_id=%s&page=2'%a)
        self.assertEqual(status,200);self.assertEqual(len(second['list']),2)
        status,other=self.request('/api/copy-tasks?account_id=%s&page=1'%b)
        self.assertEqual(other['total'],1)
        self.assertNotIn('private-secret',json.dumps(first))
        self.assertEqual(self.request('/api/copy-tasks?account_id=%s'%a,authenticated=False)[0],401)

    def test_leader_records_exclude_copy_notifications(self):
        with self.store.db() as db:
            for kind in ('order','transfer','copy'):
                db.execute('INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,?,?,?,0)',('separate','leader',kind,json.dumps({'side':'BUY','positionSide':'LONG'}),1))
        status,data=self.request('/api/events?target=separate&kind=leader')
        self.assertEqual(status,200);self.assertEqual({x['kind'] for x in data['list']},{'order','transfer'})

    def test_copy_record_pnl_is_order_scoped_and_validated(self):
        with self.store.db() as db:
            db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('pnl-test','key','secret')")
            account=db.execute("SELECT id FROM futures_accounts WHERE name='pnl-test'").fetchone()[0]
            db.execute("INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,created,state,result) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(account,'pnl-leader','pnl-order',json.dumps({'symbol':'BTCUSDT','quoteAsset':'USDT'}),'close','1','100','pnl-client',1,'filled',json.dumps({'orderId':42,'executedQty':'1'})))
            task=db.execute("SELECT id FROM copy_tasks WHERE client_id='pnl-client'").fetchone()[0]
        trades=[{'id':1,'symbol':'BTCUSDT','orderId':42,'qty':'1','realizedPnl':'2.5'}]
        with patch('app.FuturesClient') as client:
            client.return_value.trades.return_value=trades
            status,data=self.request('/api/copy-task-pnl?account_id=%s&task_id=%s'%(account,task))
            self.assertEqual((status,data['pnl'],data['asset']),(200,'2.5','USDT'))
            client.return_value.trades.assert_called_once_with('BTCUSDT',42)
            client.return_value.trades.return_value=[{**trades[0], 'qty':'0.5'}]
            self.assertIsNone(self.request('/api/copy-task-pnl?account_id=%s&task_id=%s'%(account,task))[1]['pnl'])
            self.assertNotEqual(self.request('/api/copy-task-pnl?account_id=0&task_id=%s'%task)[0],200)
            self.assertEqual(self.request('/api/copy-task-pnl?account_id=%s&task_id=%s'%(account,task),authenticated=False)[0],401)

    def test_leader_mark_prices_are_public_validated_and_fail_soft(self):
        from copy_executor import ExchangeError
        with patch('app.FuturesClient') as client:
            client.return_value.request.return_value=[{'symbol':'BTCUSDT','markPrice':'100.5'},{'symbol':'ETHUSDT','markPrice':'NaN'}]
            status,data=self.request('/api/leader-mark-prices')
            self.assertEqual((status,data['prices']),(200,{'BTCUSDT':'100.5'}))
            client.return_value.request.assert_called_once_with('/fapi/v1/premiumIndex')
            client.return_value.request.side_effect=ExchangeError('offline')
            self.assertEqual(self.request('/api/leader-mark-prices')[1]['prices'],{})
            self.assertEqual(self.request('/api/leader-mark-prices',authenticated=False)[0],401)

    def test_leader_record_pages_use_ten(self):
        with self.store.db() as db:
            for i in range(21):
                db.execute('INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,?,?,?,0)',('pages','leader','transfer','{}',i))
        status,first=self.request('/api/events?target=pages&kind=leader&limit=10&page=1')
        self.assertEqual(status,200);self.assertEqual(first['total'],21);self.assertEqual(len(first['list']),10)
        _,third=self.request('/api/events?target=pages&kind=leader&limit=10&page=3')
        self.assertEqual(len(third['list']),1)

    def test_synced_leader_history_appears_in_records_without_duplicate_events(self):
        from app import history_identity
        from test_monitor import order
        pid='history-visible';a=order(100,'2');b=order(101,'1');c=order(101,'2')
        with self.store.db() as db:
            db.execute('INSERT INTO targets(id,name) VALUES(?,?)',(pid,'leader'))
            for row in (a,b,c):
                db.execute('INSERT INTO leader_history VALUES(?,?,?,?)',(pid,history_identity(row),row['orderTime'],json.dumps(row)))
            db.execute('INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,?,?,?,0)',(pid,'leader','order',json.dumps(b),101))
            db.execute('INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,?,?,?,0)',(pid,'leader','transfer','{}',102))
        status,data=self.request('/api/events?target=history-visible&kind=leader&limit=10')
        self.assertEqual(status,200);self.assertEqual(data['total'],4)
        self.assertEqual([x['stamp'] for x in data['list']],[102,101,101,100])
        self.assertEqual(sorted(x['body']['executedQty'] for x in data['list'] if x['stamp']==101),['1','2'])
        _,orders=self.request('/api/events?target=history-visible&kind=order&limit=10')
        self.assertEqual(orders['total'],3)
        _,transfers=self.request('/api/events?target=history-visible&kind=transfer&limit=10')
        self.assertEqual(transfers['total'],1)

    def test_snapshot_shows_exchange_leverage_and_keeps_positions_on_config_error(self):
        from app import UpstreamError
        with self.store.db() as db:
            account = db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('leverage-test','key','secret')").lastrowid
        class FakeFutures:
            fail = False
            def __init__(self, *args): pass
            def get(self, path):
                if path.endswith('/balance'): return [{'asset': 'USDT', 'balance': '10'}]
                if path.endswith('/positionRisk'): return [{'symbol': 'BTCUSDT', 'positionAmt': '0.1'}, {'symbol': 'ETHUSDT', 'positionAmt': '1'}]
                if self.fail: raise UpstreamError('unavailable')
                return [{'symbol': 'BTCUSDT', 'leverage': 20}, {'symbol': 'ETHUSDT', 'leverage': '5'}]
        with patch('app.FuturesAccount', FakeFutures):
            status, result = self.request('/api/futures-snapshot?account_id=%s' % account)
            self.assertEqual(status, 200)
            self.assertEqual([p['leverage'] for p in result['positions']], [20, '5'])
            FakeFutures.fail = True
            status, result = self.request('/api/futures-snapshot?account_id=%s' % account)
            self.assertEqual(status, 200)
            self.assertEqual([p['leverage'] for p in result['positions']], [None, None])
            self.assertEqual(len(result['balances']), 1)

    def test_display_order_endpoint_requires_auth_and_returns_sorted_state(self):
        with self.store.db() as db:
            a=db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('sort-a','key','secret')").lastrowid
            b=db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('sort-b','key','secret')").lastrowid
        move={'kind':'accounts','id':str(b),'relative_id':str(a),'side':'before'}
        self.assertEqual(self.request('/api/display-order', move, authenticated=False)[0],401)
        self.assertEqual(self.request('/api/display-order', move)[0],200)
        status, data=self.request('/api/state')
        self.assertEqual(status,200)
        ids=[x['id'] for x in data['futures_accounts']]
        self.assertLess(ids.index(b),ids.index(a))
        self.assertEqual(self.request('/api/display-order',{**move,'relative_id':'999999'})[0],400)

    def test_static_resources_revalidate_after_updates(self):
        for path in ('/', '/app.js?v=20261117-2', '/style.css?v=20261117-2'):
            with urllib.request.urlopen(self.url + path) as response:
                self.assertEqual(response.headers.get('Cache-Control'), 'no-cache')

    def test_futures_network_diagnostic_is_authenticated_and_sanitized(self):
        from copy_executor import ExchangeError
        self.assertEqual(self.request('/api/futures-network',authenticated=False)[0],401)
        with patch('app.FuturesClient.request',return_value={}), patch('app.urllib.request.getproxies',return_value={'https':'http://private-user:private-pass@proxy:123'}), patch('app.urllib.request.proxy_bypass',return_value=False):
            status,result=self.request('/api/futures-network')
        self.assertEqual(status,200);self.assertTrue(result['ok']);self.assertTrue(result['proxy_enabled'])
        self.assertNotIn('private-pass',json.dumps(result))
        with patch('app.FuturesClient.request',side_effect=ExchangeError('HTTP 451; 接口 /fapi/v1/ping')), patch('app.urllib.request.getproxies',return_value={}):
            status,result=self.request('/api/futures-network')
        self.assertEqual(status,200);self.assertFalse(result['ok']);self.assertIn('HTTP 451',result['error'])

    def test_auth_required(self):
        self.assertEqual(self.request('/api/state', authenticated=False)[0], 401)

    def test_leader_position_and_refresh_require_auth_and_reset_sync(self):
        with self.store.db() as db:
            db.execute("INSERT OR IGNORE INTO targets(id,name,enabled) VALUES('history-test','test',1)")
            db.execute("INSERT OR REPLACE INTO leader_history_sync(target,page,complete,total,next_check) VALUES('history-test',8,1,100,9999999999)")
        self.assertEqual(self.request('/api/leader-positions?target=history-test',authenticated=False)[0],401)
        self.assertEqual(self.request('/api/leader-history-refresh',{'target':'history-test'},authenticated=False)[0],401)
        self.assertEqual(self.request('/api/leader-history-refresh',{'target':'history-test'})[0],200)
        status,result=self.request('/api/leader-positions?target=history-test')
        self.assertEqual(status,200)
        self.assertEqual(result['sync']['complete'],0)
        with self.store.db() as db:
            self.assertEqual(db.execute("SELECT page FROM leader_history_sync WHERE target='history-test'").fetchone()[0],1)
        self.assertEqual(self.request('/api/targets', {'id': '123'}, authenticated=False)[0], 401)

    def test_settings_never_return_secrets(self):
        status, data = self.request('/api/state')
        self.assertEqual(status, 200)
        self.assertTrue(data['settings']['telegram_token_configured'])
        self.assertNotIn('not-for-response', json.dumps(data))
        self.assertNotIn('private-key', json.dumps(data))

    def test_bark_test_reports_http_status_without_credentials(self):
        error = urllib.error.HTTPError('https://api.day.app/private-key', 403, 'forbidden', {}, None)
        with patch('app.send_notification', side_effect=error):
            status, body = self.request('/api/test-notification', {'channel': 'bark'})
        self.assertEqual(status, 400)
        self.assertIn('HTTP 403', body['error'])
        self.assertNotIn('private-key', body['error'])

    def test_cross_origin_write_rejected(self):
        self.assertEqual(self.request('/api/settings', {'interval': 20}, headers={'Origin': 'https://untrusted.example'})[0], 403)

    def test_invalid_target_and_payload(self):
        self.assertEqual(self.request('/api/targets', {'id': '../oops'})[0], 400)
        self.assertEqual(self.request('/api/settings', [1, 2])[0], 400)

    def test_add_toggle_and_events(self):
        self.assertEqual(self.request('/api/targets', {'id': '123'})[0], 200)
        self.assertEqual(self.request('/api/toggle', {'id': '123', 'enabled': False})[0], 200)
        self.assertEqual(self.request('/api/events?page=1')[0], 200)

    def test_edit_futures_account_preserves_or_replaces_credentials(self):
        with self.store.db() as db:
            account=db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('edit-test','old-key','old-secret')").lastrowid
        self.assertEqual(self.request('/api/futures-account',{'account_id':account,'name':'edited'})[0],200)
        with self.store.db() as db:
            self.assertEqual(tuple(db.execute('SELECT name,api_key,api_secret FROM futures_accounts WHERE id=?',(account,)).fetchone()),('edited','old-key','old-secret'))
        self.assertEqual(self.request('/api/futures-account',{'account_id':account,'name':'edited','api_key':'new-key','api_secret':'new-secret'})[0],200)
        with self.store.db() as db:
            self.assertEqual(tuple(db.execute('SELECT api_key,api_secret FROM futures_accounts WHERE id=?',(account,)).fetchone()),('new-key','new-secret'))
            db.execute('INSERT INTO copied_positions VALUES(?,?,?,?,?)',(account,'leader','BTCUSDT','LONG','1'))
        self.assertEqual(self.request('/api/futures-account',{'account_id':account,'name':'edited','api_key':'other','api_secret':'other-secret'})[0],400)

    def test_delete_paused_target_with_stale_copy_position(self):
        with self.store.db() as db:
            account=db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('stale-delete','key','secret')").lastrowid
            db.execute("INSERT INTO targets(id,name,enabled) VALUES('87654','stale-target',0)")
            db.execute('INSERT INTO copied_positions VALUES(?,?,?,?,?)',(account,'87654','BTCUSDT','LONG','1'))
        with patch('app.FuturesClient.positions',return_value=[]):
            status,_=self.request('/api/delete-target',{'id':'87654'})
        self.assertEqual(status,200)
        with self.store.db() as db:
            self.assertIsNone(db.execute("SELECT id FROM targets WHERE id='87654'").fetchone())
            self.assertEqual(db.execute('SELECT COUNT(*) FROM copied_positions WHERE target_id=?',('87654',)).fetchone()[0],0)

    def test_delete_target_keeps_records_when_position_exists_or_cannot_be_verified(self):
        from copy_executor import ExchangeError
        with self.store.db() as db:
            account=db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('verify-delete','key','secret')").lastrowid
            db.execute("INSERT INTO targets(id,name,enabled) VALUES('87655','verify-target',0)")
            db.execute('INSERT INTO copied_positions VALUES(?,?,?,?,?)',(account,'87655','BTCUSDT','LONG','1'))
        cases=([{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'0.1'}],
               [{'symbol':'BTCUSDT','positionAmt':'0'}], ExchangeError('timeout'))
        for value in cases:
            with self.subTest(value=value):
                with patch('app.FuturesClient.positions',side_effect=value if isinstance(value,Exception) else None,return_value=value):
                    self.assertEqual(self.request('/api/delete-target',{'id':'87655'})[0],400)
                with self.store.db() as db:
                    self.assertIsNotNone(db.execute("SELECT id FROM targets WHERE id='87655'").fetchone())
                    self.assertEqual(db.execute("SELECT quantity FROM copied_positions WHERE target_id='87655'").fetchone()[0],'1')

    def test_delete_objects_checks_positions_and_removes_bindings(self):
        with self.store.db() as db:
            account=db.execute("INSERT INTO futures_accounts(name,api_key,api_secret) VALUES('delete-test','key','secret')").lastrowid
            db.execute("INSERT INTO targets(id,name) VALUES('98765','delete-target')")
            db.execute('INSERT INTO copy_bindings VALUES(?,?)',(account,'98765'))
            db.execute('INSERT INTO copied_positions VALUES(?,?,?,?,?)',(account,'98765','BTCUSDT','LONG','1'))
        self.assertEqual(self.request('/api/delete-account',{'id':account})[0],400)
        with patch('app.FuturesClient.positions',return_value=[{'symbol':'BTCUSDT','positionSide':'LONG','positionAmt':'1'}]):
            self.assertEqual(self.request('/api/delete-target',{'id':'98765'})[0],400)
        with self.store.db() as db: db.execute('DELETE FROM copied_positions WHERE account_id=?',(account,))
        self.assertEqual(self.request('/api/delete-target',{'id':'98765'})[0],200)
        with self.store.db() as db: self.assertIsNone(db.execute('SELECT 1 FROM copy_bindings WHERE account_id=?',(account,)).fetchone())
        self.assertEqual(self.request('/api/delete-account',{'id':account})[0],200)
