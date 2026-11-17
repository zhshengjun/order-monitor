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
        self.assertEqual(self.request('/api/copy-live',{'account_id':account_id,'target_id':'4321','enabled':True})[0],400)
        with self.store.db() as db: db.execute("UPDATE targets SET initialized=1 WHERE id='4321'")
        self.assertEqual(self.request('/api/copy-live',{'account_id':account_id,'target_id':'4321','enabled':True})[0],200)
        with self.store.db() as db:
            self.assertEqual(db.execute('SELECT enabled FROM copy_live WHERE account_id=?',(account_id,)).fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM virtual_positions WHERE account_id=?',(account_id,)).fetchone()[0],0)
        self.assertEqual(self.request('/api/copy-live',{'account_id':account_id,'target_id':'4321','enabled':False})[0],200)
        self.assertEqual(self.request('/api/copy-live',{'account_id':account_id,'target_id':'4321','enabled':True})[0],200)

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
        status,first=self.request('/api/copy-tasks?account_id=%s&page=1'%a)
        self.assertEqual(status,200);self.assertEqual(first['total'],52);self.assertEqual(len(first['list']),50)
        self.assertEqual(first['list'][0]['result']['orderId'],'1234567890123456789')
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

    def test_leader_record_pages_use_ten(self):
        with self.store.db() as db:
            for i in range(21):
                db.execute('INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,?,?,?,0)',('pages','leader','transfer','{}',i))
        status,first=self.request('/api/events?target=pages&kind=leader&limit=10&page=1')
        self.assertEqual(status,200);self.assertEqual(first['total'],21);self.assertEqual(len(first['list']),10)
        _,third=self.request('/api/events?target=pages&kind=leader&limit=10&page=3')
        self.assertEqual(len(third['list']),1)

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

    def test_auth_required(self):
        self.assertEqual(self.request('/api/state', authenticated=False)[0], 401)
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
