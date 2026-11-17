import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

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

    def test_auth_required(self):
        self.assertEqual(self.request('/api/state', authenticated=False)[0], 401)
        self.assertEqual(self.request('/api/targets', {'id': '123'}, authenticated=False)[0], 401)

    def test_settings_never_return_secrets(self):
        status, data = self.request('/api/state')
        self.assertEqual(status, 200)
        self.assertTrue(data['settings']['telegram_token_configured'])
        self.assertNotIn('not-for-response', json.dumps(data))
        self.assertNotIn('private-key', json.dumps(data))

    def test_cross_origin_write_rejected(self):
        self.assertEqual(self.request('/api/settings', {'interval': 20}, headers={'Origin': 'https://untrusted.example'})[0], 403)

    def test_invalid_target_and_payload(self):
        self.assertEqual(self.request('/api/targets', {'id': '../oops'})[0], 400)
        self.assertEqual(self.request('/api/settings', [1, 2])[0], 400)

    def test_add_toggle_and_events(self):
        self.assertEqual(self.request('/api/targets', {'id': '123'})[0], 200)
        self.assertEqual(self.request('/api/toggle', {'id': '123', 'enabled': False})[0], 200)
        self.assertEqual(self.request('/api/events?page=1')[0], 200)
