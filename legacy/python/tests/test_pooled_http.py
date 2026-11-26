import json
import base64
import concurrent.futures
import select
import socket
import ssl
import subprocess
import tempfile
import time
import threading
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
import pooled_http
import urllib3


class Handler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    ports=[]
    posts=0
    paths=[]
    proxy_leaks=[]
    def log_message(self,*args): pass
    def do_GET(self):
        self.ports.append(self.client_address[1]);self.paths.append(self.path)
        self.proxy_leaks.append(self.headers.get('Proxy-Authorization'))
        if self.path.startswith('/slowread'):
            self.send_response(200);self.send_header('Content-Length','100');self.end_headers();self.wfile.write(b'partial');self.wfile.flush();time.sleep(0.3);self.close_connection=True;return
        if self.path.startswith('/short'):
            self.send_response(200);self.send_header('Content-Length','100');self.end_headers();self.wfile.write(b'partial');self.wfile.flush();self.close_connection=True;return
        if self.path.startswith('/redirect'):
            self.send_response(302);self.send_header('Location','/redirected');self.send_header('Content-Length','0');self.end_headers();return
        if self.path.startswith('/large-error'):
            payload=b'x'*70000;self.send_response(503)
        elif self.path.startswith('/error'):
            payload=b'{"code":-2015}';self.send_response(401)
        elif self.path.startswith('/gzip'):
            payload=b'bad';self.send_response(200);self.send_header('Content-Encoding','gzip')
        else:
            payload=json.dumps({'ok':True,'trace':self.headers.get('X-Trace')}).encode();self.send_response(200)
        self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
    def do_POST(self):
        type(self).posts+=1
        self.rfile.read(int(self.headers.get('Content-Length','0')))
        if self.path=='/status':
            self.send_response(503);self.send_header('Content-Length','0');self.end_headers();return
        if self.path=='/timeout':time.sleep(0.3)
        try:self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:pass
        self.connection.close()
        self.close_connection=True


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    auth=[]
    def log_message(self,*args): pass
    def do_CONNECT(self):
        self.auth.append(self.headers.get('Proxy-Authorization'))
        host,port=self.path.rsplit(':',1)
        with socket.create_connection((host.strip('[]'),int(port)),timeout=3) as upstream:
            self.send_response(200);self.end_headers()
            self.connection.settimeout(None)
            while True:
                readable,_,_=select.select([self.connection,upstream],[],[],3)
                if not readable: break
                for sock in readable:
                    data=sock.recv(65536)
                    if not data:return
                    (upstream if sock is self.connection else self.connection).sendall(data)


class TestHTTPServer(ThreadingHTTPServer):
    request_queue_size=64


class PoolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();d=Path(cls.tmp.name)
        key=d/'key.pem';cert=d/'cert.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        cls.cert=cert;cls.key=key
        cls.server=TestHTTPServer(('127.0.0.1',0),Handler)
        cls.server.daemon_threads=True
        ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(cert,key);cls.server.socket=ctx.wrap_socket(cls.server.socket,server_side=True)
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
        cls.url='https://localhost:%s/' % cls.server.server_port
    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown();cls.server.server_close();cls.thread.join();cls.tmp.cleanup()
    def setUp(self):
        Handler.ports=[];Handler.posts=0;Handler.paths=[];Handler.proxy_leaks=[];ProxyHandler.auth=[];pooled_http._managers.clear()
        ctx=ssl.create_default_context(cafile=str(self.cert))
        self.patchers=[patch('pooled_http.urllib.request.proxy_bypass',return_value=True),patch('pooled_http.ssl.create_default_context',return_value=ctx)]
        for p in self.patchers:p.start();self.addCleanup(p.stop)
    def tearDown(self):
        for manager in pooled_http._managers.values():manager.clear()
        pooled_http._managers.clear()
    def test_reuses_connection_and_recovers_without_replaying_post(self):
        with pooled_http.open_url(self.url,timeout=2) as response:self.assertEqual(json.loads(response.read(100)),{'ok':True,'trace':None})
        with pooled_http.open_url(self.url,timeout=2) as response:self.assertEqual(json.loads(response.read(100)),{'ok':True,'trace':None})
        self.assertEqual(Handler.ports[0],Handler.ports[1])
        request=urllib.request.Request(self.url,data=b'order',method='POST')
        with self.assertRaises(urllib.error.URLError):pooled_http.open_url(request,timeout=2)
        self.assertEqual(Handler.posts,1)
        with self.assertRaises(urllib.error.HTTPError):pooled_http.open_url(urllib.request.Request(self.url+'status',data=b'order',method='POST'),timeout=2)
        self.assertEqual(Handler.posts,2)
        with self.assertRaises(urllib.error.URLError):pooled_http.open_url(urllib.request.Request(self.url+'timeout',data=b'order',method='POST'),timeout=0.05)
        self.assertEqual(Handler.posts,3)
        with pooled_http.open_url(self.url,timeout=2) as response:self.assertEqual(json.loads(response.read(100)),{'ok':True,'trace':None})
        self.assertNotEqual(Handler.ports[1],Handler.ports[2])

    def test_invalid_certificate_is_rejected(self):
        with patch('pooled_http.ssl.create_default_context',side_effect=lambda:ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)):
            with self.assertRaises(urllib.error.URLError):pooled_http.open_url(self.url,timeout=2)

    def test_redirect_error_compression_and_unread_response(self):
        with self.assertRaises(urllib.error.HTTPError) as error:
            pooled_http.open_url(urllib.request.Request(self.url+'error?signature=private'),timeout=2)
        self.assertEqual(error.exception.code,401)
        self.assertEqual(json.loads(error.exception.read()),{'code':-2015})
        self.assertNotIn('private',error.exception.url)
        with self.assertRaises(urllib.error.HTTPError):pooled_http.open_url(self.url+'redirect',timeout=2)
        self.assertNotIn('/redirected',Handler.paths)
        with self.assertRaises(urllib.error.URLError):pooled_http.open_url(self.url+'gzip',timeout=2)
        with self.assertRaises(urllib.error.URLError):
            with pooled_http.open_url(self.url+'short',timeout=2) as response:response.read(200)
        with pooled_http.open_url(self.url,timeout=2) as response:self.assertEqual(response.read(3),b'{"o')
        previous=Handler.ports[-1]
        with pooled_http.open_url(self.url,timeout=2) as response:response.read(100)
        self.assertNotEqual(previous,Handler.ports[-1])

    def test_concurrent_requests_do_not_mix_headers_or_responses(self):
        def fetch(i):
            request=urllib.request.Request(self.url,headers={'X-Trace':str(i)})
            with pooled_http.open_url(request,timeout=4) as response:return json.loads(response.read(100))['trace']
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(list(pool.map(fetch,range(16))),[str(i) for i in range(16)])

    def test_http_and_https_proxy_auth_is_confined_to_connect(self):
        for scheme in ('http','https'):
            with self.subTest(scheme=scheme):
                proxy=ThreadingHTTPServer(('127.0.0.1',0),ProxyHandler);proxy.daemon_threads=True
                if scheme=='https':
                    ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(self.cert,self.key)
                    proxy.socket=ctx.wrap_socket(proxy.socket,server_side=True)
                thread=threading.Thread(target=proxy.serve_forever,daemon=True);thread.start()
                address='%s://user:secret@localhost:%s' % (scheme,proxy.server_port)
                try:
                    with patch('pooled_http.urllib.request.getproxies',return_value={'https':address}), patch('pooled_http.urllib.request.proxy_bypass',return_value=False):
                        with pooled_http.open_url(self.url,timeout=3) as response:self.assertEqual(json.loads(response.read(100))['ok'],True)
                    self.assertEqual(ProxyHandler.auth[-1],'Basic '+base64.b64encode(b'user:secret').decode())
                    self.assertIsNone(Handler.proxy_leaks[-1])
                finally:
                    for manager in pooled_http._managers.values():manager.clear()
                    pooled_http._managers.clear();proxy.shutdown();proxy.server_close();thread.join()

    def test_https_proxy_certificate_is_verified_independently(self):
        proxy=TestHTTPServer(('127.0.0.1',0),ProxyHandler);proxy.daemon_threads=True
        server_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_ctx.load_cert_chain(self.cert,self.key)
        proxy.socket=server_ctx.wrap_socket(proxy.socket,server_side=True)
        thread=threading.Thread(target=proxy.serve_forever,daemon=True);thread.start()
        trusted=ssl.create_default_context(cafile=str(self.cert));untrusted=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        address='https://user:secret@localhost:%s' % proxy.server_port
        try:
            with patch('pooled_http.urllib.request.getproxies',return_value={'https':address}), patch('pooled_http.urllib.request.proxy_bypass',return_value=False), patch('pooled_http.ssl.create_default_context',side_effect=[trusted,untrusted]):
                with self.assertRaises(urllib.error.URLError) as error:pooled_http.open_url(self.url,timeout=2)
                self.assertNotIn('secret',str(error.exception))
            self.assertEqual(ProxyHandler.auth,[])
        finally:
            for manager in pooled_http._managers.values():manager.clear()
            pooled_http._managers.clear();proxy.shutdown();proxy.server_close();thread.join()

    def test_no_proxy_bypasses_configured_proxy(self):
        with patch('pooled_http.urllib.request.getproxies',return_value={'https':'http://localhost:1'}), patch('pooled_http.urllib.request.proxy_bypass',return_value=True):
            with pooled_http.open_url(self.url,timeout=2) as response:self.assertTrue(json.loads(response.read(100))['ok'])

    def test_repeated_errors_do_not_exhaust_bounded_pool(self):
        for _ in range(12):
            with self.assertRaises(urllib.error.HTTPError) as error:
                pooled_http.open_url(self.url+'large-error?signature=secret',timeout=0.3)
            self.assertEqual(error.exception.code,503)
        with pooled_http.open_url(self.url,timeout=1) as response:
            self.assertTrue(json.loads(response.read(100))['ok'])

    def test_repeated_partial_reads_do_not_exhaust_bounded_pool(self):
        for _ in range(12):
            with pooled_http.open_url(self.url,timeout=0.3) as response:
                response.read(3)
        with pooled_http.open_url(self.url,timeout=1) as response:
            self.assertTrue(json.loads(response.read(100))['ok'])

    def test_saturated_pool_times_out_without_sending_order_and_recovers(self):
        held=[]
        try:
            for _ in range(8):
                response=pooled_http.open_url(self.url,timeout=2)
                held.append(response)
            with self.assertRaises(urllib.error.URLError):
                pooled_http.open_url(urllib.request.Request(self.url,data=b'order',method='POST'),timeout=0.1)
            self.assertEqual(Handler.posts,0)
        finally:
            for response in held:response.__exit__(None,None,None)
        with pooled_http.open_url(self.url,timeout=2) as response:self.assertTrue(json.loads(response.read(100))['ok'])

    def test_no_proxy_matching_includes_explicit_port(self):
        with patch('pooled_http.urllib.request.getproxies',return_value={}), patch('pooled_http.urllib.request.proxy_bypass',return_value=True) as bypass:
            with pooled_http.open_url(self.url,timeout=2) as response:response.read(100)
        bypass.assert_called_once_with('localhost:%s' % self.server.server_port)

    def test_transport_error_never_exposes_signed_query(self):
        error=urllib3.exceptions.ProtocolError('signature=private-secret')
        with patch('pooled_http._manager') as manager:
            manager.return_value.request.side_effect=error
            with self.assertRaises(urllib.error.URLError) as caught:
                pooled_http.open_url('https://example.com/order?signature=private-secret',timeout=2)
        self.assertNotIn('private-secret',str(caught.exception))
        self.assertFalse(manager.return_value.request.call_args.kwargs['retries'])

    def test_timeout_during_read_is_classified_and_pool_recovers(self):
        with self.assertRaises(urllib.error.URLError) as error:
            with pooled_http.open_url(self.url+'slowread',timeout=0.05) as response:response.read(100)
        self.assertIsInstance(error.exception.reason,TimeoutError)
        with pooled_http.open_url(self.url,timeout=2) as response:self.assertTrue(json.loads(response.read(100))['ok'])

    def test_request_url_with_userinfo_is_rejected(self):
        with self.assertRaises(ValueError):pooled_http.open_url('https://:password@localhost/',timeout=1)

    def test_real_copy_flow_does_not_resubmit_after_lost_post_response(self):
        from app import FuturesClient, ExchangeError
        from test_copy_execution import CopyExecutionTests, FakeExchange
        from test_monitor import order, PID
        case=CopyExecutionTests();case.setUp()
        class LocalExchange(FakeExchange):
            def place(self,params):return FuturesClient('key','secret').place(params)
            def order(self,symbol,client_id):raise ExchangeError('unknown')
        try:
            row=order(101,'1');row['avgPrice']='100';case.api.orders.insert(0,row);case.monitor.poll(PID)
            with patch('copy_executor.BASE',self.url.rstrip('/')),patch('app.FuturesClient',LocalExchange):
                case.monitor.execute_copy()
                with case.store.db() as db:db.execute('UPDATE copy_tasks SET next_check=0')
                case.monitor.execute_copy()
            self.assertEqual(Handler.posts,1)
            self.assertEqual(case.rows('SELECT state FROM copy_tasks')[0]['state'],'submitted')
        finally:case.tearDown()
