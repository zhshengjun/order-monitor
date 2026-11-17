import hashlib
import hmac
import io
import urllib.error
import urllib.parse
import unittest
from unittest.mock import patch
from app import FuturesAccount, UpstreamError

class Response:
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self, limit): return b'[{"asset":"USDT","availableBalance":"12.30000000"}]'

class FuturesTests(unittest.TestCase):
    def test_signature_and_read_only(self):
        with patch('app.urllib.request.urlopen', return_value=Response()) as call, patch('app.time.time', return_value=1234):
            result = FuturesAccount('test-key','test-secret').get('/fapi/v3/balance')
        request = call.call_args.args[0]
        query = urllib.parse.urlsplit(request.full_url).query
        unsigned, signature = query.split('&signature=')
        self.assertEqual(signature, hmac.new(b'test-secret',unsigned.encode(),hashlib.sha256).hexdigest())
        self.assertEqual(request.get_header('X-mbx-apikey'),'test-key')
        self.assertEqual(request.get_method(),'GET')
        self.assertEqual(result[0]['availableBalance'],'12.30000000')
        with self.assertRaises(ValueError): FuturesAccount('k','s').get('/fapi/v1/order')

    def test_error_does_not_expose_body(self):
        error=urllib.error.HTTPError('https://example.invalid',401,'',{},io.BytesIO(b'{"code":-2015,"msg":"secret-credential"}'))
        with patch('app.urllib.request.urlopen',side_effect=error):
            with self.assertRaises(UpstreamError) as caught: FuturesAccount('key','secret').get('/fapi/v3/positionRisk')
        self.assertIn('-2015',str(caught.exception))
        self.assertNotIn('secret',str(caught.exception))
