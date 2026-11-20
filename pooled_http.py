"""Thread-safe HTTPS connection pools for Binance requests, with no retries."""
import io
import socket
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
import urllib3

_lock = threading.Lock()
_managers = {}


def _manager(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise ValueError('Only HTTPS URLs without credentials are supported')
    proxies = urllib.request.getproxies()
    proxy = None if urllib.request.proxy_bypass(parsed.netloc) else proxies.get('https') or proxies.get('all')
    with _lock:
        if proxy not in _managers:
            options = {'ssl_context': ssl.create_default_context(), 'retries': False, 'maxsize':8, 'block':True}
            if proxy:
                info = urllib.parse.urlsplit(proxy)
                if info.scheme not in ('http', 'https') or not info.hostname:
                    raise ValueError('Unsupported HTTPS proxy')
                host = '[%s]' % info.hostname if ':' in info.hostname else info.hostname
                address = '%s://%s%s' % (info.scheme, host, ':' + str(info.port) if info.port else '')
                headers = None
                if info.username is not None:
                    credentials = urllib.parse.unquote(info.username) + ':' + urllib.parse.unquote(info.password or '')
                    headers = urllib3.make_headers(proxy_basic_auth=credentials)
                _managers[proxy] = urllib3.ProxyManager(address, proxy_headers=headers, proxy_ssl_context=ssl.create_default_context() if info.scheme == 'https' else None, **options)
            else:
                _managers[proxy] = urllib3.PoolManager(**options)
        return _managers[proxy]


def _safe_url_error(exc):
    reason = exc
    for _ in range(4):
        nested = getattr(reason, 'reason', None) or getattr(reason, '__cause__', None)
        if not nested or nested is reason: break
        reason = nested
    if isinstance(reason, socket.gaierror): safe = socket.gaierror('DNS resolution failed')
    elif isinstance(reason, ssl.SSLCertVerificationError): safe = ssl.SSLCertVerificationError('TLS verification failed')
    elif isinstance(reason, ssl.SSLError) or isinstance(exc, urllib3.exceptions.SSLError): safe = ssl.SSLError('TLS connection failed')
    elif isinstance(reason, (TimeoutError, socket.timeout)) or isinstance(exc, urllib3.exceptions.TimeoutError): safe = socket.timeout('connection timed out')
    elif isinstance(reason, ConnectionRefusedError): safe = ConnectionRefusedError('connection refused')
    else: safe = type(reason).__name__
    return urllib.error.URLError(safe)


class _Response:
    def __init__(self, response):
        self.response = response
    def __enter__(self): return self
    def __exit__(self, *args):
        if not self.response.isclosed(): self.response.close()
        self.response.release_conn()
    def read(self, limit):
        try:
            return self.response.read(limit)
        except urllib3.exceptions.HTTPError as exc:
            raise _safe_url_error(exc) from None


def open_url(request, timeout):
    if isinstance(request, str):
        request = urllib.request.Request(request)
    url = request.full_url
    try:
        headers=dict(request.header_items())
        if not any(key.lower()=='accept-encoding' for key in headers): headers['Accept-Encoding']='identity'
        response = _manager(url).request(request.get_method(), url, body=request.data,
            headers=headers, timeout=timeout, pool_timeout=min(timeout,5), retries=False,
            redirect=False, preload_content=False)
        if response.headers.get('Content-Encoding','identity').lower() not in ('','identity'):
            response.close()
            response.release_conn()
            raise urllib.error.URLError('Unsupported content encoding')
        if not 200 <= response.status < 300:
            try: body = response.read(65536)
            finally:
                response.close()
                response.release_conn()
            # Strip URL query so signatures and proxy credentials never enter errors.
            safe_url=urllib.parse.urlunsplit((*urllib.parse.urlsplit(url)[:3], '', ''))
            raise urllib.error.HTTPError(safe_url,response.status,'HTTP error',response.headers,io.BytesIO(body))
        return _Response(response)
    except urllib3.exceptions.HTTPError as exc:
        raise _safe_url_error(exc) from None
