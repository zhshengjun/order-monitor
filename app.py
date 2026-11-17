"""Single-process Binance lead portfolio monitor. Python standard library only."""
import contextlib
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from virtual_position import transition

ROOT = Path(__file__).parent
API_PATH = '/bapi/futures/v1/friendly/future/copy-trade/'
DEFAULTS = {'interval': 60, 'page_size': 5, 'balance_threshold': '0', 'telegram_enabled': False,
            'telegram_token': '', 'telegram_chat': '', 'bark_enabled': False,
            'bark_url': 'https://api.day.app', 'bark_key': ''}
SECRETS = ('telegram_token', 'bark_key')
LOG = logging.getLogger('monitor')


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), default=str)


def number(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('数值必须有限')
    return result


def decimal_text(value):
    return format(number(value), 'f')


def direction(row):
    return {('BUY', 'LONG'): '开多', ('SELL', 'LONG'): '平多',
            ('SELL', 'SHORT'): '开空', ('BUY', 'SHORT'): '平空'}.get(
                (row.get('side'), row.get('positionSide')), row.get('side', '未知'))


def identity(kind, row):
    fields = ('symbol', 'side', 'positionSide', 'type', 'orderTime') if kind == 'order' else (
        'time', 'coin', 'amount', 'from', 'to', 'transType')
    return hashlib.sha256(dumps([row.get(k) for k in fields]).encode()).hexdigest()


class UpstreamError(Exception):
    pass


class Binance:
    def __init__(self, base=None):
        self.base = (base or os.environ.get('BINANCE_BASE_URL', 'https://www.binance.com')).rstrip('/')
        if not self.base.startswith('https://'):
            raise ValueError('BINANCE_BASE_URL 必须为 HTTPS')

    def call(self, endpoint, body=None):
        url = self.base + API_PATH + endpoint
        request = urllib.request.Request(url, data=dumps(body).encode() if body is not None else None,
                                         headers={'Content-Type': 'application/json', 'clienttype': 'web',
                                                  'User-Agent': 'Mozilla/5.0 OrderMonitor/1.0'})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                result = json.loads(response.read(8_000_000), parse_float=str)
        except urllib.error.HTTPError as exc:
            message = '上游 HTTP %s' % exc.code
            LOG.warning('Binance request failed endpoint=%s reason=%s', endpoint.split('?')[0], message)
            raise UpstreamError(message) from None
        except (urllib.error.URLError, OSError, ValueError) as exc:
            reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
            if isinstance(reason, socket.gaierror):
                category = 'DNS 解析失败'
            elif isinstance(reason, ssl.SSLCertVerificationError):
                category = 'TLS 证书验证失败'
            elif isinstance(reason, ssl.SSLError):
                category = 'TLS 握手失败'
            elif isinstance(reason, (socket.timeout, TimeoutError)):
                category = '连接超时'
            elif isinstance(reason, json.JSONDecodeError):
                category = '上游响应不是有效 JSON'
            elif isinstance(reason, ConnectionRefusedError):
                category = '连接被拒绝'
            else:
                category = '上游连接或响应失败'
            # Exception messages may contain proxy credentials or URLs. Record only type and errno.
            detail = '%s (%s%s)' % (category, type(reason).__name__,
                                  ', errno=%s' % reason.errno if isinstance(getattr(reason, 'errno', None), int) else '')
            LOG.warning('Binance request failed endpoint=%s reason=%s', endpoint.split('?')[0], detail)
            raise UpstreamError(detail + '；请检查容器网络和代理') from None
        if not isinstance(result, dict):
            LOG.warning('Binance request failed endpoint=%s reason=unexpected JSON root', endpoint.split('?')[0])
            raise UpstreamError('上游响应格式错误：JSON 根节点不是对象')
        if result.get('success') is not True or str(result.get('code')) != '000000':
            code = str(result.get('code', 'unknown'))[:40]
            LOG.warning('Binance request failed endpoint=%s business_code=%s', endpoint.split('?')[0], code)
            raise UpstreamError('上游业务错误 %s' % code)
        if result.get('data') is None:
            raise UpstreamError('上游缺少 data，未推进同步位置')
        return result['data']

    def detail(self, pid):
        data = self.call('lead-portfolio/detail?' + urllib.parse.urlencode({'portfolioId': pid}))
        if str(data.get('leadPortfolioId')) != pid or 'marginBalance' not in data:
            raise UpstreamError('详情缺少组合 ID 或余额')
        number(data['marginBalance'])
        return data

    def search(self, nickname, page=1):
        return self.call('home-page/query-list', {'favoriteOnly': False, 'dataType': 'PNL',
            'timeRange': '30D', 'apiKeyOnly': 'false', 'pageNumber': page, 'portfolioType': 'ALL',
            'hideFull': 'false', 'nickname': nickname, 'pageSize': 20})

    def history(self, pid, kind, page, page_size=20):
        name = 'order-history' if kind == 'order' else 'transfer-history'
        data = self.call('lead-portfolio/' + name, {'portfolioId': pid, 'pageSize': page_size, 'pageNumber': page})
        if not isinstance(data, dict) or not isinstance(data.get('list'), list) or 'total' not in data:
            raise UpstreamError('历史响应结构异常，未推进同步位置')
        for row in data['list']:
            required = ('orderTime', 'symbol', 'side', 'positionSide', 'executedQty', 'avgPrice') if kind == 'order' else ('time', 'coin', 'amount', 'transType')
            if any(k not in row for k in required):
                raise UpstreamError('历史记录字段缺失，未推进同步位置')
        return data


class FuturesAccount:
    """Read-only USD-M futures account queries using HMAC API keys."""
    def __init__(self, api_key, api_secret):
        self.api_key, self.api_secret = api_key, api_secret

    def get(self, path):
        if path not in ('/fapi/v3/balance', '/fapi/v3/positionRisk'):
            raise ValueError('不支持的查询')
        query = urllib.parse.urlencode({'timestamp': int(time.time() * 1000), 'recvWindow': 5000})
        signature = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        request = urllib.request.Request('https://fapi.binance.com' + path + '?' + query + '&signature=' + signature,
            headers={'X-MBX-APIKEY': self.api_key})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                result = json.loads(response.read(4000000), parse_float=str)
        except urllib.error.HTTPError as exc:
            try:
                code = json.loads(exc.read(65536)).get('code', 'unknown')
                if not isinstance(code, int): code = 'unknown'
            except Exception:
                code = 'unknown'
            raise UpstreamError('Binance 合约查询失败：HTTP %s，错误码 %s' % (exc.code, code)) from None
        except Exception:
            raise UpstreamError('Binance 合约查询连接失败，请检查网络、代理和系统时间') from None
        if not isinstance(result, list):
            raise UpstreamError('Binance 合约查询响应格式异常')
        return result


class Store:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.db() as db:
            db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS settings (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS targets (
              id TEXT PRIMARY KEY, name TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
              initialized INTEGER NOT NULL DEFAULT 0, detail TEXT NOT NULL DEFAULT '{}',
              balance TEXT, last_check REAL, next_check REAL NOT NULL DEFAULT 0,
              failures INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS records (
              target TEXT NOT NULL, kind TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL,
              stamp INTEGER NOT NULL, PRIMARY KEY(target,kind,key));
            CREATE TABLE IF NOT EXISTS events (
              id INTEGER PRIMARY KEY AUTOINCREMENT, target TEXT NOT NULL, name TEXT NOT NULL,
              kind TEXT NOT NULL, body TEXT NOT NULL, stamp INTEGER NOT NULL, historical INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS event_time ON events(stamp DESC,id DESC);
            CREATE TABLE IF NOT EXISTS futures_accounts (
              id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
              api_key TEXT NOT NULL, api_secret TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS copy_bindings (
              account_id INTEGER NOT NULL, target_id TEXT NOT NULL,
              PRIMARY KEY(account_id, target_id));
            CREATE TABLE IF NOT EXISTS virtual_sessions (
              account_id INTEGER NOT NULL, target_id TEXT NOT NULL, started_at INTEGER NOT NULL,
              active INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(account_id,target_id));
            CREATE TABLE IF NOT EXISTS virtual_positions (
              account_id INTEGER NOT NULL, target_id TEXT NOT NULL, symbol TEXT NOT NULL,
              position_side TEXT NOT NULL, quantity TEXT NOT NULL, PRIMARY KEY(account_id,target_id,symbol,position_side));
            CREATE TABLE IF NOT EXISTS virtual_events (
              account_id INTEGER NOT NULL, target_id TEXT NOT NULL, order_key TEXT NOT NULL,
              symbol TEXT NOT NULL, position_side TEXT NOT NULL, stamp INTEGER NOT NULL,
              action TEXT NOT NULL, quantity TEXT NOT NULL, before_qty TEXT NOT NULL, after_qty TEXT NOT NULL,
              PRIMARY KEY(account_id,target_id,order_key));
            CREATE TABLE IF NOT EXISTS outbox (
              id INTEGER PRIMARY KEY AUTOINCREMENT, event INTEGER NOT NULL, channel TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
              next_try REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', UNIQUE(event,channel));
            ''')
            db.execute('INSERT OR IGNORE INTO settings VALUES(1,?)', (dumps(DEFAULTS),))
        os.chmod(self.path, 0o600)

    @contextlib.contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def settings(self, db=None):
        if db is not None:
            return {**DEFAULTS, **json.loads(db.execute('SELECT value FROM settings WHERE id=1').fetchone()[0])}
        with self.db() as conn:
            return self.settings(conn)

    def save_settings(self, patch):
        with self.db() as db:
            settings = self.settings(db)
            for key in DEFAULTS:
                if key in patch and not (key in SECRETS and patch[key] == ''):
                    settings[key] = patch[key]
            for key in SECRETS:
                if patch.get('clear_' + key) is True:
                    settings[key] = ''
            settings['interval'] = int(settings['interval'])
            if not 10 <= settings['interval'] <= 86400:
                raise ValueError('监听频率必须为 10～86400 秒')
            settings['page_size'] = int(settings['page_size'])
            if not 1 <= settings['page_size'] <= 20:
                raise ValueError('轮询每页数量必须为 1～20 条')
            settings['balance_threshold'] = decimal_text(settings['balance_threshold'])
            if number(settings['balance_threshold']) < 0:
                raise ValueError('余额通知阈值不能为负数')
            for key in ('telegram_enabled', 'bark_enabled'):
                if not isinstance(settings[key], bool):
                    raise ValueError('通知开关必须是布尔值')
            for key in ('telegram_token', 'telegram_chat', 'bark_key', 'bark_url'):
                if not isinstance(settings[key], str) or len(settings[key]) > 1000:
                    raise ValueError('通知配置格式错误')
            url = urllib.parse.urlsplit(settings['bark_url'])
            if url.scheme != 'https' or not url.hostname or url.username or url.query or url.fragment:
                raise ValueError('Bark 地址必须为不含账号、参数的 HTTPS 地址')
            if settings['telegram_enabled'] and not (settings['telegram_token'] and settings['telegram_chat']):
                raise ValueError('启用 Telegram 需要 Token 和 Chat ID')
            if settings['bark_enabled'] and not settings['bark_key']:
                raise ValueError('启用 Bark 需要 Device Key')
            db.execute('UPDATE settings SET value=? WHERE id=1', (dumps(settings),))
            db.execute('UPDATE targets SET next_check=MIN(next_check,?)', (time.time() + settings['interval'],))

    def event(self, db, pid, name, kind, body, stamp, historical, settings):
        eid = db.execute('INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,?,?,?,?)',
                         (pid, name, kind, dumps(body), stamp, int(historical))).lastrowid
        if not historical:
            for channel in ('telegram', 'bark'):
                if settings[channel + '_enabled']:
                    db.execute('INSERT INTO outbox(event,channel) VALUES(?,?)', (eid, channel))


class Monitor:
    def __init__(self, store, api=None):
        self.store, self.api = store, api or Binance()
        self.lock = threading.RLock()
        self.stop = threading.Event()

    def add(self, pid):
        if not isinstance(pid, str) or not pid.isdigit() or not 1 <= len(pid) <= 30:
            raise ValueError('组合 ID 格式错误')
        detail = self.api.detail(pid)
        with self.lock, self.store.db() as db:
            db.execute('INSERT OR IGNORE INTO targets(id,name,detail) VALUES(?,?,?)',
                       (pid, detail['nickname'], dumps(detail)))

    def toggle(self, pid, enabled):
        if not isinstance(enabled, bool):
            raise ValueError('enabled 必须是布尔值')
        with self.lock, self.store.db() as db:
            if not db.execute('UPDATE targets SET enabled=?,next_check=0 WHERE id=?', (int(enabled), pid)).rowcount:
                raise ValueError('监听对象不存在')
            if not enabled:
                db.execute("UPDATE outbox SET status='cancelled' WHERE status='pending' AND event IN (SELECT id FROM events WHERE target=?)", (pid,))

    def collect(self, pid, kind, initialized):
        with self.store.db() as db:
            stamps = db.execute('SELECT MAX(stamp) FROM records WHERE target=? AND kind=?', (pid, kind)).fetchone()
            watermark = stamps[0]
            page_size = self.store.settings(db)['page_size'] if initialized else 20
        rows = []
        time_key = 'orderTime' if kind == 'order' else 'time'
        for page in range(1, 51):
            data = self.api.history(pid, kind, page, page_size)
            batch = data['list']
            rows.extend(batch)
            if not initialized:
                return rows  # Initial page is a baseline, not a notification backlog.
            if not batch or len(rows) >= int(data['total']):
                return rows
            if watermark is not None and min(int(x[time_key]) for x in batch) < watermark:
                return rows
        raise UpstreamError('新增记录超过 %s 条，未推进同步位置；可增加每页数量后重试' % (50 * page_size))

    def virtual_apply(self, db, pid, key, row):
        sessions = db.execute('SELECT account_id,started_at FROM virtual_sessions WHERE target_id=? AND active=1', (pid,)).fetchall()
        for session in sessions:
            aid, started = session['account_id'], session['started_at']
            if int(row['orderTime']) <= started:
                continue
            if db.execute('SELECT 1 FROM virtual_events WHERE account_id=? AND target_id=? AND order_key=?', (aid, pid, key)).fetchone():
                continue
            symbol, side = row['symbol'], row['positionSide']
            position = db.execute('SELECT quantity FROM virtual_positions WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?', (aid, pid, symbol, side)).fetchone()
            change = transition(position['quantity'] if position else '0', row['side'], side, row['executedQty'])
            db.execute('INSERT INTO virtual_events VALUES(?,?,?,?,?,?,?,?,?,?)', (aid, pid, key, symbol, side, int(row['orderTime']), change['action'], str(row['executedQty']), change['before'], change['after']))
            if change['action'] != 'unmatched_close':
                db.execute('INSERT OR REPLACE INTO virtual_positions VALUES(?,?,?,?,?)', (aid,pid,symbol,side,change['after']))

    def poll(self, pid):
        with self.lock:
            with self.store.db() as db:
                target = db.execute('SELECT * FROM targets WHERE id=?', (pid,)).fetchone()
            if not target or not target['enabled']:
                return
            try:
                detail = self.api.detail(pid)
                histories = {kind: self.collect(pid, kind, target['initialized']) for kind in ('order', 'transfer')}
                with self.store.db() as db:
                    settings = self.store.settings(db)
                    first = not target['initialized']
                    for kind, rows in histories.items():
                        time_key = 'orderTime' if kind == 'order' else 'time'
                        watermark = db.execute('SELECT MAX(stamp) FROM records WHERE target=? AND kind=?', (pid, kind)).fetchone()[0]
                        for row in sorted(rows, key=lambda r: int(r[time_key])):
                            key, stamp = identity(kind, row), int(row[time_key])
                            previous = db.execute('SELECT body FROM records WHERE target=? AND kind=? AND key=?', (pid, kind, key)).fetchone()
                            if previous and json.loads(previous[0]) == row:
                                continue
                            db.execute('INSERT OR REPLACE INTO records VALUES(?,?,?,?,?)', (pid, kind, key, dumps(row), stamp))
                            body = dict(row)
                            if kind == 'order':
                                body.update(direction=direction(row), notional=decimal_text(number(row['executedQty']) * number(row['avgPrice'])), updated=bool(previous))
                            if previous:
                                # Refresh the original display row; never enqueue an update as a new order.
                                candidates = db.execute('SELECT id,body FROM events WHERE target=? AND kind=? AND stamp=?', (pid, kind, stamp)).fetchall()
                                for event in candidates:
                                    if identity(kind, json.loads(event['body'])) == key:
                                        db.execute('UPDATE events SET body=? WHERE id=?', (dumps(body), event['id']))
                                continue
                            historical = first or (watermark is not None and stamp < watermark)
                            self.store.event(db, pid, detail['nickname'], kind, body, stamp, historical, settings)
                            if kind == 'order' and not historical:
                                self.virtual_apply(db, pid, key, row)
                    balance = decimal_text(detail['marginBalance'])
                    db.execute('UPDATE targets SET name=?,detail=?,balance=?,initialized=1,last_check=?,next_check=?,failures=0,error=\'\' WHERE id=?',
                               (detail['nickname'], dumps(detail), balance, time.time(), time.time() + settings['interval'], pid))
            except Exception as exc:
                message = str(exc) if isinstance(exc, (UpstreamError, ValueError, InvalidOperation)) else '同步失败，保留原同步位置'
                with self.store.db() as db:
                    failures = target['failures'] + 1
                    delay = min(3600, self.store.settings(db)['interval'] * 2 ** min(failures, 6))
                    db.execute('UPDATE targets SET error=?,failures=?,next_check=? WHERE id=?', (message[:300], failures, time.time() + delay, pid))
                LOG.warning('portfolio %s: %s', pid, message)

    @staticmethod
    def message(event):
        body = json.loads(event['body'])
        if event['kind'] == 'order':
            body['direction'] = direction(body)
        title = event['name'] + ' · ' + {'order': '交易操作', 'transfer': '资金划转', 'balance': '余额变化'}[event['kind']]
        if event['kind'] == 'order':
            text = '%s %s%s\n数量：%s %s\n均价：%s %s\n成交金额：%s %s\n已实现盈亏：%s %s' % (
                body['symbol'], body['direction'], '（记录更新）' if body.get('updated') else '',
                body['executedQty'], body.get('baseAsset', ''), body['avgPrice'], body.get('quoteAsset', ''),
                body['notional'], body.get('quoteAsset', ''), body.get('totalPnl', '—'), body.get('quoteAsset', ''))
        elif event['kind'] == 'transfer':
            text = '%s\n%s %s\n%s → %s' % (body['transType'], body['amount'], body['coin'], body.get('from', ''), body.get('to', ''))
        else:
            text = '%s → %s %s\n变动：%s\n余额快照差异，可能包含浮动盈亏。' % (body['before'], body['after'], body['coin'], body['delta'])
        text += '\n时间（UTC）：' + time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(event['stamp'] / 1000))
        return title, text

    def deliver(self):
        with self.lock:
            settings = self.store.settings()
            with self.store.db() as db:
                tasks = db.execute("SELECT o.id AS task_id,o.channel,o.attempts,e.* FROM outbox o JOIN events e ON e.id=o.event WHERE o.status='pending' AND e.kind != 'balance' AND o.next_try<=? ORDER BY o.id LIMIT 10", (time.time(),)).fetchall()
            for task in tasks:
                channel = task['channel']
                if not settings[channel + '_enabled']:
                    with self.store.db() as db:
                        db.execute("UPDATE outbox SET status='cancelled' WHERE id=?", (task['task_id'],))
                    continue
                title, message = self.message(task)
                try:
                    send_notification(channel, settings, title, message)
                    with self.store.db() as db:
                        db.execute("UPDATE outbox SET status='sent',error='' WHERE id=?", (task['task_id'],))
                except Exception:
                    attempts = task['attempts'] + 1
                    with self.store.db() as db:
                        db.execute('UPDATE outbox SET attempts=?,next_try=?,error=? WHERE id=?',
                                   (attempts, time.time() + min(3600, 10 * 2 ** min(attempts, 8)), '发送失败，请检查通知凭证及网络', task['task_id']))

    def run(self):
        while not self.stop.is_set():
            with self.store.db() as db:
                ids = [r[0] for r in db.execute('SELECT id FROM targets WHERE enabled=1 AND next_check<=? ORDER BY next_check', (time.time(),))]
            for pid in ids:
                if self.stop.is_set():
                    break
                self.poll(pid)
            self.deliver()
            self.stop.wait(1)


def send_notification(channel, settings, title, message):
    if channel == 'telegram':
        url = 'https://api.telegram.org/bot' + settings['telegram_token'] + '/sendMessage'
        body = {'chat_id': settings['telegram_chat'], 'text': title + '\n' + message}
    else:
        url = settings['bark_url'].rstrip('/') + '/push'
        body = {'device_key': settings['bark_key'], 'title': title, 'body': message, 'group': '订单监听'}
    request = urllib.request.Request(url, data=dumps(body).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.loads(response.read(1_000_000))
    if (channel == 'telegram' and result.get('ok') is not True) or (channel == 'bark' and result.get('code') != 200):
        raise ValueError('通知服务拒绝请求')


def handler_factory(store, monitor, token):
    session = hmac.new(token.encode(), b'order-monitor-session-v1', hashlib.sha256).hexdigest()

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(30)

        def log_message(self, fmt, *args):
            pass  # Never log credentials, query strings or session cookies.

        def reply(self, data, status=200, cookie=None):
            raw = dumps(data).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            if cookie:
                self.send_header('Set-Cookie', cookie)
            self.end_headers()
            self.wfile.write(raw)

        def authorized(self):
            jar = cookies.SimpleCookie()
            try:
                jar.load(self.headers.get('Cookie', ''))
            except cookies.CookieError:
                return False
            value = jar.get('om_session')
            return bool(value and hmac.compare_digest(value.value, session))

        def do_GET(self):
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == '/healthz':
                return self.reply({'ok': True})
            if not parsed.path.startswith('/api/'):
                path = {'/': 'index.html', '/app.js': 'app.js', '/style.css': 'style.css'}.get(parsed.path)
                if not path:
                    return self.reply({'error': 'Not found'}, 404)
                raw = (ROOT / 'static' / path).read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', {'html': 'text/html; charset=utf-8', 'js': 'text/javascript; charset=utf-8', 'css': 'text/css; charset=utf-8'}[path.rsplit('.', 1)[1]])
                self.send_header('Content-Length', str(len(raw)))
                self.send_header('X-Content-Type-Options', 'nosniff')
                self.send_header('Content-Security-Policy', "default-src 'self'; img-src 'self' https:; style-src 'self'; script-src 'self'; frame-ancestors 'none'; base-uri 'none'")
                self.end_headers()
                return self.wfile.write(raw)
            if not self.authorized():
                return self.reply({'error': '请先登录'}, 401)
            try:
                query = urllib.parse.parse_qs(parsed.query)
                if parsed.path == '/api/state':
                    with store.db() as db:
                        targets = [dict(r) for r in db.execute('SELECT * FROM targets ORDER BY name')]
                        for row in targets:
                            row['detail'] = json.loads(row['detail'])
                        accounts = [dict(r) for r in db.execute("SELECT id,name FROM futures_accounts ORDER BY id")]
                        bindings = [dict(r) for r in db.execute('SELECT account_id,target_id FROM copy_bindings')]
                        virtual_sessions = [dict(r) for r in db.execute('SELECT account_id,target_id,active,started_at FROM virtual_sessions')]
                        virtual_positions = [dict(r) for r in db.execute("SELECT account_id,target_id,symbol,position_side,quantity FROM virtual_positions WHERE quantity != '0'")]
                        virtual_events = [dict(r) for r in db.execute("SELECT account_id,target_id,symbol,position_side,stamp,action,quantity FROM virtual_events WHERE action='unmatched_close' ORDER BY stamp DESC LIMIT 20")]
                        counts = dict(db.execute('SELECT status,COUNT(*) FROM outbox GROUP BY status').fetchall())
                        errors = [dict(r) for r in db.execute("SELECT channel,attempts,error FROM outbox WHERE status='pending' AND error!='' ORDER BY id DESC LIMIT 5")]
                    settings = store.settings()
                    for key in SECRETS:
                        settings[key + '_configured'] = bool(settings.pop(key))
                    return self.reply({'targets': targets, 'settings': settings, 'notifications': counts, 'notification_errors': errors, 'futures_accounts': accounts, 'copy_bindings': bindings, 'virtual_sessions': virtual_sessions, 'virtual_positions': virtual_positions, 'virtual_events': virtual_events})
                if parsed.path == '/api/futures-snapshot':
                    account_id = int(query.get('account_id', ['0'])[0])
                    with store.db() as db:
                        account = db.execute('SELECT api_key,api_secret FROM futures_accounts WHERE id=?', (account_id,)).fetchone()
                    if not account:
                        raise ValueError('账号不存在')
                    client = FuturesAccount(account['api_key'], account['api_secret'])
                    balances = client.get('/fapi/v3/balance')
                    positions = client.get('/fapi/v3/positionRisk')
                    return self.reply({'balances': balances, 'positions': positions, 'queried_at': int(time.time() * 1000)})
                if parsed.path == '/api/events':
                    page = max(1, int(query.get('page', ['1'])[0]))
                    target, kind = query.get('target', [''])[0], query.get('kind', [''])[0]
                    where, args = "kind != 'balance'", []
                    for column, value in [('target', target), ('kind', kind)]:
                        if value:
                            where += ' AND ' + column + '=?'
                            args.append(value)
                    with store.db() as db:
                        total = db.execute('SELECT COUNT(*) FROM events WHERE ' + where, args).fetchone()[0]
                        rows = [dict(r) for r in db.execute('SELECT * FROM events WHERE ' + where + ' ORDER BY stamp DESC,id DESC LIMIT 50 OFFSET ?', args + [(page - 1) * 50])]
                    for row in rows:
                        row['body'] = json.loads(row['body'])
                        if row['kind'] == 'order':
                            row['body']['direction'] = direction(row['body'])
                    return self.reply({'list': rows, 'total': total, 'page': page})
                if parsed.path == '/api/search':
                    nickname = query.get('q', [''])[0].strip()
                    if not nickname or len(nickname) > 80:
                        raise ValueError('请输入 1～80 字的昵称')
                    return self.reply(monitor.api.search(nickname, max(1, min(100, int(query.get('page', ['1'])[0])))))
                self.reply({'error': 'Not found'}, 404)
            except (ValueError, UpstreamError) as exc:
                self.reply({'error': str(exc)}, 400)
            except Exception:
                self.reply({'error': '服务器处理失败'}, 500)

        def do_POST(self):
            try:
                origin = self.headers.get('Origin')
                if origin and urllib.parse.urlsplit(origin).netloc != self.headers.get('Host'):
                    return self.reply({'error': '跨站请求已拒绝'}, 403)
                if self.headers.get('Content-Type', '').split(';')[0] != 'application/json':
                    return self.reply({'error': '需要 application/json'}, 415)
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 16384:
                    raise ValueError('请求体大小不正确')
                data = json.loads(self.rfile.read(length))
                if not isinstance(data, dict):
                    raise ValueError('请求体必须为对象')
                if self.path == '/api/login':
                    if not isinstance(data.get('token'), str) or not hmac.compare_digest(data['token'], token):
                        time.sleep(0.5)
                        return self.reply({'error': '访问口令错误'}, 401)
                    secure = '; Secure' if os.environ.get('COOKIE_SECURE') == '1' else ''
                    return self.reply({'ok': True}, cookie='om_session=' + session + '; HttpOnly; SameSite=Strict; Path=/' + secure)
                if not self.authorized():
                    return self.reply({'error': '请先登录'}, 401)
                if self.path == '/api/logout':
                    return self.reply({'ok': True}, cookie='om_session=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/')
                if self.path == '/api/targets':
                    monitor.add(data.get('id'))
                elif self.path == '/api/toggle':
                    monitor.toggle(str(data.get('id')), data.get('enabled'))
                elif self.path == '/api/futures-account':
                    name, key, secret = (data.get(k, '').strip() for k in ('name', 'api_key', 'api_secret'))
                    if not all((name, key, secret)) or any(len(x) > 256 for x in (name, key, secret)):
                        raise ValueError('请填写账号名称、API Key 和 Secret Key')
                    with store.db() as db:
                        try:
                            db.execute('INSERT INTO futures_accounts(name,api_key,api_secret) VALUES(?,?,?)', (name, key, secret))
                        except sqlite3.IntegrityError:
                            raise ValueError('账号名称已存在') from None
                elif self.path == '/api/copy-binding':
                    account_id, target_id, selected = data.get('account_id'), str(data.get('target_id', '')), data.get('selected')
                    if not isinstance(account_id, int) or not isinstance(selected, bool):
                        raise ValueError('关联参数错误')
                    with store.db() as db:
                        if not db.execute('SELECT 1 FROM futures_accounts WHERE id=?', (account_id,)).fetchone() or not db.execute('SELECT 1 FROM targets WHERE id=?', (target_id,)).fetchone():
                            raise ValueError('账号或带单员不存在')
                        if selected:
                            db.execute('INSERT OR IGNORE INTO copy_bindings VALUES(?,?)', (account_id, target_id))
                        else:
                            db.execute('UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?', (account_id,target_id))
                            db.execute('DELETE FROM copy_bindings WHERE account_id=? AND target_id=?', (account_id, target_id))
                elif self.path == '/api/virtual-session':
                    account_id, target_id, active = data.get('account_id'), str(data.get('target_id', '')), data.get('active')
                    if not isinstance(account_id, int) or not isinstance(active, bool):
                        raise ValueError('参数错误')
                    with monitor.lock, store.db() as db:
                        binding = db.execute('SELECT t.initialized,t.enabled FROM copy_bindings b JOIN targets t ON t.id=b.target_id WHERE b.account_id=? AND b.target_id=?', (account_id,target_id)).fetchone()
                        if not binding:
                            raise ValueError('请先关联带单员')
                        if active:
                            if not binding['initialized'] or not binding['enabled']:
                                raise ValueError('请先开启监听并完成首次同步')
                            if db.execute('SELECT 1 FROM virtual_sessions WHERE account_id=? AND target_id=?', (account_id,target_id)).fetchone():
                                raise ValueError('已有跟踪记录，不能重置为零')
                            db.execute('INSERT INTO virtual_sessions VALUES(?,?,?,1)', (account_id,target_id,int(time.time()*1000)))
                        else:
                            db.execute('UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?', (account_id,target_id))
                elif self.path == '/api/test-notification':
                    channel = data.get('channel')
                    if channel not in ('telegram', 'bark'):
                        raise ValueError('未知通知渠道')
                    settings = store.settings()
                    keys = ('telegram_token', 'telegram_chat') if channel == 'telegram' else ('bark_url', 'bark_key')
                    for key in keys:
                        value = data.get(key, '')
                        if not isinstance(value, str) or len(value) > 1000:
                            raise ValueError('通知配置格式错误')
                        if value:
                            settings[key] = value
                    if channel == 'telegram' and not (settings['telegram_token'] and settings['telegram_chat']):
                        raise ValueError('请填写 Telegram Token 和 Chat ID')
                    if channel == 'bark':
                        url = urllib.parse.urlsplit(settings['bark_url'])
                        if url.scheme != 'https' or not url.hostname or url.username or url.query or url.fragment or not settings['bark_key']:
                            raise ValueError('请填写 HTTPS Bark 服务地址和 Device Key')
                    try:
                        send_notification(channel, settings, '订单监听 · 测试通知', '通知配置正常，这是一条测试消息。')
                    except Exception:
                        raise ValueError('测试发送失败，请检查通知配置及服务器网络') from None
                elif self.path == '/api/settings':
                    store.save_settings(data)
                else:
                    return self.reply({'error': 'Not found'}, 404)
                self.reply({'ok': True})
            except (ValueError, UpstreamError, InvalidOperation) as exc:
                self.reply({'error': str(exc)}, 400)
            except Exception:
                self.reply({'error': '服务器处理失败'}, 500)
    return Handler


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    token = os.environ.get('ADMIN_TOKEN', '')
    if len(token) < 16:
        raise SystemExit('请设置至少 16 位的 ADMIN_TOKEN 环境变量')
    database = os.environ.get('DATABASE_PATH', str(ROOT / 'data' / 'monitor.db'))
    try:
        store = Store(database)
    except (OSError, sqlite3.Error) as exc:
        raise SystemExit('数据库初始化失败：%s；请检查数据目录及文件写权限。%s: %s' % (database, type(exc).__name__, exc)) from None
    monitor = Monitor(store)
    worker = threading.Thread(target=monitor.run, daemon=True)
    worker.start()
    server = ThreadingHTTPServer((os.environ.get('HOST', '127.0.0.1'), int(os.environ.get('PORT', '8080'))), handler_factory(store, monitor, token))
    LOG.info('订单监听服务已启动')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        monitor.stop.set()
        server.server_close()


if __name__ == '__main__':
    main()
