"""Single-process Binance lead portfolio monitor. Python standard library only."""
import contextlib
from concurrent.futures import ThreadPoolExecutor
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
from decimal import Decimal, InvalidOperation, localcontext
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from virtual_position import transition
from leader_replay import replay
from copy_executor import FuturesClient, ExchangeError, plan
from fractions import Fraction

ROOT = Path(__file__).parent
API_PATH = '/bapi/futures/v1/friendly/future/copy-trade/'
DEFAULTS = {'interval': 60, 'page_size': 20, 'balance_threshold': '0', 'telegram_enabled': False,
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


def notification_number(value):
    """Format a finite decimal to at most four places without losing precision in storage."""
    try:
        value = Fraction(number(value))
    except (ValueError, TypeError, InvalidOperation, OverflowError):
        return '—'
    negative = value < 0
    scaled = abs(value) * 10000
    units, remainder = divmod(scaled.numerator, scaled.denominator)
    if remainder * 2 >= scaled.denominator:
        units += 1
    whole, fractional = divmod(units, 10000)
    suffix = ('.%04d' % fractional).rstrip('0').rstrip('.')
    return ('-' if negative and units else '') + str(whole) + suffix


def actual_copy_notional(result, executed):
    """Use exchange cumulative quote, or actual quantity and exchange average price."""
    if executed == 0:
        return '0'
    from virtual_position import decimal_string
    for field in ('cumQuote', 'avgPrice'):
        try:
            value = number(result[field])
            if value > 0:
                return decimal_string(Fraction(value) * (Fraction(executed) if field == 'avgPrice' else 1))
        except (KeyError, ValueError, TypeError, InvalidOperation, OverflowError):
            continue
    return None


def direction(row):
    return {('BUY', 'LONG'): '开多', ('SELL', 'LONG'): '平多',
            ('SELL', 'SHORT'): '开空', ('BUY', 'SHORT'): '平空'}.get(
                (row.get('side'), row.get('positionSide')), row.get('side', '未知'))


def identity(kind, row):
    fields = ('symbol', 'side', 'positionSide', 'type', 'orderTime') if kind == 'order' else (
        'time', 'coin', 'amount', 'from', 'to', 'transType')
    return hashlib.sha256(dumps([row.get(k) for k in fields]).encode()).hexdigest()


def history_identity(row):
    def decimal_key(value):
        text = format(number(value), 'f')
        return text.rstrip('0').rstrip('.') if '.' in text else text
    return hashlib.sha256(dumps([str(row.get('symbol','')).strip().upper(),
                                  str(row.get('side','')).strip().upper(),
                                  str(row.get('positionSide','')).strip().upper(),
                                  int(row['orderTime']),decimal_key(row['executedQty']),
                                  decimal_key(row['avgPrice']),decimal_key(row.get('totalPnl') or '0')]).encode()).hexdigest()


def clean_history(row):
    row = dict(row)
    for field in ('symbol','side','positionSide'):
        if not isinstance(row.get(field),str) or not row[field].strip():
            raise ValueError('历史交易字段缺失：'+field)
        row[field] = row[field].strip().upper()
    row['orderTime'] = int(row['orderTime'])
    if row['orderTime'] < 0:
        raise ValueError('历史交易时间无效')
    return row


def merge_order_fills(rows):
    """Merge executions from the same symbol, side and order timestamp."""
    groups = {}
    for row in rows:
        row = clean_history(row)
        key = (row['symbol'],row['side'],row['positionSide'],row['orderTime'])
        groups.setdefault(key, {})[history_identity(row)] = row
    merged = []
    for entries in groups.values():
        fills = sorted(entries.values(), key=lambda r:(str(r.get('type','')),history_identity(r)))
        if len(fills) == 1:
            merged.append(fills[0]); continue
        qty = sum((Fraction(number(r['executedQty'])) for r in fills),Fraction(0))
        if qty <= 0: raise ValueError('合并成交数量无效')
        cost = sum((Fraction(number(r['executedQty']))*Fraction(number(r['avgPrice'])) for r in fills),Fraction(0))
        pnl = sum((Fraction(number(r.get('totalPnl') or '0')) for r in fills),Fraction(0))
        from virtual_position import decimal_string
        with localcontext() as ctx:
            ctx.prec = 40
            average = cost/qty
            avg = format(Decimal(average.numerator)/Decimal(average.denominator),'f')
        merged.append({**fills[0], 'executedQty':decimal_string(qty), 'avgPrice':avg, 'totalPnl':decimal_string(pnl)})
    return sorted(merged,key=lambda r:(r['orderTime'],r['symbol'],r['side'],r['positionSide']))


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
        if path not in ('/fapi/v3/balance', '/fapi/v3/positionRisk', '/fapi/v2/account', '/fapi/v1/symbolConfig'):
            raise ValueError('不支持的查询')
        query = urllib.parse.urlencode({'timestamp': int(time.time() * 1000), 'recvWindow': 5000})
        signature = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        request = urllib.request.Request('https://fapi.binance.com' + path + '?' + query + '&signature=' + signature,
            headers={'X-MBX-APIKEY': self.api_key})
        try:
            with urllib.request.urlopen(request, timeout=5 if path == '/fapi/v1/symbolConfig' else 20) as response:
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
        if not isinstance(result, dict if path == '/fapi/v2/account' else list):
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
            CREATE INDEX IF NOT EXISTS records_target_time ON records(target,kind,stamp DESC);
            CREATE TABLE IF NOT EXISTS leader_history (
              target TEXT NOT NULL, key TEXT NOT NULL, stamp INTEGER NOT NULL, body TEXT NOT NULL,
              PRIMARY KEY(target,key));
            CREATE TABLE IF NOT EXISTS leader_history_stage (
              target TEXT NOT NULL, key TEXT NOT NULL, stamp INTEGER NOT NULL, body TEXT NOT NULL,
              PRIMARY KEY(target,key));
            CREATE INDEX IF NOT EXISTS leader_history_time ON leader_history(target,stamp);
            CREATE TABLE IF NOT EXISTS leader_history_sync (
              target TEXT PRIMARY KEY, page INTEGER NOT NULL DEFAULT 1, complete INTEGER NOT NULL DEFAULT 0,
              total INTEGER NOT NULL DEFAULT 0, next_check REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', updated REAL);
            CREATE TABLE IF NOT EXISTS app_migrations (name TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS baseline_watermarks (
              target TEXT NOT NULL, kind TEXT NOT NULL, stamp INTEGER NOT NULL,
              PRIMARY KEY(target,kind));
            CREATE TABLE IF NOT EXISTS events (
              id INTEGER PRIMARY KEY AUTOINCREMENT, target TEXT NOT NULL, name TEXT NOT NULL,
              kind TEXT NOT NULL, body TEXT NOT NULL, stamp INTEGER NOT NULL, historical INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS event_time ON events(stamp DESC,id DESC);
            CREATE TABLE IF NOT EXISTS futures_accounts (
              id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
              api_key TEXT NOT NULL, api_secret TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS display_order (
              kind TEXT NOT NULL, item_id TEXT NOT NULL, position INTEGER NOT NULL,
              PRIMARY KEY(kind, item_id));
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
            CREATE TABLE IF NOT EXISTS copy_live (
              account_id INTEGER NOT NULL, target_id TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 0,
              started_at INTEGER NOT NULL, PRIMARY KEY(account_id,target_id));
            CREATE TABLE IF NOT EXISTS copy_tasks (
              id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL, target_id TEXT NOT NULL,
              order_key TEXT NOT NULL, body TEXT NOT NULL, action TEXT NOT NULL, before_qty TEXT NOT NULL,
              lead_balance TEXT NOT NULL, client_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL DEFAULT 'pending',
              quantity TEXT, result TEXT, error TEXT NOT NULL DEFAULT '', created INTEGER NOT NULL, next_check REAL NOT NULL DEFAULT 0,
              UNIQUE(account_id,target_id,order_key));
            CREATE TABLE IF NOT EXISTS copy_task_audit (
              task_id INTEGER PRIMARY KEY, balance_time INTEGER NOT NULL, asset TEXT NOT NULL,
              wallet_balance TEXT, available_balance TEXT, lead_balance TEXT NOT NULL,
              reference_price TEXT NOT NULL, planned_quantity TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS copied_positions (
              account_id INTEGER NOT NULL, target_id TEXT NOT NULL, symbol TEXT NOT NULL,
              position_side TEXT NOT NULL, quantity TEXT NOT NULL,
              PRIMARY KEY(account_id,target_id,symbol,position_side));
            CREATE TABLE IF NOT EXISTS margin_alert_state (
              account_id INTEGER NOT NULL, risk_key TEXT NOT NULL, active INTEGER NOT NULL,
              PRIMARY KEY(account_id, risk_key));
            CREATE TABLE IF NOT EXISTS outbox (
              id INTEGER PRIMARY KEY AUTOINCREMENT, event INTEGER NOT NULL, channel TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
              next_try REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', UNIQUE(event,channel));
            ''')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(copy_tasks)')}
            if 'retry_count' not in columns:
                db.execute('ALTER TABLE copy_tasks ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0')
            db.execute('INSERT OR IGNORE INTO settings VALUES(1,?)', (dumps(DEFAULTS),))
            sync_columns = {row['name'] for row in db.execute('PRAGMA table_info(leader_history_sync)')}
            if 'last_batch' not in sync_columns:
                db.execute("ALTER TABLE leader_history_sync ADD COLUMN last_batch TEXT NOT NULL DEFAULT ''")
            if 'started_at' not in sync_columns:
                db.execute('ALTER TABLE leader_history_sync ADD COLUMN started_at INTEGER NOT NULL DEFAULT 0')
            if 'failures' not in sync_columns:
                db.execute('ALTER TABLE leader_history_sync ADD COLUMN failures INTEGER NOT NULL DEFAULT 0')
            if not db.execute("SELECT 1 FROM app_migrations WHERE name='history_identity_v2'").fetchone():
                saved = db.execute('SELECT target,stamp,body FROM leader_history').fetchall()
                existing_records = db.execute("SELECT target,stamp,body FROM records WHERE kind='order'").fetchall()
                db.execute('DELETE FROM leader_history')
                for row in saved + existing_records:
                    cleaned = clean_history(json.loads(row['body']))
                    db.execute('INSERT OR REPLACE INTO leader_history VALUES(?,?,?,?)',(row['target'],history_identity(cleaned),cleaned['orderTime'],dumps(cleaned)))
                db.execute("UPDATE leader_history_sync SET page=1,complete=0,next_check=0,last_batch='',error=''")
                db.execute("INSERT INTO app_migrations VALUES('history_identity_v2')")
            if not db.execute("SELECT 1 FROM app_migrations WHERE name='history_identity_v3'").fetchone():
                db.execute('CREATE TABLE IF NOT EXISTS leader_history_before_v3 AS SELECT * FROM leader_history')
                saved = db.execute('SELECT target,stamp,body FROM leader_history').fetchall()
                db.execute('DELETE FROM leader_history')
                for row in saved:
                    cleaned = clean_history(json.loads(row['body']))
                    db.execute('INSERT OR REPLACE INTO leader_history VALUES(?,?,?,?)',(row['target'],history_identity(cleaned),cleaned['orderTime'],dumps(cleaned)))
                db.execute("UPDATE leader_history_sync SET page=1,complete=0,next_check=0,last_batch='',error=''")
                db.execute("INSERT INTO app_migrations VALUES('history_identity_v3')")
            if not db.execute("SELECT 1 FROM app_migrations WHERE name='history_page_size_100'").fetchone():
                db.execute('DELETE FROM leader_history_stage')
                db.execute("UPDATE leader_history_sync SET page=1,complete=0,next_check=0,last_batch='',error=''")
                db.execute("INSERT INTO app_migrations VALUES('history_page_size_100')")
            if not db.execute("SELECT 1 FROM app_migrations WHERE name='history_identity_v4'").fetchone():
                db.execute('CREATE TABLE IF NOT EXISTS leader_history_before_v4 AS SELECT * FROM leader_history')
                saved = db.execute('SELECT target,body FROM leader_history').fetchall()
                archived = db.execute('SELECT target,body FROM leader_history_before_v3').fetchall()
                existing = db.execute("SELECT target,body FROM records WHERE kind='order'").fetchall()
                db.execute('DELETE FROM leader_history')
                for row in saved + archived + existing:
                    cleaned = clean_history(json.loads(row['body']))
                    db.execute('INSERT OR REPLACE INTO leader_history VALUES(?,?,?,?)',(row['target'],history_identity(cleaned),cleaned['orderTime'],dumps(cleaned)))
                db.execute('DELETE FROM leader_history_stage')
                db.execute("UPDATE leader_history_sync SET page=1,complete=0,next_check=0,last_batch='',error='' ")
                db.execute("INSERT INTO app_migrations VALUES('history_identity_v4')")
            if not db.execute("SELECT 1 FROM app_migrations WHERE name='poll_page_size_100'").fetchone():
                current = json.loads(db.execute('SELECT value FROM settings WHERE id=1').fetchone()[0])
                current['page_size'] = 100
                db.execute('UPDATE settings SET value=? WHERE id=1',(dumps(current),))
                db.execute("INSERT INTO app_migrations VALUES('poll_page_size_100')")
            if not db.execute("SELECT 1 FROM app_migrations WHERE name='poll_page_size_20'").fetchone():
                current = json.loads(db.execute('SELECT value FROM settings WHERE id=1').fetchone()[0])
                current['page_size'] = 20
                db.execute('UPDATE settings SET value=? WHERE id=1',(dumps(current),))
                db.execute("INSERT INTO app_migrations VALUES('poll_page_size_20')")
            db.execute('INSERT OR IGNORE INTO baseline_watermarks SELECT r.target,r.kind,MAX(r.stamp) FROM records r JOIN targets t ON t.id=r.target WHERE t.initialized=1 GROUP BY r.target,r.kind')
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

    @staticmethod
    def ordered_rows(db, kind):
        if kind == 'targets':
            query = "SELECT t.* FROM targets t LEFT JOIN display_order d ON d.kind='targets' AND d.item_id=t.id ORDER BY d.position IS NULL,d.position,t.name,t.id"
        elif kind == 'accounts':
            query = "SELECT a.id,a.name FROM futures_accounts a LEFT JOIN display_order d ON d.kind='accounts' AND d.item_id=CAST(a.id AS TEXT) ORDER BY d.position IS NULL,d.position,a.id"
        else:
            raise ValueError('排序类型无效')
        return db.execute(query).fetchall()

    @staticmethod
    def ordered_ids(db, kind):
        return [str(row['id']) for row in Store.ordered_rows(db, kind)]

    def reorder(self, kind, item_id, relative_id, side):
        if kind not in ('targets', 'accounts') or side not in ('before', 'after'):
            raise ValueError('排序参数无效')
        if not isinstance(item_id, str) or not isinstance(relative_id, str) or not item_id.isdigit() or not relative_id.isdigit() or len(item_id) > 30 or len(relative_id) > 30:
            raise ValueError('排序对象无效')
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            ids = self.ordered_ids(db, kind)
            if item_id not in ids or relative_id not in ids:
                raise ValueError('排序对象不存在，请刷新后重试')
            if item_id == relative_id:
                return
            ids.remove(item_id)
            index = ids.index(relative_id) + (side == 'after')
            ids.insert(index, item_id)
            db.execute('DELETE FROM display_order WHERE kind=?', (kind,))
            db.executemany('INSERT INTO display_order(kind,item_id,position) VALUES(?,?,?)', ((kind, value, i) for i, value in enumerate(ids)))

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
        self.history_lock = threading.RLock()
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
                db.execute("UPDATE copy_tasks SET state='cancelled',error='监听已关闭' WHERE target_id=? AND state='pending'",(pid,))
                db.execute('UPDATE copy_live SET enabled=0 WHERE target_id=?',(pid,))
                db.execute('UPDATE virtual_sessions SET active=0 WHERE target_id=?',(pid,))

    def collect(self, pid, kind, initialized):
        with self.store.db() as db:
            stamps = db.execute('SELECT MAX(stamp) FROM records WHERE target=? AND kind=?', (pid, kind)).fetchone()
            watermark = stamps[0]
            page_size = self.store.settings(db)['page_size'] if initialized else 20
            if not initialized and kind == 'order':
                watermark = db.execute('SELECT MIN(started_at) FROM copy_live WHERE target_id=? AND enabled=1',(pid,)).fetchone()[0]
        rows = []
        time_key = 'orderTime' if kind == 'order' else 'time'
        crossed = False
        for page in range(1, 51):
            data = self.api.history(pid, kind, page, page_size)
            batch = data['list']
            rows.extend(batch)
            if not initialized and watermark is None:
                return rows  # Initial page is a baseline, not a notification backlog.
            if not batch:
                return rows
            if crossed:
                return rows
            if watermark is not None and min(int(x[time_key]) for x in batch) < watermark:
                crossed = True  # Fetch one overlap page for records arriving out of order.
        raise UpstreamError('新增记录超过 %s 条，未推进同步位置；可增加每页数量后重试' % (50 * page_size))

    def virtual_apply(self, db, pid, key, row, lead_balance):
        sessions = db.execute('SELECT account_id,started_at FROM virtual_sessions WHERE target_id=? AND active=1', (pid,)).fetchall()
        for session in sessions:
            aid, started = session['account_id'], session['started_at']
            if int(row['orderTime']) <= started:
                continue
            if db.execute('SELECT 1 FROM virtual_events WHERE account_id=? AND target_id=? AND order_key=?', (aid, pid, key)).fetchone():
                continue
            symbol, side = row['symbol'], row['positionSide']
            latest = db.execute('SELECT MAX(stamp) FROM virtual_events WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?', (aid,pid,symbol,side)).fetchone()[0]
            if latest is not None and int(row['orderTime']) < latest:
                db.execute('UPDATE copy_live SET enabled=0 WHERE account_id=? AND target_id=?', (aid,pid))
                db.execute('UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?', (aid,pid))
                db.execute("UPDATE copy_tasks SET state='cancelled',error='交易记录乱序' WHERE account_id=? AND target_id=? AND state='pending'", (aid,pid))
                self.copy_notice(db, {'target_id':pid}, '%s 交易记录延迟到达，顺序无法确认，已暂停跟单，请核对持仓' % symbol)
                continue
            position = db.execute('SELECT quantity FROM virtual_positions WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?', (aid, pid, symbol, side)).fetchone()
            change = transition(position['quantity'] if position else '0', row['side'], side, row['executedQty'])
            db.execute('INSERT INTO virtual_events VALUES(?,?,?,?,?,?,?,?,?,?)', (aid, pid, key, symbol, side, int(row['orderTime']), change['action'], str(row['executedQty']), change['before'], change['after']))
            live=db.execute('SELECT enabled,started_at FROM copy_live WHERE account_id=? AND target_id=?', (aid,pid)).fetchone()
            if not live or not live['enabled'] or int(row['orderTime']) <= live['started_at']:
                if change['action'] != 'unmatched_close':
                    db.execute('INSERT OR REPLACE INTO virtual_positions VALUES(?,?,?,?,?)',(aid,pid,symbol,side,change['after']))
                continue
            client_id = 'cp_' + hashlib.sha256((str(aid)+':'+pid+':'+key).encode()).hexdigest()[:30]
            db.execute('INSERT OR IGNORE INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,created) VALUES(?,?,?,?,?,?,?,?,?)',
                       (aid,pid,key,dumps(row),change['action'],change['before'],str(lead_balance),client_id,int(time.time()*1000)))
            if change['action'] != 'unmatched_close':
                db.execute('INSERT OR REPLACE INTO virtual_positions VALUES(?,?,?,?,?)', (aid,pid,symbol,side,change['after']))

    def poll(self, pid):
        with self.store.db() as db:
            target = db.execute('SELECT * FROM targets WHERE id=?', (pid,)).fetchone()
        if not target or not target['enabled']:
            return
        try:
            detail = self.api.detail(pid)
            histories = {kind: self.collect(pid, kind, target['initialized']) for kind in ('order', 'transfer')}
            with self.lock, self.store.db() as db:
                current = db.execute('SELECT initialized,enabled FROM targets WHERE id=?', (pid,)).fetchone()
                if not current or not current['enabled'] or current['initialized'] != target['initialized']:
                    return
                settings = self.store.settings(db)
                first = not target['initialized']
                for kind, rows in histories.items():
                    time_key = 'orderTime' if kind == 'order' else 'time'
                    baseline = db.execute('SELECT stamp FROM baseline_watermarks WHERE target=? AND kind=?', (pid,kind)).fetchone()
                    if kind == 'order':
                        for raw in rows:
                            cleaned = clean_history(raw)
                            stamp = int(cleaned['orderTime'])
                            db.execute('INSERT OR REPLACE INTO leader_history VALUES(?,?,?,?)', (pid,history_identity(cleaned),stamp,dumps(cleaned)))
                            active = db.execute('SELECT 1 FROM leader_history_sync WHERE target=? AND complete=0 AND started_at>0',(pid,)).fetchone()
                            if active:
                                db.execute('INSERT OR REPLACE INTO leader_history_stage VALUES(?,?,?,?)', (pid,history_identity(cleaned),stamp,dumps(cleaned)))
                        rows = merge_order_fills(rows)
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
                            # A reused identity with different immutable execution fields is ambiguous.
                            # Stop copying and cancel pending tasks rather than treating it as an update.
                            old = json.loads(previous['body'])
                            if kind == 'order' and any(str(old.get(field)) != str(row.get(field)) for field in ('executedQty','avgPrice')):
                                sessions = db.execute('SELECT account_id,target_id FROM copy_live WHERE target_id=? AND enabled=1', (pid,)).fetchall()
                                for session in sessions:
                                    db.execute('UPDATE copy_live SET enabled=0 WHERE account_id=? AND target_id=?', (session['account_id'],pid))
                                    db.execute('UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?', (session['account_id'],pid))
                                    db.execute("UPDATE copy_tasks SET state='cancelled',error='订单标识冲突' WHERE account_id=? AND target_id=? AND state='pending'", (session['account_id'],pid))
                                    self.copy_notice(db, {'target_id':pid}, '%s 订单标识冲突，已关闭跟单，请核对记录' % row['symbol'])
                            # Refresh the original display row; never enqueue an update as a new order.
                            candidates = db.execute('SELECT id,body FROM events WHERE target=? AND kind=? AND stamp=?', (pid, kind, stamp)).fetchall()
                            for event in candidates:
                                if identity(kind, json.loads(event['body'])) == key:
                                    db.execute('UPDATE events SET body=? WHERE id=?', (dumps(body), event['id']))
                            continue
                        live_started = db.execute('SELECT 1 FROM copy_live WHERE target_id=? AND enabled=1 AND started_at<? LIMIT 1',(pid,stamp)).fetchone() if first and kind == 'order' else None
                        historical = (first and not live_started) or (not first and baseline is not None and stamp <= baseline['stamp'])
                        self.store.event(db, pid, detail['nickname'], kind, body, stamp, historical, settings)
                        if kind == 'order' and not historical:
                            self.virtual_apply(db, pid, key, row, detail['marginBalance'])
                    if first and rows:
                        db.execute('INSERT OR REPLACE INTO baseline_watermarks VALUES(?,?,?)', (pid,kind,max(int(r[time_key]) for r in rows)))
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
        title = event['name'] + ' · ' + {'order': '交易操作', 'transfer': '资金划转', 'balance': '余额变化', 'copy': '跟单结果', 'margin': '保证金风险'}[event['kind']]
        if event['kind'] == 'order':
            text = '%s %s%s\n数量：%s %s\n均价：%s %s\n成交金额：%s %s\n已实现盈亏：%s %s' % (
                body['symbol'], body['direction'], '（记录更新）' if body.get('updated') else '',
                notification_number(body['executedQty']), body.get('baseAsset', ''), notification_number(body['avgPrice']), body.get('quoteAsset', ''),
                notification_number(body['notional']), body.get('quoteAsset', ''), notification_number(body.get('totalPnl')), body.get('quoteAsset', ''))
        elif event['kind'] in ('copy', 'margin'):
            text = body['message']
        elif event['kind'] == 'transfer':
            text = '%s\n%s %s\n%s → %s' % (body['transType'], notification_number(body['amount']), body['coin'], body.get('from', ''), body.get('to', ''))
        else:
            text = '%s → %s %s\n变动：%s\n余额快照差异，可能包含浮动盈亏。' % (body['before'], body['after'], body['coin'], body['delta'])
        text += '\n时间（UTC+8）：' + time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(event['stamp'] / 1000 + 8 * 3600))
        return title, text

    def deliver(self):
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
            except Exception as exc:
                attempts = task['attempts'] + 1
                reason = notification_error(exc)
                LOG.warning('notification channel=%s event=%s attempt=%s: %s', channel, task['id'], attempts, reason)
                with self.store.db() as db:
                    db.execute('UPDATE outbox SET attempts=?,next_try=?,error=? WHERE id=?',
                               (attempts, time.time() + min(3600, 10 * 2 ** min(attempts, 8)), reason, task['task_id']))

    def copy_notice(self, db, task, message):
        name = db.execute('SELECT name FROM targets WHERE id=?', (task['target_id'],)).fetchone()
        self.store.event(db, task['target_id'], name['name'] if name else task['target_id'], 'copy', {'message':message}, int(time.time()*1000), False, self.store.settings(db))

    def execute_copy(self):
        # Serial worker: a submitted order with unknown outcome is reconciled, never re-sent.
        with self.lock:
            with self.store.db() as db:
                task=db.execute("SELECT c.*,a.api_key,a.api_secret FROM copy_tasks c JOIN futures_accounts a ON a.id=c.account_id JOIN virtual_sessions s ON s.account_id=c.account_id AND s.target_id=c.target_id JOIN targets t ON t.id=c.target_id WHERE (c.state='submitted' AND c.next_check<=strftime('%s','now')) OR (c.state='pending' AND c.next_check<=strftime('%s','now') AND s.active=1 AND t.enabled=1 AND NOT EXISTS (SELECT 1 FROM copy_tasks u WHERE u.account_id=c.account_id AND (u.state='submitted' OR (u.state='pending' AND u.id<c.id)))) ORDER BY CASE WHEN c.state='submitted' THEN 0 ELSE 1 END,c.id LIMIT 1").fetchone()
            if not task: return
            client=FuturesClient(task['api_key'],task['api_secret']); row=json.loads(task['body'])
            if task['state']=='pending':
                if task['action']=='open' and task['retry_count']:
                    with self.store.db() as db:
                        if int(time.time()*1000) - int(row['orderTime']) > 180000:
                            reason='带单员开仓操作超过 3 分钟，取消重试'
                            db.execute("UPDATE copy_tasks SET state='skipped',error=? WHERE id=? AND state='pending'",(reason,task['id']))
                            self.copy_notice(db,task,'%s 跟单跳过：%s' % (row['symbol'],reason))
                            return
                        closed = db.execute("SELECT 1 FROM virtual_events WHERE account_id=? AND target_id=? AND symbol=? AND position_side=? AND stamp>=? AND action IN ('close','unmatched_close') AND order_key!=? LIMIT 1", (task['account_id'],task['target_id'],row['symbol'],row['positionSide'],int(row['orderTime']),task['order_key'])).fetchone()
                        if closed:
                            reason='带单员后续已平仓或减仓，取消重试开仓'
                            db.execute("UPDATE copy_tasks SET state='skipped',error=? WHERE id=? AND state='pending'",(reason,task['id']))
                            self.copy_notice(db,task,'%s 跟单跳过：%s' % (row['symbol'],reason))
                            return
                try:
                    with self.store.db() as db:
                        old=db.execute('SELECT quantity FROM copied_positions WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?',(task['account_id'],task['target_id'],row['symbol'],row['positionSide'])).fetchone()
                    positions = None
                    if task['action'] in ('close','unmatched_close'):
                        from market_guard import close_position
                        positions = client.positions()
                        if not isinstance(positions, list): raise ValueError('账户持仓响应格式错误')
                        if close_position(positions,row['symbol'],row['positionSide']) <= 0:
                            raise ValueError('没有对应方向持仓，跳过平仓')
                    balances=client.balance()
                    balance_time=int(time.time()*1000)
                    if not isinstance(balances,list): raise ValueError('账户余额响应格式错误')
                    usdt=next((asset for asset in balances if asset.get('asset')=='USDT'),None)
                    mark_price = leverage = None
                    if task['action'] == 'open':
                        mark = client.mark_price(row['symbol'])
                        configs = client.symbol_config(row['symbol'])
                        if not isinstance(mark, dict) or mark.get('symbol') != row['symbol'] or not isinstance(configs, list):
                            raise ValueError('标记价格或杠杆配置响应无效')
                        config = next((c for c in configs if isinstance(c,dict) and c.get('symbol') == row['symbol']), None)
                        if not config: raise ValueError('未获取到交易对杠杆配置')
                        mark_price, leverage = mark.get('markPrice'), config.get('leverage')
                    if positions is None: positions = client.positions()
                    mode, rules = client.mode(), client.rules()
                    if task['action'] == 'close':
                        from market_guard import close_position
                        with self.store.db() as db:
                            amounts = db.execute('SELECT quantity FROM copied_positions WHERE account_id=? AND symbol=? AND position_side=?', (task['account_id'],row['symbol'],row['positionSide'])).fetchall()
                        total = sum((Fraction(number(x['quantity'])) for x in amounts), Fraction(0))
                        if total > Fraction(close_position(positions,row['symbol'],row['positionSide'])):
                            raise ValueError('跟单持仓记录超过账户实际持仓，请先核对')
                    quote=client.quote(row['symbol'])
                    plan_action, before_qty, owned_qty = task['action'], task['before_qty'], old['quantity'] if old else '0'
                    if plan_action in ('close','unmatched_close'):
                        with self.store.db() as db:
                            sync = db.execute('SELECT complete FROM leader_history_sync WHERE target=?',(task['target_id'],)).fetchone()
                            earlier = [json.loads(x['body']) for x in db.execute('SELECT body FROM leader_history WHERE target=? AND stamp<? ORDER BY stamp,key',(task['target_id'],int(row['orderTime'])))] if sync and sync['complete'] else []
                            simultaneous = [json.loads(x['body']) for x in db.execute('SELECT body FROM leader_history WHERE target=? AND stamp=?',(task['target_id'],int(row['orderTime'])))]
                            ambiguous = any(x['symbol']==row['symbol'] and x['positionSide']==row['positionSide'] and x['side']!=row['side'] for x in simultaneous)
                        prior = next((p for p in replay(earlier) if p['symbol']==row['symbol'] and p['side']==row['positionSide']),None) if earlier and not ambiguous else None
                        if prior and not prior['uncertain'] and number(prior['quantity'])>=number(row['executedQty']):
                            from market_guard import close_position
                            unmatched = plan_action == 'unmatched_close'
                            plan_action, before_qty = 'close', prior['quantity']
                            if unmatched:
                                owned_qty = str(close_position(positions,row['symbol'],row['positionSide']))
                    params=plan(row,plan_action,before_qty,owned_qty,task['lead_balance'],balances,positions,mode,quote,rules,mark_price=mark_price,leverage=leverage)
                    price=quote['askPrice'] if row['side']=='BUY' else quote['bidPrice']
                except (ValueError,ExchangeError,KeyError,InvalidOperation) as exc:
                    LOG.warning('跟单预检查失败 account=%s symbol=%s: %s', task['account_id'], row.get('symbol',''), str(exc)[:200])
                    with self.store.db() as db:
                        transient = isinstance(exc, ExchangeError) and (exc.status is None or exc.status in (408, 418, 429, 451) or 500 <= exc.status <= 599)
                        retries = task['retry_count']
                        if transient and retries < 3:
                            retries += 1
                            reason = '预检查网络错误，等待第 %s/3 次重试：%s' % (retries,str(exc)[:180])
                            db.execute("UPDATE copy_tasks SET retry_count=?,next_check=?,error=? WHERE id=? AND state='pending'",(retries,time.time()+5*2**(retries-1),reason,task['id']))
                        else:
                            reason = ('预检查重试 3 次后失败：' if transient else '') + str(exc)[:200]
                            db.execute("UPDATE copy_tasks SET state='skipped',error=? WHERE id=? AND state='pending'",(reason,task['id']))
                            self.copy_notice(db,task,'%s 跟单跳过：%s' % (row['symbol'],reason))
                    return
                params['newClientOrderId']=task['client_id']
                # Persist intent before touching the exchange, so process crashes cannot duplicate a POST.
                with self.store.db() as db:
                    db.execute('INSERT OR REPLACE INTO copy_task_audit(task_id,balance_time,asset,wallet_balance,available_balance,lead_balance,reference_price,planned_quantity) VALUES(?,?,?,?,?,?,?,?)', (task['id'],balance_time,'USDT',str(usdt['balance']) if usdt and 'balance' in usdt else None,str(usdt['availableBalance']) if usdt and 'availableBalance' in usdt else None,task['lead_balance'],str(price),params['quantity']))
                    changed = db.execute("UPDATE copy_tasks SET state='submitted',quantity=? WHERE id=? AND state='pending' AND EXISTS (SELECT 1 FROM copy_live l JOIN copy_bindings b ON b.account_id=l.account_id AND b.target_id=l.target_id JOIN targets t ON t.id=l.target_id WHERE l.account_id=copy_tasks.account_id AND l.target_id=copy_tasks.target_id AND l.enabled=1 AND t.enabled=1)",(params['quantity'],task['id'])).rowcount
                    if not changed:
                        return
                try: result=client.place(params)
                except ExchangeError as exc:
                    reason=str(exc)[:200]
                    if exc.definitively_rejected:
                        with self.store.db() as db:
                            db.execute("UPDATE copy_tasks SET state='skipped',error=? WHERE id=? AND state='submitted'", (reason,task['id']))
                            self.copy_notice(db,task,'%s 跟单被交易所拒绝：%s' % (row['symbol'],reason))
                        return
                    LOG.warning('跟单提交结果未知 account=%s symbol=%s: %s', task['account_id'], row['symbol'], reason)
                    with self.store.db() as db:
                        db.execute("UPDATE copy_tasks SET error=?,next_check=strftime('%s','now')+15 WHERE id=?",('下单结果未知，等待查询确认：'+reason,task['id']))
                        self.copy_notice(db,task,'%s 下单结果未知，正在查询订单状态：%s' % (row['symbol'],reason))
                    return
            else:
                try: result=client.order(row['symbol'],task['client_id'])
                except ExchangeError as exc:
                    reason=str(exc)[:200]
                    LOG.warning('跟单订单查询失败 account=%s symbol=%s: %s', task['account_id'], row['symbol'], reason)
                    with self.store.db() as db: db.execute("UPDATE copy_tasks SET error=?,next_check=strftime('%s','now')+15 WHERE id=?",('订单结果未知，等待查询确认：'+reason,task['id']))
                    return
            status=result.get('status')
            if status not in ('FILLED','CANCELED','EXPIRED','REJECTED','EXPIRED_IN_MATCH'):
                with self.store.db() as db: db.execute("UPDATE copy_tasks SET next_check=strftime('%s','now')+15 WHERE id=?", (task['id'],))
                return
            executed=number(result.get('executedQty','0'))
            with self.store.db() as db:
                saved=db.execute('SELECT quantity FROM copy_tasks WHERE id=?', (task['id'],)).fetchone()
            if not saved or not saved['quantity'] or executed<0 or executed>number(saved['quantity']) or (result.get('symbol') and result['symbol']!=row['symbol']) or (result.get('clientOrderId') and result['clientOrderId']!=task['client_id']):
                with self.store.db() as db:
                    db.execute("UPDATE copy_tasks SET error='订单响应与提交信息不一致，等待人工核对',next_check=strftime('%s','now')+15 WHERE id=?",(task['id'],))
                return
            from virtual_position import decimal_string
            with self.store.db() as db:
                current=db.execute('SELECT state FROM copy_tasks WHERE id=?',(task['id'],)).fetchone()
                if not current or current['state']!='submitted': return
                pos=db.execute('SELECT quantity FROM copied_positions WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?',(task['account_id'],task['target_id'],row['symbol'],row['positionSide'])).fetchone()
                before=Fraction(number(pos['quantity'] if pos else '0')); filled=Fraction(executed)
                after=before+filled if task['action']=='open' else max(Fraction(0),before-filled)
                db.execute('INSERT OR REPLACE INTO copied_positions VALUES(?,?,?,?,?)',(task['account_id'],task['target_id'],row['symbol'],row['positionSide'],decimal_string(after)))
                actual_notional = actual_copy_notional(result, executed)
                db.execute("UPDATE copy_tasks SET state=?,result=?,error='' WHERE id=?",('filled' if status=='FILLED' else 'finished',dumps({'status':status,'executedQty':str(executed),'orderId':result.get('orderId'), 'cumQuote':actual_notional}),task['id']))
                self.copy_notice(db,task,'%s %s，状态 %s\n实际成交数量：%s %s\n跟单金额：%s %s' % (row['symbol'],direction(row),status,notification_number(executed),row.get('baseAsset',''),notification_number(actual_notional),row.get('quoteAsset','USDT')))

    def check_margin(self, account):
        client = FuturesAccount(account['api_key'], account['api_secret'])
        with ThreadPoolExecutor(max_workers=2) as pool:
            account_future = pool.submit(client.get, '/fapi/v2/account')
            positions_future = pool.submit(client.get, '/fapi/v3/positionRisk')
            info, positions = account_future.result(), positions_future.result()
        details = {(p['symbol'], p['positionSide']): p for p in info['positions']}
        ratios = {}
        cross_maint = Decimal(0)
        cross_found = False
        for position in positions:
            if number(position['positionAmt']) == 0:
                continue
            key = (position['symbol'], position['positionSide'])
            detail = details.get(key)
            if detail is None or not isinstance(detail.get('isolated'), bool):
                raise ValueError('缺少持仓保证金模式')
            maint = number(position['maintMargin'])
            if maint < 0:
                raise ValueError('维持保证金无效')
            if detail['isolated']:
                margin = number(position['isolatedMargin'])
                risk_key = 'isolated:%s:%s' % key
                ratios[risk_key] = (maint, margin, '%s %s 逐仓' % key)
            else:
                if position.get('marginAsset') != 'USDT':
                    raise ValueError('暂不支持非 USDT 全仓保证金风险计算')
                cross_found = True
                cross_maint += maint
        if cross_found:
            if info.get('multiAssetsMargin') is not False:
                raise ValueError('暂不支持多资产模式全仓保证金风险计算')
            cross_margin = number(info['totalCrossWalletBalance']) + number(info['totalCrossUnPnl'])
            ratios['cross:USDT'] = (cross_maint, cross_margin, 'USDT 全仓')
        with self.store.db() as db:
            settings = self.store.settings(db)
            if not (settings['telegram_enabled'] or settings['bark_enabled']):
                db.execute('DELETE FROM margin_alert_state WHERE account_id=?', (account['id'],))
                return
            previous = {r['risk_key']: r['active'] for r in db.execute('SELECT risk_key,active FROM margin_alert_state WHERE account_id=?', (account['id'],))}
            for risk_key, (maint, margin, label) in ratios.items():
                high = maint > 0 and (margin <= 0 or maint * 100 >= margin * 60)
                if high and not previous.get(risk_key):
                    ratio = '≥100%' if margin <= 0 else '%.2f%%' % (maint / margin * 100)
                    message = '%s 保证金风险率 %s，达到 60%%\n维持保证金：%s\n保证金余额：%s' % (label, ratio, maint, margin)
                    self.store.event(db, 'margin:%s' % account['id'], account['name'], 'margin', {'message': message}, int(time.time()*1000), False, settings)
                db.execute('INSERT OR REPLACE INTO margin_alert_state VALUES(?,?,?)', (account['id'], risk_key, int(high)))
            for old_key in previous.keys() - ratios.keys():
                db.execute('DELETE FROM margin_alert_state WHERE account_id=? AND risk_key=?', (account['id'], old_key))

    def margin_monitor(self):
        errors = {}
        while not self.stop.is_set():
            with self.store.db() as db:
                accounts = [dict(row) for row in db.execute('SELECT id,name,api_key,api_secret FROM futures_accounts')]
            for account in accounts:
                if self.stop.is_set():
                    return
                try:
                    self.check_margin(account)
                    errors.pop(account['id'], None)
                except Exception as exc:
                    LOG.warning('账户 %s 保证金风险检查失败：%s', account['id'], str(exc)[:200])
                    reason = str(exc)[:200] if isinstance(exc, (ValueError, UpstreamError)) else '账户响应异常'
                    if errors.get(account['id']) != reason:
                        with self.store.db() as db:
                            self.store.event(db, 'margin:%s' % account['id'], account['name'], 'margin', {'message': '保证金风险检查失败：%s，请检查账户及服务日志' % reason}, int(time.time()*1000), False, self.store.settings(db))
                        errors[account['id']] = reason
            self.stop.wait(30)

    def sync_history(self, pid):
        with self.history_lock:
            self._sync_history(pid)

    def _sync_history(self, pid):
        with self.store.db() as db:
            db.execute('INSERT OR IGNORE INTO leader_history_sync(target) VALUES(?)',(pid,))
            sync = db.execute('SELECT * FROM leader_history_sync WHERE target=?',(pid,)).fetchone()
        if sync['next_check'] > time.time(): return
        page = sync['page']
        try:
            if page == 1:
                with self.store.db() as db:
                    db.execute('DELETE FROM leader_history_stage WHERE target=?',(pid,))
                    db.execute('UPDATE leader_history_sync SET started_at=?,complete=0 WHERE target=?',(int(time.time()*1000),pid))
            data = self.api.history(pid,'order',page,100)
            rows, total = [clean_history(row) for row in data['list']], int(data['total'])
            if total < 0: raise ValueError('历史总数无效')
            fingerprint = hashlib.sha256(dumps([history_identity(row) for row in rows]).encode()).hexdigest() if rows else ''
            if page > 1 and fingerprint and fingerprint == sync['last_batch']:
                raise UpstreamError('上游连续返回相同的历史页面，请检查分页接口；同步未完成')
            with self.store.db() as db:
                for row in rows:
                    db.execute('INSERT OR REPLACE INTO leader_history_stage VALUES(?,?,?,?)', (pid,history_identity(row),int(row['orderTime']),dumps(row)))
                complete = not rows
                if complete:
                    started = db.execute('SELECT started_at FROM leader_history_sync WHERE target=?',(pid,)).fetchone()[0]
                    for live in db.execute("SELECT body FROM records WHERE target=? AND kind='order' AND stamp>=?",(pid,started)).fetchall():
                        recent = clean_history(json.loads(live['body']))
                        db.execute('INSERT OR REPLACE INTO leader_history_stage VALUES(?,?,?,?)',(pid,history_identity(recent),recent['orderTime'],dumps(recent)))
                    db.execute('DELETE FROM leader_history WHERE target=?',(pid,))
                    db.execute('INSERT INTO leader_history SELECT * FROM leader_history_stage WHERE target=?',(pid,))
                    db.execute('DELETE FROM leader_history_stage WHERE target=?',(pid,))
                db.execute('UPDATE leader_history_sync SET page=?,complete=?,total=?,next_check=?,error=?,updated=?,last_batch=?,failures=0 WHERE target=?',
                           (1 if complete else page+1,int(complete),total,time.time()+(3600 if complete else 5),'',time.time(),fingerprint,pid))
        except Exception as exc:
            reason = str(exc)[:200] if isinstance(exc,(UpstreamError,ValueError)) else '历史同步失败'
            with self.store.db() as db:
                failures = sync['failures'] + 1
                delay = min(900,60 * 2**min(failures-1,4))
                db.execute('UPDATE leader_history_sync SET error=?,next_check=?,failures=? WHERE target=?',(reason,time.time()+delay,failures,pid))
            LOG.warning('portfolio %s history sync: %s',pid,reason)

    def history_worker(self):
        while not self.stop.is_set():
            with self.store.db() as db:
                ids = [r[0] for r in db.execute('SELECT id FROM targets WHERE enabled=1')]
            for pid in ids:
                if self.stop.is_set(): return
                try:
                    self.sync_history(pid)
                except Exception:
                    LOG.exception('portfolio %s history worker failed',pid)
                if self.stop.wait(5): return
            self.stop.wait(1)

    def run(self):
        def copies():
            while not self.stop.is_set():
                try:
                    self.execute_copy()
                except Exception:
                    LOG.exception('跟单任务处理异常，保留当前状态')
                self.stop.wait(1)

        def notifications():
            while not self.stop.is_set():
                try:
                    self.deliver()
                except Exception:
                    LOG.exception('通知任务处理异常')
                self.stop.wait(1)

        workers = [threading.Thread(target=target, daemon=True) for target in (copies, notifications, self.history_worker)]
        for worker in workers: worker.start()
        while not self.stop.is_set():
            with self.store.db() as db:
                ids = [r[0] for r in db.execute('SELECT id FROM targets WHERE enabled=1 AND next_check<=? ORDER BY next_check', (time.time(),))]
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(self.poll, ids))
            self.stop.wait(1)
        for worker in workers: worker.join(timeout=25)


class NotificationError(Exception):
    """Pre-sanitized notification failure, safe to show in the UI."""


def notification_error(exc):
    if isinstance(exc, NotificationError):
        return str(exc)
    if isinstance(exc, urllib.error.HTTPError):
        return '通知服务 HTTP %d' % exc.code
    reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return '通知服务连接超时'
    if isinstance(reason, ssl.SSLError):
        return '通知服务 TLS 连接失败'
    if isinstance(reason, socket.gaierror):
        return '通知服务域名解析失败'
    if isinstance(reason, urllib.error.URLError) or isinstance(exc, urllib.error.URLError):
        return '通知服务网络连接失败'
    return '通知发送失败，请检查通知配置及服务器网络'


def send_notification(channel, settings, title, message):
    if channel == 'telegram':
        url = 'https://api.telegram.org/bot' + settings['telegram_token'] + '/sendMessage'
        body = {'chat_id': settings['telegram_chat'], 'text': title + '\n' + message}
    else:
        url = settings['bark_url'].rstrip('/') + '/push'
        body = {'device_key': settings['bark_key'], 'title': title, 'body': message, 'group': '订单监听'}
    request = urllib.request.Request(url, data=dumps(body).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=15) as response:
        try:
            result = json.loads(response.read(1_000_000))
        except (ValueError, UnicodeDecodeError):
            raise NotificationError('通知服务响应不是有效 JSON') from None
    if not isinstance(result, dict):
        raise NotificationError('通知服务响应格式错误')
    if (channel == 'telegram' and result.get('ok') is not True) or (channel == 'bark' and result.get('code') != 200):
        code = result.get('code') if channel == 'bark' else result.get('error_code')
        detail = '，错误码 %d' % code if type(code) is int and 0 <= code <= 999999 else ''
        raise NotificationError('通知服务拒绝请求' + detail)


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
                self.send_header('Cache-Control', 'no-cache')
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
                        targets = [dict(row) for row in store.ordered_rows(db, 'targets')]
                        for row in targets:
                            row['detail'] = json.loads(row['detail'])
                        accounts = [dict(row) for row in store.ordered_rows(db, 'accounts')]
                        bindings = [dict(r) for r in db.execute('SELECT account_id,target_id FROM copy_bindings')]
                        virtual_sessions = [dict(r) for r in db.execute('SELECT account_id,target_id,active,started_at FROM virtual_sessions')]
                        virtual_positions = [dict(r) for r in db.execute("SELECT account_id,target_id,symbol,position_side,quantity FROM virtual_positions WHERE quantity != '0'")]
                        copy_live = [dict(r) for r in db.execute('SELECT account_id,target_id,enabled FROM copy_live')]
                        copy_tasks = [dict(r) for r in db.execute("SELECT c.id,c.account_id,c.target_id,c.state,c.error,c.created,c.result,c.body,a.balance_time,a.asset,a.wallet_balance,a.available_balance,a.lead_balance,a.reference_price,a.planned_quantity FROM copy_tasks c LEFT JOIN copy_task_audit a ON a.task_id=c.id ORDER BY c.id DESC LIMIT 50")]
                        virtual_events = [dict(r) for r in db.execute("SELECT account_id,target_id,symbol,position_side,stamp,action,quantity FROM virtual_events WHERE action='unmatched_close' ORDER BY stamp DESC LIMIT 20")]
                        counts = dict(db.execute('SELECT status,COUNT(*) FROM outbox GROUP BY status').fetchall())
                        errors = [dict(r) for r in db.execute("SELECT channel,attempts,error FROM outbox WHERE status='pending' AND error!='' ORDER BY id DESC LIMIT 5")]
                    settings = store.settings()
                    for key in SECRETS:
                        settings[key + '_configured'] = bool(settings.pop(key))
                    return self.reply({'targets': targets, 'settings': settings, 'notifications': counts, 'notification_errors': errors, 'futures_accounts': accounts, 'copy_bindings': bindings, 'virtual_sessions': virtual_sessions, 'virtual_positions': virtual_positions, 'virtual_events': virtual_events, 'copy_tasks': copy_tasks, 'copy_live': copy_live})
                if parsed.path == '/api/futures-network':
                    proxies = urllib.request.getproxies()
                    via_proxy = bool(proxies.get('https')) and not urllib.request.proxy_bypass('fapi.binance.com')
                    try:
                        FuturesClient('', '').request('/fapi/v1/ping')
                        return self.reply({'ok': True, 'proxy_enabled': via_proxy})
                    except ExchangeError as exc:
                        return self.reply({'ok': False, 'proxy_enabled': via_proxy, 'error': str(exc)})
                if parsed.path == '/api/futures-snapshot':
                    account_id = int(query.get('account_id', ['0'])[0])
                    with store.db() as db:
                        account = db.execute('SELECT api_key,api_secret FROM futures_accounts WHERE id=?', (account_id,)).fetchone()
                    if not account:
                        raise ValueError('账号不存在')
                    client = FuturesAccount(account['api_key'], account['api_secret'])
                    with ThreadPoolExecutor(max_workers=3) as pool:
                        balances_future = pool.submit(client.get, '/fapi/v3/balance')
                        positions_future = pool.submit(client.get, '/fapi/v3/positionRisk')
                        config_future = pool.submit(client.get, '/fapi/v1/symbolConfig')
                        balances, positions = balances_future.result(), positions_future.result()
                        try:
                            configs = config_future.result()
                            leverage = {c['symbol']: c['leverage'] for c in configs if isinstance(c, dict) and isinstance(c.get('symbol'), str) and str(c.get('leverage', '')).isdigit() and int(c['leverage']) > 0}
                        except Exception:
                            LOG.warning('账户 %s 杠杆配置查询失败', account_id)
                            leverage = {}
                    positions = [{**p, 'leverage': leverage.get(p.get('symbol'))} for p in positions]
                    return self.reply({'balances': balances, 'positions': positions, 'queried_at': int(time.time() * 1000)})
                if parsed.path == '/api/leader-positions':
                    pid = query.get('target', [''])[0]
                    with store.db() as db:
                        if not db.execute('SELECT 1 FROM targets WHERE id=?',(pid,)).fetchone():
                            raise ValueError('带单员不存在')
                        rows = [json.loads(r['body']) for r in db.execute('SELECT body FROM leader_history WHERE target=? ORDER BY stamp,key',(pid,))]
                        sync = db.execute('SELECT page,complete,total,error,updated,next_check,failures FROM leader_history_sync WHERE target=?',(pid,)).fetchone()
                        staged = db.execute('SELECT COUNT(*) FROM leader_history_stage WHERE target=?',(pid,)).fetchone()[0]
                    return self.reply({'positions':replay(rows), 'count':len(rows), 'staged':staged, 'sync':dict(sync) if sync else None})
                if parsed.path == '/api/copy-tasks':
                    account_id=int(query.get('account_id', ['0'])[0])
                    page=max(1,min(100000,int(query.get('page',['1'])[0])))
                    with store.db() as db:
                        if not db.execute('SELECT 1 FROM futures_accounts WHERE id=?',(account_id,)).fetchone():
                            raise ValueError('账号不存在')
                        total=db.execute('SELECT COUNT(*) FROM copy_tasks WHERE account_id=?',(account_id,)).fetchone()[0]
                        rows=[dict(r) for r in db.execute("SELECT c.id,c.target_id,c.state,c.error,c.created,c.result,c.body,a.asset,a.wallet_balance,a.available_balance,a.lead_balance,a.planned_quantity,t.name AS target_name FROM copy_tasks c LEFT JOIN copy_task_audit a ON a.task_id=c.id LEFT JOIN targets t ON t.id=c.target_id WHERE c.account_id=? ORDER BY c.id DESC LIMIT 50 OFFSET ?",(account_id,(page-1)*50))]
                    for row in rows:
                        row['body']=json.loads(row['body'])
                        row['result']=json.loads(row['result']) if row['result'] else None
                        if row['result'] and row['result'].get('orderId') is not None:
                            row['result']['orderId']=str(row['result']['orderId'])
                    return self.reply({'list':rows,'total':total,'page':page})
                if parsed.path == '/api/events':
                    page = max(1, min(100000, int(query.get('page', ['1'])[0])))
                    limit = max(1, min(50, int(query.get('limit', ['50'])[0])))
                    target, kind = query.get('target', [''])[0], query.get('kind', [''])[0]
                    if target and kind in ('leader','order','transfer'):
                        with store.db() as db:
                            exists = db.execute('SELECT name FROM targets WHERE id=?',(target,)).fetchone()
                            if exists:
                                rows = []
                                if kind in ('leader','order'):
                                    raw = [json.loads(r['body']) for r in db.execute('SELECT body FROM leader_history WHERE target=? ORDER BY stamp,key',(target,))]
                                    for body in merge_order_fills(raw):
                                        body['direction']=direction(body)
                                        body['notional']=decimal_text(number(body['executedQty'])*number(body['avgPrice']))
                                        rows.append({'target':target,'name':exists['name'],'kind':'order','body':body,'stamp':int(body['orderTime']),'historical':0})
                                if kind in ('leader','transfer'):
                                    for row in db.execute("SELECT target,name,kind,body,stamp,historical FROM events WHERE target=? AND kind='transfer'",(target,)):
                                        entry=dict(row);entry['body']=json.loads(entry['body']);rows.append(entry)
                                rows.sort(key=lambda r:(r['stamp'],r['kind'],str(r['body'].get('symbol',''))),reverse=True)
                                return self.reply({'list':rows[(page-1)*limit:page*limit],'total':len(rows),'page':page})
                    where, args = ("kind IN ('order','transfer')" if kind == 'leader' else "kind IN ('order','transfer','copy')"), []
                    if kind == 'leader': kind = ''
                    for column, value in [('target', target), ('kind', kind)]:
                        if value:
                            where += ' AND ' + column + '=?'
                            args.append(value)
                    with store.db() as db:
                        total = db.execute('SELECT COUNT(*) FROM events WHERE ' + where, args).fetchone()[0]
                        rows = [dict(r) for r in db.execute('SELECT * FROM events WHERE ' + where + ' ORDER BY stamp DESC,id DESC LIMIT ? OFFSET ?', args + [limit, (page - 1) * limit])]
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
                if self.path == '/api/display-order':
                    store.reorder(data.get('kind'), data.get('id'), data.get('relative_id'), data.get('side'))
                elif self.path == '/api/targets':
                    monitor.add(data.get('id'))
                elif self.path == '/api/leader-history-refresh':
                    pid = str(data.get('target',''))
                    with monitor.history_lock, store.db() as db:
                        if not db.execute('SELECT 1 FROM targets WHERE id=? AND enabled=1',(pid,)).fetchone():
                            raise ValueError('请先开启该带单员的监听')
                        db.execute('DELETE FROM leader_history_stage WHERE target=?',(pid,))
                        db.execute("INSERT INTO leader_history_sync(target,page,complete,total,next_check,error,last_batch,failures) VALUES(?,1,0,0,0,'','',0) ON CONFLICT(target) DO UPDATE SET page=1,complete=0,total=0,next_check=0,error='',last_batch='',failures=0",(pid,))
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
                    with monitor.lock, store.db() as db:
                        if not db.execute('SELECT 1 FROM futures_accounts WHERE id=?', (account_id,)).fetchone() or not db.execute('SELECT 1 FROM targets WHERE id=?', (target_id,)).fetchone():
                            raise ValueError('账号或带单员不存在')
                        if selected:
                            db.execute('INSERT OR IGNORE INTO copy_bindings VALUES(?,?)', (account_id, target_id))
                        else:
                            db.execute('UPDATE copy_live SET enabled=0 WHERE account_id=? AND target_id=?',(account_id,target_id))
                            db.execute('UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?', (account_id,target_id))
                            db.execute("UPDATE copy_tasks SET state='cancelled',error='跟单已关闭' WHERE account_id=? AND target_id=? AND state='pending'",(account_id,target_id))
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
                            db.execute('UPDATE copy_live SET enabled=0 WHERE account_id=? AND target_id=?',(account_id,target_id))
                            db.execute('UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?', (account_id,target_id))
                            db.execute("UPDATE copy_tasks SET state='cancelled',error='跟单已关闭' WHERE account_id=? AND target_id=? AND state='pending'",(account_id,target_id))
                elif self.path == '/api/copy-live':
                    account_id,target_id,enabled=data.get('account_id'),str(data.get('target_id','')),data.get('enabled')
                    if not isinstance(account_id,int) or not isinstance(enabled,bool): raise ValueError('参数错误')
                    with monitor.lock, store.db() as db:
                        binding=db.execute('SELECT t.initialized,t.enabled FROM copy_bindings b JOIN targets t ON t.id=b.target_id WHERE b.account_id=? AND b.target_id=?',(account_id,target_id)).fetchone()
                        if not binding: raise ValueError('请先关联带单员')
                        if enabled:
                            if not binding['enabled']: raise ValueError('请先开启监听')
                            current_live=db.execute('SELECT enabled FROM copy_live WHERE account_id=? AND target_id=?',(account_id,target_id)).fetchone()
                            if current_live and current_live['enabled']:
                                return self.reply({'ok': True})
                            if db.execute("SELECT 1 FROM copy_tasks WHERE account_id=? AND state='submitted'", (account_id,)).fetchone(): raise ValueError('该账号有结果未确认的订单，请先核对')
                            now=int(time.time()*1000)
                            db.execute('INSERT OR REPLACE INTO virtual_sessions VALUES(?,?,?,1)',(account_id,target_id,now))
                            db.execute('INSERT OR REPLACE INTO copy_live VALUES(?,?,1,?)',(account_id,target_id,now))
                            db.execute('UPDATE targets SET next_check=0 WHERE id=?',(target_id,))
                        else:
                            db.execute('UPDATE copy_live SET enabled=0 WHERE account_id=? AND target_id=?',(account_id,target_id))
                            db.execute('UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?',(account_id,target_id))
                            db.execute("UPDATE copy_tasks SET state='cancelled',error='跟单已关闭' WHERE account_id=? AND target_id=? AND state='pending'",(account_id,target_id))
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
                    except Exception as exc:
                        reason = notification_error(exc)
                        LOG.warning('notification test channel=%s: %s', channel, reason)
                        raise ValueError('测试发送失败：' + reason) from None
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
    threading.Thread(target=monitor.margin_monitor, daemon=True).start()
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
