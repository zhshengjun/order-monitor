"""Pure sizing for a user-confirmed USD-M futures position close."""
import re
from decimal import Decimal, InvalidOperation
from fractions import Fraction

from copy_calculation import steps_quantity
from copy_executor import market_rule
from virtual_position import decimal_string


def position_quantity(symbol, side, positions, mode, allow_empty=False):
    if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z0-9]{3,30}', symbol):
        raise ValueError('交易对无效')
    if side not in ('LONG', 'SHORT'):
        raise ValueError('平仓方向无效')
    if not isinstance(positions, list) or not isinstance(mode, dict) or type(mode.get('dualSidePosition')) is not bool:
        raise ValueError('账户持仓或持仓模式响应无效')
    hedged = mode['dualSidePosition']
    position_side = side if hedged else 'BOTH'
    for item in positions:
        if not isinstance(item,dict) or not isinstance(item.get('symbol'),str) or item.get('positionSide') not in ('BOTH','LONG','SHORT') or 'positionAmt' not in item:
            raise ValueError('账户持仓响应无效')
        if item['symbol']==symbol and ((hedged and item['positionSide']=='BOTH') or (not hedged and item['positionSide']!='BOTH')):
            raise ValueError('持仓模式与账户持仓不一致')
    matches = [p for p in positions if isinstance(p, dict) and p.get('symbol') == symbol and p.get('positionSide') == position_side]
    if not matches and allow_empty:
        return Decimal(0)
    if len(matches) != 1:
        raise ValueError('找不到唯一对应持仓，请刷新后重试')
    try:
        amount = Decimal(str(matches[0]['positionAmt']))
    except (KeyError, InvalidOperation, ValueError):
        raise ValueError('持仓数量无效') from None
    if not amount.is_finite() or (side == 'LONG' and amount < 0) or (side == 'SHORT' and amount > 0) or (not allow_empty and amount == 0):
        raise ValueError('持仓数量或方向已变化，请刷新后重试')
    return abs(amount)


def plan_manual_close(symbol, side, percent, positions, mode, exchange, attributed_qty):
    if type(percent) is not int or percent not in (20, 50, 100):
        raise ValueError('平仓比例无效')
    current = position_quantity(symbol, side, positions, mode)
    hedged = mode['dualSidePosition']
    position_side = side if hedged else 'BOTH'
    try:
        lot, _ = market_rule(exchange, symbol)
        step, minimum, maximum = (Decimal(str(lot[k])) for k in ('stepSize', 'minQty', 'maxQty'))
        if not all(x.is_finite() for x in (step, minimum, maximum)) or step <= 0 or minimum < 0 or maximum <= 0:
            raise ValueError
    except (KeyError, TypeError, InvalidOperation, ArithmeticError, ValueError):
        raise ValueError('无法取得有效市价单数量规则') from None
    try:
        attributed = Decimal(str(attributed_qty))
        if not attributed.is_finite() or attributed <= 0 or attributed > current:
            raise ValueError
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError('该带单员的跟单持仓无效或超过账户持仓') from None
    quantity = steps_quantity(Fraction(attributed) * percent / 100, step)
    if quantity <= 0 or quantity < minimum:
        raise ValueError('按该比例计算的数量低于最小下单量')
    if quantity > maximum or quantity > current:
        raise ValueError('平仓数量超过市价单上限或当前持仓')
    params = {'symbol': symbol, 'side': 'SELL' if side == 'LONG' else 'BUY', 'type': 'MARKET',
              'positionSide': position_side, 'quantity': format(quantity, 'f'), 'newOrderRespType': 'RESULT'}
    if not hedged:
        params['reduceOnly'] = 'true'
    return {'symbol': symbol, 'side': side, 'percent': percent, 'position_qty': format(current, 'f'),
            'attributed_qty': format(attributed, 'f'), 'quantity': format(quantity, 'f'),
            'remaining_if_filled': decimal_string(Fraction(attributed) - Fraction(quantity)), 'step':format(step,'f'),
            'mode': 'hedge' if hedged else 'oneway', 'order': params}


# Manual orders are persisted before submission and reconciled by client order ID.
import json
import secrets
import time
import threading

from copy_executor import ExchangeError


class ManualCloseService:
    def __init__(self, store, lock, client_factory, clock=time.time):
        self.store, self.lock, self.client_factory, self.clock = store, lock, client_factory, clock
        self.reconcile_lock = threading.Lock()

    def _account(self, db, account_id):
        if type(account_id) is not int or account_id <= 0:
            raise ValueError('账户参数无效')
        account = db.execute('SELECT id,name,api_key,api_secret FROM futures_accounts WHERE id=?', (account_id,)).fetchone()
        if not account:
            raise ValueError('账户不存在')
        return account

    def _ledger(self, db, account_id, symbol, side):
        rows = db.execute('SELECT p.target_id,p.quantity,t.name FROM copied_positions p LEFT JOIN targets t ON t.id=p.target_id WHERE p.account_id=? AND p.symbol=? AND p.position_side=? ORDER BY p.target_id', (account_id, symbol, side)).fetchall()
        ledger = []
        for row in rows:
            try:
                quantity = Decimal(str(row['quantity']))
                if not quantity.is_finite() or quantity < 0:
                    raise ValueError
            except (InvalidOperation, ValueError):
                raise ValueError('跟单持仓记录异常，请先核对') from None
            if quantity > 0:
                ledger.append({'target_id': str(row['target_id']), 'name': row['name'] or str(row['target_id']), 'quantity': decimal_string(Fraction(quantity))})
        return ledger

    def _sources(self, db, account_id, symbol, side, actual):
        ledger = self._ledger(db, account_id, symbol, side)
        total = sum((Fraction(Decimal(x['quantity'])) for x in ledger), Fraction(0))
        if total > Fraction(actual):
            raise ValueError('各带单员的跟单数量合计超过账户实际持仓，请先核对')
        manual = decimal_string(Fraction(actual) - total)
        return ledger, manual

    @staticmethod
    def _ledger_quantities(ledger):
        return [(item['target_id'],item['quantity']) for item in ledger]

    def _ensure_clear(self, db, account_id):
        if db.execute("SELECT 1 FROM manual_closes WHERE account_id=? AND state IN ('submitted','needs_review')", (account_id,)).fetchone():
            raise ValueError('该账户有待确认或待核对的手动平仓，请先处理')
        if db.execute("SELECT 1 FROM copy_tasks WHERE account_id=? AND state='submitted'", (account_id,)).fetchone():
            raise ValueError('该账户有结果待确认的跟单，请先核对')

    def candidates(self, account_id, symbol, side):
        with self.store.db() as db:
            account = self._account(db, account_id)
            self._ensure_clear(db, account_id)
        client = self.client_factory(account['api_key'], account['api_secret'])
        actual = position_quantity(symbol, side, client.positions(), client.mode())
        with self.lock, self.store.db() as db:
            current = self._account(db, account_id)
            self._ensure_clear(db, account_id)
            if (current['api_key'],current['api_secret']) != (account['api_key'],account['api_secret']):
                raise ValueError('账户配置已变化，请重试')
            ledger, manual = self._sources(db, account_id, symbol, side, actual)
        return {'account_name': current['name'], 'symbol': symbol, 'side': side,
                'position_qty': format(actual, 'f'), 'leaders': ledger, 'manual_qty': manual}

    def preview(self, account_id, symbol, side, percent, target_id):
        if not isinstance(target_id, str):
            raise ValueError('请选择要平仓的来源')
        with self.store.db() as db:
            account = self._account(db, account_id)
            self._ensure_clear(db, account_id)
        client = self.client_factory(account['api_key'], account['api_secret'])
        positions, mode, rules = client.positions(), client.mode(), client.rules()
        actual = position_quantity(symbol, side, positions, mode)
        with self.lock, self.store.db() as db:
            current = self._account(db, account_id)
            self._ensure_clear(db, account_id)
            if (current['api_key'],current['api_secret']) != (account['api_key'],account['api_secret']):
                raise ValueError('账户配置已变化，请重试')
            ledger, manual = self._sources(db, account_id, symbol, side, actual)
            selected = next((x for x in ledger if x['target_id'] == target_id), None)
            if target_id and not selected:
                raise ValueError('该带单员没有对应跟单持仓')
            amount = selected['quantity'] if selected else manual
            plan = plan_manual_close(symbol, side, percent, positions, mode, rules, amount)
            preview = {'plan': plan, 'ledger': ledger, 'target_id': target_id,
                       'source_name': selected['name'] if selected else '我的未关联仓位',
                       'account_name': current['name'], 'manual_qty': manual}
            request_id = secrets.token_hex(16)
            now = self.clock()
            db.execute("DELETE FROM manual_closes WHERE state='preview' AND created<?", (now - 3600,))
            db.execute("INSERT INTO manual_closes(id,account_id,target_id,symbol,side,percent,preview,client_id,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                       (request_id, account_id, target_id, symbol, side, percent, json.dumps(preview, ensure_ascii=False), 'mc_'+request_id, 'preview', now, now))
        return {'id': request_id, **preview, 'expires_in': 30}

    def status(self, account_id, request_id):
        if type(account_id) is not int or not isinstance(request_id, str) or not re.fullmatch(r'[a-f0-9]{32}', request_id):
            raise ValueError('平仓请求参数无效')
        with self.store.db() as db:
            row = db.execute('SELECT id,account_id,target_id,symbol,side,percent,state,result,error,preview FROM manual_closes WHERE id=? AND account_id=?', (request_id, account_id)).fetchone()
        if not row:
            raise ValueError('平仓请求不存在')
        preview = json.loads(row['preview'])
        result = json.loads(row['result']) if row['result'] else {}
        return {'id': row['id'], 'account_id': row['account_id'], 'symbol': row['symbol'],
                'side': row['side'], 'percent': row['percent'], 'state': row['state'],
                'source_name': preview['source_name'], 'quantity': preview['plan']['quantity'],
                'executed_qty': str(result['executedQty']) if result.get('executedQty') is not None else None,
                'order_id': str(result['orderId']) if result.get('orderId') is not None else None,
                'error': row['error']}

    def submit(self, account_id, request_id):
        if type(account_id) is not int or not isinstance(request_id, str) or not re.fullmatch(r'[a-f0-9]{32}', request_id):
            raise ValueError('平仓请求参数无效')
        with self.store.db() as db:
            request = db.execute('SELECT * FROM manual_closes WHERE id=? AND account_id=?', (request_id, account_id)).fetchone()
            if not request:
                raise ValueError('平仓请求不存在')
            if request['state'] != 'preview':
                return self.status(account_id, request_id)
            if self.clock() - request['created'] > 30:
                raise ValueError('平仓预览已过期，请重新预览')
            account = self._account(db, account_id)
            self._ensure_clear(db, account_id)
        client = self.client_factory(account['api_key'], account['api_secret'])
        positions, mode, rules = client.positions(), client.mode(), client.rules()
        actual = position_quantity(request['symbol'], request['side'], positions, mode)
        with self.lock, self.store.db() as db:
            current_request = db.execute('SELECT * FROM manual_closes WHERE id=? AND account_id=?', (request_id, account_id)).fetchone()
            if not current_request:
                raise ValueError('平仓请求不存在')
            if current_request['state'] != 'preview':
                return self.status(account_id, request_id)
            current_account = self._account(db, account_id)
            self._ensure_clear(db, account_id)
            if (current_account['api_key'],current_account['api_secret']) != (account['api_key'],account['api_secret']):
                raise ValueError('账户配置已变化，请重新预览')
            ledger, manual = self._sources(db, account_id, request['symbol'], request['side'], actual)
            old = json.loads(request['preview'])
            if self._ledger_quantities(ledger) != self._ledger_quantities(old['ledger']) or Fraction(actual) != Fraction(Decimal(old['plan']['position_qty'])):
                raise ValueError('仓位或跟单归属已变化，请重新预览')
            selected = next((x for x in ledger if x['target_id'] == request['target_id']), None)
            if request['target_id'] and not selected:
                raise ValueError('跟单持仓已变化，请重新预览')
            fresh = plan_manual_close(request['symbol'], request['side'], request['percent'], positions, mode, rules, selected['quantity'] if selected else manual)
            for key in ('position_qty','attributed_qty','quantity','step','mode'):
                if fresh[key] != old['plan'][key] and (key == 'mode' or Decimal(fresh[key]) != Decimal(old['plan'][key])):
                    raise ValueError('仓位或交易规则已变化，请重新预览')
            if self.clock() - request['created'] > 30:
                raise ValueError('平仓预览已过期，请重新预览')
            now = self.clock()
            db.execute("UPDATE manual_closes SET state='submitted',updated=?,next_check=? WHERE id=? AND state='preview'", (now, now+3, request_id))
        params = dict(fresh['order'], newClientOrderId=request['client_id'])
        try:
            result = client.place(params)
        except ExchangeError as exc:
            if exc.definitively_rejected:
                self._mark(request_id, 'failed', '交易所明确拒绝平仓订单', result=None)
            else:
                self._mark(request_id, 'submitted', '下单结果待确认，系统只查询原订单，不会重复提交')
        except Exception:
            self._mark(request_id, 'submitted', '下单结果待确认，系统只查询原订单，不会重复提交')
        else:
            self._accept_result(request_id, result)
            self._reconcile(request_id, client)
        return self.status(account_id, request_id)

    def _mark(self, request_id, state, error, result=None):
        warn = state in ('submitted','needs_review') and error not in ('','订单结果核对中')
        settings = self.store.settings() if warn or state == 'finished' else None
        with self.lock, self.store.db() as db:
            request=db.execute('SELECT * FROM manual_closes WHERE id=?',(request_id,)).fetchone()
            if not request or request['state'] not in ('submitted','needs_review'):
                return
            if result is None:
                db.execute('UPDATE manual_closes SET state=?,error=?,updated=?,next_check=? WHERE id=?', (state, error, self.clock(), self.clock()+5, request_id))
            else:
                db.execute('UPDATE manual_closes SET state=?,error=?,result=?,updated=?,next_check=? WHERE id=?', (state, error, json.dumps(result, ensure_ascii=False), self.clock(), self.clock()+5, request_id))
            if warn and not request['unknown_notified']:
                self._queue_notice(db, request, settings, False, error, result)
                db.execute('UPDATE manual_closes SET unknown_notified=1 WHERE id=?',(request_id,))
            elif state == 'finished' and request['unknown_notified']:
                self._queue_notice(db, request, settings, True, error, result)

    def _queue_notice(self, db, request, settings, resolved, error='', result=None):
        preview=json.loads(request['preview']);plan=preview['plan']
        heading='手动平仓结果已确认' if resolved else '手动平仓结果待确认'
        lines=[heading, '账户：'+preview['account_name'], '交易对：'+request['symbol']+' '+('多' if request['side']=='LONG' else '空'),
               '来源：'+preview['source_name'], '请求数量：'+plan['quantity'], '客户端订单号：'+request['client_id']]
        if resolved:
            lines.append('实际成交数量：'+str((result or {}).get('executedQty','0')))
            if Decimal(str((result or {}).get('executedQty','0'))) < Decimal(plan['quantity']):
                lines.append('本次未全部成交，请核对该来源的剩余仓位。')
        else:
            lines.append(error or '系统正在核对订单结果。')
            lines.append('系统只查询原订单，不会重复提交；其他跟单继续执行。')
        self.store.event(db,request['target_id'] or 'manual',preview['account_name'],'copy',{'message':'\n'.join(lines),'manual_close_id':request['id']},int(self.clock()*1000),False,settings)

    def _accept_result(self, request_id, result):
        if not isinstance(result, dict):
            self._mark(request_id, 'needs_review', '交易所订单响应无效，等待核对')
            return
        self._mark(request_id, 'submitted', '订单结果核对中', result=result)

    def _reconcile(self, request_id, client):
        if not self.reconcile_lock.acquire(blocking=False):
            return
        try:
            self._reconcile_locked(request_id, client)
        finally:
            self.reconcile_lock.release()

    def _reconcile_locked(self, request_id, client):
        with self.store.db() as db:
            request = db.execute('SELECT * FROM manual_closes WHERE id=?', (request_id,)).fetchone()
        if not request or request['state'] not in ('submitted','needs_review'):
            return
        preview = json.loads(request['preview']); plan = preview['plan']
        result = json.loads(request['result']) if request['result'] else {}
        terminal = ('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED')
        if result.get('status') not in terminal or request['state'] == 'needs_review':
            try:
                result = client.order(request['symbol'], request['client_id'])
                if not isinstance(result, dict):
                    raise ValueError
                self._mark(request_id, 'submitted', '订单结果核对中', result=result)
            except (ExchangeError, ValueError, TypeError):
                self._mark(request_id, 'submitted', '订单结果待确认，系统只查询原订单，不会重复提交')
                return
        if result.get('status') not in terminal:
            self._mark(request_id, 'submitted', '订单尚未结束，等待交易所确认', result=result)
            return
        try:
            executed = Decimal(str(result['executedQty']))
            if not executed.is_finite() or executed < 0 or Fraction(executed) > Fraction(Decimal(plan['quantity'])) or Fraction(executed) % Fraction(Decimal(plan['step'])):
                raise ValueError
            if result['status'] == 'FILLED' and executed != Decimal(plan['quantity']):
                raise ValueError
            if result.get('symbol') and result['symbol'] != request['symbol']:
                raise ValueError
            if result.get('clientOrderId') and result['clientOrderId'] != request['client_id']:
                raise ValueError
            if result.get('side') and result['side'] != plan['order']['side']:
                raise ValueError
            if result.get('positionSide') and result['positionSide'] != plan['order']['positionSide']:
                raise ValueError
        except (KeyError, TypeError, ValueError, InvalidOperation, ZeroDivisionError):
            self._mark(request_id, 'needs_review', '交易所成交结果与平仓请求不一致，请核对', result=result)
            return
        if executed == 0:
            self._mark(request_id, 'finished', '', result=result)
            return
        try:
            positions, mode = client.positions(), client.mode()
            if not isinstance(mode,dict) or mode.get('dualSidePosition') is not (plan['mode'] == 'hedge'):
                raise ValueError
            after = position_quantity(request['symbol'], request['side'], positions, mode, allow_empty=True)
            with self.lock, self.store.db() as db:
                latest=db.execute('SELECT * FROM manual_closes WHERE id=?',(request_id,)).fetchone()
                if not latest or latest['state'] not in ('submitted','needs_review'):
                    return
                current = self._ledger(db, request['account_id'], request['symbol'], request['side'])
                current_total=sum((Fraction(Decimal(item['quantity'])) for item in current),Fraction(0))
                if request['target_id']:
                    selected=next((item for item in current if item['target_id']==request['target_id']),None)
                    if not selected:
                        raise ValueError
                    old_qty = Fraction(Decimal(selected['quantity']))
                    if Fraction(executed) > old_qty:
                        raise ValueError
                    if current_total-Fraction(executed)>Fraction(after):
                        raise ValueError
                    new_qty = decimal_string(old_qty - Fraction(executed))
                    updated = db.execute('UPDATE copied_positions SET quantity=? WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?', (new_qty, request['account_id'], request['target_id'], request['symbol'], request['side']))
                    if updated.rowcount != 1:
                        raise ValueError
                elif current_total>Fraction(after):
                    raise ValueError
                if latest['unknown_notified']:
                    self._queue_notice(db, latest, self.store.settings(), True, result=result)
                db.execute("UPDATE manual_closes SET state='finished',error='',result=?,updated=? WHERE id=? AND state IN ('submitted','needs_review')", (json.dumps(result,ensure_ascii=False),self.clock(),request_id))
        except (ExchangeError, ValueError, TypeError, KeyError, InvalidOperation):
            review = self.clock() - request['created'] > 60
            self._mark(request_id, 'needs_review' if review else 'submitted', '账户持仓或跟单记录暂未匹配，请核对；系统会继续查询', result=result)

    def check_pending(self):
        with self.store.db() as db:
            row = db.execute("SELECT m.id,a.api_key,a.api_secret FROM manual_closes m JOIN futures_accounts a ON a.id=m.account_id WHERE m.state IN ('submitted','needs_review') AND m.next_check<=? ORDER BY m.updated LIMIT 1", (self.clock(),)).fetchone()
        if row:
            self._reconcile(row['id'], self.client_factory(row['api_key'], row['api_secret']))
