"""Durable market-copy execution. Unknown submissions are reconciled, never resent."""
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from fractions import Fraction
from copy_calculation import opening_quantity, closing_quantity, steps_quantity
from market_guard import check_price, close_position

BASE = 'https://fapi.binance.com'

class ExchangeError(Exception): pass

class FuturesClient:
    def __init__(self, key, secret):
        self.key, self.secret = key, secret

    def request(self, path, params=None, method='GET', signed=False):
        params = dict(params or {})
        if signed:
            params.update(timestamp=int(time.time()*1000), recvWindow=5000)
        query = urllib.parse.urlencode(params)
        if signed:
            query += '&signature=' + hmac.new(self.secret.encode(),query.encode(),hashlib.sha256).hexdigest()
        headers={'X-M-Monitor':'order-monitor'}
        if signed: headers['X-MBX-APIKEY']=self.key
        req = urllib.request.Request(BASE+path+('?' + query if query else ''),headers=headers,method=method)
        try:
            with urllib.request.urlopen(req,timeout=15) as response:
                return json.loads(response.read(4000000),parse_float=str)
        except urllib.error.HTTPError as e:
            try:
                body=json.loads(e.read(65536)); code=body.get('code')
            except Exception: code=None
            code = code if type(code) is int else 'unknown'
            detail = 'HTTP %s; code %s; 接口 %s' % (e.code,code,path)
            if e.code == 451: detail += '；请检查容器代理与合约 API 出口'
            raise ExchangeError(detail) from None
        except Exception:
            raise ExchangeError('接口 %s 网络请求失败或响应无效' % path) from None

    def balance(self): return self.request('/fapi/v3/balance',signed=True)
    def positions(self): return self.request('/fapi/v3/positionRisk',signed=True)
    def mode(self): return self.request('/fapi/v1/positionSide/dual',signed=True)
    def quote(self,symbol): return self.request('/fapi/v1/ticker/bookTicker',{'symbol':symbol})
    def rules(self): return self.request('/fapi/v1/exchangeInfo')
    def mark_price(self,symbol): return self.request('/fapi/v1/premiumIndex',{'symbol':symbol})
    def symbol_config(self,symbol): return self.request('/fapi/v1/symbolConfig',{'symbol':symbol},signed=True)
    def place(self,params): return self.request('/fapi/v1/order',params,method='POST',signed=True)
    def order(self,symbol,client_id): return self.request('/fapi/v1/order',{'symbol':symbol,'origClientOrderId':client_id},signed=True)


def market_rule(exchange, symbol):
    item=next((x for x in exchange['symbols'] if x['symbol']==symbol),None)
    if not item or item.get('status')!='TRADING': raise ValueError('交易对不可交易')
    rules={x['filterType']:x for x in item['filters']}
    lot=rules.get('MARKET_LOT_SIZE')
    if not lot or Decimal(lot['stepSize'])<=0: raise ValueError('缺少有效市价单步长')
    return lot, rules.get('MIN_NOTIONAL',{})


def plan(row, action, before, attributed, lead_balance, balances, positions, mode, quote, exchange, mark_price=None, leverage=None):
    symbol=row['symbol']; side=row['side']; ps=row['positionSide']
    if (side,ps) not in (('BUY','LONG'),('SELL','LONG'),('SELL','SHORT'),('BUY','SHORT')): raise ValueError('订单方向无效')
    if action=='unmatched_close': raise ValueError('虚拟仓位不足，跳过平仓')
    bid,ask=quote['bidPrice'],quote['askPrice']
    guard=check_price(row['avgPrice'],bid,ask,side)
    if not guard['allowed']: raise ValueError('价格偏离超过 3%')
    px=guard['price']; lot,min_notional=market_rule(exchange,symbol)
    step=Decimal(str(lot['stepSize'])); min_qty=Decimal(str(lot['minQty'])); max_qty=Decimal(str(lot['maxQty']))
    if not step.is_finite() or not min_qty.is_finite() or not max_qty.is_finite() or step<=0 or min_qty<0 or max_qty<=0: raise ValueError('市价单数量规则无效')
    if Fraction(min_qty) % Fraction(step): raise ValueError('交易对最小数量与步长不兼容')
    min_steps=max(1, -(-Fraction(min_qty)//Fraction(step)))
    hedged=mode.get('dualSidePosition')
    if not isinstance(hedged,bool): raise ValueError('持仓模式未知')
    params={'symbol':symbol,'side':side,'type':'MARKET','newOrderRespType':'RESULT'}
    if hedged: params['positionSide']=ps
    else: params['positionSide']='BOTH'
    if not hedged and action=='open':
        opposite='SHORT' if ps=='LONG' else 'LONG'
        if close_position(positions,symbol,opposite)>0:
            raise ValueError('单向持仓存在反向仓位，无法按方向开仓')
    if action=='close':
        actual=close_position(positions,symbol,ps)
        if actual<=0: raise ValueError('没有对应方向持仓，跳过平仓')
        owned=Decimal(str(attributed))
        if owned<=0: raise ValueError('该跟单关联没有已成交持仓，跳过平仓')
        qty=closing_quantity(before,row['executedQty'],min(actual,owned),lot['stepSize'])
        if Fraction(qty)<Fraction(step)*min_steps:
            qty=steps_quantity(Fraction(step)*min_steps,step)
            if qty>min(actual,owned): raise ValueError('可平仓数量低于交易对最小数量')
        if not hedged: params['reduceOnly']='true'
    elif action=='open':
        usdt=next((x for x in balances if x.get('asset')=='USDT'),None)
        if not usdt: raise ValueError('未获取到 USDT 账户余额')
        try:
            wallet=Decimal(str(usdt['balance'])); available=Decimal(str(usdt['availableBalance']))
        except (KeyError, ValueError, ArithmeticError):
            raise ValueError('未获取到有效的 USDT 钱包余额或可用余额') from None
        if not wallet.is_finite() or wallet<=0: raise ValueError('USDT 钱包余额必须大于零')
        if not available.is_finite() or available<0: raise ValueError('USDT 可用余额无效')
        qty=opening_quantity(row['executedQty'],row['avgPrice'],lead_balance,wallet,px,lot['stepSize'])
        try:
            mark=Decimal(str(mark_price)); actual_leverage=Decimal(str(leverage))
        except (ValueError, ArithmeticError):
            raise ValueError('未获取到有效的标记价格或当前杠杆') from None
        if not mark.is_finite() or mark<=0: raise ValueError('未获取到有效标记价格')
        if not actual_leverage.is_finite() or actual_leverage<=0: raise ValueError('未获取到有效的当前杠杆')
        minimum=Decimal(str(min_notional.get('notional',min_notional.get('minNotional','0'))))
        if not minimum.is_finite() or minimum<0: raise ValueError('最小名义金额规则无效')
        if minimum>0:
            min_steps=max(min_steps, -(-Fraction(minimum)//(Fraction(mark)*Fraction(step))))
        minimum_qty=steps_quantity(Fraction(step)*min_steps,step)
        if qty<minimum_qty:
            qty=minimum_qty
        estimated_margin=Fraction(qty)*max(Fraction(mark),Fraction(Decimal(px)))/Fraction(actual_leverage)
        if estimated_margin>Fraction(available): raise ValueError('跟单所需保证金超过可用余额')
    else: raise ValueError('无效跟单操作')
    if qty<=0 or qty<min_qty: raise ValueError('跟单数量低于最小数量')
    if qty>max_qty: raise ValueError('跟单数量超过最大数量')
    params['quantity']=format(qty,'f')
    return params
