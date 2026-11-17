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
from copy_calculation import opening_quantity, closing_quantity
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
        req = urllib.request.Request(BASE+path+('?' + query if query else ''),headers={'X-M-Monitor':'order-monitor','X-MBX-APIKEY':self.key},method=method)
        try:
            with urllib.request.urlopen(req,timeout=15) as response:
                return json.loads(response.read(4000000),parse_float=str)
        except urllib.error.HTTPError as e:
            try:
                body=json.loads(e.read(65536)); code=body.get('code')
            except Exception: code=None
            raise ExchangeError('HTTP %s; code %s' % (e.code,code)) from None
        except Exception:
            raise ExchangeError('网络请求失败或响应无效') from None

    def balance(self): return self.request('/fapi/v3/balance',signed=True)
    def positions(self): return self.request('/fapi/v3/positionRisk',signed=True)
    def mode(self): return self.request('/fapi/v1/positionSide/dual',signed=True)
    def quote(self,symbol): return self.request('/fapi/v1/ticker/bookTicker',{'symbol':symbol})
    def rules(self): return self.request('/fapi/v1/exchangeInfo')
    def place(self,params): return self.request('/fapi/v1/order',params,method='POST',signed=True)
    def order(self,symbol,client_id): return self.request('/fapi/v1/order',{'symbol':symbol,'origClientOrderId':client_id},signed=True)


def market_rule(exchange, symbol):
    item=next((x for x in exchange['symbols'] if x['symbol']==symbol),None)
    if not item or item.get('status')!='TRADING': raise ValueError('交易对不可交易')
    rules={x['filterType']:x for x in item['filters']}
    lot=rules.get('MARKET_LOT_SIZE')
    if not lot or Decimal(lot['stepSize'])<=0: raise ValueError('缺少有效市价单步长')
    return lot, rules.get('MIN_NOTIONAL',{})


def plan(row, action, before, attributed, lead_balance, balances, positions, mode, quote, exchange):
    symbol=row['symbol']; side=row['side']; ps=row['positionSide']
    if (side,ps) not in (('BUY','LONG'),('SELL','LONG'),('SELL','SHORT'),('BUY','SHORT')): raise ValueError('订单方向无效')
    if action=='unmatched_close': raise ValueError('虚拟仓位不足，跳过平仓')
    bid,ask=quote['bidPrice'],quote['askPrice']
    guard=check_price(row['avgPrice'],bid,ask,side)
    if not guard['allowed']: raise ValueError('价格偏离超过 0.5%')
    px=guard['price']; lot,min_notional=market_rule(exchange,symbol)
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
        if not hedged: params['reduceOnly']='true'
    elif action=='open':
        usdt=next((x for x in balances if x.get('asset')=='USDT'),None)
        if not usdt: raise ValueError('未获取到 USDT 可用余额')
        qty=opening_quantity(row['executedQty'],row['avgPrice'],lead_balance,usdt['availableBalance'],px,lot['stepSize'])
    else: raise ValueError('无效跟单操作')
    if qty<=0 or qty<Decimal(lot['minQty']): raise ValueError('跟单数量低于最小数量')
    if qty>Decimal(lot['maxQty']): raise ValueError('跟单数量超过最大数量')
    minimum=Decimal(str(min_notional.get('notional',min_notional.get('minNotional','0'))))
    if action=='open' and Fraction(qty)*Fraction(Decimal(px))<Fraction(minimum): raise ValueError('跟单金额低于最小名义金额')
    params['quantity']=format(qty,'f')
    return params
