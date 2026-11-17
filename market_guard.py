"""Read-only market-order safeguards; no exchange orders are sent here."""
from decimal import Decimal, InvalidOperation
from fractions import Fraction


def positive(value, name):
    try:
        value = Decimal(str(value))
        if not value.is_finite() or value <= 0:
            raise ValueError
        return Fraction(value)
    except (InvalidOperation, ValueError):
        raise ValueError(name + ' 必须是正的有限数值') from None


def check_price(lead_price, bid, ask, side, limit='0.01'):
    reference = positive(lead_price, '带单成交均价')
    bid_text, ask_text = str(bid), str(ask)
    bid, ask = positive(bid, '买一价'), positive(ask, '卖一价')
    if bid > ask:
        raise ValueError('买一价高于卖一价')
    if side not in ('BUY', 'SELL'):
        raise ValueError('无效的订单方向')
    executable = ask if side == 'BUY' else bid
    deviation = abs(executable - reference) / reference
    threshold = positive(limit, '最大偏离比例')
    return {'allowed': deviation <= threshold, 'price': ask_text if side == 'BUY' else bid_text, 'deviation': deviation}


def close_position(positions, symbol, lead_position_side):
    if lead_position_side not in ('LONG', 'SHORT'):
        raise ValueError('无效的持仓方向')
    matches = []
    for p in positions:
        if p.get('symbol') != symbol:
            continue
        try:
            amount = Decimal(str(p['positionAmt']))
        except (KeyError, InvalidOperation):
            raise ValueError('持仓数据无效') from None
        if not amount.is_finite():
            raise ValueError('持仓数据无效')
        ps = p.get('positionSide')
        if ps == lead_position_side and amount != 0:
            matches.append(amount.copy_abs())
        elif ps == 'BOTH' and ((lead_position_side == 'LONG' and amount > 0) or (lead_position_side == 'SHORT' and amount < 0)):
            matches.append(amount.copy_abs())
    if len(matches) > 1:
        raise ValueError('持仓模式不一致')
    return matches[0] if matches else Decimal('0')
