"""Reconstruct an approximate leader position from available executions."""
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction


def replay(rows):
    positions = {}
    stamps = {}
    for row in sorted(rows, key=lambda r: int(r['orderTime'])):
        symbol, side, position_side = row['symbol'], row['side'], row['positionSide']
        if (side, position_side) not in (('BUY','LONG'),('SELL','LONG'),('SELL','SHORT'),('BUY','SHORT')):
            raise ValueError('交易方向无效，无法重放持仓')
        try:
            quantity, price = Decimal(str(row['executedQty'])), Decimal(str(row['avgPrice']))
            if not quantity.is_finite() or not price.is_finite() or quantity <= 0 or price <= 0:
                raise ValueError
        except (InvalidOperation, ValueError, TypeError):
            raise ValueError('成交数量或价格无效，无法重放持仓') from None
        key = (symbol, position_side)
        position = positions.setdefault(key, {'qty': Fraction(0), 'cost': Fraction(0), 'uncertain': False})
        stamp_key = (symbol, position_side, int(row['orderTime']))
        opening = (side, position_side) in (('BUY','LONG'),('SELL','SHORT'))
        previous_action = stamps.get(stamp_key)
        simultaneous = previous_action is not None and previous_action != opening
        if simultaneous:
            position['uncertain'] = True  # No stable ordering within the same millisecond.
        stamps[stamp_key] = opening
        qty, cost, amount = position['qty'], position['cost'], Fraction(quantity)
        if opening:
            position['qty'] = qty + amount
            position['cost'] = cost + amount * Fraction(price)
        elif amount > qty:
            position.update(qty=Fraction(0), cost=Fraction(0), uncertain=simultaneous)
        else:
            position['qty'] = qty - amount
            position['cost'] = cost * (qty - amount) / qty
    result = []
    for (symbol, side), pos in sorted(positions.items()):
        if not pos['qty'] and not pos['uncertain']:
            continue
        def text(value):
            with localcontext() as ctx:
                ctx.prec = 40
                return format(Decimal(value.numerator) / Decimal(value.denominator), 'f')
        result.append({'symbol':symbol, 'side':side,
                       'quantity':None if pos['uncertain'] else text(pos['qty']),
                       'avg_price':None if pos['uncertain'] or not pos['qty'] else text(pos['cost']/pos['qty']),
                       'uncertain':pos['uncertain']})
    return result
