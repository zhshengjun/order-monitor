"""Exact virtual leader position transition; does not submit trades."""
from fractions import Fraction
from decimal import Decimal, InvalidOperation


def parse_amount(value):
    try:
        d = Decimal(str(value))
        if not d.is_finite() or d < 0:
            raise ValueError
        return Fraction(d)
    except (InvalidOperation, ValueError):
        raise ValueError('数量必须是非负有限数值') from None


def decimal_string(value):
    value = Fraction(value)
    denominator = value.denominator
    twos = fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        raise ValueError('数量无法精确表示为十进制')
    scale = max(twos, fives)
    units = value.numerator * 2 ** (scale - twos) * 5 ** (scale - fives)
    sign = '-' if units < 0 else ''
    digits = str(abs(units)).zfill(scale + 1)
    return sign + (digits if not scale else digits[:-scale] + '.' + digits[-scale:])


def transition(before, side, position_side, executed_qty):
    current = parse_amount(before)
    quantity = parse_amount(executed_qty)
    if not quantity:
        raise ValueError('成交数量必须大于零')
    if (side, position_side) in (('BUY', 'LONG'), ('SELL', 'SHORT')):
        return {'before': decimal_string(current), 'after': decimal_string(current + quantity), 'action': 'open'}
    if (side, position_side) in (('SELL', 'LONG'), ('BUY', 'SHORT')):
        if current < quantity:
            return {'before': decimal_string(current), 'after': decimal_string(current), 'action': 'unmatched_close'}
        return {'before': decimal_string(current), 'after': decimal_string(current - quantity), 'action': 'close'}
    raise ValueError('不支持的订单方向')
