"""Exact proportional-copy arithmetic; this module does not submit orders."""
from decimal import Decimal, InvalidOperation
from fractions import Fraction


def positive(value, name):
    try:
        n = Decimal(str(value))
        if not n.is_finite() or n <= 0:
            raise ValueError
        return n
    except (InvalidOperation, ValueError):
        raise ValueError(name + ' must be a positive finite number') from None


def steps_quantity(ratio, step):
    s = positive(step, 'step')
    units = ratio // Fraction(s)
    # Construct the result directly, avoiding Decimal context rounding.
    tup = s.as_tuple()
    coefficient = int(''.join(map(str, tup.digits))) * units
    return Decimal((0, tuple(map(int, str(coefficient))), tup.exponent))


def floor_quantity(quantity, step):
    return steps_quantity(Fraction(positive(quantity, 'quantity')), step)


def opening_quantity(lead_qty, lead_price, lead_balance, own_balance, execution_price, step):
    ratio = (Fraction(positive(lead_qty, 'lead quantity')) * Fraction(positive(lead_price, 'lead price'))
             * Fraction(positive(own_balance, 'own balance')) / Fraction(positive(lead_balance, 'lead balance'))
             / Fraction(positive(execution_price, 'execution price')))
    return steps_quantity(ratio, step)


def closing_quantity(lead_before, lead_reduction, own_position, step):
    before = positive(lead_before, 'lead position')
    reduction = positive(lead_reduction, 'lead reduction')
    if reduction > before:
        raise ValueError('lead reduction exceeds tracked position')
    increment = Fraction(positive(step, 'step'))
    target = Fraction(positive(own_position, 'own position')) * Fraction(reduction) / Fraction(before)
    units = -(-target // increment)
    return steps_quantity(units * increment, step)
