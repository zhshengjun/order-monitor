import unittest
from decimal import Decimal
from copy_calculation import opening_quantity, closing_quantity, floor_quantity

class CopyCalculationTests(unittest.TestCase):
    def test_opening_scales_notional_and_floors(self):
        self.assertEqual(opening_quantity('2', '50000', '10000', '100', '51000', '0.00001'), Decimal('0.01960'))
        self.assertEqual(opening_quantity('2', '50000', '10000', '100', '51000', '0.00001','2'), Decimal('0.03921'))
        self.assertEqual(opening_quantity('2', '50000', '10000', '100', '51000', '0.00001','0.5'), Decimal('0.00980'))
    def test_closing_uses_leader_fraction(self):
        self.assertEqual(closing_quantity('3', '1', '0.01', '0.001'), Decimal('0.004'))
        self.assertEqual(closing_quantity('2', '1', '0.012', '0.001'), Decimal('0.006'))
        self.assertEqual(closing_quantity('3', '1', '0.00000000000000000001', '0.00000000000000000001'), Decimal('0.00000000000000000001'))
    def test_floor_does_not_round_up_at_decimal_context_boundary(self):
        self.assertEqual(floor_quantity('0.99999999999999999999999999999', '0.01'), Decimal('0.99'))
        self.assertEqual(floor_quantity('0.00001', '0.001'), Decimal('0.000'))
    def test_invalid_or_inconsistent_positions_rejected(self):
        for call in (lambda: closing_quantity('1', '2', '1', '0.001'), lambda: floor_quantity('1', '0'), lambda: opening_quantity('1','1','0','1','1','0.1')):
            with self.assertRaises(ValueError): call()
