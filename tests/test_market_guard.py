import unittest
from decimal import Decimal
from market_guard import check_price, close_position

class MarketGuardTests(unittest.TestCase):
    def test_executable_side_and_exact_limit(self):
        self.assertTrue(check_price('100','99.8','100.5','BUY')['allowed'])
        self.assertFalse(check_price('100','99.8','100.50000000000000000001','BUY')['allowed'])
        self.assertTrue(check_price('100','99.5','100.1','SELL')['allowed'])
        self.assertFalse(check_price('100','99.49999999999999999999','100.1','SELL')['allowed'])
    def test_close_skips_missing_zero_and_opposite(self):
        positions=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'-0.01'}, {'symbol':'ETHUSDT','positionSide':'LONG','positionAmt':'0'}]
        self.assertEqual(close_position(positions,'BTCUSDT','LONG'),Decimal('0'))
        self.assertEqual(close_position(positions,'BTCUSDT','SHORT'),Decimal('0.01'))
        self.assertEqual(close_position(positions,'ETHUSDT','LONG'),Decimal('0'))
    def test_invalid_data_fails_closed(self):
        with self.assertRaises(ValueError): check_price('100','101','100','BUY')
        with self.assertRaises(ValueError): close_position([{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'NaN'}],'BTCUSDT','LONG')
