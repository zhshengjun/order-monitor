import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from app import Monitor, Store


class NotificationFailureTests(unittest.TestCase):
    def test_bark_http_failure_is_visible_and_does_not_leak_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'db')
            store.save_settings({'bark_enabled': True, 'bark_key': 'private-device-key'})
            with store.db() as db:
                store.event(db, '123', '带单员', 'copy', {'message': '开仓结果'}, 1000, False, store.settings(db))
            error = urllib.error.HTTPError('https://api.day.app/private-device-key', 413, 'failure', {}, None)
            with patch('app.urllib.request.urlopen', side_effect=error):
                Monitor(store).deliver()
            with store.db() as db:
                result = dict(db.execute('SELECT status,attempts,error FROM outbox').fetchone())
            self.assertEqual(result['status'], 'pending')
            self.assertEqual(result['attempts'], 1)
            self.assertIn('HTTP 413', result['error'])
            self.assertNotIn('private-device-key', result['error'])

    def test_bark_business_error_is_safe_and_distinguishable(self):
        from app import NotificationError, notification_error, send_notification
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size): return b'{"code":400,"message":"private-device-key"}'
        with patch('app.urllib.request.urlopen', return_value=Response()):
            with self.assertRaises(NotificationError) as error:
                send_notification('bark', {'bark_url': 'https://api.day.app', 'bark_key': 'private-device-key'}, '标题', '内容')
        self.assertEqual(notification_error(error.exception), '通知服务拒绝请求，错误码 400')

    def test_network_failures_report_category_without_url(self):
        import socket
        from app import notification_error
        error = urllib.error.URLError(socket.gaierror(-2, 'private-device-key'))
        self.assertEqual(notification_error(error), '通知服务域名解析失败')
        error = urllib.error.URLError(TimeoutError('private-device-key'))
        self.assertEqual(notification_error(error), '通知服务连接超时')


class NotificationFormattingTests(unittest.TestCase):
    def test_order_notification_china_time_and_precision(self):
        import json
        event={'name':'带单员','kind':'order','stamp':0,'body':json.dumps({'symbol':'BTCUSDT','side':'BUY','positionSide':'LONG','executedQty':'596.18000000','baseAsset':'BTC','avgPrice':'160.64040800','quoteAsset':'USDT','notional':'95770.5984414400000000','totalPnl':'0.00000000'})}
        _,text=Monitor.message(event)
        for expected in ('数量：596.18 BTC','均价：160.6404 USDT','成交金额：95770.5984 USDT','已实现盈亏：0 USDT','时间（UTC+8）：1970-01-01 08:00:00'):
            self.assertIn(expected,text)

    def test_combined_message_preserves_skipped_reason_and_separate_times(self):
        import json
        leader={'name':'带单员','kind':'order','stamp':0,'body':json.dumps({'symbol':'BTCUSDT','side':'BUY','positionSide':'LONG','executedQty':'1','baseAsset':'BTC','avgPrice':'100','quoteAsset':'USDT','notional':'100','totalPnl':'0'})}
        result={'name':'带单员','kind':'copy','stamp':1000,'body':json.dumps({'message':'BTCUSDT 跟单跳过：价格偏离超过 3%'})}
        title,text=Monitor.combined_message(leader,result)
        self.assertEqual(title,'带单员 · 交易与跟单')
        self.assertIn('【带单交易】',text)
        self.assertIn('【我的跟单】',text)
        self.assertIn('BTCUSDT 跟单跳过：价格偏离超过 3%',text)
        self.assertIn('订单时间（UTC+8）：1970-01-01 08:00:00',text)
        self.assertIn('结果时间（UTC+8）：1970-01-01 08:00:01',text)

    def test_realized_pnl_requires_complete_matching_fills(self):
        from decimal import Decimal
        from app import actual_copy_realized_pnl
        trades=[{'id':1,'symbol':'BTCUSDT','orderId':42,'qty':'0.01','realizedPnl':'1.235','marginAsset':'USDT'},
                {'id':2,'symbol':'BTCUSDT','orderId':42,'qty':'0.011','realizedPnl':'-0.035','marginAsset':'USDT'}]
        calc=lambda rows: actual_copy_realized_pnl(rows,'BTCUSDT',42,Decimal('0.021'),'USDT')
        self.assertEqual(calc(trades),'1.2')
        self.assertIsNone(calc(trades[:1]))
        self.assertIsNone(calc(trades+[trades[0]]))
        self.assertIsNone(calc([dict(trades[0],orderId=43),trades[1]]))
        self.assertIsNone(calc([dict(trades[0],realizedPnl='NaN'),trades[1]]))
        self.assertEqual(calc([dict(trades[0],realizedPnl='0'),dict(trades[1],realizedPnl='0')]),'0')

    def test_actual_notional_uses_only_exchange_fills(self):
        from decimal import Decimal
        from app import actual_copy_notional, notification_number
        qty=Decimal('0.021')
        self.assertEqual(actual_copy_notional({'cumQuote':'19.234500','avgPrice':'999'},qty),'19.2345')
        self.assertEqual(actual_copy_notional({'avgPrice':'917.24'},qty),'19.26204')
        self.assertIsNone(actual_copy_notional({'cumQuote':'0','avgPrice':'0'},qty))
        self.assertEqual(actual_copy_notional({},Decimal('0')),'0')
        self.assertEqual(notification_number('0.00005'),'0.0001')
        self.assertEqual(notification_number('-1.23455'),'-1.2346')
        self.assertEqual(notification_number('100000000000000000000.00000'),'100000000000000000000')
