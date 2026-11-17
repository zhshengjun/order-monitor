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
