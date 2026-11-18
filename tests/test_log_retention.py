import calendar
import logging
import tempfile
import time
import unittest
from pathlib import Path

from app import HourlyLogHandler


class LogRetentionTests(unittest.TestCase):
    def test_hourly_files_are_retained_until_all_entries_are_24_hours_old(self):
        with tempfile.TemporaryDirectory() as directory:
            handler=HourlyLogHandler(directory)
            try:
                root=Path(directory)
                now=calendar.timegm((2026,11,18,12,30,0))
                expired=root/'monitor-2026-11-17-11.log'
                recent=root/'monitor-2026-11-17-12.log'
                unrelated=root/'other.log'
                for path in (expired,recent,unrelated): path.write_text('test')
                handler.cleanup(now)
                self.assertFalse(expired.exists())
                self.assertTrue(recent.exists())
                self.assertTrue(unrelated.exists())
                handler.emit(logging.LogRecord('test',logging.INFO,'',0,'hello',(),None))
                current=root/('monitor-'+time.strftime('%Y-%m-%d-%H',time.gmtime())+'.log')
                self.assertIn('hello',current.read_text())
                self.assertEqual(current.stat().st_mode & 0o777,0o600)
            finally:
                handler.close()
