import tempfile
import unittest
from pathlib import Path

from app import Store


class DisplayOrderingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'db'
        self.store = Store(self.path)
        with self.store.db() as db:
            for i in range(1, 9):
                db.execute('INSERT INTO targets(id,name) VALUES(?,?)', (str(i), 'leader%02d' % i))
                db.execute('INSERT INTO futures_accounts(id,name,api_key,api_secret) VALUES(?,?,?,?)', (i, 'account%d' % i, 'key', 'secret'))

    def tearDown(self): self.temp.cleanup()

    def order(self, kind):
        with self.store.db() as db: return self.store.ordered_ids(db, kind)

    def test_orders_are_independent_persist_and_allow_cross_page_moves(self):
        self.store.reorder('accounts', '7', '6', 'before')
        self.store.reorder('targets', '1', '8', 'after')
        self.assertEqual(self.order('accounts'), ['1','2','3','4','5','7','6','8'])
        self.assertEqual(self.order('targets'), ['2','3','4','5','6','7','8','1'])
        reopened = Store(self.path)
        with reopened.db() as db:
            self.assertEqual(reopened.ordered_ids(db, 'accounts'), self.order('accounts'))
            self.assertEqual(reopened.ordered_ids(db, 'targets'), self.order('targets'))
            db.execute("INSERT INTO targets(id,name) VALUES('9','leader00')")
        self.assertEqual(self.order('targets')[-1], '9')

    def test_invalid_request_does_not_change_order(self):
        original = self.order('accounts')
        for args in [('accounts','1','99','before'), ('accounts','1','2','other'), ('invalid','1','2','before'), ('accounts',1,'2','before')]:
            with self.assertRaises(ValueError): self.store.reorder(*args)
            self.assertEqual(self.order('accounts'), original)
