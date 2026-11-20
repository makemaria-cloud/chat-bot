import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from bot import Store


class StoreTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "bot.sqlite3"
        self.store = Store(self.path)
        self.store.migrate()
        self.event = dict(portal="demo", chat="1", session="2", message="3", kind="message")

    def test_deduplication_across_connections_and_restart(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(self.store.accept, [self.event] * 16)), 1)
        reopened = Store(self.path)
        reopened.migrate()
        self.assertFalse(reopened.accept(self.event))
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(lambda _: reopened.process_one(), range(16))), 1)
        self.assertEqual(reopened.pending(), 0)

    def test_human_state_survives_later_message(self):
        self.store.accept({**self.event, "kind": "human"})
        self.store.accept({**self.event, "message": "4"})
        self.assertTrue(self.store.process_one())
        self.assertTrue(self.store.process_one())
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT state FROM sessions").fetchall(), [("HUMAN_ACTIVE",)])

    def test_invalid_event_is_not_queued(self):
        with self.assertRaises(ValueError):
            self.store.accept({**self.event, "session": ""})
        self.assertEqual(self.store.pending(), 0)


if __name__ == "__main__":
    unittest.main()
