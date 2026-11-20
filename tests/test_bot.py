import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from http.client import HTTPConnection
from pathlib import Path
from threading import Thread

from bot import EventConflict, Store, make_server

ROOT = Path(__file__).resolve().parents[1]


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

    def test_same_message_id_preserves_each_control_kind_and_deduplicates_replays(self):
        for session, kind, expected in (
            ("2", "human", "HUMAN_ACTIVE"),
            ("3", "closed", "CLOSED"),
            ("4", "stop", "DO_NOT_CONTACT"),
        ):
            with self.subTest(kind=kind):
                message = {**self.event, "session": session}
                control = {**message, "kind": kind}
                for event in (message, control, {**message, "message": "4"}):
                    self.assertTrue(self.store.accept(event))
                    self.assertFalse(self.store.accept(event))
                    self.assertTrue(self.store.process_one())
                with closing(self.store.connect()) as db:
                    self.assertEqual(
                        db.execute("SELECT state FROM sessions WHERE session = ?", (session,)).fetchone(),
                        (expected,),
                    )
        self.assertEqual(self.store.pending(), 0)

    def test_conflicting_event_identity_reports_error(self):
        control = {**self.event, "kind": "stop", "event_id": "control-1"}
        self.assertTrue(self.store.accept(control))
        with self.assertRaises(EventConflict):
            self.store.accept({**control, "message": "4"})
        self.assertFalse(self.store.accept(control))
        self.assertEqual(self.store.pending(), 1)

    def test_event_and_state_are_isolated_by_portal_chat_and_session(self):
        self.store.accept({**self.event, "kind": "stop"})
        for field in ("portal", "chat", "session"):
            self.assertTrue(self.store.accept({**self.event, field: "separate"}))
        while self.store.process_one():
            pass
        with closing(self.store.connect()) as db:
            self.assertEqual(set(db.execute("SELECT * FROM sessions")), {
                ("demo", "1", "2", "DO_NOT_CONTACT"),
                ("separate", "1", "2", "QUALIFYING"),
                ("demo", "separate", "2", "QUALIFYING"),
                ("demo", "1", "separate", "QUALIFYING"),
            })

    def test_processing_failure_rolls_back_state_and_leaves_event_pending(self):
        self.store.accept(self.event)
        with closing(self.store.connect()) as db, db:
            db.execute(
                """CREATE TRIGGER fail_processing BEFORE UPDATE ON inbox
                   BEGIN SELECT RAISE(ABORT, 'test failure'); END"""
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.process_one()
        self.assertEqual(self.store.pending(), 1)
        with closing(self.store.connect()) as db, db:
            self.assertEqual(db.execute("SELECT * FROM sessions").fetchall(), [])
            db.execute("DROP TRIGGER fail_processing")
        self.assertTrue(Store(self.path).process_one())
        self.assertEqual(self.store.pending(), 0)

    def test_replay_recovers_event_accepted_by_another_process(self):
        insert = (
            "import sys; sys.path.insert(0, '.'); from bot import Store; "
            f"assert Store(sys.argv[1]).accept({self.event!r})"
        )
        subprocess.run([sys.executable, "-c", insert, str(self.path)], cwd=ROOT, check=True)
        self.assertEqual(self.store.pending(), 1)
        result = subprocess.run(
            [sys.executable, "bot.py", "--db", str(self.path), "replay", "examples/events.jsonl"],
            cwd=ROOT, check=True, capture_output=True, text=True,
        )
        self.assertEqual(json.loads(result.stdout), {"accepted": 1, "processed": 2})

    def test_upgrade_preserves_v1_data_and_updates_unique_key(self):
        old = self.path.with_name("old.sqlite3")
        with closing(sqlite3.connect(old)) as db, db:
            db.executescript((ROOT / "migrations" / "001.sql").read_text(encoding="utf-8"))
            db.execute("PRAGMA user_version = 1")
            db.execute("INSERT INTO sessions VALUES ('demo', '1', '2', 'QUALIFYING')")
            db.execute("INSERT INTO inbox VALUES (7, 'demo', '1', '2', '3', 'message', 1)")
            db.execute("INSERT INTO inbox VALUES (9, 'demo', '1', '2', '9', 'human', 0)")
        upgraded = Store(old)
        upgraded.migrate()
        upgraded.migrate()
        with closing(upgraded.connect()) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone(), (2,))
            indexes = [row[1] for row in db.execute("PRAGMA index_list(inbox)") if row[2]]
            self.assertEqual(len(indexes), 1)
            key = [row[2] for row in db.execute("SELECT * FROM pragma_index_info(?)", indexes)]
            self.assertEqual(key, ["portal", "chat", "session", "kind", "event_id"])
            self.assertEqual(db.execute(
                "SELECT sequence, kind, event_id, processed FROM inbox ORDER BY sequence"
            ).fetchall(), [(7, "message", "3", 1), (9, "human", "9", 0)])
            self.assertEqual(db.execute("SELECT state FROM sessions").fetchone(), ("QUALIFYING",))
        self.assertFalse(upgraded.accept(self.event))
        self.assertTrue(upgraded.accept({**self.event, "kind": "stop"}))
        self.assertTrue(upgraded.process_one())
        self.assertTrue(upgraded.process_one())
        with closing(upgraded.connect()) as db:
            self.assertEqual(db.execute("SELECT state FROM sessions").fetchone(), ("DO_NOT_CONTACT",))

    def test_http_health_readiness_and_storage_failure(self):
        with make_server(self.store, 0) as server:
            thread = Thread(target=server.serve_forever)
            thread.start()
            try:
                with closing(HTTPConnection(*server.server_address, timeout=3)) as connection:
                    def check(path, status, body):
                        connection.request("GET", path)
                        response = connection.getresponse()
                        self.assertEqual(response.status, status)
                        self.assertEqual(json.loads(response.read()), body)

                    self.store.accept(self.event)
                    check("/health", 200, {"status": "ok"})
                    check("/ready", 200, {"status": "ok", "pending": 1})
                    check("/other", 404, {"status": "not_found"})
                    self.store.path = self.path / "invalid.sqlite3"
                    check("/ready", 503, {"status": "unavailable"})
                    check("/health", 200, {"status": "ok"})
            finally:
                server.shutdown()
                thread.join()


if __name__ == "__main__":
    unittest.main()
