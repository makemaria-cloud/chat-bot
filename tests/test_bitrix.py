import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from http.client import HTTPConnection
from pathlib import Path
from threading import Thread
from urllib.parse import urlencode

from bitrix import MAX_BODY, MessageBinding, Webhook
from bot import Store, make_server


class WebhookTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "bot.sqlite3"
        self.store = Store(self.path)
        self.store.migrate()
        binding = MessageBinding("12", "synthetic-avito", "321", "27", "client", ("321", 1, "bot"))
        self.bindings = {("112", "790"): binding}
        self.webhook = Webhook(
            self.store, "demo.example", "456", "synthetic-token",
            {("12", "synthetic-avito")}, lambda portal, chat, message: self.bindings.get((chat, message)),
        )
        self.form = {
            "auth[application_token]": "customsynthetic-token",
            "auth[domain]": "demo.example",
            "auth[server_endpoint]": "https://ignored.example",
            "event": "ONIMBOTV2MESSAGEADD",
            "data[bot][id]": "456",
            "data[chat][entityType]": "LINES",
            "data[chat][id]": "112",
            "data[message][chatId]": "112",
            "data[message][authorId]": "27",
            "data[message][id]": "790",
            "data[message][text]": "Синтетический запрос по бытовке",
        }
        self.server = make_server(self.store, 0, self.webhook)
        thread = Thread(target=self.server.serve_forever)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def post(self, changes=None, *, body=None, content_type="application/x-www-form-urlencoded", address=None):
        if body is None:
            body = urlencode({**self.form, **(changes or {})}).encode()
        with closing(HTTPConnection(*(address or self.server.server_address), timeout=5)) as connection:
            connection.request("POST", "/bitrix24/events", body, {"Content-Type": content_type})
            response = connection.getresponse()
            response.read()
            return response.status

    def test_authentication_scope_types_and_limits(self):
        for changes, status in (
            ({"auth[application_token]": "synthetic-invalid"}, 403),
            ({"auth[domain]": "foreign.example"}, 403),
            ({"data[bot][id]": "457"}, 403),
            ({"data[message][id]": "unknown"}, 400),
            ({"data[message][chatId]": "999"}, 400),
            ({"data[message][authorId]": "28"}, 403),
        ):
            with self.subTest(changes=changes):
                self.assertEqual(self.post(changes), status)
        binding = self.bindings["112", "790"]
        for alternate in (replace(binding, line="99"), replace(binding, connector="unknown")):
            self.bindings["112", "790"] = alternate
            self.assertEqual(self.post(), 403)
        self.assertEqual(self.post(body=b"a" * (MAX_BODY + 1)), 413)
        self.assertEqual(self.post(content_type="application/json"), 415)
        self.assertEqual(self.post(body=urlencode(self.form).encode() + b"&auth%5Bdomain%5D=demo.example"), 400)
        self.assertEqual(self.post(body=b"broken=%ff"), 400)
        self.assertEqual(self.store.pending(), 0)

    def test_filters_system_and_own_messages_without_answer(self):
        self.assertEqual(self.post({"data[message][isSystem]": "Y"}), 200)
        self.assertEqual(self.post({"data[message][authorId]": "456"}), 200)
        self.assertEqual(self.post({"data[chat][entityType]": "PRIVATE"}), 200)
        self.assertEqual(self.store.pending(), 0)

    def test_unconfigured_endpoint_and_rate_limit(self):
        with make_server(self.store, 0) as disabled:
            thread = Thread(target=disabled.handle_request)
            thread.start()
            try:
                self.assertEqual(self.post(address=disabled.server_address), 503)
            finally:
                thread.join()
        self.webhook.request_limit = 1
        self.assertEqual(self.post(), 200)
        self.assertEqual(self.post(), 429)

    def test_concurrent_repeat_and_conflict_preserve_minimal_context(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(list(pool.map(lambda _: self.post(), range(8))), [200] * 8)
        self.assertEqual(self.post({"data[message][text]": "Изменённое сообщение"}), 409)
        restarted = Store(self.path)
        self.assertEqual(restarted.pending(), 1)
        self.assertTrue(restarted.process_one())
        with closing(restarted.connect()) as db:
            context = json.loads(db.execute("SELECT context FROM inbox").fetchone()[0])
            self.assertEqual(context, {
                "source": "imbot.v2", "role": "client", "line": "12", "connector": "synthetic-avito",
                "text": self.form["data[message][text]"],
            })

    def test_stale_event_cannot_reactivate_an_old_session(self):
        self.assertEqual(self.post(), 200)
        original = self.bindings["112", "790"]
        self.bindings["112", "791"] = replace(original, session="322", head=("322", 2, "bot"))
        self.bindings["112", "792"] = original
        self.assertEqual(self.post({"data[message][id]": "791"}), 200)
        self.assertEqual(self.post({"data[message][id]": "792"}), 200)
        while self.store.process_one():
            pass
        with closing(self.store.connect()) as db:
            self.assertEqual(db.execute("SELECT session FROM sessions").fetchall(), [("322",)])
            self.assertEqual(db.execute("SELECT session, revision FROM chat_heads").fetchone(), ("322", 2))

    def test_operator_ownership_applies_before_worker_catches_up(self):
        self.assertEqual(self.post(), 200)
        original = self.bindings["112", "790"]
        self.bindings["112", "791"] = replace(original, author="32", role="operator", head=("321", 2, "human"))
        self.assertEqual(self.post({"data[message][id]": "791", "data[message][authorId]": "32"}), 200)
        self.assertTrue(self.store.process_one())
        with closing(self.store.connect()) as db:
            self.assertEqual(db.execute("SELECT state FROM sessions").fetchall(), [])
        self.assertTrue(self.store.process_one())
        with closing(self.store.connect()) as db:
            self.assertEqual(db.execute("SELECT state FROM sessions").fetchone(), ("HUMAN_ACTIVE",))
            self.assertEqual(json.loads(db.execute(
                "SELECT context FROM inbox WHERE kind = 'human'"
            ).fetchone()[0])["text"], "")
