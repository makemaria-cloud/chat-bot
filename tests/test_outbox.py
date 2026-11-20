import io
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.error import HTTPError

from bitrix_rest import BitrixRest, interpret_response
from bot import Store
from outbox import Action, Dispatcher, Outcome, Plan, record_crm_link

SCOPE = ("demo.example", "12", "synthetic-avito")
KEY = (SCOPE[0], "112", "321")
CONTEXT = {"source": "imbot.v2", "role": "client", "line": SCOPE[1], "connector": SCOPE[2], "text": "Тест"}


class FakeTransport:
    def __init__(self, outcome=Outcome("api_accepted")):
        self.calls = []
        self.outcome = outcome

    def execute(self, action):
        self.calls.append(action)
        return self.outcome


class OutboxTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "bot.sqlite3"
        self.store = Store(self.path)
        self.store.migrate()
        self.fake = FakeTransport()
        self.dispatch = Dispatcher(self.store, approved_scope=SCOPE, transport=self.fake, verify=lambda _: True)

    def event(self, message="790", kind="message", *, session=KEY[2], head=None, portal=KEY[0], chat=KEY[1]):
        data = dict(portal=portal, chat=chat, session=session, message=message, kind=kind)
        return self.store.accept(data, context=CONTEXT if head else None, head=head)

    def plan(self, plan, message="790", *, session=KEY[2], revision=1, portal=KEY[0], chat=KEY[1]):
        self.assertTrue(self.event(message, session=session, head=(session, revision, "bot"), portal=portal, chat=chat))
        self.assertTrue(self.store.process_one(lambda _: plan))

    def rows(self, query, params=()):
        with closing(self.store.connect()) as db:
            return db.execute(query, params).fetchall()

    def drain(self, dispatcher):
        while dispatcher.dispatch_one():
            pass

    def test_state_and_intent_rollback_together(self):
        self.event(head=("321", 1, "bot"))
        with closing(self.store.connect()) as db, db:
            db.execute(
                """CREATE TRIGGER fail_intent BEFORE INSERT ON outbox
                   BEGIN SELECT RAISE(ABORT, 'test failure'); END"""
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.process_one(lambda _: Plan(reply="Тестовый ответ"))
        self.assertEqual(self.rows("SELECT state FROM sessions"), [])
        self.assertEqual(self.rows("SELECT status FROM outbox"), [])
        self.assertEqual(self.store.pending(), 1)
        with closing(self.store.connect()) as db, db:
            db.execute("DROP TRIGGER fail_intent")
        self.assertTrue(self.store.process_one(lambda _: Plan(reply="Тестовый ответ")))
        self.assertEqual(self.rows("SELECT state FROM sessions"), [("QUALIFYING",)])
        self.assertEqual(self.rows("SELECT status FROM outbox"), [("planned",)])

    def test_pending_control_event_prevents_send_after_restart(self):
        for index, kind in enumerate(("human", "stop", "closed"), start=1):
            with self.subTest(kind=kind):
                chat = str(112 + index)
                self.plan(Plan(reply="Тестовый ответ"), chat=chat)
                self.event(kind=kind, chat=chat)
                new_dispatch = Dispatcher(
                    Store(self.path), approved_scope=SCOPE, transport=self.fake, verify=lambda _: True,
                )
                self.drain(new_dispatch)
                self.assertTrue(self.store.process_one())
        self.assertEqual(self.rows("SELECT status FROM outbox"), [("cancelled",)] * 3)
        self.assertEqual(self.fake.calls, [])

    def test_move_to_new_session_cancels_old_and_skips_delayed_event(self):
        self.plan(Plan(reply="Старый ответ"))
        self.plan(Plan(reply="Новый ответ"), "791", session="322", revision=2)
        self.event("792", head=("321", 1, "bot"))
        self.assertTrue(self.store.process_one(lambda _: Plan(reply="Запоздалый ответ")))
        self.drain(self.dispatch)
        self.assertEqual([(call.session, call.params["text"]) for call in self.fake.calls], [("322", "Новый ответ")])
        self.assertEqual(self.rows("SELECT status FROM outbox ORDER BY id"), [("cancelled",), ("api_accepted",)])

    def test_rechecks_controls_after_external_verification(self):
        self.plan(Plan(reply="Тестовый ответ"))

        def verify(_):
            self.assertTrue(self.event(kind="stop"))
            return True

        dispatcher = Dispatcher(self.store, approved_scope=SCOPE, transport=self.fake, verify=verify)
        self.assertTrue(dispatcher.dispatch_one())
        self.assertEqual(self.fake.calls, [])
        self.assertEqual(self.rows("SELECT status FROM outbox"), [("cancelled",)])

    def test_sender_does_not_hold_sqlite_write_lock(self):
        self.plan(Plan(reply="Тестовый ответ"))
        case = self

        class Transport:
            def execute(self, action):
                with ThreadPoolExecutor(max_workers=1) as pool:
                    case.assertTrue(pool.submit(case.event, kind="stop").result(timeout=3))
                return Outcome("api_accepted")

        dispatcher = Dispatcher(self.store, approved_scope=SCOPE, transport=Transport(), verify=lambda _: True)
        self.assertTrue(dispatcher.dispatch_one())
        self.assertEqual(self.rows("SELECT status FROM outbox"), [("api_accepted",)])
        self.assertEqual(self.store.pending(), 1)

    def test_timeout_cannot_be_retried_and_only_one_worker_calls_api(self):
        self.plan(Plan(reply="Тестовый ответ"))
        self.fake.outcome = Outcome("uncertain", "timeout")
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(lambda _: self.dispatch.dispatch_one(), range(16))), 1)
        self.assertEqual(len(self.fake.calls), 1)
        self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual(self.dispatch.review_queue(), [
            {"id": 1, "kind": "message", "status": "uncertain", "reason": "timeout"},
        ])
        self.assertTrue(self.dispatch.confirm(1))
        self.assertEqual(self.rows("SELECT status FROM outbox"), [("confirmed",)])

    def test_recover_abandoned_claim_without_second_attempt(self):
        self.plan(Plan(handoff=True))
        self.assertIsNotNone(self.dispatch.claim())
        with closing(self.store.connect()) as db, db:
            db.execute("UPDATE outbox SET claimed_at = 0")
        self.assertEqual(self.dispatch.recover_expired(), 1)
        self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual(self.fake.calls, [])
        self.assertEqual(self.rows("SELECT status, reason FROM outbox"), [("uncertain", "worker_interrupted")])

    def test_handoff_acceptance_and_failure_are_not_confirmation(self):
        self.plan(Plan(handoff=True))
        self.assertTrue(self.dispatch.dispatch_one())
        self.assertEqual(self.rows("SELECT status FROM outbox"), [("api_accepted",)])
        self.assertEqual(self.rows("SELECT state FROM sessions"), [("QUALIFYING",)])
        self.fake.outcome = Outcome("not_sent", "permission_denied")
        self.plan(Plan(handoff=True), chat="113")
        self.assertTrue(self.dispatch.dispatch_one())
        self.assertEqual([row["status"] for row in self.dispatch.review_queue()], ["api_accepted", "not_sent"])

    def test_queued_reply_is_cancelled_during_uncertain_handoff(self):
        self.plan(Plan(handoff=True))
        self.fake.outcome = Outcome("uncertain", "timeout")
        self.assertTrue(self.dispatch.dispatch_one())
        self.plan(Plan(reply="Ответ после передачи"), "791")
        self.assertTrue(self.dispatch.dispatch_one())
        self.assertEqual([action.kind for action in self.fake.calls], ["handoff"])
        self.assertEqual(self.rows("SELECT status FROM outbox ORDER BY id"), [("uncertain",), ("cancelled",)])

    def test_dry_run_never_calls_transport_even_with_all_actions_pending(self):
        self.plan(Plan(reply="Тестовый ответ", handoff=True, crm_fields={"synthetic": "value"}, crm_create_type=1))
        self.drain(Dispatcher(self.store, transport=self.fake))
        self.assertEqual(self.fake.calls, [])
        self.assertEqual(self.rows("SELECT status, reason FROM outbox"), [("not_sent", "dry_run")] * 3)

    def test_other_portal_scope_does_not_call_verifier_or_transport(self):
        self.plan(Plan(reply="Тестовый ответ"), portal="foreign.example")
        self.assertTrue(self.dispatch.dispatch_one())
        self.assertEqual(self.fake.calls, [])
        self.assertEqual(self.rows("SELECT status, reason FROM outbox"), [("not_sent", "scope_denied")])

    def test_crm_uses_known_link_and_reserves_unknown_creation_through_uncertainty(self):
        with closing(self.store.connect()) as db, db:
            self.assertTrue(record_crm_link(db, KEY, 2, 17))
        plan = Plan(crm_fields={"synthetic": "value"}, crm_create_type=1)
        self.plan(plan)
        self.fake.outcome = Outcome("confirmed")
        self.assertTrue(self.dispatch.dispatch_one())
        self.assertEqual(self.fake.calls[0].kind, "crm_update")
        self.assertEqual(self.fake.calls[0].params["id"], 17)
        self.plan(plan, chat="113")
        self.fake.outcome = Outcome("uncertain", "timeout")
        self.assertTrue(self.dispatch.dispatch_one())
        self.plan(plan, "791", chat="113")
        self.plan(plan, "792", chat="113", session="322", revision=2)
        self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual([action.kind for action in self.fake.calls], ["crm_update", "crm_create"])
        self.assertFalse(self.dispatch.confirm(2))
        self.assertTrue(self.dispatch.confirm(2, crm_item=18))
        self.assertEqual(self.rows("SELECT item FROM crm_links ORDER BY item"), [(17,), (18,)])

    def test_crm_creation_requires_explicit_policy(self):
        self.plan(Plan(crm_fields={"synthetic": "value"}))
        self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual(self.rows("SELECT kind FROM outbox"), [])


class RestContractTest(unittest.TestCase):
    def setUp(self):
        self.rest = BitrixRest("https://demo.example/rest/1/synthetic-placeholder", approved_test_scope=SCOPE)
        self.action = Action(1, *KEY, 1, 1, *SCOPE[1:], "message", {"text": "Тестовый ответ"})

    def test_explicit_scope_and_well_known_path_with_fake_http(self):
        class FakeHTTP:
            def open(_, request, timeout):
                self.assertEqual(request.host, "demo.example")
                self.assertTrue(request.full_url.endswith("/imopenlines.bot.session.message.send"))
                self.assertEqual(json.loads(request.data), {"CHAT_ID": 112, "NAME": "DEFAULT", "MESSAGE": "Тестовый ответ"})
                return io.BytesIO(b'{"result":true}')

        self.rest.opener = FakeHTTP()
        self.assertEqual(self.rest.execute(self.action), Outcome("api_accepted"))
        with self.assertRaises(ValueError):
            BitrixRest("https://foreign.example/rest/1/synthetic-placeholder", approved_test_scope=SCOPE)

    def test_api_errors_and_invalid_responses_have_distinct_states(self):
        for code, outcome in (
            (503, Outcome("uncertain", "api_unavailable")),
            (429, Outcome("not_sent", "rate_limited")),
            (403, Outcome("not_sent", "permission_denied")),
        ):
            with self.subTest(code=code):
                class FakeHTTP:
                    def open(_, request, timeout):
                        raise HTTPError(request.full_url, code, "", {}, io.BytesIO())

                self.rest.opener = FakeHTTP()
                self.assertEqual(self.rest.execute(self.action), outcome)
        self.assertEqual(interpret_response("message", {}, {"result": "true"}), Outcome("uncertain", "invalid_result"))
        self.assertEqual(interpret_response("handoff", {}, {"result": False}), Outcome("not_sent", "rejected_by_api"))
        self.assertEqual(interpret_response("crm_create", {}, {"result": {"item": {"id": 17}}}),
                         Outcome("confirmed", crm_item=17))

    def test_read_timeout_is_unknown_even_after_request_was_accepted(self):
        accepted = []

        class FakeHTTP:
            def open(_, request, timeout):
                accepted.append(json.loads(request.data))
                raise TimeoutError

        self.rest.opener = FakeHTTP()
        self.assertEqual(self.rest.execute(self.action), Outcome("uncertain", "timeout"))
        self.assertEqual(len(accepted), 1)
