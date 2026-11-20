import io
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import patch

from bitrix_rest import BitrixRest, interpret_response
from bot import MIGRATIONS, Store
from outbox import HANDOFF_COOLDOWN, Action, Dispatcher, Outcome, Plan, record_crm_link

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

    def test_new_crm_session_waits_for_its_own_verified_binding(self):
        self.plan(Plan(crm_fields={"quantity": 2}, crm_create_type=1))
        self.fake.outcome = Outcome("confirmed", crm_item=17)
        self.assertTrue(self.dispatch.dispatch_one())
        self.plan(Plan(crm_fields={"quantity": 4}, crm_create_type=1), "791", session="322", revision=2)
        self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual(self.dispatch.review_queue(), [
            {"id": 2, "kind": "crm_projection", "status": "deferred", "reason": "waiting_session_binding"},
        ])
        self.assertEqual(self.rows("SELECT session, item FROM crm_links"), [("321", 17)])
        self.assertTrue(Dispatcher(Store(self.path)).bind_crm(2, 1, 17))
        self.fake.outcome = Outcome("confirmed")
        self.assertTrue(self.dispatch.dispatch_one())
        self.assertEqual([call.kind for call in self.fake.calls], ["crm_create", "crm_update"])
        self.assertEqual(self.fake.calls[-1].session, "322")
        self.assertEqual(self.fake.calls[-1].params, {"entityTypeId": 1, "id": 17, "fields": {"quantity": 4}})

    def test_crm_bind_recovers_update_after_message_without_new_fields(self):
        for offset, later_plan in enumerate((Plan(reply="Тестовый ответ"), Plan(), None)):
            with self.subTest(later_plan=later_plan):
                chat = str(int(KEY[1]) + offset)
                with closing(self.store.connect()) as db, db:
                    self.assertTrue(record_crm_link(db, (KEY[0], chat, KEY[2]), 1, 17))
                self.plan(Plan(crm_fields={"quantity": 2}), chat=chat)
                self.plan(later_plan, "791", chat=chat)
                self.drain(self.dispatch)
                projection_id = next(row["id"] for row in self.dispatch.review_queue()
                                     if row["kind"] == "crm_projection")
                restarted = Dispatcher(Store(self.path), approved_scope=SCOPE, transport=self.fake, verify=lambda _: True)
                with ThreadPoolExecutor(max_workers=4) as pool:
                    self.assertTrue(all(pool.map(lambda _: restarted.bind_crm(projection_id, 1, 17), range(8))))
                self.fake.outcome = Outcome("confirmed")
                self.drain(restarted)
                self.assertTrue(restarted.bind_crm(projection_id, 1, 17))
                self.assertFalse(restarted.dispatch_one())
                updates = [call for call in self.fake.calls if call.chat == chat and call.kind.startswith("crm_")]
                self.assertEqual([(action.kind, action.params["fields"]) for action in updates],
                                 [("crm_update", {"quantity": 2})])

    def test_crm_bind_recovers_latest_projection_after_another_version_change(self):
        with closing(self.store.connect()) as db, db:
            self.assertTrue(record_crm_link(db, KEY, 1, 17))
        self.plan(Plan(crm_fields={"quantity": 2}))
        self.plan(Plan(), "791")
        self.drain(self.dispatch)
        self.plan(Plan(crm_fields={"quantity": 4}), "792")
        self.plan(Plan(), "793")
        self.drain(self.dispatch)
        self.assertTrue(self.dispatch.bind_crm(1, 1, 17))
        self.plan(Plan(), "794")
        self.drain(self.dispatch)
        self.assertTrue(self.dispatch.bind_crm(1, 1, 17))
        self.fake.outcome = Outcome("confirmed")
        self.drain(self.dispatch)
        self.assertEqual([action.params["fields"] for action in self.fake.calls], [{"quantity": 4}])
        self.assertEqual(self.rows("SELECT status FROM outbox ORDER BY id"),
                         [("cancelled",), ("cancelled",), ("cancelled",), ("confirmed",)])

    def test_crm_bind_does_not_recover_update_after_controls_or_route_change(self):
        for offset, change in enumerate(("human", "stop", "closed", "session", "route")):
            with self.subTest(change=change):
                chat = str(int(KEY[1]) + offset)
                with closing(self.store.connect()) as db, db:
                    self.assertTrue(record_crm_link(db, (KEY[0], chat, KEY[2]), 1, 17))
                self.plan(Plan(crm_fields={"quantity": 2}), chat=chat)
                if change == "session":
                    self.plan(Plan(), "791", chat=chat, session="322", revision=2)
                elif change == "route":
                    event = dict(portal=KEY[0], chat=chat, session=KEY[2], message="791", kind="message")
                    self.store.accept(event, context={**CONTEXT, "line": "13"}, head=("321", 1, "bot"))
                    self.assertTrue(self.store.process_one())
                else:
                    self.event("791", change, chat=chat)
                    self.assertTrue(self.store.process_one())
                self.drain(self.dispatch)
                self.assertTrue(self.dispatch.bind_crm(offset + 1, 1, 17))
                self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual(self.fake.calls, [])

    def test_crm_bind_does_not_replace_in_flight_or_uncertain_update(self):
        with closing(self.store.connect()) as db, db:
            self.assertTrue(record_crm_link(db, KEY, 1, 17))
        self.plan(Plan(crm_fields={"quantity": 2}))
        attempt = self.dispatch.claim()
        self.plan(Plan(), "791")
        self.assertTrue(self.dispatch.bind_crm(1, 1, 17))
        self.assertFalse(self.dispatch.dispatch_one())
        self.dispatch.finish(attempt, Outcome("uncertain", "timeout"))
        self.assertTrue(self.dispatch.bind_crm(1, 1, 17))
        self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual(self.rows("SELECT status FROM outbox"), [("uncertain",)])

    def test_crm_edits_during_attempt_survive_restart_and_concurrent_confirmation(self):
        self.plan(Plan(crm_fields={"quantity": 2}, crm_create_type=1))
        attempt = self.dispatch.claim()
        self.plan(Plan(crm_fields={"quantity": 3, "region": "demo"}, crm_create_type=1), "791")
        self.assertFalse(self.dispatch.dispatch_one())
        self.dispatch.finish(attempt, Outcome("uncertain", "timeout"))
        for index in range(792, 795):
            data = dict(portal=KEY[0], chat=KEY[1], session=KEY[2], message=str(index), kind="message")
            context = {**CONTEXT, "text": str(index)}
            self.assertTrue(self.store.accept(data, context=context, head=("321", 1, "bot")))
        self.assertFalse(self.store.accept(data, context=context, head=("321", 1, "bot")))

        def process(_):
            return Store(self.path).process_one(lambda context: Plan(crm_fields={"quantity": int(context["text"])}))

        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(process, range(8))), 3)
        restarted = Dispatcher(Store(self.path), approved_scope=SCOPE, transport=self.fake, verify=lambda _: True)
        self.assertEqual(restarted.review_queue()[-1], {
            "id": 1, "kind": "crm_projection", "status": "deferred", "reason": "crm_attempt_unresolved",
        })
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(lambda _: restarted.confirm(1, crm_item=17), range(8))), 1)
        self.fake.outcome = Outcome("confirmed")
        self.drain(restarted)
        self.assertEqual([call.kind for call in self.fake.calls], ["crm_update"])
        self.assertEqual(self.fake.calls[0].params["fields"], {"quantity": 794, "region": "demo"})
        self.assertEqual(self.rows("SELECT kind, status FROM outbox ORDER BY id"), [
            ("crm_create", "confirmed"), ("crm_update", "confirmed"),
        ])

    def test_create_success_releases_edits_queued_while_api_was_running(self):
        self.plan(Plan(crm_fields={"quantity": 2}, crm_create_type=1))
        attempt = self.dispatch.claim()
        self.plan(Plan(crm_fields={"quantity": 3}), "791")
        self.dispatch.finish(attempt, Outcome("confirmed", crm_item=17))
        self.fake.outcome = Outcome("confirmed")
        self.assertTrue(self.dispatch.dispatch_one())
        self.assertEqual(self.fake.calls[0].params, {"entityTypeId": 1, "id": 17, "fields": {"quantity": 3}})

    def test_controls_and_new_session_guard_crm_update_and_reply_after_confirmation(self):
        for index, kind in enumerate(("human", "stop", "new_session"), start=1):
            with self.subTest(kind=kind):
                chat = str(112 + index)
                self.plan(Plan(crm_fields={"quantity": 2}, crm_create_type=1), chat=chat)
                attempt = self.dispatch.claim()
                self.dispatch.finish(attempt, Outcome("uncertain", "timeout"))
                self.plan(Plan(reply="Старый ответ", crm_fields={"quantity": 3}), "791", chat=chat)
                self.assertTrue(self.dispatch.confirm(attempt.id, crm_item=17))
                if kind == "new_session":
                    self.plan(Plan(), "792", chat=chat, session="322", revision=2)
                else:
                    self.event("792", kind, chat=chat)
                    self.assertTrue(self.store.process_one())
                self.drain(self.dispatch)
        self.assertEqual(self.fake.calls, [])
        self.assertEqual(self.rows("SELECT count(*) FROM outbox WHERE status = 'cancelled'"), [(6,)])
        self.assertEqual(sum(row["kind"] == "crm_projection" for row in self.dispatch.review_queue()), 3)

    def test_handoff_recovery_needs_new_request_cooldown_and_one_approval(self):
        for index, failure in enumerate(("rate_limited", "permission_denied"), start=1):
            with self.subTest(failure=failure):
                chat = str(112 + index)
                self.plan(Plan(handoff=True), chat=chat)
                self.fake.outcome = Outcome("not_sent", failure)
                with patch("outbox.time.time", return_value=100):
                    self.assertTrue(self.dispatch.dispatch_one())
                failed_id = self.fake.calls[-1].id
                restarted = Dispatcher(Store(self.path), approved_scope=SCOPE, transport=self.fake, verify=lambda _: True)
                with patch("outbox.time.time", return_value=100 + HANDOFF_COOLDOWN):
                    self.assertFalse(restarted.retry_handoff(failed_id))
                self.plan(Plan(handoff=True), "791", chat=chat)
                self.plan(Plan(handoff=True), "792", chat=chat)
                self.assertFalse(restarted.dispatch_one())
                self.assertIn({"id": failed_id, "kind": "handoff_request", "status": "deferred", "reason": failure},
                              restarted.review_queue())
                with patch("outbox.time.time", return_value=100 + HANDOFF_COOLDOWN - 1):
                    self.assertFalse(restarted.retry_handoff(failed_id))
                with patch("outbox.time.time", return_value=100 + HANDOFF_COOLDOWN):
                    with ThreadPoolExecutor(max_workers=4) as pool:
                        self.assertEqual(sum(pool.map(lambda _: restarted.retry_handoff(failed_id), range(8))), 1)
                self.fake.outcome = Outcome("api_accepted")
                self.assertTrue(restarted.dispatch_one())
                self.assertFalse(restarted.dispatch_one())
                self.assertEqual(self.rows("SELECT status FROM outbox WHERE chat = ? ORDER BY id", (chat,)),
                                 [("not_sent",), ("api_accepted",)])

    def test_handoff_cannot_be_retried_after_ambiguous_or_accepted_outcome(self):
        for index, outcome in enumerate((Outcome("uncertain", "timeout"), Outcome("api_accepted")), start=1):
            with self.subTest(outcome=outcome):
                chat = str(112 + index)
                self.plan(Plan(handoff=True), chat=chat)
                self.fake.outcome = outcome
                with patch("outbox.time.time", return_value=100):
                    self.assertTrue(self.dispatch.dispatch_one())
                action_id = self.fake.calls[-1].id
                self.plan(Plan(handoff=True), "791", chat=chat)
                with patch("outbox.time.time", return_value=100 + HANDOFF_COOLDOWN):
                    self.assertFalse(self.dispatch.retry_handoff(action_id))
                    self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual(len(self.fake.calls), 2)

    def test_handoff_recovery_rechecks_controls_before_queueing(self):
        self.plan(Plan(handoff=True))
        self.fake.outcome = Outcome("not_sent", "rate_limited")
        with patch("outbox.time.time", return_value=100):
            self.assertTrue(self.dispatch.dispatch_one())
        self.plan(Plan(handoff=True), "791")
        self.event("792", "stop")
        with patch("outbox.time.time", return_value=100 + HANDOFF_COOLDOWN):
            self.assertFalse(self.dispatch.retry_handoff(self.fake.calls[0].id))
        self.assertFalse(self.dispatch.dispatch_one())
        self.assertEqual(len(self.fake.calls), 1)

    def test_upgrade_of_v4_preserves_existing_outbox_and_crm_projection(self):
        old = self.path.with_name("v4.sqlite3")
        migrations = Path(__file__).resolve().parents[1] / "migrations"
        with closing(sqlite3.connect(old)) as db, db:
            for filename in MIGRATIONS[:4]:
                db.executescript((migrations / filename).read_text(encoding="utf-8"))
            db.execute("PRAGMA user_version = 4")
            db.execute(
                """INSERT INTO outbox (portal, chat, session, revision, version, line, connector,
                                       kind, dedup, params, status, reason)
                   VALUES (?, ?, ?, 1, 1, ?, ?, 'crm_create', '1', ?, 'uncertain', 'timeout'),
                          (?, ?, ?, 1, 2, ?, ?, 'handoff', '2', '{}', 'not_sent', 'rate_limited')""",
                (*KEY, *SCOPE[1:], json.dumps({"entityTypeId": 1, "fields": {"quantity": 2}}), *KEY, *SCOPE[1:]),
            )
        upgraded = Store(old)
        upgraded.migrate()
        upgraded.migrate()
        with closing(upgraded.connect()) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone(), (5,))
            self.assertEqual(db.execute("SELECT id, status FROM outbox ORDER BY id").fetchall(),
                             [(1, "uncertain"), (2, "not_sent")])
            self.assertIsInstance(db.execute("SELECT finished_at FROM outbox WHERE id = 2").fetchone()[0], int)
            fields, action_id = db.execute("SELECT fields, action_id FROM crm_desired").fetchone()
            self.assertEqual((json.loads(fields), action_id), ({"quantity": 2}, 1))


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
