import json
import time
from contextlib import closing
from dataclasses import dataclass


@dataclass(frozen=True)
class Plan:
    reply: str | None = None
    handoff: bool = False
    crm_fields: dict | None = None
    crm_create_type: int | None = None


@dataclass(frozen=True)
class Action:
    id: int
    portal: str
    chat: str
    session: str
    revision: int
    version: int
    line: str
    connector: str
    kind: str
    params: dict

    @property
    def scope(self):
        return self.portal, self.line, self.connector

    @property
    def key(self):
        return self.portal, self.chat, self.session


@dataclass(frozen=True)
class Outcome:
    status: str
    reason: str | None = None
    crm_item: int | None = None


def enqueue_actions(db, key, revision, version, metadata, plan):
    if plan is None:
        return
    db.execute(
        """UPDATE outbox SET status = 'cancelled', reason = 'newer_state'
           WHERE (portal, chat, session) = (?, ?, ?) AND version < ? AND status = 'planned'""",
        (*key, version),
    )
    desired = []
    if plan.reply:
        desired.append(("message", str(version), {"text": plan.reply}))
    if plan.handoff:
        desired.append(("handoff", str(version), {}))
    if plan.crm_fields:
        binding = crm_link(db, key)
        if binding:
            entity_type, item = binding
            desired.append(("crm_update", str(version), {
                "entityTypeId": entity_type, "id": item, "fields": plan.crm_fields,
            }))
        elif plan.crm_create_type:
            desired.append(("crm_create", str(version), {
                "entityTypeId": plan.crm_create_type, "fields": plan.crm_fields,
            }))
    for kind, dedup, params in desired:
        db.execute(
            """INSERT INTO outbox (portal, chat, session, revision, version, line,
                                   connector, kind, dedup, params)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""",
            (*key, revision, version, metadata["line"], metadata["connector"],
             kind, dedup, json.dumps(params, ensure_ascii=False)),
        )


def crm_link(db, key):
    return db.execute(
        "SELECT entity_type, item FROM crm_links WHERE (portal, chat, session) = (?, ?, ?)", key
    ).fetchone()


def record_crm_link(db, key, entity_type, item):
    db.execute(
        """INSERT INTO crm_links (portal, chat, session, entity_type, item)
           VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""",
        (*key, entity_type, item),
    )
    return crm_link(db, key) == (entity_type, item)


class Dispatcher:
    def __init__(self, store, *, approved_scope=None, transport=None, verify=None):
        if approved_scope is not None and (transport is None or verify is None):
            raise ValueError("Для тестовой отправки нужны транспорт и свежая проверка привязки")
        self.store = store
        self.approved_scope = approved_scope
        self.transport = transport
        self.verify = verify

    def claim(self):
        with closing(self.store.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """SELECT id, portal, chat, session, revision, version, line, connector, kind, params
                   FROM outbox AS pending
                   WHERE status = 'planned' AND NOT EXISTS (
                       SELECT 1 FROM outbox AS active
                       WHERE (active.portal, active.chat) = (pending.portal, pending.chat)
                         AND active.status = 'in_flight'
                   )
                   ORDER BY id LIMIT 1"""
            ).fetchone()
            if row is None:
                return None
            db.execute(
                "UPDATE outbox SET status = 'in_flight', claimed_at = ? WHERE id = ?",
                (int(time.time()), row[0]),
            )
        return Action(*row[:-1], json.loads(row[-1]))

    def can_dispatch(self, action):
        with closing(self.store.connect()) as db:
            db.execute("BEGIN")
            return db.execute(
                """SELECT EXISTS (
                       SELECT 1 FROM outbox AS o
                       JOIN chat_heads AS h ON (h.portal, h.chat) = (o.portal, o.chat)
                       JOIN sessions AS s ON (s.portal, s.chat, s.session) = (o.portal, o.chat, o.session)
                       WHERE o.id = ? AND o.status = 'in_flight'
                         AND h.session = o.session AND h.revision = o.revision AND h.owner = 'bot'
                         AND s.version = o.version AND s.state NOT IN ('HUMAN_ACTIVE', 'CLOSED', 'DO_NOT_CONTACT')
                         AND NOT EXISTS (
                             SELECT 1 FROM inbox AS i
                             WHERE (i.portal, i.chat, i.session) = (o.portal, o.chat, o.session)
                               AND i.processed = 0 AND i.kind IN ('human', 'closed', 'stop')
                         )
                         AND (o.kind != 'message' OR NOT EXISTS (
                             SELECT 1 FROM outbox AS hnd
                             WHERE (hnd.portal, hnd.chat, hnd.session) = (o.portal, o.chat, o.session)
                               AND hnd.kind = 'handoff'
                               AND hnd.status IN ('api_accepted', 'uncertain', 'confirmed')
                         ))
                   )""",
                (action.id,),
            ).fetchone()[0] and self.binding_matches(db, action)

    def binding_matches(self, db, action):
        if action.kind == "crm_create":
            return crm_link(db, action.key) is None
        if action.kind == "crm_update":
            return crm_link(db, action.key) == (action.params["entityTypeId"], action.params["id"])
        return True

    def dispatch_one(self):
        action = self.claim()
        if action is None:
            return False
        if self.approved_scope is None:
            outcome = Outcome("not_sent", "dry_run")
        elif action.scope != self.approved_scope:
            outcome = Outcome("not_sent", "scope_denied")
        else:
            outcome = self.attempt(action)
        self.finish(action, outcome)
        return True

    def attempt(self, action):
        if not self.can_dispatch(action):
            return Outcome("cancelled", "local_state_changed")
        try:
            verified = self.verify(action)
        except (OSError, ValueError):
            return Outcome("not_sent", "verification_unavailable")
        if verified is not True:
            return Outcome("cancelled", "binding_unverified")
        if not self.can_dispatch(action):
            return Outcome("cancelled", "local_state_changed")
        try:
            return self.transport.execute(action)
        except (OSError, ValueError):
            return Outcome("uncertain", "transport_failure")

    def finish(self, action, outcome):
        with closing(self.store.connect()) as db, db:
            changed = db.execute(
                """UPDATE outbox SET status = ?, reason = ?
                   WHERE id = ? AND status = 'in_flight'""",
                (outcome.status, outcome.reason, action.id),
            ).rowcount
            if changed and action.kind == "crm_create" and outcome.crm_item:
                if not record_crm_link(db, action.key, action.params["entityTypeId"], outcome.crm_item):
                    db.execute(
                        "UPDATE outbox SET status = 'uncertain', reason = 'crm_binding_conflict' WHERE id = ?",
                        (action.id,),
                    )

    def recover_expired(self, *, grace_seconds=60):
        if grace_seconds < 60:
            raise ValueError("Срок ожидания должен составлять не менее 60 секунд")
        with closing(self.store.connect()) as db, db:
            return db.execute(
                """UPDATE outbox SET status = 'uncertain', reason = 'worker_interrupted'
                   WHERE status = 'in_flight' AND claimed_at <= ?""",
                (int(time.time()) - grace_seconds,),
            ).rowcount

    def confirm(self, action_id, *, crm_item=None):
        """Отмечает факт, который оператор проверил независимо от ответа API."""
        with closing(self.store.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """SELECT portal, chat, session, kind, params FROM outbox
                   WHERE id = ? AND status IN ('api_accepted', 'uncertain')""",
                (action_id,),
            ).fetchone()
            if row is None:
                return False
            portal, chat, session, kind, params = row
            if kind == "crm_create":
                if type(crm_item) is not int or crm_item <= 0:
                    return False
                entity_type = json.loads(params)["entityTypeId"]
                if not record_crm_link(db, (portal, chat, session), entity_type, crm_item):
                    return False
            elif crm_item is not None:
                return False
            return db.execute(
                "UPDATE outbox SET status = 'confirmed', reason = NULL WHERE id = ?",
                (action_id,),
            ).rowcount == 1

    def review_queue(self):
        with closing(self.store.connect()) as db:
            rows = db.execute(
                """SELECT id, kind, status, reason FROM outbox
                   WHERE status IN ('uncertain', 'api_accepted')
                      OR (status = 'not_sent' AND reason != 'dry_run')
                   ORDER BY id"""
            )
            return [dict(zip(("id", "kind", "status", "reason"), row)) for row in rows]
