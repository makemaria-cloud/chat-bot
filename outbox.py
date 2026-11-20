import json
import time
from contextlib import closing
from dataclasses import dataclass

HANDOFF_COOLDOWN = 60
HANDOFF_RETRY_REASONS = {
    "rate_limited", "permission_denied", "rejected_by_api", "verification_unavailable",
    "scope_denied", "dry_run",
}


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
           WHERE (portal, chat, session) = (?, ?, ?) AND version < ?
             AND kind = 'message' AND status = 'planned'""",
        (*key, version),
    )
    route = metadata["line"], metadata["connector"]
    if plan.reply:
        insert_action(db, key, revision, version, route, "message", str(version), {"text": plan.reply})
    if plan.handoff:
        enqueue_handoff(db, key, revision, version, route)
    if plan.crm_fields:
        remember_crm(db, key, revision, version, route, plan)
        schedule_crm(db, key)


def insert_action(db, key, revision, version, route, kind, dedup, params):
    return db.execute(
        """INSERT INTO outbox (portal, chat, session, revision, version, line,
                               connector, kind, dedup, params)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (*key, revision, version, *route, kind, dedup, json.dumps(params, ensure_ascii=False)),
    ).lastrowid


def enqueue_handoff(db, key, revision, version, route):
    updated = db.execute(
        """UPDATE outbox SET version = ?
           WHERE (portal, chat, session) = (?, ?, ?) AND (revision, line, connector) = (?, ?, ?)
             AND kind = 'handoff' AND status = 'planned'""",
        (version, *key, revision, *route),
    ).rowcount
    if updated:
        return
    db.execute(
        """UPDATE outbox SET status = 'cancelled', reason = 'newer_request'
           WHERE (portal, chat, session) = (?, ?, ?) AND kind = 'handoff' AND status = 'planned'""",
        key,
    )
    blocker = db.execute(
        """SELECT id FROM outbox
           WHERE (portal, chat, session) = (?, ?, ?) AND kind = 'handoff'
             AND (status != 'cancelled' OR reason = 'local_state_changed')
           ORDER BY id DESC LIMIT 1""",
        key,
    ).fetchone()
    if blocker is None:
        insert_action(db, key, revision, version, route, "handoff", str(version), {})
        return
    db.execute(
        """INSERT INTO handoff_waiting (portal, chat, session, revision, version, line, connector, blocked_by)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (portal, chat, session) DO UPDATE SET
               revision = excluded.revision, version = excluded.version, line = excluded.line,
               connector = excluded.connector, blocked_by = excluded.blocked_by""",
        (*key, revision, version, *route, blocker[0]),
    )


def session_version(db, key, revision):
    result = db.execute(
        """SELECT s.version FROM sessions AS s
           JOIN chat_heads AS h ON (h.portal, h.chat, h.session) = (s.portal, s.chat, s.session)
           WHERE (s.portal, s.chat, s.session) = (?, ?, ?) AND h.revision = ? AND h.owner = 'bot'
             AND s.state NOT IN ('HUMAN_ACTIVE', 'CLOSED', 'DO_NOT_CONTACT')
             AND NOT EXISTS (
                 SELECT 1 FROM inbox AS i
                 WHERE (i.portal, i.chat, i.session) = (s.portal, s.chat, s.session)
                   AND i.processed = 0 AND i.kind IN ('human', 'closed', 'stop')
             )""",
        (*key, revision),
    ).fetchone()
    return result[0] if result is not None else None


def remember_crm(db, key, revision, version, route, plan):
    previous = db.execute(
        "SELECT fields, line, connector FROM crm_desired WHERE (portal, chat, session) = (?, ?, ?)", key,
    ).fetchone()
    if previous and previous[1:] != route:
        raise ValueError("Маршрут CRM для сессии изменился")
    fields = json.loads(previous[0]) if previous else {}
    fields.update(plan.crm_fields)
    db.execute(
        """INSERT INTO crm_desired (portal, chat, session, revision, version, line, connector, fields, create_type)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (portal, chat, session) DO UPDATE SET
               revision = excluded.revision, version = excluded.version, line = excluded.line,
               connector = excluded.connector, fields = excluded.fields, create_type = excluded.create_type,
               action_id = NULL, reason = NULL""",
        (*key, revision, version, *route, json.dumps(fields, ensure_ascii=False), plan.crm_create_type),
    )
    db.execute(
        """UPDATE outbox SET status = 'cancelled', reason = 'newer_crm_data'
           WHERE (portal, chat, session) = (?, ?, ?) AND kind IN ('crm_create', 'crm_update')
             AND status = 'planned'""",
        key,
    )


def schedule_crm(db, key, *, recover_cancelled=False):
    desired = db.execute(
        """SELECT d.id, d.revision, d.version, d.line, d.connector, d.fields, d.create_type, o.id, o.version
           FROM crm_desired AS d LEFT JOIN outbox AS o ON o.id = d.action_id
           WHERE (d.portal, d.chat, d.session) = (?, ?, ?) AND (
               d.action_id IS NULL OR (
                   ? AND o.kind = 'crm_update' AND o.status = 'cancelled' AND o.reason = 'local_state_changed'
                   AND (o.portal, o.chat, o.session, o.revision, o.line, o.connector) =
                       (d.portal, d.chat, d.session, d.revision, d.line, d.connector)
               )
           )""",
        (*key, recover_cancelled),
    ).fetchone()
    if desired is None:
        return
    desired_id, revision, version, line, connector, fields, create_type, cancelled_id, cancelled_version = desired
    current = session_version(db, key, revision)
    binding = crm_link(db, key)
    if cancelled_id is not None and (
        binding is None or current is None or current <= cancelled_version
        or not crm_route_unchanged(db, key, cancelled_version, line, connector)
    ):
        return
    reason = crm_wait_reason(db, key, current, binding, create_type)
    if reason:
        db.execute("UPDATE crm_desired SET reason = ? WHERE id = ?", (reason, desired_id))
        return
    params = {"fields": json.loads(fields), "entityTypeId": binding[0] if binding else create_type}
    if binding:
        params["id"] = binding[1]
    kind = "crm_update" if binding else "crm_create"
    # Одна новая попытка на одну отменённую запись, с сохранением истории.
    dedup = str(version) if cancelled_id is None else f"recovery:{cancelled_id}"
    action_id = insert_action(db, key, revision, current, (line, connector), kind, dedup, params)
    db.execute("UPDATE crm_desired SET action_id = ?, reason = NULL WHERE id = ?", (action_id, desired_id))


def crm_route_unchanged(db, key, previous_version, line, connector):
    return not db.execute(
        """SELECT EXISTS (
               SELECT 1 FROM inbox WHERE (portal, chat, session) = (?, ?, ?)
                 AND sequence > ? AND kind = 'message'
                 AND (json_extract(context, '$.line') IS NOT ? OR json_extract(context, '$.connector') IS NOT ?)
           )""",
        (*key, previous_version, line, connector),
    ).fetchone()[0]


def reconsider_crm(db, key):
    schedule_crm(db, key)
    active = db.execute(
        "SELECT session FROM chat_heads WHERE (portal, chat) = (?, ?)", key[:2],
    ).fetchone()
    if active and active[0] != key[2]:
        schedule_crm(db, (*key[:2], active[0]))


def crm_wait_reason(db, key, current, binding, create_type):
    if current is None:
        return "session_unavailable"
    unresolved = db.execute(
        """SELECT 1 FROM outbox WHERE (portal, chat) = (?, ?)
           AND status IN ('in_flight', 'api_accepted', 'uncertain')
           AND (kind = 'crm_create' OR (kind = 'crm_update' AND session = ?)) LIMIT 1""",
        key,
    ).fetchone()
    if unresolved:
        return "crm_attempt_unresolved"
    if binding:
        return None
    existing = db.execute(
        """SELECT 1 FROM crm_links WHERE (portal, chat) = (?, ?)
           UNION ALL
           SELECT 1 FROM outbox WHERE (portal, chat) = (?, ?)
             AND kind = 'crm_create' AND status != 'cancelled'
           LIMIT 1""",
        (*key[:2], *key[:2]),
    ).fetchone()
    if existing or not create_type:
        return "waiting_session_binding"
    return None


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
            if session_version(db, action.key, action.revision) != action.version:
                return False
            return db.execute(
                """SELECT EXISTS (
                       SELECT 1 FROM outbox AS o
                       WHERE o.id = ? AND o.status = 'in_flight'
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
                """UPDATE outbox SET status = ?, reason = ?, finished_at = ?
                   WHERE id = ? AND status = 'in_flight'""",
                (outcome.status, outcome.reason, int(time.time()), action.id),
            ).rowcount
            if changed and action.kind == "crm_create" and outcome.crm_item:
                if not record_crm_link(db, action.key, action.params["entityTypeId"], outcome.crm_item):
                    db.execute(
                        "UPDATE outbox SET status = 'uncertain', reason = 'crm_binding_conflict' WHERE id = ?",
                        (action.id,),
                    )
            if changed and action.kind == "crm_create" and outcome.status in ("not_sent", "cancelled"):
                if crm_link(db, action.key):
                    db.execute("UPDATE crm_desired SET action_id = NULL WHERE action_id = ?", (action.id,))
            if changed and action.kind in ("crm_create", "crm_update"):
                reconsider_crm(db, action.key)

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
            changed = db.execute(
                "UPDATE outbox SET status = 'confirmed', reason = NULL WHERE id = ?",
                (action_id,),
            ).rowcount == 1
            if changed and kind in ("crm_create", "crm_update"):
                reconsider_crm(db, (portal, chat, session))
            return changed

    def bind_crm(self, projection_id, entity_type, item):
        """Сохраняет независимо подтверждённую привязку карточки для выбранной сессии."""
        if any(type(value) is not int or value <= 0 for value in (entity_type, item)):
            return False
        with closing(self.store.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            key = db.execute(
                "SELECT portal, chat, session FROM crm_desired WHERE id = ?", (projection_id,),
            ).fetchone()
            if key is None or not record_crm_link(db, key, entity_type, item):
                return False
            # Привязка разрешает обновление вместо ещё не начатого создания.
            db.execute(
                """UPDATE outbox SET status = 'cancelled', reason = 'crm_binding_verified'
                   WHERE id = (SELECT action_id FROM crm_desired WHERE id = ?)
                     AND kind = 'crm_create' AND status = 'planned'""",
                (projection_id,),
            )
            db.execute(
                """UPDATE crm_desired SET action_id = NULL WHERE id = ? AND action_id IN (
                       SELECT id FROM outbox WHERE kind = 'crm_create'
                         AND (status = 'not_sent' OR (status = 'cancelled' AND reason = 'crm_binding_verified'))
                   )""",
                (projection_id,),
            )
            schedule_crm(db, key, recover_cancelled=True)
            return True

    def retry_handoff(self, failed_action_id):
        """Разрешает одну новую попытку по зафиксированной просьбе после доказанного отказа."""
        with closing(self.store.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            request = db.execute(
                """SELECT w.portal, w.chat, w.session, w.revision, w.version, w.line, w.connector,
                          o.status, o.reason, o.finished_at
                   FROM handoff_waiting AS w JOIN outbox AS o ON o.id = w.blocked_by
                   WHERE w.blocked_by = ? AND (w.portal, w.chat, w.session) = (o.portal, o.chat, o.session)
                     AND w.version > o.version""",
                (failed_action_id,),
            ).fetchone()
            if request is None:
                return False
            portal, chat, session, revision, requested, line, connector, status, reason, finished_at = request
            definitively_failed = status == "not_sent" and reason in HANDOFF_RETRY_REASONS
            cancelled_locally = status == "cancelled" and reason == "local_state_changed"
            if not (definitively_failed or cancelled_locally):
                return False
            if finished_at is None or int(time.time()) - finished_at < HANDOFF_COOLDOWN:
                return False
            key = portal, chat, session
            version = session_version(db, key, revision)
            if version is None:
                return False
            insert_action(
                db, key, revision, version, (line, connector), "handoff",
                f"retry:{failed_action_id}:{requested}", {},
            )
            db.execute("DELETE FROM handoff_waiting WHERE (portal, chat, session) = (?, ?, ?)", key)
            return True

    def review_queue(self):
        with closing(self.store.connect()) as db:
            rows = db.execute(
                """SELECT id, kind, status, reason FROM outbox
                   WHERE status IN ('uncertain', 'api_accepted')
                      OR (status = 'not_sent' AND reason != 'dry_run')
                   ORDER BY id"""
            )
            review = [dict(zip(("id", "kind", "status", "reason"), row)) for row in rows]
            review.extend(
                {"id": projection_id, "kind": "crm_projection", "status": "deferred", "reason": reason}
                for projection_id, reason in db.execute(
                    """SELECT d.id, coalesce(d.reason, o.reason, 'waiting_session_binding')
                       FROM crm_desired AS d LEFT JOIN outbox AS o ON o.id = d.action_id
                       WHERE d.action_id IS NULL OR o.status = 'cancelled'
                       ORDER BY d.id"""
                )
            )
            review.extend(
                {"id": blocker, "kind": "handoff_request", "status": "deferred", "reason": reason}
                for blocker, reason in db.execute(
                    """SELECT w.blocked_by, coalesce(o.reason, o.status)
                       FROM handoff_waiting AS w JOIN outbox AS o ON o.id = w.blocked_by
                       ORDER BY w.blocked_by"""
                )
            )
            return review
