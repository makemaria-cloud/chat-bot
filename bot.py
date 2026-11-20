import argparse
import json
import os
import sqlite3
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

KINDS = {"message", "human", "closed", "stop"}
BLOCKED = {"HUMAN_ACTIVE", "CLOSED", "DO_NOT_CONTACT"}
MIGRATIONS = ("001.sql", "002.sql", "003.sql")


class EventConflict(ValueError):
    """Несовместимые данные для уже принятого события."""


class Store:
    def __init__(self, path):
        self.path = path

    def connect(self):
        return sqlite3.connect(self.path, timeout=10)

    def migrate(self):
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if not 0 <= version <= len(MIGRATIONS):
                raise RuntimeError("Неизвестная версия схемы")
            directory = Path(__file__).with_name("migrations")
            for number, filename in enumerate(MIGRATIONS[version:], start=version + 1):
                statement = ""
                for line in (directory / filename).read_text(encoding="utf-8").splitlines(keepends=True):
                    statement += line
                    if sqlite3.complete_statement(statement):
                        db.execute(statement)
                        statement = ""
                if statement.strip():
                    raise RuntimeError(f"Незавершённая миграция {filename}")
                db.execute(f"PRAGMA user_version = {number}")

    def accept(self, event, *, context=None, head=None):
        keys = ("portal", "chat", "session", "message", "kind")
        if not isinstance(event, dict) or set(event) - {"event_id"} != set(keys):
            raise ValueError("Неверный формат события")
        values = tuple(event[key] for key in keys)
        event_id = event.get("event_id", event["message"])
        if any(
            not isinstance(value, str) or not value.strip() or len(value) > 128
            for value in (*values, event_id)
        ):
            raise ValueError("Неверное значение поля")
        if event["kind"] not in KINDS:
            raise ValueError("Неизвестный тип события")
        portal, chat, session, message, kind = values
        saved_context = json.dumps(context, ensure_ascii=False, sort_keys=True) if context else None
        with closing(self.connect()) as db, db:
            if head is not None:
                self.save_head(db, portal, chat, head)
            result = db.execute(
                """INSERT INTO inbox (portal, chat, session, message, kind, event_id, context)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (portal, chat, session, kind, event_id) DO NOTHING""",
                (*values, event_id, saved_context),
            )
            if result.rowcount == 1:
                return True
            previous = db.execute(
                """SELECT message, context FROM inbox
                   WHERE (portal, chat, session, kind, event_id) = (?, ?, ?, ?, ?)""",
                (portal, chat, session, kind, event_id),
            ).fetchone()
            if previous != (message, saved_context):
                raise EventConflict("Данные события отличаются от сохранённых")
            return False

    def save_head(self, db, portal, chat, head):
        session, revision, owner = head
        db.execute(
            """INSERT INTO chat_heads (portal, chat, session, revision, owner) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT (portal, chat) DO UPDATE SET
                   session = excluded.session, revision = excluded.revision, owner = excluded.owner
               WHERE excluded.revision > chat_heads.revision""",
            (portal, chat, session, revision, owner),
        )
        previous = db.execute(
            "SELECT session, revision, owner FROM chat_heads WHERE (portal, chat) = (?, ?)",
            (portal, chat),
        ).fetchone()
        if previous[1] == revision and previous != head:
            raise EventConflict("Несовместимые данные об актуальной сессии")

    def process_one(self):
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """SELECT sequence, portal, chat, session, kind, context FROM inbox
                   WHERE processed = 0 ORDER BY sequence LIMIT 1"""
            ).fetchone()
            if row is None:
                return False
            sequence, portal, chat, session, kind, context = row
            key = (portal, chat, session)
            if context is not None and kind == "message":
                current = db.execute(
                    "SELECT session, owner FROM chat_heads WHERE (portal, chat) = (?, ?)", (portal, chat)
                ).fetchone()
                if current != (session, "bot"):
                    db.execute("UPDATE inbox SET processed = 1 WHERE sequence = ?", (sequence,))
                    return True
            previous = db.execute(
                "SELECT state FROM sessions WHERE (portal, chat, session) = (?, ?, ?)", key
            ).fetchone()
            state = transition(previous[0] if previous else "NEW", kind)
            db.execute(
                """INSERT INTO sessions VALUES (?, ?, ?, ?)
                   ON CONFLICT (portal, chat, session) DO UPDATE SET state = excluded.state""",
                (*key, state),
            )
            db.execute("UPDATE inbox SET processed = 1 WHERE sequence = ?", (sequence,))
            return True

    def pending(self):
        with closing(self.connect()) as db:
            return db.execute("SELECT COUNT(*) FROM inbox WHERE processed = 0").fetchone()[0]


def transition(state, kind):
    if state == "DO_NOT_CONTACT" or kind == "stop":
        return "DO_NOT_CONTACT"
    if state == "CLOSED" or kind == "closed":
        return "CLOSED"
    if kind == "human":
        return "HUMAN_ACTIVE"
    return state if state in BLOCKED else "QUALIFYING"


def replay(store, lines):
    accepted = sum(store.accept(json.loads(line)) for line in lines if line.strip())
    processed = 0
    while store.process_one():
        processed += 1
    return {"accepted": accepted, "processed": processed}


def make_server(store, port, webhook=None):
    class Handler(BaseHTTPRequestHandler):
        timeout = 5

        def do_POST(self):
            if self.path != "/bitrix24/events":
                self.respond(404, {"status": "not_found"})
                return
            if webhook is None:
                self.respond(503, {"status": "unavailable"})
                return
            try:
                status = webhook.receive(self.headers, self.rfile)
            except EventConflict:
                status = 409
            except sqlite3.Error:
                status = 503
            self.respond(status, {"status": "ok" if status == 200 else "rejected"})

        def do_GET(self):
            if self.path == "/health":
                self.respond(200, {"status": "ok"})
            elif self.path == "/ready":
                try:
                    self.respond(200, {"status": "ok", "pending": store.pending()})
                except sqlite3.Error:
                    self.respond(503, {"status": "unavailable"})
            else:
                self.respond(404, {"status": "not_found"})

        def respond(self, status, result):
            payload = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):
            # Путь запроса может содержать данные, которые нельзя писать в лог.
            pass

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def serve(store, port, webhook=None):
    with make_server(store, port, webhook) as server:
        server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Локальный контур BOT-001")
    parser.add_argument("--db", default=os.environ.get("BOT_DB", "bot.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    replay_command = commands.add_parser("replay")
    replay_command.add_argument("file", type=Path)
    serve_command = commands.add_parser("serve")
    serve_command.add_argument("--port", type=int, default=8080)
    serve_command.add_argument("--bitrix-fixture", type=Path)
    worker_command = commands.add_parser("worker")
    worker_command.add_argument("--once", action="store_true")
    args = parser.parse_args()
    store = Store(args.db)
    store.migrate()
    if args.command == "replay":
        with args.file.open(encoding="utf-8") as events:
            print(json.dumps(replay(store, events)))
    elif args.command == "serve":
        from bitrix import fixture_webhook

        webhook = fixture_webhook(store, args.bitrix_fixture) if args.bitrix_fixture else None
        serve(store, args.port, webhook)
    else:
        while True:
            if not store.process_one():
                if args.once:
                    break
                time.sleep(0.25)


if __name__ == "__main__":
    main()
