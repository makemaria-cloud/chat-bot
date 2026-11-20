import argparse
import json
import os
import sqlite3
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

KINDS = {"message", "human", "closed", "stop"}
BLOCKED = {"HUMAN_ACTIVE", "CLOSED", "DO_NOT_CONTACT"}


class Store:
    def __init__(self, path):
        self.path = path

    def connect(self):
        return sqlite3.connect(self.path, timeout=10)

    def migrate(self):
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version == 1:
                return
            if version != 0:
                raise RuntimeError("Неизвестная версия схемы")
            migration = Path(__file__).with_name("migrations") / "001.sql"
            for statement in migration.read_text(encoding="utf-8").split(";"):
                if statement.strip():
                    db.execute(statement)
            db.execute("PRAGMA user_version = 1")

    def accept(self, event):
        keys = ("portal", "chat", "session", "message", "kind")
        if not isinstance(event, dict) or set(event) != set(keys):
            raise ValueError("Неверный формат события")
        values = tuple(event[key] for key in keys)
        if any(not isinstance(value, str) or not value.strip() or len(value) > 128 for value in values):
            raise ValueError("Неверное значение поля")
        if event["kind"] not in KINDS:
            raise ValueError("Неизвестный тип события")
        with closing(self.connect()) as db, db:
            result = db.execute(
                """INSERT INTO inbox (portal, chat, session, message, kind)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT (portal, chat, session, message) DO NOTHING""",
                values,
            )
            return result.rowcount == 1

    def process_one(self):
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """SELECT sequence, portal, chat, session, kind FROM inbox
                   WHERE processed = 0 ORDER BY sequence LIMIT 1"""
            ).fetchone()
            if row is None:
                return False
            sequence, portal, chat, session, kind = row
            key = (portal, chat, session)
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


def serve(store, port):
    class Handler(BaseHTTPRequestHandler):
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

    with ThreadingHTTPServer(("127.0.0.1", port), Handler) as server:
        server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Локальный контур BOT-001")
    parser.add_argument("--db", default=os.environ.get("BOT_DB", "bot.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    replay_command = commands.add_parser("replay")
    replay_command.add_argument("file", type=Path)
    serve_command = commands.add_parser("serve")
    serve_command.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    store = Store(args.db)
    store.migrate()
    if args.command == "replay":
        with args.file.open(encoding="utf-8") as events:
            print(json.dumps(replay(store, events)))
    else:
        serve(store, args.port)


if __name__ == "__main__":
    main()
