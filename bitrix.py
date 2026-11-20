import hmac
import json
import os
import re
import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from urllib.parse import parse_qs

MAX_BODY = 32_768
MAX_FIELDS = 256
MAX_TEXT = 8_192
MESSAGE_EVENT = "ONIMBOTV2MESSAGEADD"


@dataclass(frozen=True)
class MessageBinding:
    """Данные из доверенного источника о сообщении и текущей сессии чата."""

    line: str
    connector: str
    session: str
    author: str
    role: str
    head: tuple[str, int, str]


class Webhook:
    def __init__(self, store, domain, bot_id, bot_token, routes, resolve, request_limit=120):
        if not domain or not bot_id or not bot_token or not routes or request_limit < 1:
            raise ValueError("Не настроен обязательный параметр вебхука")
        self.store = store
        self.domain = domain.lower()
        self.bot_id = bot_id
        self.token = ("custom" + bot_token).encode("utf-8")
        self.routes = frozenset(routes)
        self.resolve = resolve
        self.request_limit = request_limit
        self.requests = deque()
        self.lock = Lock()

    def allow_request(self):
        with self.lock:
            now = time.monotonic()
            while self.requests and self.requests[0] <= now - 60:
                self.requests.popleft()
            if len(self.requests) >= self.request_limit:
                return False
            self.requests.append(now)
            return True

    def receive(self, headers, stream):
        if not self.allow_request():
            return 429
        if headers.get_content_type() != "application/x-www-form-urlencoded":
            return 415
        if headers.get_all("Transfer-Encoding"):
            return 400
        lengths = headers.get_all("Content-Length", [])
        if len(lengths) != 1 or not re.fullmatch(r"[0-9]{1,10}", lengths[0]):
            return 400
        length = int(lengths[0])
        if length > MAX_BODY:
            return 413
        try:
            body = stream.read(length)
        except TimeoutError:
            return 408
        if len(body) != length:
            return 400
        return self.accept_form(body)

    def accept_form(self, body):
        try:
            raw = body.decode("utf-8")
            if re.search(r"%(?![0-9A-Fa-f]{2})", raw):
                return 400
            form = parse_qs(raw, keep_blank_values=True, errors="strict", max_num_fields=MAX_FIELDS)
        except ValueError:
            return 400
        try:
            return self.accept_fields(form)
        except InvalidField:
            return 400

    def accept_fields(self, form):
        token = field(form, "auth[application_token]")
        if not hmac.compare_digest(token.encode("utf-8"), self.token):
            return 403
        if field(form, "auth[domain]").lower() != self.domain:
            return 403
        if field(form, "event") != MESSAGE_EVENT:
            return 200
        if identifier(form, "data[bot][id]") != self.bot_id:
            return 403
        if field(form, "data[chat][entityType]") != "LINES":
            return 200
        chat = identifier(form, "data[chat][id]")
        if field(form, "data[message][chatId]") not in ("", chat):
            return 400
        author = identifier(form, "data[message][authorId]", allow_zero=True)
        system = field(form, "data[message][isSystem]").lower()
        if system not in {"", "true", "false", "1", "0", "y", "n"}:
            return 400
        if author in {"0", self.bot_id} or system in {"true", "1", "y"}:
            return 200
        message = identifier(form, "data[message][id]")
        text = field(form, "data[message][text]")
        if len(text) > MAX_TEXT or "\x00" in text:
            return 400
        binding = self.resolve(self.domain, chat, message)
        if binding is None:
            return 403
        if (binding.line, binding.connector) not in self.routes or binding.author != author:
            return 403
        kind = {"client": "message", "operator": "human"}.get(binding.role)
        if kind is None:
            return 200
        event = dict(
            portal=self.domain, chat=chat, session=binding.session,
            message=message, kind=kind, event_id=message,
        )
        context = dict(
            source="imbot.v2", role=binding.role, line=binding.line, connector=binding.connector,
            text=text if kind == "message" else "",
        )
        self.store.accept(event, context=context, head=binding.head)
        return 200


class InvalidField(ValueError):
    """Неверное поле формы."""


def field(form, name):
    values = form.get(name, [""])
    if len(values) != 1:
        raise InvalidField("Повтор скалярного поля")
    return values[0]


def identifier(form, name, *, allow_zero=False):
    value = field(form, name)
    if not re.fullmatch(r"[1-9][0-9]{0,18}", value) and not (allow_zero and value == "0"):
        raise InvalidField("Неверный идентификатор")
    return value


def fixture_webhook(store, path):
    """Подключает только локальный заранее проверенный набор синтетических данных."""
    fixture = json.loads(path.read_text(encoding="utf-8"))

    def resolve(portal, chat, message):
        space = fixture["chats"].get(chat)
        record = space["messages"].get(message) if space else None
        if record is None:
            return None
        return MessageBinding(
            **record, line=space["line"], connector=space["connector"], head=tuple(space["head"]),
        )

    return Webhook(
        store, fixture["domain"], fixture["bot_id"], os.environ.get("BOT_BITRIX_TOKEN", ""),
        [tuple(route) for route in fixture["routes"]], resolve,
    )
