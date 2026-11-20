import json
import os
import re
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from outbox import Outcome

LIMIT = 65_536


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class BitrixRest:
    @classmethod
    def from_environment(cls, *, approved_test_scope, crm_fields=None, crm_create_types=()):
        return cls(
            os.environ.get("BOT_BITRIX_REST_ROOT", ""),
            approved_test_scope=approved_test_scope,
            crm_fields=crm_fields,
            crm_create_types=crm_create_types,
        )

    def __init__(self, rest_root, *, approved_test_scope, crm_fields=None, crm_create_types=()):
        portal, line, connector = approved_test_scope
        url = urlsplit(rest_root)
        if (
            not re.fullmatch(r"[a-z0-9-]+(?:\.[a-z0-9-]+)+", portal) or not line or not connector
            or url.scheme != "https" or url.netloc != portal
            or url.query or url.fragment
            or not re.fullmatch(r"/rest/[1-9][0-9]*/[a-zA-Z0-9_-]+/?", url.path)
        ):
            raise ValueError("REST-адрес должен принадлежать разрешённому тестовому порталу")
        self.root = rest_root.rstrip("/")
        self.scope = approved_test_scope
        self.crm_fields = crm_fields or {}
        self.crm_create_types = frozenset(crm_create_types)
        self.opener = build_opener(NoRedirect())

    def execute(self, action):
        if action.scope != self.scope:
            return Outcome("not_sent", "scope_denied")
        if action.kind == "message":
            method = "imopenlines.bot.session.message.send"
            params = {"CHAT_ID": int(action.chat), "NAME": "DEFAULT", "MESSAGE": action.params["text"]}
        elif action.kind == "handoff":
            method = "imopenlines.bot.session.operator"
            params = {"CHAT_ID": int(action.chat)}
        elif action.kind in {"crm_create", "crm_update"}:
            params = action.params
            entity_type = params["entityTypeId"]
            approved = self.crm_fields.get(entity_type, ())
            if not params["fields"] or not set(params["fields"]).issubset(approved):
                return Outcome("not_sent", "crm_mapping_unapproved")
            if action.kind == "crm_create" and entity_type not in self.crm_create_types:
                return Outcome("not_sent", "crm_creation_unapproved")
            method = "crm.item.add" if action.kind == "crm_create" else "crm.item.update"
        else:
            return Outcome("not_sent", "unsupported_action")
        return self.request(method, params, action.kind)

    def request(self, method, params, kind):
        data = json.dumps(params, ensure_ascii=False).encode("utf-8")
        request = Request(
            f"{self.root}/{method}", data=data, headers={"Content-Type": "application/json"},
        )
        try:
            with self.opener.open(request, timeout=5) as response:
                body = response.read(LIMIT + 1)
        except HTTPError as error:
            error.close()
            if error.code in {401, 403}:
                return Outcome("not_sent", "permission_denied")
            if error.code == 429:
                return Outcome("not_sent", "rate_limited")
            return Outcome("uncertain", "api_unavailable" if error.code >= 500 else "unexpected_http")
        except TimeoutError:
            return Outcome("uncertain", "timeout")
        except URLError as error:
            return Outcome("uncertain", "timeout" if isinstance(error.reason, TimeoutError) else "api_unavailable")
        except (OSError, HTTPException):
            return Outcome("uncertain", "api_unavailable")
        try:
            response = json.loads(body) if len(body) <= LIMIT else None
        except (ValueError, RecursionError):
            response = None
        return interpret_response(kind, params, response)


def interpret_response(kind, params, response):
    if not isinstance(response, dict):
        return Outcome("uncertain", "invalid_result")
    if "error" in response:
        code = response["error"]
        if code in ("ACCESS_DENIED", "ACCESS_ERROR", "insufficient_scope", "invalid_token"):
            return Outcome("not_sent", "permission_denied")
        if code in ("QUERY_LIMIT_EXCEEDED", "OPERATION_TIME_LIMIT"):
            return Outcome("not_sent", "rate_limited")
        return Outcome("uncertain", "unverified_api_error")
    result = response.get("result")
    if kind in ("message", "handoff"):
        if result is True:
            return Outcome("api_accepted")
        if result is False:
            return Outcome("not_sent", "rejected_by_api")
    elif isinstance(result, dict) and isinstance(result.get("item"), dict):
        item = result["item"].get("id")
        if type(item) is int and item > 0:
            if kind == "crm_create":
                return Outcome("confirmed", crm_item=item)
            if item == params["id"]:
                return Outcome("confirmed")
    return Outcome("uncertain", "invalid_result")
