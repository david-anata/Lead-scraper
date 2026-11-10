"""Reply webhook auth, dedupe, background handling, and idempotent registration."""

import hashlib
import hmac
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

import abby
import abby_runner
from sales_support_agent.api import instantly_abby_webhook as webhook_api
from sales_support_agent.api.abby_job import router as abby_router
from sales_support_agent.services.instantly_abby_webhook import (
    TARGET_URL,
    WEBHOOK_HEADER,
    _marker_due,
    derived_webhook_secret,
    process_reply,
    register_abby_webhooks,
    resolve_internal_api_key,
    webhook_authorized,
)

NOW = datetime(2026, 10, 27, 16, 0, tzinfo=timezone.utc)
LINK = abby.BOOKING_LINK


def test_secret_is_hmac_of_the_internal_key_and_accepts_a_bearer_prefix():
    secret = derived_webhook_secret("internal-key")
    assert secret == hmac.new(b"internal-key", b"instantly-webhook", hashlib.sha256).hexdigest()
    assert webhook_authorized(secret, "internal-key")
    assert webhook_authorized(f"Bearer {secret}", "internal-key")
    assert webhook_authorized(f'"{secret}"', "internal-key")
    assert not webhook_authorized("nope", "internal-key")
    assert not webhook_authorized(secret, "")
    assert not webhook_authorized("", "internal-key")


def _app():
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal-key")
    app.include_router(webhook_api.webhook_router)
    app.include_router(webhook_api.job_router)
    app.include_router(abby_router)
    return TestClient(app)


def _headers():
    return {WEBHOOK_HEADER: derived_webhook_secret("internal-key")}


def test_webhook_rejects_a_missing_secret_and_ignores_other_events():
    client = _app()
    denied = client.post("/api/webhooks/instantly/reply", json={"event_type": "reply_received", "email_id": "e1"})
    assert denied.status_code == 401
    ignored = client.post(
        "/api/webhooks/instantly/reply",
        headers=_headers(),
        json={"event_type": "email_sent", "email_id": "e1"},
    )
    assert ignored.status_code == 200
    assert ignored.json()["reason"] == "event_type"


def test_webhook_returns_200_and_the_background_task_sees_the_payload(monkeypatch):
    seen = []
    monkeypatch.setattr(webhook_api, "_settled", lambda _email_id: False)
    monkeypatch.setattr(webhook_api, "_run_reply", seen.append)
    response = _app().post(
        "/api/webhooks/instantly/reply",
        headers=_headers(),
        json={"event_type": "reply_received", "email_id": "e1", "lead_email": "ann@huppy.com"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    assert seen == [{"event_type": "reply_received", "email_id": "e1", "lead_email": "ann@huppy.com"}]


def test_webhook_duplicate_does_not_process(monkeypatch):
    seen = []
    monkeypatch.setattr(webhook_api, "_settled", lambda _email_id: True)
    monkeypatch.setattr(webhook_api, "_run_reply", seen.append)
    response = _app().post(
        "/api/webhooks/instantly/reply",
        headers=_headers(),
        json={"event_type": "auto_reply_received", "email_id": "e1"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "duplicate"
    assert seen == []


class _Client(abby_runner.Instantly.__bases__[0] if False else object):
    def __init__(self):
        self.replies = []

    def thread(self, _thread_id):
        return []

    def reply(self, **kwargs):
        self.replies.append(kwargs)
        return 200, {"id": "sent-1"}

    def set_interest(self, lead, value, campaign_id):
        self._marked = getattr(self, "_marked", [])
        self._marked.append({
            "email": lead, "campaign": campaign_id, "lt_interest_status": value,
        })
        return 200

    def leads_for_email(self, email):
        return [row for row in getattr(self, "_marked", []) if row.get("email") == email]

    def block(self, _value):
        return 200

    def lead(self, _email):
        return {}


def test_process_reply_sends_in_live_mode_and_dedupes(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_CAMPAIGN_IDS", "camp")
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2020-01-01T00:00:00Z")
    engine = create_engine("sqlite://")
    client = _Client()
    payload = {
        "event_type": "reply_received",
        "email_id": "e1",
        "lead_email": "ann@huppy.com",
        "email_account": "abby@x.com",
        "reply_subject": "Idea",
        "reply_text": "yes please",
        "campaign_id": "camp",
        "timestamp": "2026-10-27T10:00:00Z",
    }
    decision = abby.Decision("interested", "reply", reply=f"Hi {LINK}", interest=1)
    first = process_reply(payload, engine=engine, client=client, now=NOW, think=lambda *_a, **_k: decision)
    second = process_reply(payload, engine=engine, client=client, now=NOW, think=lambda *_a, **_k: decision)
    assert first["status"] == "processed" and first["counts"]["replied"] == 1
    assert second["status"] == "duplicate"
    assert len(client.replies) == 1


def test_process_reply_does_not_send_when_mode_is_off(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "off")
    engine = create_engine("sqlite://")
    client = _Client()
    result = process_reply(
        {"event_type": "reply_received", "email_id": "e1", "lead_email": "ann@huppy.com", "campaign_id": "camp"},
        engine=engine, client=client, now=NOW,
    )
    assert result["reason"] == "ABBY_MODE is off"
    assert client.replies == []


def test_process_reply_ignores_an_unwatched_campaign(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.delenv("ABBY_CAMPAIGN_IDS", raising=False)
    engine = create_engine("sqlite://")
    result = process_reply(
        {"event_type": "reply_received", "email_id": "e9", "lead_email": "ann@huppy.com",
         "campaign_id": "not-a-watched-campaign"},
        engine=engine, client=_Client(), now=NOW,
    )
    assert result["reason"] == "campaign_not_watched"


def test_auto_reply_webhook_schedules_the_ooo_followup(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_CAMPAIGN_IDS", "camp")
    engine = create_engine("sqlite://")
    payload = {
        "event_type": "auto_reply_received",
        "email_id": "ooo-1",
        "lead_email": "ann@huppy.com",
        "email_account": "abby@x.com",
        "reply_subject": "Automatic reply",
        "reply_text": "I am out of the office until November 4.",
        "campaign_id": "camp",
    }
    result = process_reply(payload, engine=engine, client=_Client(), now=NOW)
    assert result["counts"]["ignored"] == 1
    with engine.connect() as conn:
        row = conn.execute(text("SELECT kind, due_at FROM abby_followups_scheduled")).fetchone()
    assert row[0] == "out_of_office" and "2026-11-05" in str(row[1])


class _Hooks:
    def __init__(self, items=None, fail_auto=True):
        self.hooks = list(items or [])
        self.created = []
        self.fail_auto = fail_auto

    def list_webhooks(self):
        return 200, {"items": self.hooks}

    def create_webhook(self, payload):
        if self.fail_auto and payload["event_type"] == "auto_reply_received":
            return 403, {"message": "Webhooks are not included in your plan"}
        item = {"id": f"wh-{payload['event_type']}", "target_hook_url": payload["target_hook_url"],
                "event_type": payload["event_type"], "headers": payload["headers"]}
        self.hooks.append(item)
        self.created.append(payload)
        return 200, item


def test_registration_is_idempotent_and_keeps_the_plan_error():
    hooks = _Hooks()
    first = register_abby_webhooks(hooks, "secret-value")
    assert first["ok"] is False
    assert first["created"] == [{"id": "wh-reply_received", "event_type": "reply_received", "status": 200}]
    assert first["errors"] == [{
        "event_type": "auto_reply_received",
        "status": 403,
        "body": {"message": "Webhooks are not included in your plan"},
    }]
    assert "secret-value" not in json.dumps(first)
    assert hooks.created[0]["headers"][WEBHOOK_HEADER] == "secret-value"
    assert hooks.created[0]["target_hook_url"] == TARGET_URL

    second = register_abby_webhooks(hooks, "secret-value")
    assert [item["event_type"] for item in second["existing"]] == ["reply_received"]
    assert len(hooks.created) == 1
    assert second["errors"][0]["body"]["message"] == "Webhooks are not included in your plan"


def test_registration_does_not_create_when_the_list_call_fails():
    class ListFails:
        def list_webhooks(self):
            return 402, {"error": "Upgrade your plan to use webhooks"}

        def create_webhook(self, _payload):
            raise AssertionError("create must not run when listing fails")

    result = register_abby_webhooks(ListFails(), "secret-value")
    assert result["ok"] is False
    assert result["error"] == {"error": "Upgrade your plan to use webhooks"}
    assert result["status"] == 402


def test_scheduler_routes_require_the_job_guard(monkeypatch):
    monkeypatch.delenv("INSTANTLY_API_KEY", raising=False)
    monkeypatch.delenv("INSTANTLY_AI", raising=False)
    client = _app()
    assert client.post("/api/jobs/instantly/register-webhook").status_code == 401
    assert client.post("/api/jobs/abby/followups").status_code == 401
    allowed = client.post(
        "/api/jobs/instantly/register-webhook",
        headers={"X-Internal-Api-Key": "internal-key"},
    )
    assert allowed.status_code == 200
    assert allowed.json()["registration"]["reason"] == "missing key"


def test_handler_and_registration_resolve_the_same_internal_key(monkeypatch):
    monkeypatch.setenv("SALES_AGENT_INTERNAL_API_KEY", "from-env")
    settings = SimpleNamespace(internal_api_key="from-settings")
    assert resolve_internal_api_key(settings) == "from-settings"
    assert resolve_internal_api_key(SimpleNamespace(internal_api_key="")) == "from-env"
    assert derived_webhook_secret(resolve_internal_api_key(settings)) == derived_webhook_secret("from-settings")


def test_authenticated_odd_payloads_return_200_and_missing_auth_stays_401():
    client = _app()
    denied = client.post(
        "/api/webhooks/instantly/reply",
        content=b"not-json",
        headers={"content-type": "application/json"},
    )
    assert denied.status_code == 401
    assert denied.json()["detail"] == "Invalid webhook secret."
    ping = client.post(
        "/api/webhooks/instantly/reply",
        content=b"not-json",
        headers={**_headers(), "content-type": "application/json"},
    )
    assert ping.status_code == 200
    assert ping.json()["reason"] == "payload"
    unexpected = client.post(
        "/api/webhooks/instantly/reply",
        headers=_headers(),
        json={"event_type": "test", "message": "ping"},
    )
    assert unexpected.status_code == 200
    assert unexpected.json()["reason"] == "event_type"
    missing = client.post(
        "/api/webhooks/instantly/reply",
        headers=_headers(),
        json={"event_type": "reply_received"},
    )
    assert missing.status_code == 200
    assert missing.json()["reason"] == "missing_email_id"


def test_marker_is_due_when_the_derived_secret_changes():
    now = datetime(2026, 11, 2, 16, tzinfo=timezone.utc)
    raw = json.dumps({
        "ok": True,
        "checked_at": "2026-11-02T15:30:00+00:00",
        "secret_fingerprint": "old-fingerprint",
    })
    assert _marker_due(raw, now, "new-fingerprint")
    assert not _marker_due(raw, now, "old-fingerprint")
    legacy = json.dumps({"ok": True, "checked_at": "2026-11-02T15:30:00+00:00"})
    assert _marker_due(legacy, now, "current-fingerprint")


def test_registration_refuses_an_empty_secret_without_calling_instantly():
    class Boom:
        def list_webhooks(self):
            raise AssertionError("must not list")

        def create_webhook(self, _payload):
            raise AssertionError("must not create")

    result = register_abby_webhooks(Boom(), "  ")
    assert result["ok"] is False
    assert result["created"] == []


_ENUM_REJECTION = {
    "statusCode": 400,
    "error": "Bad Request",
    "message": "body/event_type must be equal to one of the allowed values",
}


class _RepairHooks:
    def __init__(self, hook):
        self.hooks = [hook]
        self.calls = []

    def list_webhooks(self):
        return 200, {"items": self.hooks}

    def create_webhook(self, payload):
        self.calls.append(("create", payload["event_type"]))
        if payload["event_type"] == "auto_reply_received":
            return 400, dict(_ENUM_REJECTION)
        raise AssertionError("reply_received already exists")

    def patch_webhook(self, webhook_id, payload):
        self.calls.append(("patch", webhook_id, payload["headers"][WEBHOOK_HEADER]))
        self.hooks[0]["headers"] = payload["headers"]
        body = dict(self.hooks[0])
        return 200, body

    def resume_webhook(self, webhook_id):
        self.calls.append(("resume", webhook_id))
        self.hooks[0]["status"] = 1
        return 200, dict(self.hooks[0])


def _reply_hook(**extra):
    hook = {
        "id": "wh-reply",
        "target_hook_url": TARGET_URL,
        "event_type": "reply_received",
        "headers": {WEBHOOK_HEADER: "current-secret"},
        "status": 1,
    }
    hook.update(extra)
    return hook


def test_registration_patches_a_stale_header_and_resumes_a_disabled_webhook():
    hooks = _RepairHooks(_reply_hook(headers={WEBHOOK_HEADER: "stale-secret"}, status=-1))
    result = register_abby_webhooks(hooks, "current-secret")
    assert result["ok"] is True
    assert result["created"] == []
    assert result["existing"] == []
    assert result["updated"] == [{
        "id": "wh-reply",
        "event_type": "reply_received",
        "target_hook_url": TARGET_URL,
        "header_updated": True,
        "resumed": True,
    }]
    assert result["unsupported"][0]["event_type"] == "auto_reply_received"
    assert result["errors"] == []
    assert hooks.calls[:2] == [("patch", "wh-reply", "current-secret"), ("resume", "wh-reply")]
    assert "current-secret" not in json.dumps(result)
    assert "stale-secret" not in json.dumps(result)


def test_registration_does_not_resume_when_the_header_patch_fails():
    class FailPatch(_RepairHooks):
        def patch_webhook(self, webhook_id, payload):
            self.calls.append(("patch", webhook_id, payload["headers"][WEBHOOK_HEADER]))
            return 500, {"message": "nope"}

        def resume_webhook(self, webhook_id):
            raise AssertionError("must not resume a hook whose header is still stale")

    hooks = FailPatch(_reply_hook(headers={WEBHOOK_HEADER: "stale-secret"}, status=-1))
    result = register_abby_webhooks(hooks, "current-secret")
    assert result["ok"] is False
    assert result["updated"] == []
    assert result["errors"][0]["event_type"] == "reply_received"
    assert result["errors"][0]["status"] == 500
    assert hooks.calls[0] == ("patch", "wh-reply", "current-secret")
    assert "resume" not in [call[0] for call in hooks.calls]
    assert "current-secret" not in json.dumps(result)


def test_registration_resumes_a_disabled_hook_whose_header_already_matches():
    hooks = _RepairHooks(_reply_hook(status=-1))
    result = register_abby_webhooks(hooks, "current-secret")
    assert result["ok"] is True
    assert result["updated"] == [{
        "id": "wh-reply",
        "event_type": "reply_received",
        "target_hook_url": TARGET_URL,
        "resumed": True,
    }]
    assert ("patch", "wh-reply", "current-secret") not in hooks.calls
    assert ("resume", "wh-reply") in hooks.calls


def test_registration_patches_when_the_stored_header_is_missing_and_keeps_case_differences():
    missing = _RepairHooks(_reply_hook(headers=None, status=1))
    patched = register_abby_webhooks(missing, "current-secret")
    assert patched["updated"][0]["header_updated"] is True
    assert "resumed" not in patched["updated"][0]

    cased = _RepairHooks(_reply_hook(headers={"x-instantly-webhook-secret": "current-secret"}, status=1))
    kept = register_abby_webhooks(cased, "current-secret")
    assert kept["existing"][0]["event_type"] == "reply_received"
    assert kept["updated"] == []
    assert "patch" not in [call[0] for call in cased.calls]


def test_registration_patches_an_active_hook_without_resuming_it():
    hooks = _RepairHooks(_reply_hook(headers={WEBHOOK_HEADER: "stale-secret"}, status=1))
    result = register_abby_webhooks(hooks, "current-secret")
    assert result["ok"] is True
    assert result["updated"][0]["header_updated"] is True
    assert "resumed" not in result["updated"][0]
    assert [call[0] for call in hooks.calls] == ["patch", "create"]


def test_unsupported_auto_reply_event_does_not_fail_a_healthy_reply_hook():
    class Hooks:
        def list_webhooks(self):
            return 200, {"items": []}

        def create_webhook(self, payload):
            if payload["event_type"] == "auto_reply_received":
                return 400, dict(_ENUM_REJECTION)
            assert payload["headers"][WEBHOOK_HEADER] == "secret-value"
            return 200, {"id": "wh-reply", "event_type": payload["event_type"]}

    result = register_abby_webhooks(Hooks(), "secret-value")
    assert result["ok"] is True
    assert result["created"] == [{"id": "wh-reply", "event_type": "reply_received", "status": 200}]
    assert result["errors"] == []
    assert result["unsupported"][0]["status"] == 400
    assert "secret-value" not in json.dumps(result)
