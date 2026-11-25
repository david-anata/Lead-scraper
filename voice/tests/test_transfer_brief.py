"""The line David hears, the text he reads, and the facts required first."""

from __future__ import annotations

import asyncio

import pytest

from voice.bridge import CallBridge, whisper_line
from voice.caller_index import build_directory_record, merge_learned
from voice.known_callers import apply_known_caller
from voice.lines import LineContext
from voice.main import create_app
from voice.security import compute_signature, public_request_url
from voice.session import CallSession
from voice.tools import execute_tool
from voice.transfer_brief import missing_transfer_facts, transfer_announcement
from voice.tests.test_store_and_app import _settings
from voice.twiml import whisper_twiml


def _session(**overrides: object) -> CallSession:
    session = CallSession(
        call_sid="CA123",
        caller_number="+14155550100",
        dialed_number="+13853363562",
        line=LineContext(id="anata", label="Anata"),
    )
    for key, value in overrides.items():
        setattr(session, key, value)
    return session


def _transfer(session: CallSession, **args: object) -> dict[str, object]:
    payload = {
        "category": "lead",
        "reason": "Booking a December party",
        "wants_live_person": True,
        "caller_asked_for_person": True,
    }
    payload.update(args)
    return execute_tool(
        "request_transfer",
        payload,
        session=session,
        settings=_settings(),
        calendar=None,  # type: ignore[arg-type]
        known_numbers=frozenset(),
    )


def test_whisper_and_sms_use_the_same_plain_sentence() -> None:
    session = _session(
        caller_name="Jane Smith",
        company="Acme Events",
        reason="Booking a December party",
    )
    sms = transfer_announcement(session)
    spoken = whisper_line(session)
    assert sms == "Call from Jane Smith with Acme Events about booking a December party."
    assert spoken == "Call from Jane Smith with Acme Events about booking a December party. Press 1 to accept."
    xml = whisper_twiml(spoken, "https://voice.example.com/voice/accept?parent=CA123")
    assert "Call from Jane Smith with Acme Events about booking a December party. Press 1 to accept." in xml
    assert "Press 1 to accept. Press 1" not in xml
    assert "Reason:" not in sms
    assert "Source:" not in sms
    assert len(sms) <= 160


def test_a_tenant_is_announced_without_asking_who_they_are() -> None:
    session = _session(reason="The heat is out")
    apply_known_caller(
        session,
        {"id": "k1", "name": "Jamie Lee", "company": "", "category": "tenant", "ring_policy": ""},
    )
    assert missing_transfer_facts(session) == []
    assert transfer_announcement(session) == "Call from Jamie Lee, tenant, about the heat is out."


def test_transfer_waits_for_the_three_facts_unless_they_refuse() -> None:
    waiting = _session()
    held = _transfer(waiting)
    assert held["transfer"] == "hold"
    assert held["missing"] == ["name", "affiliation"]
    assert waiting.transfer_requested is False

    ready = _session(caller_name="Jane Smith", company="Acme Events")
    approved = _transfer(ready)
    assert approved["transfer"] == "approved"
    assert ready.transfer_requested is True

    refused = _session()
    anyway = _transfer(refused, caller_refused_details=True, reason="They would not say")
    assert anyway["transfer"] == "approved"
    assert refused.transfer_requested is True
    assert transfer_announcement(refused).startswith("Call from Someone, personal, about")


def test_a_known_caller_is_only_missing_the_reason() -> None:
    tenant = _session()
    apply_known_caller(
        tenant,
        {"id": "k1", "name": "Jamie Lee", "company": "North", "category": "tenant", "ring_policy": ""},
    )
    assert missing_transfer_facts(tenant) == ["reason"]
    client = _session()
    apply_known_caller(
        client,
        {"id": "k2", "name": "Jamie Lee", "company": "North", "category": "client", "ring_policy": ""},
    )
    held = _transfer(client, reason="", category="other")
    assert held["transfer"] == "hold"
    assert held["missing"] == ["reason"]
    assert client.transfer_requested is False


def test_spam_still_does_not_transfer() -> None:
    session = _session(caller_name="Reina", company="Truework")
    result = _transfer(session, reason="Employment verification for David", caller_refused_details=True)
    assert result["transfer"] == "declined"
    assert result["category"] == "spam"
    assert session.transfer_requested is False


def test_auto_index_learns_a_lead_and_does_not_replace_a_manual_name() -> None:
    learned = build_directory_record(
        _session(
            caller_name="Cynthia Wren",
            company="Cascade Healthcare",
            category="lead",
            reason="Wants a callback about a December booking",
        )
    )
    assert learned is not None
    assert learned["category"] == "lead"
    assert learned["company"] == "Cascade Healthcare"
    fresh = merge_learned(None, learned)
    assert fresh["source"] == "auto"
    manual = merge_learned(
        {
            "phone": "+14155550100",
            "name": "Cynthia Wren",
            "company": "Cascade Healthcare",
            "category": "client",
            "source": "QuickBooks Online customer",
        },
        {**learned, "name": "Cindy", "category": "lead", "company": "Somewhere Else"},
    )
    assert manual["name"] == "Cynthia Wren"
    assert manual["category"] == "client"
    assert manual["company"] == "Cascade Healthcare"
    assert manual["source"] == "QuickBooks Online customer"
    assert manual["last_reason"] == "Wants a callback about a December booking"
    stub = merge_learned(
        {
            "phone": "+14155550100",
            "name": "",
            "company": "",
            "category": "unknown",
            "source": "inbound_call",
        },
        learned,
    )
    assert stub["source"] == "auto"
    assert stub["name"] == "Cynthia Wren"
    assert build_directory_record(_session(caller_name="Someone", category="lead")) is None
    assert build_directory_record(_session(caller_name="Reina", company="Truework", category="other")) is not None
    assert build_directory_record(_session(caller_name="Reina", company="Truework", category="other"))["category"] == "spam"


class _Twilio:
    def __init__(self) -> None:
        self.updates: list[str] = []
        self.calls: list[dict[str, str]] = []
        self.sms: list[dict[str, str]] = []

    def update_call(self, call_sid: str, twiml: str) -> bool:
        self.updates.append(twiml)
        return True

    def create_call(self, **kwargs: str) -> str:
        self.calls.append({key: str(value) for key, value in kwargs.items()})
        return "CAAGENT"

    def send_sms(self, **kwargs: str) -> str:
        self.sms.append({key: str(value) for key, value in kwargs.items()})
        return "SM1"


def test_starting_the_transfer_texts_the_announcement_and_rings_the_forward_number() -> None:
    settings = _settings()
    session = _session(
        caller_name="Jane Smith",
        company="Acme Events",
        reason="Booking a December party",
        transfer_requested=True,
    )
    twilio = _Twilio()
    bridge = CallBridge(
        settings=settings,
        session=session,
        store=None,  # type: ignore[arg-type]
        twilio=twilio,  # type: ignore[arg-type]
        calendar=None,  # type: ignore[arg-type]
        websocket=None,
    )
    asyncio.run(bridge._start_dial())
    assert twilio.calls[0]["to_number"] == settings.forward_to_number
    assert "/voice/whisper?parent=CA123" in twilio.calls[0]["url"]
    assert len(twilio.sms) == 1
    assert twilio.sms[0]["to_number"] == settings.notify_sms_number
    assert twilio.sms[0]["body"] == "Call from Jane Smith with Acme Events about booking a December party."
    assert "Press 1" not in twilio.sms[0]["body"]


def test_whisper_webhook_speaks_the_announcement() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = create_app(_settings())
    session = _session(
        caller_name="Jane Smith",
        company="Acme Events",
        reason="Booking a December party",
    )
    app.state.sessions.put(session)
    url = public_request_url("https://voice.example.com", "/voice/whisper", "parent=CA123")
    form = {"CallSid": "CAAGENT", "ParentCallSid": "CA123"}
    signature = compute_signature(url, list(form.items()), "12345")
    response = TestClient(app).post(
        "/voice/whisper?parent=CA123",
        data=form,
        headers={"X-Twilio-Signature": signature},
    )
    assert response.status_code == 200
    assert "Call from Jane Smith with Acme Events about booking a December party. Press 1 to accept." in response.text
