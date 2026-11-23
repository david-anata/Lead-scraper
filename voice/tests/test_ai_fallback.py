"""When the realtime session cannot start, ring David, then take a recording."""

from __future__ import annotations

import asyncio

import pytest

from voice.bridge import CallBridge
from voice.leads import build_call_row, ensure_call_summary
from voice.lines import LineContext
from voice.main import create_app
from voice.notify import SMS_SEGMENT, call_summary_text
from voice.security import compute_signature, public_request_url
from voice.session import CallSession
from voice.tests.test_store_and_app import _settings


class _Response:
    def __init__(self, status: int, payload: object) -> None:
        self.status_code = status
        self._payload = payload

    def json(self) -> object:
        return self._payload


class _ConnectFailure:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    async def __aenter__(self) -> None:
        raise self.exc

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _Twilio:
    def __init__(self, *, accept_update: bool = True) -> None:
        self.updates: list[str] = []
        self.sms: list[dict[str, str]] = []
        self.accept_update = accept_update
        self._updates_seen = 0

    def update_call(self, call_sid: str, twiml: str) -> bool:
        self.updates.append(twiml)
        self._updates_seen += 1
        if not self.accept_update and self._updates_seen == 1:
            return False
        return True

    def send_sms(self, **kwargs: str) -> str:
        self.sms.append({key: str(value) for key, value in kwargs.items()})
        return "SM1"


def _session() -> CallSession:
    return CallSession(
        call_sid="CA123",
        caller_number="+14155550100",
        dialed_number="+13853363562",
        line=LineContext(id="front_desk", label="Anata Building front desk"),
    )


def _bridge(twilio: _Twilio) -> CallBridge:
    return CallBridge(
        settings=_settings(),
        session=_session(),
        store=None,  # type: ignore[arg-type]
        twilio=twilio,  # type: ignore[arg-type]
        calendar=None,  # type: ignore[arg-type]
        websocket=None,
    )


@pytest.mark.parametrize(
    "exc",
    [
        OSError("server rejected WebSocket connection: HTTP 402"),
        TimeoutError("timed out"),
        ConnectionError("xAI realtime session failed"),
    ],
)
def test_a_failed_realtime_session_forwards_to_the_configured_cell(monkeypatch: pytest.MonkeyPatch, exc: Exception) -> None:
    monkeypatch.setattr("voice.bridge._connect_xai", lambda settings: _ConnectFailure(exc))
    twilio = _Twilio()
    bridge = _bridge(twilio)
    asyncio.run(bridge._xai_loop())
    assert twilio.sms == []
    assert len(twilio.updates) == 1
    xml = twilio.updates[0]
    assert "<Number>+14155550199</Number>" in xml
    assert 'action="https://voice.example.com/voice/ai-fallback?parent=CA123"' in xml
    assert "Sorry about that" not in xml
    assert "<Hangup" not in xml
    assert bridge.session.holding_for_transfer is True
    assert bridge.session.ai_fallback is True


def test_an_error_event_before_the_greeting_forwards_and_a_later_one_does_not() -> None:
    twilio = _Twilio()
    bridge = _bridge(twilio)
    asyncio.run(bridge._on_xai({"type": "error"}))
    assert "<Number>+14155550199</Number>" in twilio.updates[0]
    bridge._greeted = True
    before = len(twilio.updates)
    asyncio.run(bridge._on_xai({"type": "error"}))
    assert len(twilio.updates) == before


def test_a_rejected_forward_records_voicemail_and_texts_david_once() -> None:
    twilio = _Twilio(accept_update=False)
    bridge = _bridge(twilio)
    asyncio.run(bridge._forward_when_ai_down())
    assert "<Record" in twilio.updates[-1]
    assert len(twilio.sms) == 1
    body = twilio.sms[0]["body"]
    assert "415-555-0100" in body
    assert "Call back." in body
    assert len(body) <= SMS_SEGMENT
    assert twilio.sms[0]["to_number"] == "+14155550198"
    asyncio.run(bridge._text_missed_fallback())
    assert len(twilio.sms) == 1


def _post(client, path: str, form: dict[str, str], query: str = ""):
    url = public_request_url("https://voice.example.com", path, query)
    signature = compute_signature(url, list(form.items()), "12345")
    target = f"{path}?{query}" if query else path
    return client.post(target, data=form, headers={"X-Twilio-Signature": signature})


def test_unanswered_forward_records_and_texts_once_and_an_answer_does_not() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = create_app(_settings())
    sent: list[dict[str, str]] = []

    def capture(url: str, *, data: dict[str, str], auth: tuple[str, str], timeout: int) -> _Response:
        sent.append(dict(data))
        return _Response(201, {"sid": "SM1"})

    app.state.twilio.http = type("Client", (), {"post": staticmethod(capture)})()
    saved: list[CallSession] = []

    def save_call(item: CallSession, *, sms_notified: bool, create_inquiry: bool | None = None) -> dict[str, str]:
        saved.append(item)
        return {"call_log": "saved", "inquiry_id": ""}

    app.state.store.save_call = save_call  # type: ignore[method-assign]
    missed = _session()
    missed.ai_fallback = True
    missed.holding_for_transfer = True
    missed.transfer_state = "dialing"
    app.state.sessions.put(missed)
    response = _post(
        TestClient(app),
        "/voice/ai-fallback",
        {"CallSid": "CA123", "DialCallStatus": "no-answer", "CallStatus": "in-progress"},
        "parent=CA123",
    )
    assert response.status_code == 200
    assert "<Record" in response.text
    assert len(sent) == 1
    assert sent[0]["To"] == "+14155550198"
    assert "415-555-0100" in sent[0]["Body"]
    assert "Call back." in sent[0]["Body"]
    assert len(sent[0]["Body"]) <= SMS_SEGMENT
    assert saved and saved[0].action == "needs_callback"

    answered = CallSession(
        call_sid="CA999",
        caller_number="+14155550123",
        dialed_number="+13853363562",
        line=LineContext(id="front_desk", label="Anata Building"),
        ai_fallback=True,
        holding_for_transfer=True,
        transfer_state="dialing",
    )
    app.state.sessions.put(answered)
    before = len(sent)
    ok = _post(
        TestClient(app),
        "/voice/ai-fallback",
        {"CallSid": "CA999", "DialCallStatus": "completed", "CallStatus": "completed"},
        "parent=CA999",
    )
    assert ok.status_code == 200
    assert "<Record" not in ok.text
    assert len(sent) == before
    assert answered.action == "transferred"
    assert answered.fallback_sms_queued is True


def test_early_hangup_summary_uses_the_caller_transcript() -> None:
    session = _session()
    session.category = "other"
    session.add_turn("Anata", "Thanks for calling the Anata Building front desk.")
    session.add_turn("Caller", "I want the Arena for a birthday in June.")
    row = build_call_row(session, inquiry_id="", sms_notified=False)
    assert "birthday in June" in row["transcript_summary"]
    assert not row["transcript_summary"].endswith(": other")
    assert "+14155550100: other" not in row["transcript_summary"]
    ensure_call_summary(session)
    text = call_summary_text(session)
    assert "birthday" in text.casefold()
    assert "415-555-0100" in text
    assert len(text) <= SMS_SEGMENT
