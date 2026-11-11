"""Conference transfer, press 1, and voicemail must not count as an answer."""

from __future__ import annotations

import pytest

from voice.lines import LineContext
from voice.main import create_app
from voice.notify import TwilioRest
from voice.security import compute_signature, public_request_url
from voice.session import CallSession
from voice.settings import Settings
from voice.twiml import HOLD_MUSIC_URL, caller_hold_twiml, join_conference_twiml, whisper_twiml
from voice.tests.test_store_and_app import _settings


class _Response:
    def __init__(self, status: int, payload: object) -> None:
        self.status_code = status
        self._payload = payload

    def json(self) -> object:
        return self._payload


class _Twilio:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []

    def post(self, url: str, *, data: dict[str, str], auth: tuple[str, str], timeout: int) -> _Response:
        self.calls.append((url, dict(data)))
        return _Response(201, {"sid": "CAAGENT"})


def _session(**overrides: object) -> CallSession:
    session = CallSession(
        call_sid="CA123",
        caller_number="+14155550100",
        dialed_number="+15550001111",
        line=LineContext(id="anata", label="Anata"),
        transfer_started=True,
        holding_for_transfer=True,
        transfer_state="dialing",
        agent_call_sid="CAAGENT",
        caller_name="Jamie",
        reason="Saturday party",
    )
    for key, value in overrides.items():
        setattr(session, key, value)
    return session


def _post(client, path: str, form: dict[str, str], query: str = ""):
    url = public_request_url("https://voice.example.com", path, query)
    signature = compute_signature(url, list(form.items()), "12345")
    target = f"{path}?{query}" if query else path
    return client.post(target, data=form, headers={"X-Twilio-Signature": signature})


def test_caller_hears_hold_music_instead_of_ringing() -> None:
    xml = caller_hold_twiml(http_base="https://voice.example.com", call_sid="CA123")
    assert "answerOnBridge" not in xml
    assert "<Number" not in xml
    assert "Conference" in xml
    assert HOLD_MUSIC_URL in xml
    assert 'startConferenceOnEnter="false"' in xml
    assert "anata-CA123" in xml
    whisper = whisper_twiml("Anata transfer. Jamie. Press 1 to accept.", "https://voice.example.com/voice/accept?parent=CA123")
    assert "<Gather" in whisper
    assert 'timeout="25"' in whisper
    assert "Press 1 to accept" in whisper
    assert "<Hangup" in whisper
    joined = join_conference_twiml("CA123")
    assert 'startConferenceOnEnter="true"' in joined
    assert "anata-CA123" in joined


def test_outbound_dial_uses_press_1_and_machine_detection() -> None:
    settings = _settings()
    http = _Twilio()
    twilio = TwilioRest(settings, http=http)
    sid = twilio.create_call(
        to_number=settings.forward_to_number,
        from_number="+15550001111",
        url="https://voice.example.com/voice/whisper?parent=CA123",
        status_callback="https://voice.example.com/voice/agent-status?parent=CA123",
        amd_status_callback="https://voice.example.com/voice/agent-amd?parent=CA123",
        timeout=25,
    )
    assert sid == "CAAGENT"
    _url, data = http.calls[0]
    assert data["Timeout"] == "25"
    assert data["MachineDetection"] == "Enable"
    assert data["AsyncAmd"] == "true"
    assert "parent=CA123" in data["Url"]
    sent = twilio.send_sms(
        from_number="+15550001111",
        to_number=settings.notify_sms_number,
        body="hello",
        status_callback="https://voice.example.com/voice/sms-status?call_sid=CA123",
    )
    assert sent == "accepted" or sent == "CAAGENT"
    sms = http.calls[1][1]
    assert sms["StatusCallback"].endswith("call_sid=CA123")
    assert sms.get("Status") != "delivered"


def test_voicemail_returns_the_caller_and_is_not_a_successful_transfer() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = create_app(_settings())
    twilio = _Twilio()
    app.state.twilio.http = twilio
    session = _session()
    app.state.sessions.put(session)
    saved: dict[str, object] = {}

    def save_call(item: CallSession, *, sms_notified: bool) -> dict[str, str]:
        saved["sms"] = sms_notified
        saved["outcome"] = item.transfer_outcome
        return {"call_log": "saved", "inquiry_id": ""}

    app.state.store.save_call = save_call  # type: ignore[method-assign]
    client = TestClient(app)
    response = _post(
        client,
        "/voice/agent-amd",
        {"CallSid": "CAAGENT", "AnsweredBy": "machine_start"},
        "parent=CA123",
    )
    assert response.status_code == 200
    assert session.voicemail_detected is True
    assert session.transfer_outcome == "voicemail"
    assert session.transfer_accepted is False
    assert session.callback_needed is True
    assert session.resume == "message"
    assert session.action == "transfer_no_answer_message"
    bodies = [data for _url, data in twilio.calls]
    assert any(data.get("Status") == "completed" for data in bodies)
    assert any("Connect" in data.get("Twiml", "") for data in bodies)
    assert saved == {}

    missed = _post(
        client,
        "/voice/dial-result",
        {"CallSid": "CA123", "DialCallStatus": "completed", "DialCallDuration": "18", "CallStatus": "in-progress"},
    )
    assert missed.status_code == 200
    assert "<Connect" in missed.text
    assert session.transfer_outcome == "voicemail"
    assert session.transfer_accepted is False
    assert session.action != "transferred"


def test_hold_time_without_press_1_does_not_count_as_answered() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = create_app(_settings())
    app.state.twilio.http = _Twilio()
    session = _session()
    app.state.sessions.put(session)
    saved: dict[str, object] = {}

    def save_call(item: CallSession, *, sms_notified: bool) -> dict[str, str]:
        saved["sms"] = sms_notified
        saved["outcome"] = item.transfer_outcome
        saved["summary"] = item.summary
        return {"call_log": "saved", "inquiry_id": ""}

    app.state.store.save_call = save_call  # type: ignore[method-assign]
    app.state.twilio.send_sms = lambda **kwargs: kwargs["status_callback"] and "SM1"  # type: ignore[method-assign]
    client = TestClient(app)
    response = _post(
        client,
        "/voice/dial-result",
        {
            "CallSid": "CA123",
            "DialCallStatus": "completed",
            "DialCallDuration": "22",
            "CallStatus": "completed",
        },
    )
    assert response.status_code == 200
    assert session.transfer_outcome == "no_answer"
    assert session.transfer_outcome != "answered"
    assert session.action == "transfer_no_answer_message"
    assert session.callback_needed is True
    assert saved["sms"] is False
    assert saved["outcome"] == "no_answer"


def test_press_1_joins_the_conference_and_ends_as_answered() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = create_app(_settings())
    app.state.twilio.http = _Twilio()
    session = _session(summary="Jamie wants the Arena on Saturday.")
    app.state.sessions.put(session)
    saved: dict[str, object] = {}

    def save_call(item: CallSession, *, sms_notified: bool) -> dict[str, str]:
        saved["sms"] = sms_notified
        saved["transcript"] = item.transcript()
        saved["summary"] = item.summary
        return {"call_log": "saved", "inquiry_id": ""}

    app.state.store.save_call = save_call  # type: ignore[method-assign]
    app.state.twilio.send_sms = lambda **kwargs: "SM1"  # type: ignore[method-assign]
    client = TestClient(app)
    whisper = _post(client, "/voice/whisper", {"CallSid": "CAAGENT"}, "parent=CA123")
    assert whisper.status_code == 200
    assert "Press 1 to accept" in whisper.text
    assert "/voice/accept?parent=CA123" in whisper.text
    accepted = _post(client, "/voice/accept", {"CallSid": "CAAGENT", "Digits": "1"}, "parent=CA123")
    assert accepted.status_code == 200
    assert 'startConferenceOnEnter="true"' in accepted.text
    assert session.transfer_accepted is True
    finished = _post(
        client,
        "/voice/dial-result",
        {"CallSid": "CA123", "DialCallStatus": "completed", "DialCallDuration": "90", "CallStatus": "completed"},
    )
    assert finished.status_code == 200
    assert session.transfer_outcome == "answered"
    assert session.action == "transferred"
    assert session.callback_needed is False
    assert saved["sms"] is False
    assert saved["summary"] == "Jamie wants the Arena on Saturday."


def test_sms_status_keeps_undelivered_30034_false_and_marks_delivered_true() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = create_app(_settings())
    seen: list[tuple[str, bool]] = []

    def set_sms_notified(call_sid: str, notified: bool) -> None:
        seen.append((call_sid, notified))

    app.state.store.set_sms_notified = set_sms_notified  # type: ignore[method-assign]
    client = TestClient(app)
    failed = _post(
        client,
        "/voice/sms-status",
        {"MessageStatus": "undelivered", "ErrorCode": "30034", "MessageSid": "SM1"},
        "call_sid=CA123",
    )
    assert failed.status_code == 200
    delivered = _post(
        client,
        "/voice/sms-status",
        {"MessageStatus": "delivered", "ErrorCode": "", "MessageSid": "SM1"},
        "call_sid=CA123",
    )
    assert delivered.status_code == 200
    queued = _post(
        client,
        "/voice/sms-status",
        {"MessageStatus": "sent", "MessageSid": "SM1"},
        "call_sid=CA123",
    )
    assert queued.status_code == 200
    assert seen == [("CA123", False), ("CA123", True)]


def test_health_logging_ready_comes_from_the_privilege_probe() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = create_app(
        _settings(
            voice_database_url="postgresql://voice_logger:secret@pooler.example/postgres",
            supabase_url="",
            supabase_service_key="",
        )
    )

    class _Probe:
        def __init__(self, allowed: bool) -> None:
            self.allowed = allowed

        def cursor(self) -> "_Probe":
            return self

        def execute(self, sql: str, params: object = None) -> None:
            assert "has_table_privilege(current_user, 'public.call_log', 'SELECT')" in sql
            assert "has_column_privilege" in sql

        def fetchone(self) -> tuple[bool]:
            return (self.allowed,)

        def __enter__(self) -> "_Probe":
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    app.state.store.postgres._connect = lambda dsn: _Probe(False)
    client = TestClient(app)
    denied = client.get("/health")
    assert denied.status_code == 200
    assert denied.json()["logging_ready"] is False
    assert denied.json()["logging_probe"] == "denied"
    assert "secret" not in denied.text
    assert "VOICE_DATABASE_URL" not in denied.json()["missing"]
    app.state.store.postgres._probe_ok = False
    app.state.store.postgres._connect = lambda dsn: _Probe(True)
    ready = client.get("/health")
    assert ready.json()["logging_ready"] is True
    assert ready.json()["logging_probe"] == "ok"
