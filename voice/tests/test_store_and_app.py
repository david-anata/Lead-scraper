"""Logging writes and the webhook shell. No network."""

from __future__ import annotations

import pytest

from voice.leads import SupabaseStore
from voice.lines import LineContext
from voice.main import create_app
from voice.security import compute_signature, public_request_url
from voice.session import CallSession
from voice.settings import Settings


class _Response:
    def __init__(self, status: int, payload: object) -> None:
        self.status_code = status
        self._payload = payload

    def json(self) -> object:
        return self._payload


class _Http:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def post(self, url: str, *, json: object, headers: dict[str, str], timeout: int) -> _Response:
        assert headers["apikey"] == "service-key"
        assert "Bearer service-key" == headers["Authorization"]
        self.calls.append((url, json))
        return _Response(201, [json] if isinstance(json, dict) else [])

    def get(self, url: str, *, headers: dict[str, str], timeout: int) -> _Response:
        self.calls.append((url, None))
        return _Response(200, [])

    def patch(self, url: str, *, json: object, headers: dict[str, str], timeout: int) -> _Response:
        self.calls.append((url, json))
        return _Response(200, [])


def _settings(**overrides: object) -> Settings:
    values = dict(
        xai_api_key="test-key",
        xai_model="grok-voice-latest",
        xai_voice="eve",
        xai_realtime_url="wss://api.x.ai/v1/realtime",
        twilio_account_sid="AC123",
        twilio_auth_token="12345",
        twilio_messaging_service_sid="",
        forward_to_number="+14155550199",
        notify_sms_number="+14155550198",
        public_base_url="https://voice.example.com",
        render_external_url="",
        supabase_url="https://example.supabase.co",
        supabase_service_key="service-key",
        calendar_id="calendar@group.calendar.google.com",
        google_service_account_json="",
        google_delegated_subject="",
        line_contexts_json="",
        known_caller_numbers="",
        lead_owner="building-operator",
        sms_autoreply=False,
        recording_enabled=False,
        validate_signatures=True,
        max_call_seconds=720,
    )
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _lead() -> CallSession:
    return CallSession(
        call_sid="CA123",
        caller_number="+14155550100",
        dialed_number="+15550001111",
        line=LineContext(id="anata", label="Anata"),
        category="lead",
        caller_name="Jamie Lee",
        event_date="2026-12-01",
        event_or_interest_type="tour of the arena",
        summary="Asked for a tour.",
        action="message",
    )


def test_lead_is_written_to_call_log_and_building_inquiries() -> None:
    http = _Http()
    store = SupabaseStore("https://example.supabase.co", "service-key", http=http)
    result = store.save_call(_lead(), sms_notified=True)
    urls = [url for url, _ in http.calls]
    assert any("/rest/v1/building_inquiries" in url for url in urls)
    assert any("/rest/v1/call_log" in url for url in urls)
    assert result["call_log"] == "saved"
    assert result["inquiry_id"]


def test_spam_is_logged_and_does_not_create_an_inquiry() -> None:
    http = _Http()
    store = SupabaseStore("https://example.supabase.co", "service-key", http=http)
    session = _lead()
    session.category = "spam"
    session.action = "logged_only"
    store.save_call(session, sms_notified=False)
    urls = [url for url, _ in http.calls]
    assert not any("building_inquiries" in url for url in urls)
    assert any("call_log" in url for url in urls)


def test_inbound_webhook_rejects_a_bad_signature_and_accepts_a_good_one() -> None:
    httpx = pytest.importorskip("httpx")
    assert httpx
    from fastapi.testclient import TestClient

    app = create_app(_settings())
    client = TestClient(app)
    form = {
        "CallSid": "CA123",
        "From": "+14155550100",
        "To": "+15550001111",
        "CallStatus": "ringing",
    }
    bad = client.post("/voice/inbound", data=form, headers={"X-Twilio-Signature": "nope"})
    assert bad.status_code == 403
    url = public_request_url("https://voice.example.com", "/voice/inbound", "")
    signature = compute_signature(url, list(form.items()), "12345")
    good = client.post(
        "/voice/inbound",
        data=form,
        headers={"X-Twilio-Signature": signature},
    )
    assert good.status_code == 200
    assert 'url="wss://voice.example.com/voice/media"' in good.text
    assert "media?token=" not in good.text
    assert '<Parameter name="token" value="' in good.text
    assert "This call may be recorded" not in good.text
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["calls_ready"] is True
    assert "12345" not in health.text
    assert "+14155550199" not in health.text


def test_media_socket_rejects_a_start_without_a_valid_token() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    from starlette.websockets import WebSocketDisconnect

    app = create_app(_settings())
    client = TestClient(app)
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/voice/media") as socket:
            socket.send_json({"event": "connected"})
            socket.send_json(
                {
                    "event": "start",
                    "start": {
                        "callSid": "CA123",
                        "streamSid": "MZ1",
                        "customParameters": {"token": "not-a-real-token"},
                    },
                }
            )
            socket.receive_text()


def test_sms_autoreply_is_off_by_default_and_still_forwards() -> None:
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = create_app(_settings())
    sent: list[dict[str, str]] = []

    def capture(url: str, *, data: dict[str, str], auth: tuple[str, str], timeout: int) -> _Response:
        sent.append(data)
        return _Response(201, {})

    app.state.twilio.http = type("Client", (), {"post": staticmethod(capture)})()
    app.state.store.http = _Http()
    client = TestClient(app)
    form = {
        "MessageSid": "SM123",
        "From": "+14155550100",
        "To": "+15550001111",
        "Body": "How much is the Arena?",
    }
    url = public_request_url("https://voice.example.com", "/voice/sms", "")
    signature = compute_signature(url, list(form.items()), "12345")
    response = client.post("/voice/sms", data=form, headers={"X-Twilio-Signature": signature})
    assert response.status_code == 200
    assert "<Message>" not in response.text
    assert sent and sent[0]["To"] == "+14155550198"
    assert "How much is the Arena?" in sent[0]["Body"]
    assert "Reply sent: no" in sent[0]["Body"]
    assert app.state.store.http.calls
