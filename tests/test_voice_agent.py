import asyncio
import json
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.models.database import Base, _register_models, session_scope
from sales_support_agent.models.entities import BuildingAuditEvent, BuildingInquiry
from sales_support_agent.models.voice import VoiceCall, VoiceConsent, VoiceDoNotCall
from sales_support_agent.services.voice import google_calendar, prompt, tools, twilio
from sales_support_agent.services.voice.bridge import CallBridge
from sales_support_agent.services.voice.config import load_voice_config
from sales_support_agent.services.voice.post_call import finalize_call

VOICE_ENV = (
    "VOICE_AGENT_ENABLED", "XAI_API_KEY", "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_PHONE_NUMBER",
    "VOICE_TRANSFER_NUMBER", "VOICE_PUBLIC_BASE_URL", "GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET",
    "VOICE_GOOGLE_REFRESH_TOKEN", "VOICE_TOUR_CALENDAR_ID", "VOICE_SUMMARY_EMAIL_TO", "VOICE_BUILDING_FACTS",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in VOICE_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def session_factory():
    _register_models()
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _enable(monkeypatch, **extra):
    monkeypatch.setenv("VOICE_AGENT_ENABLED", "true")
    monkeypatch.setenv("XAI_API_KEY", "x")
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC1")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "secret")
    monkeypatch.setenv("VOICE_PUBLIC_BASE_URL", "https://agent.example.com")
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


def test_flag_is_off_by_default():
    config = load_voice_config()
    assert config.enabled is False
    assert config.xai_voice_model == "grok-voice-latest"
    assert "AI assistant" in config.disclosure and "recorded" in config.disclosure


def test_twilio_signature_matches_reference_vector():
    params = {"CallSid": "CA1234567890ABCDE", "Caller": "+12349013030", "Digits": "1234", "From": "+12349013030", "To": "+18005551212"}
    url = "https://mycompany.com/myapp.php?foo=1&bar=2"
    assert twilio.compute_signature("12345", url, params) == "0/KCTR6DLpKmkAf8muzZqo1nDgQ="
    assert twilio.valid_signature("12345", url, params, "0/KCTR6DLpKmkAf8muzZqo1nDgQ=")
    assert not twilio.valid_signature("12345", url, params, "bad")


def test_stream_token_is_bound_to_call_and_expires():
    token = twilio.stream_token("secret", "CA1", now=1000)
    assert twilio.valid_stream_token("secret", "CA1", token, now=1050)
    assert not twilio.valid_stream_token("secret", "CA2", token, now=1050)
    assert not twilio.valid_stream_token("secret", "CA1", token, now=2000)
    assert not twilio.valid_stream_token("secret", "CA1", "garbage", now=1050)


def _app():
    from sales_support_agent.api.voice_router import router

    app = FastAPI()
    app.include_router(router)
    return app


def test_inbound_webhook_is_hidden_when_flag_off():
    response = TestClient(_app()).post("/voice/twilio/inbound", data={"CallSid": "CA1"})
    assert response.status_code == 404


def test_inbound_webhook_rejects_bad_signature_and_returns_stream_twiml(monkeypatch):
    _enable(monkeypatch)
    client = TestClient(_app())
    data = {"CallSid": "CA9", "From": "+18015550100", "To": "+18015550199"}
    assert client.post("/voice/twilio/inbound", data=data, headers={"X-Twilio-Signature": "nope"}).status_code == 403
    signature = twilio.compute_signature("secret", "https://agent.example.com/voice/twilio/inbound", data)
    response = client.post("/voice/twilio/inbound", data=data, headers={"X-Twilio-Signature": signature})
    assert response.status_code == 200
    body = response.text
    assert '<Stream url="wss://agent.example.com/voice/twilio/stream">' in body
    assert 'name="from" value="+18015550100"' in body
    assert 'name="token"' in body


def test_session_update_uses_mulaw_server_vad_and_tools():
    config = load_voice_config()
    update = prompt.session_update(config)["session"]
    assert update["audio"]["input"]["format"]["type"] == "audio/pcmu"
    assert update["audio"]["output"]["format"]["type"] == "audio/pcmu"
    assert update["turn_detection"]["type"] == "server_vad"
    names = {t["name"] for t in update["tools"]}
    assert names == {"lookup_caller", "get_building_info", "get_tour_slots", "book_tour", "transfer_to_human", "opt_out"}
    opening = prompt.disclosure_message(config)["item"]
    assert opening["type"] == "force_message" and opening["interruptible"] is False


def test_free_slots_skip_busy_weekends_and_short_notice(monkeypatch):
    _enable(monkeypatch, VOICE_TOUR_START_HOUR="10", VOICE_TOUR_END_HOUR="12")
    config = load_voice_config()
    now = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)  # Thursday
    busy_start = datetime(2026, 11, 6, 17, 0, tzinfo=timezone.utc)  # Fri 10:00 MST
    slots = google_calendar.free_slots(config, start_day=date(2026, 11, 6), days=4, now=now, busy=[(busy_start, busy_start + timedelta(minutes=30))], limit=20)
    local = [s.strftime("%a %H:%M") for s in slots]
    assert "Fri 10:00" not in local and "Fri 10:30" in local
    assert not any(s.startswith(("Sat", "Sun")) for s in local)
    assert "Mon 10:00" in local


def test_lookup_caller_and_opt_out(session_factory):
    with session_scope(session_factory) as session:
        session.add(BuildingInquiry(id="inq1", idempotency_key="k1", kind="event", name="Jordan Lee", email="j@example.com", phone="(801) 555-0100", payload_json={"event_type": "Gala"}))
        session.add(VoiceConsent(id="c1", phone="8015550100", source="event_inquiry_form", consent_text="text"))
    ctx = tools.CallContext(config=load_voice_config(), session_factory=session_factory, call_sid="CA1", from_number="+18015550100")
    found = tools.dispatch(ctx, "lookup_caller", {})
    assert found["found"] and found["first_name"] == "Jordan" and found["event_type"] == "Gala"
    assert tools.dispatch(ctx, "opt_out", {}) == {"recorded": True}
    with session_scope(session_factory) as session:
        assert session.get(VoiceDoNotCall, "8015550100") is not None
        assert session.get(VoiceConsent, "c1").revoked_at is not None


def test_tool_failures_do_not_raise(session_factory):
    ctx = tools.CallContext(config=load_voice_config(), session_factory=session_factory, call_sid="CA1", from_number="")
    assert tools.dispatch(ctx, "nope", {}) == {"error": "unknown_tool"}
    assert tools.dispatch(ctx, "get_tour_slots", {})["available"] is False
    ctx.transfer = lambda config, sid: {"ok": False}
    assert tools.dispatch(ctx, "transfer_to_human", {})["transferred"] is False


class FakeTwilio:
    def __init__(self, messages):
        self.incoming = asyncio.Queue()
        for m in messages:
            self.incoming.put_nowait(json.dumps(m))
        self.sent = []

    async def receive_text(self):
        return await self.incoming.get()

    async def send_text(self, text):
        self.sent.append(json.loads(text))

    async def close(self, code=1000):
        self.closed = code


class FakeXai:
    def __init__(self, twilio_ws, events):
        self.twilio_ws = twilio_ws
        self.events = events
        self.sent = []

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def close(self):
        pass

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for event in self.events:
            await asyncio.sleep(0)
            yield json.dumps(event)
        await asyncio.sleep(0.05)
        self.twilio_ws.incoming.put_nowait(json.dumps({"event": "stop"}))


def test_bridge_plays_disclosure_streams_audio_runs_tools_and_logs(monkeypatch, session_factory):
    _enable(monkeypatch)
    config = load_voice_config()
    token = twilio.stream_token("secret", "CA42")
    fake_twilio = FakeTwilio([
        {"event": "connected"},
        {"event": "start", "start": {"callSid": "CA42", "streamSid": "MZ1", "customParameters": {"token": token, "from": "+18015550100", "to": "+18015550199"}}},
        {"event": "media", "media": {"payload": "AAAA"}},
    ])
    xai_events = [
        {"type": "response.output_audio.delta", "delta": "BBBB"},
        {"type": "input_audio_buffer.speech_started"},
        {"type": "conversation.item.input_audio_transcription.completed", "transcript": "Please stop calling me"},
        {"type": "response.function_call_arguments.done", "name": "opt_out", "call_id": "fc1", "arguments": "{}"},
        {"type": "response.done"},
        {"type": "response.output_audio_transcript.done", "transcript": "Done, you won't be called again."},
    ]
    holder = {}

    async def connect(_config):
        holder["xai"] = FakeXai(fake_twilio, xai_events)
        return holder["xai"]

    emails = []
    bridge = CallBridge(
        config=config,
        session_factory=session_factory,
        twilio_ws=fake_twilio,
        xai_connect=connect,
        finalize=lambda ctx, outcome: finalize_call(ctx, outcome, summarize=lambda c, x: "", send_email=lambda *a: emails.append(a) or "sent"),
    )
    asyncio.run(asyncio.wait_for(bridge.run(), timeout=5))

    sent = holder["xai"].sent
    assert sent[0]["type"] == "session.update"
    assert sent[1]["item"]["type"] == "force_message"
    assert sent[1]["item"]["content"][0]["text"] == config.disclosure
    assert {"type": "input_audio_buffer.append", "audio": "AAAA"} in sent
    assert any(m.get("item", {}).get("type") == "function_call_output" for m in sent)
    assert {"type": "response.create"} in sent
    assert {"event": "media", "media": {"payload": "BBBB"}, "streamSid": "MZ1"} in fake_twilio.sent
    assert {"event": "clear", "streamSid": "MZ1"} in fake_twilio.sent
    with session_scope(session_factory) as session:
        call = session.query(VoiceCall).filter_by(call_sid="CA42").one()
        assert call.outcome == "opted_out" and call.status == "ended"
        assert call.disclosure_played_at is not None and call.disclosure_text == config.disclosure
        assert [t["speaker"] for t in call.transcript_json] == ["caller", "agent"]
        assert call.summary_email_status == "skipped_no_recipient"
        assert session.get(VoiceDoNotCall, "8015550100") is not None
    assert emails == []


def test_bridge_rejects_bad_stream_token(monkeypatch, session_factory):
    _enable(monkeypatch)
    fake_twilio = FakeTwilio([{"event": "start", "start": {"callSid": "CA1", "streamSid": "MZ", "customParameters": {"token": "1.bad"}}}])

    async def connect(_config):
        raise AssertionError("must not connect to xAI")

    bridge = CallBridge(config=load_voice_config(), session_factory=session_factory, twilio_ws=fake_twilio, xai_connect=connect)
    asyncio.run(bridge.run())
    assert fake_twilio.closed == 1008


def test_finalize_writes_crm_note_and_emails_summary(monkeypatch, session_factory):
    _enable(monkeypatch, VOICE_SUMMARY_EMAIL_TO="ops@example.com")
    with session_scope(session_factory) as session:
        session.add(VoiceCall(id="v1", call_sid="CA7", from_number="+18015550100", started_at=datetime.now(timezone.utc)))
    ctx = tools.CallContext(config=load_voice_config(), session_factory=session_factory, call_sid="CA7", from_number="+18015550100")
    ctx.state.update(inquiry_id="inq9", caller_name="Jordan", transcript=[{"speaker": "caller", "text": "Tour please"}])
    emails = []
    finalize_call(ctx, "completed", summarize=lambda c, x: "Wants a tour.", send_email=lambda to, subject, body: emails.append((to, subject, body)) or "sent")
    assert emails[0][0] == "ops@example.com" and "Wants a tour." in emails[0][2]
    with session_scope(session_factory) as session:
        note = session.query(BuildingAuditEvent).filter_by(entity_id="inq9").one()
        assert note.action == "voice_call_note" and note.after_json["summary"] == "Wants a tour."
        assert session.get(VoiceCall, "v1").summary_email_status == "sent"
