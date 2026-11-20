"""Twilio webhook posts for the inbound SMS conversation."""

from __future__ import annotations

import json

import pytest

from voice.main import create_app
from voice.security import compute_signature, public_request_url
from voice.settings import Settings
from voice.sms_conversation import CONTINUED_SENDER, SmsThreadStore, handle_inbound_sms
from voice.tests.test_store_and_app import _Http, _settings


EVENT_LINE = "+13853363562"
AGENCY_LINE = "+13853363455"
SENDER = "+18015550123"


def _app():
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    lines = {
        EVENT_LINE: {"id": "track_building_events", "label": "Anata events", "source": "events-page"},
        AGENCY_LINE: {"id": "track_inc_services", "label": "anata Inc.", "source": "anatainc.com"},
    }
    settings = _settings(
        sms_autoreply=True,
        line_contexts_json=json.dumps(lines),
        twilio_messaging_service_sid="MG123",
    )
    app = create_app(settings)
    sent: list[dict[str, str]] = []

    def capture(url: str, *, data: dict[str, str], auth: tuple[str, str], timeout: int) -> _HttpResponse:
        sent.append(dict(data))
        return _HttpResponse(201, {"sid": "SM" + str(len(sent))})

    app.state.twilio.http = type("Client", (), {"post": staticmethod(capture)})()
    app.state.store.http = _Http()
    return TestClient(app), app, sent


class _HttpResponse:
    def __init__(self, status: int, payload: object) -> None:
        self.status_code = status
        self._payload = payload

    def json(self) -> object:
        return self._payload


def _post(client, settings: Settings, form: dict[str, str]):
    url = public_request_url(settings.public_base_url, "/voice/sms", "")
    signature = compute_signature(url, list(form.items()), settings.twilio_auth_token)
    return client.post("/voice/sms", data=form, headers={"X-Twilio-Signature": signature})


def _texts_to(sent: list[dict[str, str]], number: str) -> list[dict[str, str]]:
    return [item for item in sent if item.get("To") == number]


def test_first_text_gets_a_reply_and_does_not_alert_david() -> None:
    client, app, sent = _app()
    response = _post(
        client,
        app.state.settings,
        {
            "MessageSid": "SM1",
            "From": SENDER,
            "To": EVENT_LINE,
            "Body": "Rent for my new VELORA event",
        },
    )
    assert response.status_code == 200
    assert "<Message>" not in response.text
    replies = _texts_to(sent, SENDER)
    assert len(replies) == 1
    body = replies[0]["Body"]
    assert replies[0]["From"] == EVENT_LINE
    assert replies[0]["MessagingServiceSid"] == "MG123"
    assert "VELORA" in body
    assert "date" in body.casefold()
    assert "STOP" in body
    assert "reserved" not in body.casefold() or "doesn't reserve" in body.casefold() or "does not reserve" in body.casefold()
    assert _texts_to(sent, app.state.settings.notify_sms_number) == []
    assert not any("building_inquiries" in url for url, _body in app.state.store.http.calls)


def test_multi_message_exchange_creates_a_complete_lead_and_alerts_once() -> None:
    client, app, sent = _app()
    settings = app.state.settings
    messages = (
        "Rent for my new VELORA event",
        "June 6 2027 from 6pm to 11pm for 120 guests",
        "Alex Rivera, alex@example.com",
        "yes",
    )
    for index, body in enumerate(messages, start=1):
        response = _post(
            client,
            settings,
            {"MessageSid": f"SM{index}", "From": SENDER, "To": EVENT_LINE, "Body": body},
        )
        assert response.status_code == 200

    customer = _texts_to(sent, SENDER)
    assert len(customer) == 4
    assert "security" in customer[2]["Body"].casefold()
    assert "optional" in customer[2]["Body"].casefold()
    assert "doesn't reserve" in customer[3]["Body"] or "does not reserve" in customer[3]["Body"]
    alerts = _texts_to(sent, settings.notify_sms_number)
    assert len(alerts) == 1
    assert "Alex Rivera" in alerts[0]["Body"]
    assert "801-555-0123" in alerts[0]["Body"]
    assert "VELORA" in alerts[0]["Body"]
    assert len(alerts[0]["Body"]) <= 160

    inquiries = [body for url, body in app.state.store.http.calls if "building_inquiries" in url and isinstance(body, dict)]
    assert len(inquiries) == 1
    lead = inquiries[0]
    assert lead["name"] == "Alex Rivera"
    assert lead["email"] == "alex@example.com"
    assert lead["phone"] == SENDER
    assert lead["preferred_date"] == "2027-06-06"
    assert lead["source"] == "voice"
    assert lead["kind"] == "event"
    assert lead["payload_json"]["channel"] == "sms"
    assert lead["payload_json"]["eventType"] == "VELORA event"
    assert lead["payload_json"]["groupSize"] == "120"
    assert lead["payload_json"]["security_preference"] == "yes"
    assert lead["payload_json"]["_event_interview"]["time_window"] == "6pm to 11pm"
    logs = [body for url, body in app.state.store.http.calls if "/call_log" in url and isinstance(body, dict)]
    assert logs[-1]["channel"] == "sms"
    assert logs[-1]["category"] == "lead"
    assert logs[-1]["building_inquiry_id"]


def test_stop_is_honored_and_a_later_text_gets_no_reply() -> None:
    client, app, sent = _app()
    settings = app.state.settings
    first = _post(
        client,
        settings,
        {"MessageSid": "SM1", "From": SENDER, "To": EVENT_LINE, "Body": "STOP"},
    )
    assert first.status_code == 200
    opted = _texts_to(sent, SENDER)
    assert len(opted) == 1
    assert "opted out" in opted[0]["Body"].casefold()
    before = len(sent)
    second = _post(
        client,
        settings,
        {"MessageSid": "SM2", "From": SENDER, "To": EVENT_LINE, "Body": "June 6 2027, 120 guests"},
    )
    assert second.status_code == 200
    assert "<Message>" not in second.text
    assert _texts_to(sent[before:], SENDER) == []
    assert _texts_to(sent, settings.notify_sms_number) == []
    assert not any("building_inquiries" in url for url, _body in app.state.store.http.calls)


def test_continued_velora_thread_does_not_greet_from_scratch() -> None:
    client, app, sent = _app()
    _post(
        client,
        app.state.settings,
        {
            "MessageSid": "SM1",
            "From": CONTINUED_SENDER,
            "To": EVENT_LINE,
            "Body": "June 6 2027 from 6pm to 11pm",
        },
    )
    reply = _texts_to(sent, CONTINUED_SENDER)[0]["Body"]
    assert "Thanks for texting" not in reply
    assert "VELORA" in reply
    assert "guest" in reply.casefold()


def test_spam_and_david_and_weekday_price_and_landlord() -> None:
    client, app, sent = _app()
    settings = app.state.settings

    def resolve(phone: str) -> dict[str, str]:
        if phone == "+18015550111":
            return {
                "id": "legal-1",
                "name": "Pat Fondell",
                "company": "Fondell",
                "category": "landlord_legal",
                "ring_policy": "",
            }
        if phone == "+18015550112":
            return {
                "id": "tenant-1",
                "name": "Jamie Lee",
                "company": "",
                "category": "tenant",
                "ring_policy": "",
            }
        return {}

    app.state.store.resolve_caller = resolve  # type: ignore[method-assign]

    spam = _post(
        client,
        settings,
        {
            "MessageSid": "SM1",
            "From": "+18015550120",
            "To": EVENT_LINE,
            "Body": "We help businesses rank on Google with SEO",
        },
    )
    assert spam.status_code == 200
    assert _texts_to(sent, "+18015550120") == []

    david = _post(
        client,
        settings,
        {"MessageSid": "SM2", "From": settings.notify_sms_number, "To": EVENT_LINE, "Body": "hello from me"},
    )
    assert david.status_code == 200
    assert _texts_to(sent, settings.notify_sms_number) == []

    price = _post(
        client,
        settings,
        {"MessageSid": "SM3", "From": "+18015550121", "To": EVENT_LINE, "Body": "How much is a Wednesday workshop?"},
    )
    assert price.status_code == 200
    quoted = _texts_to(sent, "+18015550121")[0]["Body"]
    assert "1,500" not in quoted
    assert "1500" not in quoted
    assert "team" in quoted.casefold()

    legal = _post(
        client,
        settings,
        {"MessageSid": "SM4", "From": "+18015550111", "To": EVENT_LINE, "Body": "What is the rent and the lease balance?"},
    )
    assert legal.status_code == 200
    legal_reply = _texts_to(sent, "+18015550111")[0]["Body"]
    assert "Pat" in legal_reply
    assert "rent" not in legal_reply.casefold()
    assert "$" not in legal_reply
    assert "1657" not in legal_reply
    alerts = _texts_to(sent, settings.notify_sms_number)
    assert len(alerts) == 1
    before = len(sent)
    _post(
        client,
        settings,
        {"MessageSid": "SM5", "From": "+18015550111", "To": EVENT_LINE, "Body": "Any update on the money?"},
    )
    assert _texts_to(sent[before:], settings.notify_sms_number) == []
    assert "rent" not in _texts_to(sent, "+18015550111")[-1]["Body"].casefold()

    locked = _post(
        client,
        settings,
        {"MessageSid": "SM6", "From": "+18015550112", "To": EVENT_LINE, "Body": "We're locked out of the suite"},
    )
    assert locked.status_code == 200
    assert "Jamie" in _texts_to(sent, "+18015550112")[0]["Body"]
    assert len(_texts_to(sent, settings.notify_sms_number)) == 2
    assert not any(
        isinstance(body, dict) and body.get("phone") == settings.notify_sms_number
        for _url, body in app.state.store.http.calls
        if isinstance(body, dict) and "building_inquiries" in _url
    )


def test_echo_and_agency_fee_are_not_invented() -> None:
    client, app, sent = _app()
    settings = app.state.settings
    _post(
        client,
        settings,
        {"MessageSid": "SM1", "From": SENDER, "To": AGENCY_LINE, "Body": "How much do you charge for Amazon ads?"},
    )
    quote = _texts_to(sent, SENDER)[0]["Body"]
    assert "aren't published" in quote or "are not published" in quote
    assert "$" not in quote
    echo = quote
    before = len(_texts_to(sent, SENDER))
    _post(client, settings, {"MessageSid": "SM2", "From": SENDER, "To": AGENCY_LINE, "Body": echo})
    assert len(_texts_to(sent, SENDER)) == before


def test_saved_thread_reloads_after_the_process_forgets_it() -> None:
    saved: dict[str, object] = {}

    class Store:
        def latest_sms(self, caller_number: str) -> dict[str, object] | None:
            return saved.get("row")  # type: ignore[return-value]

        def save_call(self, session: object, *, sms_notified: bool, create_inquiry: bool | None = None) -> dict[str, str]:
            saved["session"] = session
            return {"call_log": "saved", "inquiry_id": ""}

        def resolve_caller(self, phone: str) -> dict[str, str]:
            return {}

    settings = _settings(
        sms_autoreply=True,
        line_contexts_json=json.dumps(
            {EVENT_LINE: {"id": "track_building_events", "label": "Anata events", "source": "events-page"}}
        ),
    )
    first = SmsThreadStore()
    handle_inbound_sms(
        settings=settings,
        store=Store(),
        threads=first,
        sender=SENDER,
        recipient=EVENT_LINE,
        body="Rent for my new VELORA event",
        message_sid="SM1",
    )
    session = saved["session"]
    row = {
        "call_sid": session.call_sid,  # type: ignore[attr-defined]
        "caller_number": SENDER,
        "dialed_number": EVENT_LINE,
        "line_id": "track_building_events",
        "started_at": "2026-11-13T19:00:00+00:00",
        "updated_at": "2026-11-13T19:10:00+00:00",
        "transcript": session.transcript(),  # type: ignore[attr-defined]
        "transcript_summary": session.summary,  # type: ignore[attr-defined]
        "category": session.category,  # type: ignore[attr-defined]
        "lead_fields": session.lead_fields(),  # type: ignore[attr-defined]
        "action_taken": session.action,  # type: ignore[attr-defined]
        "building_inquiry_id": "",
        "known_caller_id": "",
        "known_caller_category": "",
        "heard_about_raw": "",
        "heard_about_source": "",
    }
    saved["row"] = row
    fresh = SmsThreadStore()
    handle_inbound_sms(
        settings=settings,
        store=Store(),
        threads=fresh,
        sender=SENDER,
        recipient=EVENT_LINE,
        body="June 6 2027",
        message_sid="SM2",
        now=__import__("datetime").datetime.fromisoformat("2026-11-13T19:20:00+00:00"),
    )
    reply = saved["session"].sms_state["last_reply"]  # type: ignore[attr-defined]
    assert "Thanks for texting" not in reply
    assert "time" in reply.casefold()
    assert saved["session"].event_or_interest_type == "VELORA event"  # type: ignore[attr-defined]
