from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow import collections as collections_module
from sales_support_agent.services.cashflow.collections import (
    build_collections,
    get_contacts,
    send_email_reminder,
    set_contact,
)

AS_OF = date(2026, 7, 24)
SETTINGS = SimpleNamespace(resend_api_key="key", resend_from="Anata <billing@anatainc.com>")


class _FakeResend:
    """Stands in for the real sender so no email leaves the test."""

    sent: list[dict] = []

    def __init__(self, settings):
        self.settings = settings

    def is_configured(self, **_):
        return bool(getattr(self.settings, "resend_api_key", ""))

    def send_message(self, *, to, subject, text, reply_to="", idempotency_key="", from_address=""):
        _FakeResend.sent.append({"to": to, "subject": subject, "text": text})
        return "msg-123"


class _FailingResend(_FakeResend):
    def send_message(self, **kwargs):
        raise RuntimeError("provider rejected the message")


@pytest.fixture(autouse=True)
def _patch_sender(monkeypatch):
    _FakeResend.sent = []
    import sales_support_agent.integrations.resend as resend_module
    monkeypatch.setattr(resend_module, "ResendClient", _FakeResend)
    yield


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id="inv1", source="qbo", source_id="inv1",
            record_kind="obligation", event_type="inflow", category="revenue",
            name="Acme Co", vendor_or_customer="Acme Co", amount_cents=12000_00,
            due_date=date(2026, 6, 1), status="overdue", confidence="estimated",
            created_at=now, updated_at=now,
        )
    return engine


def test_contact_can_be_saved_and_is_surfaced():
    _setup()
    set_contact("Acme Co", email="billing@acme.com")
    assert get_contacts()["acme co"]["email"] == "billing@acme.com"
    cust = build_collections(as_of=AS_OF)["customers"][0]
    assert cust["contact_email"] == "billing@acme.com"


def test_invalid_email_is_rejected():
    _setup()
    with pytest.raises(ValueError):
        set_contact("Acme Co", email="not-an-email")


def test_send_requires_an_address_on_file():
    _setup()
    with pytest.raises(ValueError, match="no email address"):
        send_email_reminder("Acme Co", subject="Hi", body="Please pay", settings=SETTINGS)
    assert _FakeResend.sent == []


def test_sending_records_status_and_blocks_a_second_send():
    engine = _setup()
    set_contact("Acme Co", email="billing@acme.com")

    result = send_email_reminder(
        "Acme Co", subject="Reminder", body="Please pay", settings=SETTINGS, actor="qa@example.com",
    )
    assert result["sent"] is True and result["recipient"] == "billing@acme.com"
    assert len(_FakeResend.sent) == 1

    cust = build_collections(as_of=AS_OF)["customers"][0]
    assert cust["email_status"] == "sent"

    with engine.connect() as connection:
        row = connection.execute(text(
            "SELECT sent_at, provider_message_id FROM finance_collection_drafts "
            "WHERE customer_key='acme co' AND channel='email'"
        )).fetchone()
    assert row._mapping["sent_at"] is not None
    assert row._mapping["provider_message_id"] == "msg-123"

    # A second identical send is refused rather than silently duplicated.
    with pytest.raises(ValueError, match="already sent"):
        send_email_reminder("Acme Co", subject="Reminder", body="Please pay", settings=SETTINGS)
    assert len(_FakeResend.sent) == 1

    # An explicit resend is allowed.
    send_email_reminder(
        "Acme Co", subject="Reminder", body="Please pay", settings=SETTINGS, force=True,
    )
    assert len(_FakeResend.sent) == 2


def test_test_send_goes_to_the_override_and_does_not_mark_the_customer():
    _setup()
    set_contact("Acme Co", email="billing@acme.com")
    result = send_email_reminder(
        "Acme Co", subject="Reminder", body="Please pay", settings=SETTINGS,
        to_override="me@anatainc.com",
    )
    assert result["test"] is True
    assert _FakeResend.sent[0]["to"] == "me@anatainc.com"
    # The customer is still unsent.
    assert build_collections(as_of=AS_OF)["customers"][0]["email_status"] == "draft"


def test_unconfigured_sender_refuses_to_send():
    _setup()
    set_contact("Acme Co", email="billing@acme.com")
    with pytest.raises(ValueError, match="not configured"):
        send_email_reminder(
            "Acme Co", subject="Reminder", body="Please pay",
            settings=SimpleNamespace(resend_api_key="", resend_from=""),
        )
    assert _FakeResend.sent == []


def test_provider_failure_is_reported_and_recorded(monkeypatch):
    engine = _setup()
    set_contact("Acme Co", email="billing@acme.com")
    import sales_support_agent.integrations.resend as resend_module
    monkeypatch.setattr(resend_module, "ResendClient", _FailingResend)

    with pytest.raises(ValueError, match="could not be sent"):
        send_email_reminder("Acme Co", subject="R", body="B", settings=SETTINGS)

    assert build_collections(as_of=AS_OF)["customers"][0]["email_status"] != "sent"


def test_subject_and_body_are_required():
    _setup()
    set_contact("Acme Co", email="billing@acme.com")
    with pytest.raises(ValueError):
        send_email_reminder("Acme Co", subject="  ", body="B", settings=SETTINGS)
    with pytest.raises(ValueError):
        send_email_reminder("Acme Co", subject="S", body="  ", settings=SETTINGS)
    assert _FakeResend.sent == []
