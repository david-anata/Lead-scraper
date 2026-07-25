from datetime import date, datetime, timezone

import pytest

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.collections import (
    build_collections,
    list_overdue_receivables,
    set_draft_status,
)

AS_OF = date(2026, 7, 24)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _add_invoice(engine, *, cid, customer, amount, due, status="overdue", record_kind="obligation"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="qbo", source_id=cid,
            record_kind=record_kind, event_type="inflow", category="revenue",
            name=customer, vendor_or_customer=customer, amount_cents=amount,
            due_date=due, status=status, confidence="estimated",
            created_at=now, updated_at=now,
        )


def test_groups_overdue_by_customer_oldest_first():
    engine = _setup()
    _add_invoice(engine, cid="a1", customer="Acme Co", amount=5000_00, due=date(2026, 6, 1))
    _add_invoice(engine, cid="a2", customer="Acme Co", amount=7000_00, due=date(2026, 6, 13))
    _add_invoice(engine, cid="b1", customer="Beta LLC", amount=8500_00, due=date(2026, 7, 12))

    receivables = list_overdue_receivables(as_of=AS_OF)
    assert [r["customer"] for r in receivables] == ["Acme Co", "Beta LLC"]  # most days late first
    acme = receivables[0]
    assert acme["owed_cents"] == 12000_00
    assert acme["invoice_count"] == 2
    assert acme["days_late"] == (AS_OF - date(2026, 6, 1)).days


def test_excludes_not_yet_due_and_posted_transactions():
    engine = _setup()
    _add_invoice(engine, cid="future", customer="Acme", amount=100_00, due=date(2026, 8, 30))
    _add_invoice(engine, cid="paid", customer="Acme", amount=100_00, due=date(2026, 6, 1),
                 status="posted", record_kind="transaction")
    assert list_overdue_receivables(as_of=AS_OF) == []


def test_build_collections_produces_drafts_and_total():
    engine = _setup()
    _add_invoice(engine, cid="a1", customer="Acme Co", amount=12000_00, due=date(2026, 6, 1))
    data = build_collections(as_of=AS_OF)
    assert data["total_owed_cents"] == 12000_00
    assert data["customer_count"] == 1
    cust = data["customers"][0]
    assert "Acme Co" in cust["email"]["body"]
    assert "$12,000.00" in cust["email"]["body"]
    assert cust["email"]["subject"]
    assert "Acme Co" in cust["sms"]["body"]
    assert cust["email_status"] == "draft"


def test_mark_sent_status_is_recorded_and_reflected():
    engine = _setup()
    _add_invoice(engine, cid="a1", customer="Acme Co", amount=12000_00, due=date(2026, 6, 1))
    key = build_collections(as_of=AS_OF)["customers"][0]["customer_key"]

    set_draft_status(key, "email", "sent")
    data = build_collections(as_of=AS_OF)
    cust = data["customers"][0]
    assert cust["email_status"] == "sent"
    assert cust["sms_status"] == "draft"

    # Upsert path: change it again.
    set_draft_status(key, "email", "skipped")
    assert build_collections(as_of=AS_OF)["customers"][0]["email_status"] == "skipped"


def test_set_draft_status_validates_channel_and_status():
    _setup()
    with pytest.raises(ValueError):
        set_draft_status("acme", "carrier-pigeon", "sent")
    with pytest.raises(ValueError):
        set_draft_status("acme", "email", "teleported")
