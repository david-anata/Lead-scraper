from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.vendors import (
    create_vendor,
    deactivate_vendor,
    get_vendor,
    list_vendors_with_progress,
    update_vendor,
)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _add_outflow(engine, *, source_id, name, amount_cents, due, status="posted"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=source_id, source="plaid", source_id=source_id,
            record_kind="transaction", event_type="outflow", category="loan",
            name=name, description=name, vendor_or_customer=name,
            amount_cents=amount_cents, due_date=due, status=status,
            confidence="confirmed", created_at=now, updated_at=now,
        )


def test_recurring_vendor_tracks_paid_remaining_and_percent():
    engine = _setup()
    _add_outflow(engine, source_id="p1", name="Fora Capital", amount_cents=9800_00, due=date(2026, 5, 1))
    _add_outflow(engine, source_id="p2", name="FORA payment", amount_cents=9800_00, due=date(2026, 6, 1))
    _add_outflow(engine, source_id="other", name="Adobe", amount_cents=52_99, due=date(2026, 6, 1))

    create_vendor({
        "name": "Fora", "terms_type": "recurring", "payment_amount_cents": 9800_00,
        "frequency": "month", "total_committed_cents": 117600_00, "match_terms": "fora",
    })

    vendor = list_vendors_with_progress()[0]
    assert vendor["paid_cents"] == 19600_00           # only the two Fora rows
    assert vendor["matched_count"] == 2
    assert vendor["remaining_cents"] == 98000_00      # 117600 - 19600
    assert vendor["percent_bps"] == round(19600_00 * 10000 / 117600_00)


def test_start_date_excludes_earlier_payments():
    engine = _setup()
    _add_outflow(engine, source_id="old", name="Fora", amount_cents=5000_00, due=date(2026, 1, 1))
    _add_outflow(engine, source_id="new", name="Fora", amount_cents=5000_00, due=date(2026, 6, 1))
    create_vendor({
        "name": "Fora", "terms_type": "recurring", "payment_amount_cents": 5000_00,
        "total_committed_cents": 20000_00, "match_terms": "fora", "start_date": "2026-05-01",
    })
    vendor = list_vendors_with_progress()[0]
    assert vendor["paid_cents"] == 5000_00
    assert vendor["matched_count"] == 1


def test_ongoing_vendor_without_total_has_no_remaining_or_percent():
    engine = _setup()
    _add_outflow(engine, source_id="r1", name="Lehi rent", amount_cents=12000_00, due=date(2026, 6, 1))
    create_vendor({"name": "Lehi", "terms_type": "recurring", "match_terms": "lehi rent"})
    vendor = list_vendors_with_progress()[0]
    assert vendor["paid_cents"] == 12000_00
    assert vendor["remaining_cents"] is None
    assert vendor["percent_bps"] is None
    assert vendor["payoff_date"] == ""


def test_explicit_end_date_is_used_as_payoff():
    engine = _setup()
    create_vendor({
        "name": "Loan", "terms_type": "recurring", "payment_amount_cents": 1000_00,
        "total_committed_cents": 5000_00, "match_terms": "loan", "end_date": "2027-03-15",
    })
    assert list_vendors_with_progress()[0]["payoff_date"] == "2027-03-15"


def test_update_and_deactivate_vendor():
    _setup()
    vid = create_vendor({"name": "Old", "match_terms": "old"})
    update_vendor(vid, {"name": "New Name", "terms_type": "recurring", "match_terms": "new"})
    assert get_vendor(vid)["name"] == "New Name"

    deactivate_vendor(vid)
    assert list_vendors_with_progress() == []


def test_create_rejects_empty_name():
    _setup()
    with pytest.raises(ValueError):
        create_vendor({"name": "  ", "match_terms": "x"})
