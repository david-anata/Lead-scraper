from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, inspect, text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.vendors import (
    cancellation_deadline,
    agreement_mismatches,
    create_vendor,
    deactivate_vendor,
    get_vendor,
    list_vendors_with_progress,
    preview_agreement_obligations,
    update_vendor,
)
from sales_support_agent.services.cashflow.overview import render_vendor_agreement_preview


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


def test_evergreen_agreement_calculates_cancellation_deadline_and_obligations():
    engine = _setup()
    vendor_id = create_vendor({
        "name": "Elementor", "agreement_name": "Annual license",
        "agreement_status": "active", "term_type": "evergreen",
        "payment_amount_cents": 999_00, "frequency": "month",
        "amount_type": "fixed", "start_date": "2026-08-15",
        "renewal_date": "2027-08-15", "auto_renewal": "yes",
        "cancellation_notice_days": 30, "owner": "David",
        "evidence_note": "Vendor order form", "match_terms": "elementor",
    }, actor="david@anatainc.com")
    vendor = get_vendor(vendor_id)

    assert cancellation_deadline(vendor) == date(2027, 7, 16)
    preview = preview_agreement_obligations(
        vendor, as_of=date(2026, 8, 10), horizon_days=70,
    )
    assert [row["due_date"] for row in preview] == [
        date(2026, 8, 15), date(2026, 9, 15), date(2026, 10, 15),
    ]
    assert all(row["record_kind"] == "obligation" for row in preview)
    with engine.connect() as connection:
        assert connection.execute(text("""
            SELECT count(*) FROM cash_events WHERE source='vendor_agreement'
        """)).scalar_one() == 0
        assert connection.execute(text("""
            SELECT count(*) FROM finance_action_audit
            WHERE action_type='vendor_agreement_created' AND actor='david@anatainc.com'
        """)).scalar_one() == 1


def test_variable_or_ended_agreement_never_manufactures_a_forecast_amount():
    base = {
        "id": "vendor", "name": "Variable service", "agreement_status": "active",
        "amount_type": "variable", "payment_amount_cents": 50_000,
        "frequency": "month", "start_date": date(2026, 8, 15),
    }
    assert preview_agreement_obligations(base, as_of=date(2026, 8, 1)) == []
    assert preview_agreement_obligations(
        {**base, "amount_type": "fixed", "agreement_status": "ended"},
        as_of=date(2026, 8, 1),
    ) == []


def test_ending_agreement_stops_after_explicit_end_and_preserves_prior_dates():
    preview = preview_agreement_obligations({
        "id": "vendor", "name": "Service", "agreement_status": "ending",
        "amount_type": "fixed", "payment_amount_cents": 10_000,
        "frequency": "month", "start_date": date(2026, 8, 5),
        "end_date": date(2026, 9, 5),
    }, as_of=date(2026, 8, 1), horizon_days=120)
    assert [row["due_date"] for row in preview] == [date(2026, 8, 5), date(2026, 9, 5)]


def test_agreement_save_uses_a_full_page_calendar_preview():
    _setup()
    page = render_vendor_agreement_preview({
        "name": "Elementor", "agreement_status": "active",
        "amount_type": "fixed", "payment_amount_cents": 9_900,
        "frequency": "month", "start_date": "2026-08-15",
        "terms_type": "recurring", "term_type": "evergreen",
        "auto_renewal": "yes", "cancellation_notice_days": 30,
    })
    assert "Review the Calendar effect" in page
    assert "Nothing has been saved" in page
    assert "Before" in page and "After" in page
    assert "Save agreement" in page
    assert 'action="/admin/finances/vendors"' in page


def test_agreement_mismatches_route_amount_changes_duplicates_and_late_charges_to_review():
    vendor = {
        "match_terms": "elementor", "payment_amount_cents": 10_000,
        "amount_type": "fixed", "end_date": date(2026, 7, 31),
    }
    posted = [
        {"name": "Elementor", "amount_cents": 12_000, "paid_on": date(2026, 8, 2)},
        {"name": "Elementor", "amount_cents": 12_000, "paid_on": date(2026, 8, 2)},
    ]
    kinds = [item["kind"] for item in agreement_mismatches(vendor, posted)]
    assert "amount_changed" in kinds
    assert "duplicate_charge" in kinds
    assert "charge_after_end" in kinds


def test_existing_vendor_table_gets_every_agreement_column_idempotently():
    from sales_support_agent.models.database import _ensure_vendor_columns
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE finance_vendors (
                id VARCHAR(64) PRIMARY KEY, name VARCHAR(255), active BOOLEAN
            )
        """))
    _ensure_vendor_columns(engine)
    _ensure_vendor_columns(engine)
    columns = {column["name"] for column in inspect(engine).get_columns("finance_vendors")}
    assert {
        "agreement_name", "agreement_status", "term_type", "amount_type",
        "renewal_date", "auto_renewal", "cancellation_notice_days", "owner",
        "evidence_note", "created_by", "updated_by",
    } <= columns
