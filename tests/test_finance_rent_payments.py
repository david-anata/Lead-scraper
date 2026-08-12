from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from sales_support_agent.models.database import Base, _register_models
from sales_support_agent.services.cashflow.rent_payments import (
    list_rent_payment_reports,
    reconcile_rent_payment_reports,
    report_rent_payment,
    void_rent_payment_report,
)
from sales_support_agent.services.cashflow.settings import get_paydown_settings, set_paydown_settings


TODAY = date(2026, 8, 12)


def _engine():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _register_models()
    Base.metadata.create_all(bind=engine)
    set_paydown_settings(
        balance_cents=3_000_000, balance_as_of=TODAY - timedelta(days=1),
        monthly_cents=4_000_000, cash_goal_cents=1_000_000,
        emergency_floor_cents=0, actor="david@anatainc.com", engine=engine,
    )
    return engine


def test_report_is_awaiting_bank_and_does_not_change_saved_balance():
    engine = _engine()

    report = report_rent_payment(
        amount_cents=240_000, reported_on=TODAY, vendor_key="boulder ranch",
        actor="david@anatainc.com", engine=engine,
    )

    assert report["status"] == "awaiting_bank"
    assert get_paydown_settings(engine=engine)["balance_cents"] == 3_000_000
    assert list_rent_payment_reports(engine=engine)[0]["amount_cents"] == 240_000


def test_same_report_is_idempotent():
    engine = _engine()
    first = report_rent_payment(
        amount_cents=240_000, reported_on=TODAY, vendor_key="boulder ranch", engine=engine,
    )
    second = report_rent_payment(
        amount_cents=240_000, reported_on=TODAY, vendor_key="boulder ranch", engine=engine,
    )

    assert second["id"] == first["id"]
    assert len(list_rent_payment_reports(engine=engine)) == 1


def test_matching_plaid_payment_reduces_balance_exactly_once():
    engine = _engine()
    report_rent_payment(
        amount_cents=240_000, reported_on=TODAY, vendor_key="boulder ranch",
        engine=engine,
    )
    bank_row = {
        "id": "plaid-2400", "source_id": "plaid-2400", "source": "plaid",
        "event_type": "outflow", "status": "posted", "effective_date": TODAY,
        "amount_cents": 240_000, "vendor_or_customer": "Boulder Ranch Property Management",
    }

    first = reconcile_rent_payment_reports([bank_row], as_of=TODAY, engine=engine)
    second = reconcile_rent_payment_reports([bank_row], as_of=TODAY, engine=engine)

    assert first[0]["status"] == "bank_confirmed"
    assert first[0]["matched_transaction_id"] == "plaid-2400"
    assert second[0]["status"] == "bank_confirmed"
    assert get_paydown_settings(engine=engine)["balance_cents"] == 2_760_000


def test_unmatched_report_moves_to_review_after_seven_days():
    engine = _engine()
    report_rent_payment(
        amount_cents=240_000, reported_on=TODAY, vendor_key="boulder ranch",
        engine=engine,
    )

    reports = reconcile_rent_payment_reports([], as_of=TODAY + timedelta(days=8), engine=engine)

    assert reports[0]["status"] == "needs_review"


def test_unconfirmed_report_can_be_voided_but_confirmed_one_cannot():
    engine = _engine()
    report = report_rent_payment(
        amount_cents=240_000, reported_on=TODAY, vendor_key="boulder ranch",
        engine=engine,
    )
    assert void_rent_payment_report(report["id"], engine=engine)
    assert list_rent_payment_reports(engine=engine) == []
