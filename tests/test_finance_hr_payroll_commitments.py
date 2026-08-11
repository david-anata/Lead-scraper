"""HR payroll is a Finance obligation; only bank evidence can settle it."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from sales_support_agent.models import database
from sales_support_agent.models.database import create_session_factory, init_database, upsert_cash_event
from sales_support_agent.services.cashflow.cash_calendar import build_cash_calendar
from sales_support_agent.services.cashflow.cash_calendar import _historical_data
from sales_support_agent.services.cashflow.control import build_finance_control_state
from sales_support_agent.services.cashflow.obligations import list_obligations
from sales_support_agent.services.cashflow.payroll_commitments import finance_payroll_id


@pytest.fixture()
def payroll_engine(monkeypatch):
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    monkeypatch.setattr(database, "engine", engine)
    return engine


def _run(engine, run_id: str, status: str, net: int = 125_000, gross: int = 160_000):
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO hr_payroll_runs (
                base44_id, pay_period_start, pay_period_end, pay_date, status,
                total_gross_cents, total_net_cents, total_taxes_cents,
                employee_count, initiated_by, notes, created_at
            ) VALUES (
                :id, '2026-08-01', '2026-08-15', '2026-08-14', :status,
                :gross, :net, :taxes, 2, 'owner@example.com', '', :now
            )
        """), {
            "id": run_id, "status": status, "gross": gross, "net": net,
            "taxes": gross - net, "now": datetime.now(timezone.utc),
        })


def test_draft_payroll_is_expected_once_but_not_required(payroll_engine):
    _run(payroll_engine, "run-draft", "draft")
    rows = list_obligations(source="hr_payroll")

    assert len(rows) == 1
    assert rows[0]["amount_cents"] == 125_000
    assert rows[0]["source_status"] == "draft"
    state = build_finance_control_state(rows, as_of=date(2026, 8, 10))
    assert state["metrics"]["required_outgoing_cents"] == 0
    assert state["metrics"]["expected_outgoing_cents"] == 125_000

    calendar = build_cash_calendar(rows, as_of=date(2026, 8, 10))
    event = next(item for day in calendar["days"] for item in day["events"])
    assert event["state_label"] == "Expected · HR draft"
    assert calendar["totals"]["planned_cents"] == 0
    assert calendar["totals"]["warning_cents"] == 125_000


def test_processing_payroll_is_required_using_net_not_gross(payroll_engine):
    _run(payroll_engine, "run-processing", "processing")
    rows = list_obligations(source="hr_payroll")
    state = build_finance_control_state(rows, as_of=date(2026, 8, 10))

    assert rows[0]["amount_cents"] == 125_000
    assert "Gross payroll context: 160000 cents" in rows[0]["notes"]
    assert state["metrics"]["required_outgoing_cents"] == 125_000


def test_completed_payroll_waits_for_posted_allocations_and_supports_splits(payroll_engine):
    _run(payroll_engine, "run-complete", "completed")
    obligation = list_obligations(source="hr_payroll")[0]
    assert obligation["status"] != "paid"
    now = datetime.now(timezone.utc)
    with payroll_engine.begin() as connection:
        for event_id, amount in (("plaid-pay-1", 60_000), ("plaid-pay-2", 65_000)):
            upsert_cash_event(connection, {
                "id": event_id, "source": "plaid", "source_id": event_id,
                "record_kind": "transaction", "event_type": "outflow",
                "category": "payroll", "name": "Payroll withdrawal",
                "amount_cents": amount, "due_date": date(2026, 8, 14),
                "status": "posted", "confidence": "confirmed",
            })
            connection.execute(text("""
                INSERT INTO settlement_allocations (
                    id, obligation_event_id, transaction_event_id, amount_cents,
                    allocation_date, source, confidence, idempotency_key, notes, created_at
                ) VALUES (
                    :allocation, :obligation, :transaction, :amount, '2026-08-14',
                    'plaid', 'confirmed', :allocation, 'HR payroll settlement', :now
                )
            """), {
                "allocation": f"allocation-{event_id}",
                "obligation": finance_payroll_id("run-complete"),
                "transaction": event_id, "amount": amount, "now": now,
            })

    rows = list_obligations(source="hr_payroll")
    state = build_finance_control_state(
        rows,
        settlement_annotations=[{
            "obligation_event_id": finance_payroll_id("run-complete"),
            "amount_cents": 125_000,
        }],
        as_of=date(2026, 8, 10),
    )
    assert state["metrics"]["required_outgoing_cents"] == 0


def test_failed_or_removed_run_does_not_remain_in_forecast(payroll_engine):
    _run(payroll_engine, "run-failed", "failed")
    assert list_obligations(source="hr_payroll")[0]["archived_at"] is not None

    with payroll_engine.begin() as connection:
        connection.execute(text("DELETE FROM hr_payroll_runs"))
    rows = list_obligations(source="hr_payroll")
    assert all(row["archived_at"] is not None for row in rows)


def test_hr_run_suppresses_the_same_period_historical_payroll_guess(
    payroll_engine, monkeypatch,
):
    _run(payroll_engine, "run-covered", "processing")
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.bill_patterns.confirmed_bill_projections",
        lambda **_kwargs: [{
            "id": "history-payroll", "due_date": date(2026, 8, 15),
            "name": "Payroll Intuit", "vendor": "Payroll Intuit",
            "category": "payroll", "amount_cents": 125_000,
        }],
    )
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.bill_patterns.list_bill_patterns",
        lambda **_kwargs: {"patterns": [], "tracked": []},
    )

    events, _patterns = _historical_data(as_of=date(2026, 8, 10), future_days=14)

    assert events == []
