from __future__ import annotations

from datetime import date

from sqlalchemy import text

from sales_support_agent.models.database import create_session_factory, init_database, upsert_cash_event
from sales_support_agent.services.cashflow.control import build_finance_control_state
from sales_support_agent.services.cashflow.qbo_sync import _invoice_to_event
from sales_support_agent.services.cashflow.settings import get_cash_floor_cents, set_cash_floor_cents


def test_persisted_cash_floor_is_used_by_control() -> None:
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    set_cash_floor_cents(2_500_00, actor="qa")
    assert get_cash_floor_cents() == 2_500_00
    state = build_finance_control_state([], [], as_of=date(2026, 7, 14))
    assert state["metrics"]["floor_cents"] == 2_500_00


def test_qbo_keeps_face_and_source_open_amount_separate() -> None:
    event = _invoice_to_event({
        "Id": "7", "DocNumber": "INV-7", "TotalAmt": 1000,
        "Balance": 250, "DueDate": "2026-07-20", "CustomerRef": {"name": "Acme"},
    })
    assert event is not None
    assert event["amount_cents"] == 100_000
    assert event["source_open_amount_cents"] == 25_000
    assert event["source_status"] == "open"
    assert event["preserve_settlement_truth"] is True


def test_provider_terminal_does_not_close_unallocated_obligation() -> None:
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    with engine.begin() as conn:
        upsert_cash_event(conn, {
            "id": "bill", "source": "clickup", "source_id": "task-1",
            "record_kind": "obligation", "event_type": "outflow",
            "amount_cents": 100_00, "due_date": date(2026, 1, 1),
            "status": "overdue", "source_status": "open",
            "preserve_settlement_truth": True,
        })
        upsert_cash_event(conn, {
            "id": "bill", "source": "clickup", "source_id": "task-1",
            "record_kind": "obligation", "event_type": "outflow",
            "amount_cents": 100_00, "due_date": date(2026, 1, 1),
            "status": "paid", "source_status": "complete",
            "preserve_settlement_truth": True,
        })
    with engine.connect() as conn:
        status, source_status = conn.execute(text(
            "SELECT status, source_status FROM cash_events WHERE id='bill'"
        )).one()
    assert status == "overdue"
    assert source_status == "complete"
