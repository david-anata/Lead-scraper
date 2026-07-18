from __future__ import annotations

from datetime import date

from sqlalchemy import text

from sales_support_agent.models.database import create_session_factory, init_database, upsert_cash_event
from sales_support_agent.services.cashflow.control import build_finance_control_state
from sales_support_agent.services.cashflow.qbo_sync import _invoice_to_event, _reconcile_invoice_balance_evidence
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


def test_qbo_invoice_balance_creates_source_allocation_without_bank_cash() -> None:
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    with engine.begin() as conn:
        upsert_cash_event(conn, {
            "id": "qbo-inv-7", "source": "qbo", "source_id": "qbo-inv-7",
            "event_type": "inflow", "amount_cents": 100_000,
            "due_date": date(2026, 7, 20), "status": "planned",
            "source_status": "open", "source_open_amount_cents": 100_000,
        })

    assert _reconcile_invoice_balance_evidence("qbo-inv-7", 25_000, note="QBO reports current invoice balance") is True
    state = build_finance_control_state([
        {"id": "balance", "source": "csv", "record_kind": "transaction", "event_type": "inflow", "status": "posted", "amount_cents": 0, "account_balance_cents": 200_000, "due_date": date(2026, 7, 14)},
        {"id": "qbo-inv-7", "source": "qbo", "event_type": "inflow", "amount_cents": 100_000, "due_date": date(2026, 7, 20), "status": "planned", "confidence": "confirmed", "source_open_amount_cents": 25_000},
    ], settlement_annotations={"qbo-inv-7": 75_000}, as_of=date(2026, 7, 14))
    assert state["metrics"]["cash_on_hand_cents"] == 200_000
    assert state["metrics"]["confirmed_incoming_cents"] == 25_000


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
