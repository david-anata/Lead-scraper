"""The QuickBooks bookkeeping write path stops before risky mutations."""

from unittest.mock import Mock

import pytest
from datetime import date, datetime, timezone

from sqlalchemy import text

from sales_support_agent.models import database
from sales_support_agent.models.database import create_session_factory, init_database, insert_cash_event
from sales_support_agent.services.cashflow import qbo_bookkeeping


def _setup(monkeypatch):
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    monkeypatch.setattr(database, "engine", engine)
    return engine


def _purchase(engine, cid, name, category, subcategory):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=f"qbo-purchase-{cid}", source="qbo_bank",
            source_id=f"purchase-{cid}", record_kind="transaction",
            event_type="outflow", category=category, subcategory=subcategory,
            name=name, description=name, vendor_or_customer=name,
            amount_cents=1200, due_date=date(2026, 7, 29), status="posted",
            confidence="confirmed", bank_reference=cid, created_at=now, updated_at=now,
        )


def test_list_ready_for_qbo_only_returns_locally_filed_qbo_purchases(monkeypatch):
    engine = _setup(monkeypatch)
    _purchase(engine, "8", "Adobe", "software", "Uncategorized Expense")
    _purchase(engine, "9", "Rent", "rent", "Rent or Lease")

    rows = qbo_bookkeeping.list_ready_for_qbo()

    assert [row["id"] for row in rows] == ["qbo-purchase-8"]


def test_transfers_are_never_offered_as_quickbooks_expenses(monkeypatch):
    engine = _setup(monkeypatch)
    _purchase(engine, "8", "Transfer to Wise", "transfer", "Uncategorized Expense")

    assert qbo_bookkeeping.list_ready_for_qbo() == []


def test_preview_refuses_an_already_booked_purchase(monkeypatch):
    engine = _setup(monkeypatch)
    _purchase(engine, "8", "Adobe", "software", "Uncategorized Expense")
    monkeypatch.setattr(qbo_bookkeeping, "_connection", lambda settings: ("https://qbo", "realm", "token"))
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "Purchase": {
            "Id": "8", "Line": [{
                "AccountBasedExpenseLineDetail": {
                    "AccountRef": {"value": "44", "name": "Software"}
                }
            }]
        }
    }
    monkeypatch.setattr(qbo_bookkeeping.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(ValueError, match="already files this under Software"):
        qbo_bookkeeping.preview_writeback("qbo-purchase-8", object())


def test_preview_refuses_multiline_purchase(monkeypatch):
    engine = _setup(monkeypatch)
    _purchase(engine, "8", "Mixed purchase", "software", "Uncategorized Expense")
    monkeypatch.setattr(qbo_bookkeeping, "_connection", lambda settings: ("https://qbo", "realm", "token"))
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "Purchase": {"Id": "8", "Line": [
            {"AccountBasedExpenseLineDetail": {"AccountRef": {"name": "Uncategorized Expense"}}},
            {"AccountBasedExpenseLineDetail": {"AccountRef": {"name": "Uncategorized Expense"}}},
        ]}
    }
    monkeypatch.setattr(qbo_bookkeeping.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(ValueError, match="multiple or item-based lines"):
        qbo_bookkeeping.preview_writeback("qbo-purchase-8", object())


def test_confirm_records_before_and_after_evidence(monkeypatch):
    engine = _setup(monkeypatch)
    _purchase(engine, "8", "Adobe", "software", "Uncategorized Expense")
    purchase = {
        "Id": "8", "SyncToken": "1", "Line": [{
            "Id": "1", "Amount": 12,
            "AccountBasedExpenseLineDetail": {
                "AccountRef": {"value": "9", "name": "Uncategorized Expense"}
            },
        }],
    }
    monkeypatch.setattr(qbo_bookkeeping, "preview_writeback", lambda *args, **kwargs: {
        "event": {"amount_cents": 1200},
        "purchase": purchase,
        "accounts": [{"id": "44", "name": "Software"}],
    })
    monkeypatch.setattr(qbo_bookkeeping, "_connection", lambda settings: ("https://qbo", "realm", "token"))
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"Purchase": {"Id": "8", "SyncToken": "2"}}
    monkeypatch.setattr(qbo_bookkeeping.requests, "post", lambda *args, **kwargs: response)

    result = qbo_bookkeeping.confirm_writeback(
        "qbo-purchase-8", "44", settings=object(), actor="david@example.com",
    )

    assert result["account_name"] == "Software"
    with engine.connect() as connection:
        account = connection.execute(text(
            "SELECT subcategory FROM cash_events WHERE id='qbo-purchase-8'"
        )).scalar_one()
        audit = connection.execute(text(
            "SELECT action_type, actor FROM finance_action_audit"
        )).one()
    assert account == "Software"
    assert tuple(audit) == ("qbo_expense_categorized", "david@example.com")
