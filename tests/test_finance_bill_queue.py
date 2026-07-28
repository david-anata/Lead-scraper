"""Acceptance guards for the table-based bill queue and vendor aliases."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from sales_support_agent.models import database
from sales_support_agent.models.database import Base, _register_models
from sales_support_agent.models.entities import CashEvent
from sales_support_agent.services.cashflow.bill_patterns import (
    _PATTERN_CACHE, list_bill_patterns,
)
from sales_support_agent.services.cashflow.bill_queue import (
    apply_queue_action, preview_combine, undo_queue_batch,
)
from sales_support_agent.services.cashflow.bookkeeping import group_needs_decision
from sales_support_agent.services.cashflow.vendor_aliases import (
    combine_vendor_keys, ensure_vendor_alias_schema, revoke_vendor_alias,
)


@pytest.fixture()
def finance_engine(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _register_models()
    Base.metadata.create_all(bind=engine)
    monkeypatch.setattr(database, "engine", engine)
    _PATTERN_CACHE.clear()
    return engine


def _payment(engine, event_id: str, vendor: str, days_ago: int, cents: int) -> None:
    day = date.today() - timedelta(days=days_ago)
    with Session(engine) as session:
        session.add(CashEvent(
            id=event_id, source="csv", source_id=event_id,
            record_kind="transaction", event_type="outflow", category="uncategorized",
            name=vendor, description=vendor, vendor_or_customer=vendor,
            amount_cents=cents, due_date=datetime.combine(day, datetime.min.time()),
            status="posted", confidence="confirmed",
        ))
        session.commit()
    _PATTERN_CACHE.clear()


def _two_patterns(engine) -> list[dict]:
    for i, age in enumerate((150, 120, 90, 60)):
        _payment(engine, f"a{i}", "Acme Hosting", age, 10_000)
        _payment(engine, f"b{i}", "Bright Payroll", age - 8, 20_000)
    return list_bill_patterns()["patterns"]


def test_bulk_is_one_transaction_and_one_recalculation(finance_engine, monkeypatch):
    patterns = _two_patterns(finance_engine)
    import sales_support_agent.services.cashflow.bill_queue as queue

    real = queue.list_bill_patterns
    calls = 0

    def counted():
        nonlocal calls
        calls += 1
        return real()

    monkeypatch.setattr(queue, "list_bill_patterns", counted)
    result = apply_queue_action(
        [row["pattern_key"] for row in patterns], "not_a_bill", actor="qa@example.com"
    )
    assert result["applied"] == 2
    # One validation read and exactly one post-commit recalculation.
    assert calls == 2
    with finance_engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM finance_action_audit WHERE action_type='bill_queue_batch_recorded'"
        )).scalar() == 1


def test_undo_restores_rows_after_bulk_answer(finance_engine):
    patterns = _two_patterns(finance_engine)
    result = apply_queue_action(
        [row["pattern_key"] for row in patterns], "not_a_bill", actor="qa"
    )
    assert list_bill_patterns()["counts"]["unreviewed"] == 0
    undo_queue_batch(result["batch_id"], actor="qa")
    assert list_bill_patterns()["counts"]["unreviewed"] == 2


def test_combine_recalculates_raw_history_instead_of_adding_projections(finance_engine):
    # Alternating descriptors are one real monthly vendor split by bank wording.
    for i, age in enumerate((165, 135, 105, 75)):
        _payment(finance_engine, f"a{i}", "North Star Media", age, 10_000)
    for i, age in enumerate((150, 120, 90, 60)):
        _payment(finance_engine, f"b{i}", "Northstar Services", age, 12_000)
    patterns = list_bill_patterns()["patterns"]
    preview = preview_combine([row["pattern_key"] for row in patterns])
    summed = sum(row["amount_cents"] for row in preview["before"])
    assert preview["after"]["amount_cents"] != summed
    assert "not added together" in preview["explanation"]


def test_alias_survives_new_bank_rows_and_revoke_restores_grouping(finance_engine):
    combine_vendor_keys(
        ["north star", "northstar services"], canonical_key="north star",
        canonical_name_value="North Star", actor="qa",
    )
    _payment(finance_engine, "new", "Northstar Services ACH 99881", 5, 5_000)
    groups = group_needs_decision()
    assert any(group["key"] == "north star" for group in groups["groups"])
    assert revoke_vendor_alias("northstar services", actor="qa")
    groups = group_needs_decision()
    assert any(group["key"].startswith("northstar services") for group in groups["groups"])


def test_historical_amounts_are_unchanged_by_aliases(finance_engine):
    _payment(finance_engine, "one", "Alpha Vendor", 10, 1_234)
    before = finance_engine.connect().execute(
        text("SELECT amount_cents FROM cash_events WHERE id='one'")
    ).scalar()
    combine_vendor_keys(
        ["alpha vendor", "alpha co"], canonical_key="alpha vendor",
        canonical_name_value="Alpha Vendor", actor="qa",
    )
    after = finance_engine.connect().execute(
        text("SELECT amount_cents FROM cash_events WHERE id='one'")
    ).scalar()
    assert before == after == 1_234


def test_alias_migration_is_idempotent(finance_engine):
    ensure_vendor_alias_schema(finance_engine)
    ensure_vendor_alias_schema(finance_engine)
    with finance_engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM finance_vendor_aliases"
        )).scalar() == 0
