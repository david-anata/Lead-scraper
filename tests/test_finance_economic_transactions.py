"""Mirrored feeds become one auditable economic transaction, never a guess."""

from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.models import database
from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.models.database import (
    _ensure_finance_settlement_tables,
    init_database,
    upsert_cash_event,
)
from sales_support_agent.services.cashflow.budgeting import build_budget_view
from sales_support_agent.services.cashflow.bulk_resolve import list_review_items
from sales_support_agent.services.cashflow.economic_transactions import (
    plan_cross_feed_groups,
    reconcile_cross_feed_transactions,
    undo_cross_feed_group,
)


@pytest.fixture()
def engine(monkeypatch):
    value = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    init_database(sessionmaker(bind=value, future=True))
    monkeypatch.setattr(database, "engine", value)
    return value


def _transaction(engine, event_id: str, source: str, description: str, amount: int = 1299):
    with engine.begin() as connection:
        upsert_cash_event(connection, {
            "id": event_id, "source": source, "source_id": event_id,
            "record_kind": "transaction", "event_type": "outflow",
            "category": "software", "name": description,
            "vendor_or_customer": description, "description": description,
            "amount_cents": amount, "due_date": date(2026, 8, 7),
            "effective_date": date(2026, 8, 7), "status": "posted",
            "confidence": "confirmed",
        })


def _rows(engine):
    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(text(
            "SELECT * FROM cash_events ORDER BY id"
        )).fetchall()]


def test_exact_cross_feed_copy_is_excluded_once_and_is_idempotent(engine):
    _transaction(engine, "plaid-1", "plaid", "Elementor")
    _transaction(engine, "qbo-1", "qbo_bank", "Elementor")

    preview = reconcile_cross_feed_transactions(dry_run=True, actor="qa")
    applied = reconcile_cross_feed_transactions(dry_run=False, actor="qa")
    repeated = reconcile_cross_feed_transactions(dry_run=False, actor="qa")

    assert preview == {
        "dry_run": True, "exact_group_count": 1, "review_group_count": 0,
        "duplicates_excluded": 1, "groups_written": 0,
    }
    assert applied["groups_written"] == 1
    assert repeated["groups_written"] == 0
    rows = _rows(engine)
    assert next(row for row in rows if row["id"] == "plaid-1")["match_status"] == ""
    assert next(row for row in rows if row["id"] == "qbo-1")["match_status"] == "duplicate"
    budget = build_budget_view(rows, as_of=date(2026, 8, 10))
    assert budget["transaction_count"] == 1


def test_same_source_equal_charges_are_never_auto_collapsed(engine):
    _transaction(engine, "plaid-1", "plaid", "UPS")
    _transaction(engine, "plaid-2", "plaid", "UPS")
    assert plan_cross_feed_groups()["exact_group_count"] == 0
    assert reconcile_cross_feed_transactions(dry_run=False)["groups_written"] == 0


def test_uncertain_cross_feed_pair_goes_to_review_without_exclusion(engine):
    _transaction(engine, "plaid-1", "plaid", "Klingler consulting")
    _transaction(engine, "csv-1", "csv", "Check 1042")

    result = reconcile_cross_feed_transactions(dry_run=False, actor="qa")

    assert result["exact_group_count"] == 0
    assert result["review_group_count"] == 1
    assert {row["match_status"] for row in _rows(engine)} == {"review"}
    assert all(row["source_status"] != "probable_duplicate" for row in _rows(engine))
    review = list_review_items(as_of=date(2026, 8, 10))
    group = next(item for item in review["groups"] if item["reason"] == "cross-feed transaction review")
    assert group["count"] == 1
    assert group["actionable_count"] == 0


def test_group_undo_restores_every_prior_classification(engine):
    _transaction(engine, "plaid-1", "plaid", "Elementor")
    _transaction(engine, "csv-1", "csv", "Elementor")
    reconcile_cross_feed_transactions(dry_run=False, actor="qa")
    with engine.connect() as connection:
        group_id = connection.execute(text(
            "SELECT id FROM finance_economic_transaction_groups"
        )).scalar_one()

    result = undo_cross_feed_group(group_id, actor="qa")

    assert result["restored"] == 2
    assert {row["match_status"] for row in _rows(engine)} == {""}
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT status FROM finance_economic_transaction_groups WHERE id=:id"
        ), {"id": group_id}).scalar_one() == "undone"


def test_existing_database_gets_additive_group_tables(engine):
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE finance_economic_transaction_members"))
        connection.execute(text("DROP TABLE finance_economic_transaction_groups"))
    _ensure_finance_settlement_tables(engine)
    tables = set(inspect(engine).get_table_names())
    assert "finance_economic_transaction_groups" in tables
    assert "finance_economic_transaction_members" in tables


def test_preview_apply_and_undo_work_through_the_real_router(engine):
    _transaction(engine, "plaid-1", "plaid", "Elementor")
    _transaction(engine, "csv-1", "csv", "Elementor")
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        admin_session_secret="secret", admin_cookie_name="session",
        admin_session_ttl_hours=24,
    )
    app.include_router(cashflow_router)
    user = {"email": "owner@anatainc.com", "is_superadmin": True, "permissions": {"finance"}}
    patches = (
        patch("sales_support_agent.services.auth_deps.get_session_user_from_request", return_value=user),
        patch("sales_support_agent.services.auth_deps.get_current_user", return_value=user),
        patch("sales_support_agent.api.cashflow_router.get_current_user", return_value=user),
    )
    for item in patches:
        item.start()
    try:
        client = TestClient(app)
        preview = client.get("/admin/finances/api/economic-transactions/preview")
        applied = client.post("/admin/finances/api/economic-transactions/apply")
        with engine.connect() as connection:
            group_id = connection.execute(text(
                "SELECT id FROM finance_economic_transaction_groups"
            )).scalar_one()
        undone = client.post(f"/admin/finances/api/economic-transactions/{group_id}/undo")
    finally:
        for item in patches:
            item.stop()

    assert preview.status_code == 200 and preview.json()["dry_run"] is True
    assert applied.status_code == 200 and applied.json()["groups_written"] == 1
    assert undone.status_code == 200 and undone.json()["restored"] == 2
