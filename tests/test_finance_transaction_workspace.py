from __future__ import annotations

from datetime import datetime, timezone
import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from sales_support_agent.models import database
from sales_support_agent.models.database import Base, _register_models
from sales_support_agent.models.entities import CashEvent
from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.services.cashflow.transaction_workspace import (
    apply_preview,
    discard_draft,
    get_batch_receipt,
    get_finance_object,
    load_draft,
    list_saved_views,
    preview_changes,
    save_draft,
    save_view,
    search_finance,
    undo_batch,
)


@pytest.fixture()
def finance_engine(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _register_models()
    Base.metadata.create_all(bind=engine)
    monkeypatch.setattr(database, "engine", engine)
    monkeypatch.setenv("FINANCE_WORKSPACE_TOKEN_SECRET", "workspace-test-secret")
    return engine


def _cash_event(engine, event_id: str, name: str = "Elementor", category: str = "software"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        connection.execute(CashEvent.__table__.insert().values(
            id=event_id, source="plaid", source_id=event_id,
            record_kind="transaction", event_type="outflow", category=category,
            name=name, vendor_or_customer=name, amount_cents=9_900,
            status="posted", confidence="confirmed", created_at=now, updated_at=now,
        ))


def test_draft_is_encrypted_actor_scoped_and_replaceable(finance_engine):
    first = save_draft([
        {"object_type": "cash_event", "object_id": "tx-1", "action": "set_note", "value": "Review this"}
    ], actor="owner@example.com", engine=finance_engine)
    loaded = load_draft(actor="owner@example.com", engine=finance_engine)
    assert loaded["changes"][0]["value"] == "Review this"
    assert loaded["draft_revision"] == first["draft_revision"]
    assert load_draft(actor="someone@example.com", engine=finance_engine) is None
    with finance_engine.connect() as connection:
        sealed = connection.execute(text("SELECT sealed_payload FROM finance_workspace_drafts")).scalar_one()
    assert "Review this" not in sealed

    second = save_draft([], actor="owner@example.com", engine=finance_engine)
    assert second["draft_revision"] == first["draft_revision"] + 1
    assert discard_draft(actor="owner@example.com", engine=finance_engine) is True
    assert load_draft(actor="owner@example.com", engine=finance_engine) is None


def test_preview_protects_payroll_and_does_not_write(finance_engine):
    _cash_event(finance_engine, "pay-1", "Gusto Payroll", "payroll")
    preview = preview_changes([
        {"object_type": "cash_event", "object_id": "pay-1", "action": "set_savings_state", "value": "waste"}
    ], actor="owner@example.com", engine=finance_engine)
    assert preview["protected_count"] == 1
    assert preview["eligible_count"] == 0
    with finance_engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM finance_object_decisions")).scalar_one() == 0


def test_apply_is_atomic_idempotent_audited_and_undoable(finance_engine):
    _cash_event(finance_engine, "tx-1")
    _cash_event(finance_engine, "tx-2", "Canva")
    _cash_event(finance_engine, "tx-3", "Elementor")
    changes = [
        {"object_type": "cash_event", "object_id": "tx-1", "action": "set_savings_state", "value": "waste"},
        {"object_type": "cash_event", "object_id": "tx-1", "action": "set_note", "value": "Cancel before renewal"},
        {"object_type": "cash_event", "object_id": "tx-2", "action": "set_note", "value": "Check annual plan"},
    ]
    save_draft(changes, actor="owner@example.com", engine=finance_engine)
    preview = preview_changes(changes, actor="owner@example.com", draft_revision=1, engine=finance_engine)
    result = apply_preview(
        preview["preview_token"], actor="owner@example.com",
        idempotency_key="apply-001", reason="monthly trim", source_page="budget",
        engine=finance_engine,
    )
    assert result["applied"] == 3
    replay = apply_preview(
        "ignored-after-first-success", actor="owner@example.com",
        idempotency_key="apply-001", engine=finance_engine,
    )
    assert replay["idempotent_replay"] is True
    assert load_draft(actor="owner@example.com", engine=finance_engine) is None
    assert get_finance_object("cash_event", "tx-1", engine=finance_engine)["decision"]["savings_state"] == "waste"
    detail = get_finance_object("cash_event", "tx-1", engine=finance_engine)
    assert detail["payment_evidence"]["allocated_cents"] == 0
    assert detail["similar_transactions"]
    assert detail["source_identifiers"] == []
    receipt = get_batch_receipt(result["batch_id"], engine=finance_engine)
    assert receipt["applied_count"] == 3
    assert receipt["skipped_count"] == 0
    with finance_engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM finance_action_audit")).scalar_one() == 1

    undone = undo_batch(result["batch_id"], actor="owner@example.com", engine=finance_engine)
    assert undone["restored"] == 3
    assert get_finance_object("cash_event", "tx-1", engine=finance_engine)["decision"] == {}


def test_category_batch_updates_authoritative_transaction_and_undo_restores_it(finance_engine):
    _cash_event(finance_engine, "tx-category", category="other")
    changes = [
        {"object_type": "cash_event", "object_id": "tx-category", "action": "set_category", "value": "software"},
        {"object_type": "cash_event", "object_id": "tx-category", "action": "set_note", "value": "Confirmed subscription"},
    ]
    preview = preview_changes(changes, actor="owner@example.com", engine=finance_engine)
    result = apply_preview(
        preview["preview_token"], actor="owner@example.com",
        idempotency_key="category-001", source_page="bookkeeping",
        engine=finance_engine,
    )
    with finance_engine.connect() as connection:
        row = connection.execute(text("""
            SELECT category, status, amount_cents, source_id
            FROM cash_events WHERE id='tx-category'
        """)).fetchone()
    assert row.category == "software"
    assert row.status == "posted"
    assert row.amount_cents == 9_900
    assert row.source_id == "tx-category"

    undo_batch(result["batch_id"], actor="owner@example.com", engine=finance_engine)
    with finance_engine.connect() as connection:
        restored = connection.execute(text("SELECT category FROM cash_events WHERE id='tx-category'")).scalar_one()
    assert restored == "other"


def test_mark_transfer_updates_authoritative_category(finance_engine):
    _cash_event(finance_engine, "tx-transfer", category="other")
    preview = preview_changes([
        {"object_type": "cash_event", "object_id": "tx-transfer", "action": "mark_internal_transfer", "value": True},
    ], actor="owner@example.com", engine=finance_engine)
    result = apply_preview(
        preview["preview_token"], actor="owner@example.com",
        idempotency_key="transfer-001", source_page="bookkeeping",
        engine=finance_engine,
    )
    with finance_engine.connect() as connection:
        assert connection.execute(text("SELECT category FROM cash_events WHERE id='tx-transfer'" )).scalar_one() == "transfer"
    undo_batch(result["batch_id"], actor="owner@example.com", engine=finance_engine)


def test_stale_revision_is_reported_before_apply(finance_engine):
    _cash_event(finance_engine, "tx-1")
    first = preview_changes([
        {"object_type": "cash_event", "object_id": "tx-1", "action": "set_note", "value": "First"}
    ], actor="owner@example.com", engine=finance_engine)
    apply_preview(first["preview_token"], actor="owner@example.com", idempotency_key="first", engine=finance_engine)
    stale = preview_changes([
        {"object_type": "cash_event", "object_id": "tx-1", "action": "set_note", "value": "Stale", "expected_revision": 0}
    ], actor="owner@example.com", engine=finance_engine)
    # Zero intentionally means "no revision claim" for first-load compatibility.
    assert stale["eligible_count"] == 1
    conflict = preview_changes([
        {"object_type": "cash_event", "object_id": "tx-1", "action": "set_note", "value": "Stale", "expected_revision": 99}
    ], actor="owner@example.com", engine=finance_engine)
    assert conflict["conflict_count"] == 1


def _workspace_client(monkeypatch, finance_engine):
    app = FastAPI()
    app.state.settings = type("Settings", (), {
        "admin_session_secret": "test", "admin_cookie_name": "admin",
        "admin_session_ttl_hours": 1, "rbac_enabled": True,
    })()
    app.include_router(cashflow_router)
    user = {
        "email": "owner@example.com", "session_issued_at": "123",
        "is_superadmin": False, "permissions": {"finance"},
    }
    monkeypatch.setattr("sales_support_agent.services.auth_deps.get_session_user_from_request", lambda request: {"email": user["email"]})
    monkeypatch.setattr("sales_support_agent.services.auth_deps.get_current_user", lambda request: user)
    monkeypatch.setattr("sales_support_agent.api.cashflow_router.get_current_user", lambda request: user)
    monkeypatch.setattr("sales_support_agent.services.cashflow.finance_security.get_current_user", lambda request: user)
    monkeypatch.setenv("ADMIN_DASHBOARD_SESSION_SECRET", "browser-security-secret")
    return TestClient(app), user


def test_workspace_routes_expose_contract_and_enforce_csrf(monkeypatch, finance_engine):
    client, user = _workspace_client(monkeypatch, finance_engine)
    bootstrap = client.get("/admin/finances/api/workspace/bootstrap")
    assert bootstrap.status_code == 200
    token = bootstrap.json()["csrf_token"]
    assert token
    rejected = client.put(
        "/admin/finances/api/workspace/draft",
        json={"changes": []},
        headers={"Origin": "https://evil.example", "Sec-Fetch-Mode": "cors"},
    )
    assert rejected.status_code == 403
    accepted = client.put(
        "/admin/finances/api/workspace/draft",
        json={"changes": []},
        headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "cors", "X-CSRF-Token": token},
    )
    assert accepted.status_code == 200
    assert accepted.json()["state"] == "Draft"
    recovered = client.get("/admin/finances/api/workspace/bootstrap")
    assert recovered.status_code == 200
    assert recovered.json()["draft"]["updated_at"]


def test_shared_batch_updates_authoritative_savings_review(monkeypatch, finance_engine):
    key = "a" * 64
    opportunity = {
        "opportunity_key": key,
        "evidence_hash": "b" * 64,
        "display_name": "Elementor",
        "normalized_merchant": "elementor",
        "cadence": "annual",
        "monthly_potential_cents": 2_500,
        "baseline_amount_cents": 30_000,
        "reason": "Annual renewal",
        "evidence_dates": ["2026-06-01"],
    }
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.budgeting.load_budget_view",
        lambda: {"trim_items": [opportunity]},
    )
    changes = [
        {"object_type": "savings_opportunity", "object_id": key, "action": "set_note", "value": "Not in use"},
        {"object_type": "savings_opportunity", "object_id": key, "action": "set_savings_state", "value": "waste"},
    ]
    preview = preview_changes(changes, actor="owner@example.com", engine=finance_engine)
    result = apply_preview(
        preview["preview_token"], actor="owner@example.com",
        idempotency_key="savings-shared-001", source_page="budget",
        engine=finance_engine,
    )
    assert result["applied"] == 2
    with finance_engine.connect() as connection:
        row = connection.execute(text("""
            SELECT state, reason FROM finance_savings_reviews WHERE opportunity_key=:key
        """), {"key": key}).fetchone()
    assert row.state == "waste"
    assert row.reason == "Not in use"
    undo_batch(result["batch_id"], actor="owner@example.com", engine=finance_engine)
    with finance_engine.connect() as connection:
        restored = connection.execute(text("""
            SELECT state, reason FROM finance_savings_reviews WHERE opportunity_key=:key
        """), {"key": key}).fetchone()
    assert restored.state == "unknown"
    assert restored.reason == ""


def test_full_page_preview_apply_receipt_and_undo(monkeypatch, finance_engine):
    _cash_event(finance_engine, "tx-review")
    client, _ = _workspace_client(monkeypatch, finance_engine)
    bootstrap = client.get("/admin/finances/api/workspace/bootstrap").json()
    change = {"object_type": "cash_event", "object_id": "tx-review", "action": "set_note", "value": "Check contract"}
    saved = client.put(
        "/admin/finances/api/workspace/draft", json={"changes": [change]},
        headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "cors", "X-CSRF-Token": bootstrap["csrf_token"]},
    )
    assert saved.status_code == 200
    review = client.get("/admin/finances/workspace/review")
    assert review.status_code == 200
    assert "Review every change before saving" in review.text
    preview_token = re.search(r'name="preview_token" value="([^"]+)"', review.text).group(1)
    idempotency = re.search(r'name="idempotency_key" value="([^"]+)"', review.text).group(1)
    csrf = re.search(r'name="_csrf_token" value="([^"]+)"', review.text).group(1)
    applied = client.post(
        "/admin/finances/workspace/apply",
        data={"preview_token": preview_token, "idempotency_key": idempotency, "_csrf_token": csrf, "reason": "QA"},
        headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"},
        follow_redirects=False,
    )
    assert applied.status_code == 303
    receipt = client.get(applied.headers["location"])
    assert receipt.status_code == 200
    assert "Save receipt" in receipt.text
    assert "Check contract" in receipt.text


def test_global_search_covers_clean_and_raw_transaction_text(finance_engine):
    _cash_event(finance_engine, "tx-search", "Elementor")
    with finance_engine.begin() as connection:
        connection.execute(text("""
            UPDATE cash_events SET description='RAW ACH ELMTOR 8372', bank_reference='TRACE991'
            WHERE id='tx-search'
        """))
    assert search_finance("Elementor", engine=finance_engine)[0]["id"] == "tx-search"
    assert search_finance("ELMTOR", engine=finance_engine)[0]["id"] == "tx-search"
    assert search_finance("TRACE991", engine=finance_engine)[0]["id"] == "tx-search"


def test_named_views_are_actor_scoped_and_server_persisted(finance_engine):
    saved = save_view("Waste review", {"query": "elementor", "savings_state": "waste", "ignored": "no"}, actor="owner@example.com", engine=finance_engine)
    assert saved["definition"] == {"query": "elementor", "savings_state": "waste"}
    assert list_saved_views(actor="owner@example.com", engine=finance_engine)[0]["name"] == "Waste review"
    assert list_saved_views(actor="other@example.com", engine=finance_engine) == []
