from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from sales_support_agent.models import database
from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.models.database import Base, _register_models
from sales_support_agent.services.cashflow.savings_reviews import (
    load_savings_reviews,
    merge_savings_reviews,
    record_savings_review,
)


KEY = "a" * 64
EVIDENCE = "b" * 64


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
    return engine


def _opportunity(**overrides):
    item = {
        "opportunity_key": KEY,
        "evidence_hash": EVIDENCE,
        "display_name": "Design Tool",
        "normalized_merchant": "design_tool",
        "cadence": "monthly",
        "monthly_potential_cents": 24_900,
        "baseline_amount_cents": 24_900,
        "reason": "Stable recurring cost.",
        "limitations": ["Verify terms."],
        "evidence_dates": ["2026-05-01", "2026-06-01"],
    }
    item.update(overrides)
    return item


def test_keep_is_idempotent_and_suppresses_only_unchanged_evidence(finance_engine):
    first = record_savings_review(_opportunity(), "keep", "qa@example.com", request_id="keep-001")
    retry = record_savings_review(_opportunity(), "keep", "qa@example.com", request_id="keep-001")

    assert first["created"] is True
    assert retry["created"] is False
    reviews = load_savings_reviews()
    assert reviews[KEY]["state"] == "kept"
    view = {"opportunities": [_opportunity()], "headline": {"opportunity_count": 1}}
    suppressed = merge_savings_reviews(view, reviews)
    assert suppressed["opportunities"] == []
    assert suppressed["headline"]["recurring_monthly_potential_cents"] == 0
    changed = _opportunity(evidence_hash="c" * 64)
    assert merge_savings_reviews({"opportunities": [changed]}, reviews)["opportunities"][0]["opportunity_key"] == KEY


def test_follow_up_persists_task_reference_without_changing_cash_events(finance_engine):
    result = record_savings_review(
        _opportunity(), "follow_up", "qa@example.com", request_id="followup-001",
        clickup_task={"id": "task-1", "url": "https://app.clickup.com/t/task-1"},
    )

    assert result["state"] == "monitoring"
    with finance_engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM cash_events")).scalar_one() == 0
        event = connection.execute(text("SELECT event_type, next_state FROM finance_savings_review_events")).one()
    assert event.event_type == "savings_follow_up"
    assert event.next_state == "monitoring"
    assert load_savings_reviews()[KEY]["clickup_task_id"] == "task-1"


def test_invalid_or_unverified_realization_fails_closed(finance_engine):
    with pytest.raises(ValueError, match="Posted bank evidence"):
        record_savings_review(_opportunity(), "confirm_realized", "qa@example.com")
    with pytest.raises(ValueError, match="Savings opportunity is invalid"):
        record_savings_review(_opportunity(opportunity_key="bad"), "keep", "qa@example.com")


def test_monitoring_requires_later_reduced_posted_bank_charge(finance_engine):
    record_savings_review(_opportunity(), "follow_up", "qa@example.com", request_id="monitor-001")
    reviews = load_savings_reviews()
    view = {"opportunities": [_opportunity()]}
    unchanged = merge_savings_reviews(view, reviews, events=[], as_of=__import__("datetime").date.today())
    assert unchanged["opportunities"][0]["realization_ready"] is False

    # Move the review date behind the later posted evidence deterministically.
    with finance_engine.begin() as connection:
        connection.execute(text("UPDATE finance_savings_reviews SET updated_at='2026-06-01T00:00:00'"))
    reduced = merge_savings_reviews(
        view,
        load_savings_reviews(),
        events=[{
            "source": "csv", "status": "posted", "event_type": "outflow",
            "vendor_or_customer": "Design Tool", "amount_cents": 19_000,
            "due_date": "2026-07-01",
        }],
        as_of=__import__("datetime").date(2026, 7, 2),
    )
    assert reduced["opportunities"][0]["realization_ready"] is True


def test_review_route_records_confirmed_operator_action(monkeypatch):
    app = FastAPI()
    app.state.settings = type("Settings", (), {"admin_session_secret": "test", "admin_cookie_name": "admin", "admin_session_ttl_hours": 1})()
    app.include_router(cashflow_router)
    user = {"email": "finance@example.com", "is_superadmin": False, "permissions": {"finance"}}
    client = TestClient(app, follow_redirects=False)
    opportunity = _opportunity()
    calls = []

    monkeypatch.setattr("sales_support_agent.services.auth_deps.get_session_user_from_request", lambda request: {"email": user["email"]})
    monkeypatch.setattr("sales_support_agent.services.auth_deps.get_current_user", lambda request: user)
    monkeypatch.setattr("sales_support_agent.api.cashflow_router.get_current_user", lambda request: user)
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.savings_reviews.record_savings_review",
        lambda *args, **kwargs: calls.append((args, kwargs)) or {"created": True},
    )
    response = client.post(
        f"/admin/finances/savings/{KEY}/review",
        data={"action": "keep", "evidence_hash": EVIDENCE, "opportunity_json": __import__("json").dumps(opportunity)},
        headers={"Idempotency-Key": "savings-route-001"},
    )
    assert response.status_code == 303
    assert "Savings%20opportunity%20kept" in response.headers["location"]
    assert calls[0][0][:3] == (opportunity, "keep", "finance@example.com")
