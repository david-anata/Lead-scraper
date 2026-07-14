from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.models import database
from sales_support_agent.models.database import Base, _register_models
from sales_support_agent.services.cashflow.income_decisions import (
    load_finance_source_connections,
    load_income_pattern_decisions,
    record_income_pattern_decision,
)


PATTERN_KEY = "0123456789abcdef"


@pytest.fixture()
def finance_engine(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _register_models()
    Base.metadata.create_all(bind=engine)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE quickbooks_tokens (
                id TEXT PRIMARY KEY,
                access_token TEXT NOT NULL DEFAULT '',
                refresh_token TEXT NOT NULL DEFAULT '',
                realm_id TEXT NOT NULL DEFAULT '',
                expires_at TEXT,
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
        """))
    monkeypatch.setattr(database, "engine", engine)
    return engine


@pytest.mark.parametrize(
    ("pattern_key", "decision"),
    [
        ("", "track_expected"),
        ("ABCDEF0123456789", "track_expected"),
        ("0123456789abcde", "track_expected"),
        (PATTERN_KEY, "review"),
        (PATTERN_KEY, "TRACK_EXPECTED"),
    ],
)
def test_decision_validation_is_strict(finance_engine, pattern_key, decision):
    with pytest.raises(ValueError):
        record_income_pattern_decision(pattern_key, decision, "qa@example.com", {})

    with finance_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM finance_action_audit")).scalar_one()
    assert count == 0


def test_load_returns_latest_decision_per_pattern(finance_engine):
    record_income_pattern_decision(
        PATTERN_KEY,
        "track_expected",
        "first@example.com",
        {"occurrences": 4},
    )
    record_income_pattern_decision(
        PATTERN_KEY,
        "exclude",
        "second@example.com",
        {"reason": "internal transfer"},
    )
    record_income_pattern_decision(
        "fedcba9876543210",
        "one_time",
        "second@example.com",
        {"reason": "launch payment"},
    )

    decisions = load_income_pattern_decisions()

    assert decisions[PATTERN_KEY]["decision"] == "exclude"
    assert decisions[PATTERN_KEY]["actor"] == "second@example.com"
    assert decisions[PATTERN_KEY]["evidence"] == {"reason": "internal transfer"}
    assert decisions["fedcba9876543210"]["decision"] == "one_time"


def test_audit_is_append_only_and_identical_retry_is_safe(finance_engine):
    first = record_income_pattern_decision(
        PATTERN_KEY,
        "track_expected",
        "qa@example.com",
        {"cadence_days": 14},
    )
    retry = record_income_pattern_decision(
        PATTERN_KEY,
        "track_expected",
        "qa@example.com",
        {"cadence_days": 14},
    )
    changed = record_income_pattern_decision(
        PATTERN_KEY,
        "one_time",
        "qa@example.com",
        {"cadence_days": 14},
    )

    with finance_engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT id, action_type, entity_type, entity_id, actor, evidence_json
            FROM finance_action_audit ORDER BY created_at, id
        """)).fetchall()

    assert first["created"] is True
    assert retry["created"] is False
    assert retry["audit_id"] == first["audit_id"]
    assert changed["created"] is True
    assert len(rows) == 2
    assert {row.action_type for row in rows} == {"income_pattern_decision_recorded"}
    assert {row.entity_type for row in rows} == {"income_pattern"}
    assert {json.loads(row.evidence_json)["decision"] for row in rows} == {
        "track_expected",
        "one_time",
    }


def test_out_of_order_retry_cannot_restore_stale_decision(finance_engine):
    first = record_income_pattern_decision(
        PATTERN_KEY, "track_expected", "qa@example.com", {}, request_id="request-first-001"
    )
    later = record_income_pattern_decision(
        PATTERN_KEY, "exclude", "qa@example.com", {}, request_id="request-later-002"
    )
    delayed_retry = record_income_pattern_decision(
        PATTERN_KEY, "track_expected", "qa@example.com", {}, request_id="request-first-001"
    )

    assert first["created"] is True
    assert later["created"] is True
    assert delayed_retry["created"] is False
    assert delayed_retry["audit_id"] == first["audit_id"]
    assert delayed_retry["decision"] == first["decision"]
    assert load_income_pattern_decisions()[PATTERN_KEY]["decision"] == "exclude"
    with finance_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM finance_action_audit")).scalar_one()
    assert count == 2


def test_recording_decision_does_not_mutate_cash_events(finance_engine):
    with finance_engine.connect() as connection:
        before = connection.execute(text("SELECT COUNT(*) FROM cash_events")).scalar_one()

    record_income_pattern_decision(
        PATTERN_KEY,
        "exclude",
        "qa@example.com",
        {"reason": "not revenue"},
    )

    with finance_engine.connect() as connection:
        after = connection.execute(text("SELECT COUNT(*) FROM cash_events")).scalar_one()
    assert before == after == 0


def test_source_connections_use_local_configuration_and_db_tokens(
    finance_engine, monkeypatch
):
    for name in (
        "CLICKUP_API_TOKEN",
        "CLICKUP_API_KEY",
        "CLICKUP_AP_LIST_ID",
        "CLICKUP_AR_LIST_ID",
        "QBO_CLIENT_ID",
        "QBO_CLIENT_SECRET",
        "QBO_REFRESH_TOKEN",
        "QBO_REALM_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = SimpleNamespace(
        clickup_api_token="clickup-secret",
        clickup_ap_list_id="ap-list",
        clickup_ar_list_id="",
        qbo_client_id="",
        qbo_client_secret="",
        qbo_refresh_token="",
        qbo_realm_id="",
    )
    with finance_engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO quickbooks_tokens (
                id, access_token, refresh_token, realm_id, created_at, updated_at
            ) VALUES (
                'singleton', 'sealed-access', 'sealed-refresh', 'realm', :now, :now
            )
        """), {"now": "2026-07-14T12:00:00+00:00"})

    connections = load_finance_source_connections(settings)

    assert connections["clickup"] == {
        "configured": True,
        "status": "configured",
        "configuration_source": "settings",
    }
    assert connections["qbo"] == {
        "connected": True,
        "status": "connected",
        "connection_source": "database",
    }
    assert "secret" not in json.dumps(connections).lower()
    assert "token" not in json.dumps(connections).lower()


def test_source_connections_accept_complete_qbo_env_fallback(finance_engine, monkeypatch):
    monkeypatch.setenv("QBO_CLIENT_ID", "client")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "secret")
    monkeypatch.setenv("QBO_REFRESH_TOKEN", "refresh")
    monkeypatch.setenv("QBO_REALM_ID", "realm")

    connections = load_finance_source_connections()

    assert connections["qbo"]["connected"] is True
    assert connections["qbo"]["connection_source"] == "environment"


def test_income_pattern_route_redirects_and_records_authenticated_actor():
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        admin_session_secret="test-secret",
        admin_cookie_name="admin_session",
        admin_session_ttl_hours=24,
    )
    app.include_router(cashflow_router)
    client = TestClient(app, follow_redirects=False)
    user = {
        "email": "finance@example.com",
        "is_superadmin": False,
        "permissions": {"finance"},
    }

    with patch(
        "sales_support_agent.services.auth_deps.get_session_user_from_request",
        return_value={"email": user["email"]},
    ), patch(
        "sales_support_agent.services.auth_deps.get_current_user",
        return_value=user,
    ), patch(
        "sales_support_agent.api.cashflow_router.get_current_user",
        return_value=user,
    ), patch(
        "sales_support_agent.services.cashflow.income_decisions.record_income_pattern_decision"
    ) as record:
        response = client.post(
            f"/admin/finances/income-patterns/{PATTERN_KEY}/decision",
            headers={"Idempotency-Key": "route-request-001"},
            data={
                "decision": "track_expected",
                "evidence": "Reviewed against deposits",
                "evidence_occurrences": "4",
            },
        )

    assert response.status_code == 303
    assert response.headers["location"].startswith("/admin/finances?flash=")
    assert "ok%3AIncome%20pattern%20decision%20recorded" in response.headers["location"]
    record.assert_called_once_with(
        PATTERN_KEY,
        "track_expected",
        "finance@example.com",
        {"note": "Reviewed against deposits", "occurrences": "4"},
        request_id="route-request-001",
    )


def test_income_pattern_route_redirects_validation_errors():
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        admin_session_secret="test-secret",
        admin_cookie_name="admin_session",
        admin_session_ttl_hours=24,
    )
    app.include_router(cashflow_router)
    client = TestClient(app, follow_redirects=False)
    user = {
        "email": "finance@example.com",
        "is_superadmin": False,
        "permissions": {"finance"},
    }

    with patch(
        "sales_support_agent.services.auth_deps.get_session_user_from_request",
        return_value={"email": user["email"]},
    ), patch(
        "sales_support_agent.services.auth_deps.get_current_user",
        return_value=user,
    ), patch(
        "sales_support_agent.api.cashflow_router.get_current_user",
        return_value=user,
    ):
        response = client.post(
            f"/admin/finances/income-patterns/{PATTERN_KEY}/decision",
            data={"decision": "review"},
        )

    assert response.status_code == 303
    assert "err%3Adecision%20must%20be%20one%20of" in response.headers["location"]
