from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from sales_support_agent.models.database import Base, _register_models
from sales_support_agent.services.cashflow.settings import (
    CashFloorUnavailableError,
    get_cash_floor_cents,
    get_cash_floor_health,
    resolve_cash_floor_cents,
    set_cash_floor_cents,
)


def _engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _register_models()
    Base.metadata.create_all(bind=engine)
    return engine


def test_cash_floor_is_persisted_and_audited():
    engine = _engine()

    result = set_cash_floor_cents(275_000, actor="qa@example.com", engine=engine)

    assert result["cash_floor_cents"] == 275_000
    assert get_cash_floor_cents(engine=engine) == 275_000
    with engine.connect() as connection:
        audit = connection.exec_driver_sql(
            "SELECT actor, action_type FROM finance_action_audit"
        ).one()
    assert tuple(audit) == ("qa@example.com", "cash_floor_updated")


def test_cash_floor_rejects_negative_values():
    engine = _engine()

    with pytest.raises(ValueError, match="cannot be negative"):
        set_cash_floor_cents(-1, engine=engine)


def test_resolve_cash_floor_fails_closed_when_persistence_is_unavailable(monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.settings.get_cash_floor_cents",
        unavailable,
    )

    with pytest.raises(CashFloorUnavailableError, match="confidence is low"):
        resolve_cash_floor_cents(None)


def test_cash_floor_health_exposes_low_confidence_without_default(monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.settings.get_cash_floor_cents",
        unavailable,
    )

    status = get_cash_floor_health()

    assert status == {
        "available": False,
        "confidence": "low",
        "reason": "configured cash floor could not be loaded",
        "error_type": "RuntimeError",
    }
    assert "cash_floor_cents" not in status


def test_explicit_cash_floor_override_does_not_require_persistence(monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.settings.get_cash_floor_cents",
        unavailable,
    )

    assert resolve_cash_floor_cents(425_000) == 425_000


def test_pre_startup_calculation_uses_documented_baseline(monkeypatch):
    def not_initialized(*args, **kwargs):
        raise RuntimeError("Database engine not initialized. Call init first.")

    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.settings.get_cash_floor_cents",
        not_initialized,
    )

    assert resolve_cash_floor_cents(None) == 1_000_000
