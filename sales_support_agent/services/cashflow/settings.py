"""Persisted Finance operator controls."""

from __future__ import annotations

from datetime import date, datetime
import json
from uuid import uuid4

from sqlalchemy import text

DEFAULT_SCOPE_KEY = "default"
DEFAULT_CASH_FLOOR_CENTS = 1_000_000
DEFAULT_EMERGENCY_FLOOR_CENTS = 0
DEFAULT_PAYDOWN_VENDOR_KEY = "boulder ranch"
DEFAULT_PAYDOWN_VENDOR_LABEL = "Boulder Ranch Property Management"
DEFAULT_PAYDOWN_MONTHLY_CENTS = 4_000_000
DEFAULT_PAYDOWN_BALANCE_CENTS = 3_000_000
DEFAULT_PAYDOWN_BALANCE_AS_OF = date(2026, 8, 11)


class CashFloorUnavailableError(RuntimeError):
    """Raised when the configured reserve cannot be loaded safely."""


def get_finance_settings(*, scope_key: str = DEFAULT_SCOPE_KEY, engine=None) -> dict:
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.utcnow()
    with db_engine.begin() as conn:
        row = conn.execute(text("""
            SELECT scope_key, cash_floor_cents, emergency_floor_cents,
                   paydown_vendor_key, paydown_vendor_label,
                   paydown_monthly_cents, paydown_balance_cents,
                   paydown_balance_as_of, active_actual_source,
                   updated_by, created_at, updated_at
            FROM finance_settings WHERE scope_key=:scope_key
        """), {"scope_key": scope_key}).fetchone()
        if row is None:
            conn.execute(text("""
                INSERT INTO finance_settings (
                    scope_key, cash_floor_cents, emergency_floor_cents,
                    paydown_vendor_key, paydown_vendor_label,
                    paydown_monthly_cents, paydown_balance_cents,
                    paydown_balance_as_of, active_actual_source,
                    updated_by, created_at, updated_at
                ) VALUES (
                    :scope_key, :cash_floor_cents, :emergency_floor_cents,
                    :paydown_vendor_key, :paydown_vendor_label,
                    :paydown_monthly_cents, :paydown_balance_cents,
                    :paydown_balance_as_of, 'csv', 'system', :now, :now
                )
            """), {
                "scope_key": scope_key,
                "cash_floor_cents": DEFAULT_CASH_FLOOR_CENTS,
                "emergency_floor_cents": DEFAULT_EMERGENCY_FLOOR_CENTS,
                "paydown_vendor_key": DEFAULT_PAYDOWN_VENDOR_KEY,
                "paydown_vendor_label": DEFAULT_PAYDOWN_VENDOR_LABEL,
                "paydown_monthly_cents": DEFAULT_PAYDOWN_MONTHLY_CENTS,
                "paydown_balance_cents": DEFAULT_PAYDOWN_BALANCE_CENTS,
                "paydown_balance_as_of": DEFAULT_PAYDOWN_BALANCE_AS_OF,
                "now": now,
            })
            row = conn.execute(text("""
                SELECT scope_key, cash_floor_cents, emergency_floor_cents,
                       paydown_vendor_key, paydown_vendor_label,
                       paydown_monthly_cents, paydown_balance_cents,
                       paydown_balance_as_of, active_actual_source,
                       updated_by, created_at, updated_at
                FROM finance_settings WHERE scope_key=:scope_key
            """), {"scope_key": scope_key}).one()
    return dict(row._mapping)


def get_paydown_settings(*, scope_key: str = DEFAULT_SCOPE_KEY, engine=None) -> dict:
    settings = get_finance_settings(scope_key=scope_key, engine=engine)
    return {
        "vendor_key": str(settings["paydown_vendor_key"] or DEFAULT_PAYDOWN_VENDOR_KEY),
        "vendor_label": str(settings["paydown_vendor_label"] or DEFAULT_PAYDOWN_VENDOR_LABEL),
        "monthly_cents": int(settings["paydown_monthly_cents"] or 0),
        "balance_cents": int(settings["paydown_balance_cents"] or 0),
        "balance_as_of": settings["paydown_balance_as_of"],
        "cash_goal_cents": int(settings["cash_floor_cents"] or 0),
        "emergency_floor_cents": int(settings["emergency_floor_cents"] or 0),
    }


def set_paydown_settings(
    *,
    balance_cents: int,
    balance_as_of: date,
    monthly_cents: int,
    cash_goal_cents: int,
    emergency_floor_cents: int = 0,
    vendor_key: str = DEFAULT_PAYDOWN_VENDOR_KEY,
    vendor_label: str = DEFAULT_PAYDOWN_VENDOR_LABEL,
    scope_key: str = DEFAULT_SCOPE_KEY,
    actor: str = "operator",
    engine=None,
) -> dict:
    """Persist the operator's authoritative payoff facts with an audit record."""
    amounts = (balance_cents, monthly_cents, cash_goal_cents, emergency_floor_cents)
    if any(int(value) < 0 for value in amounts):
        raise ValueError("Payoff amounts cannot be negative")
    if not str(vendor_label or "").strip():
        raise ValueError("Payee is required")
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.utcnow()
    evidence = {
        "vendor_key": str(vendor_key).strip().lower(),
        "vendor_label": str(vendor_label).strip(),
        "monthly_cents": int(monthly_cents),
        "balance_cents": int(balance_cents),
        "balance_as_of": balance_as_of.isoformat(),
        "cash_goal_cents": int(cash_goal_cents),
        "emergency_floor_cents": int(emergency_floor_cents),
    }
    with db_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO finance_settings (
                scope_key, cash_floor_cents, emergency_floor_cents,
                paydown_vendor_key, paydown_vendor_label,
                paydown_monthly_cents, paydown_balance_cents,
                paydown_balance_as_of, active_actual_source,
                updated_by, created_at, updated_at
            ) VALUES (
                :scope_key, :cash_goal_cents, :emergency_floor_cents,
                :vendor_key, :vendor_label, :monthly_cents, :balance_cents,
                :balance_as_of, 'csv', :actor, :now, :now
            )
            ON CONFLICT(scope_key) DO UPDATE SET
                cash_floor_cents=excluded.cash_floor_cents,
                emergency_floor_cents=excluded.emergency_floor_cents,
                paydown_vendor_key=excluded.paydown_vendor_key,
                paydown_vendor_label=excluded.paydown_vendor_label,
                paydown_monthly_cents=excluded.paydown_monthly_cents,
                paydown_balance_cents=excluded.paydown_balance_cents,
                paydown_balance_as_of=excluded.paydown_balance_as_of,
                updated_by=excluded.updated_by,
                updated_at=excluded.updated_at
        """), {**evidence, "scope_key": scope_key, "actor": actor, "now": now})
        conn.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id,
                actor, evidence_json, created_at
            ) VALUES (
                :id, :scope_key, 'rent_paydown_updated', 'finance_settings',
                :scope_key, :actor, :evidence, :now
            )
        """), {
            "id": uuid4().hex, "scope_key": scope_key, "actor": actor,
            "evidence": json.dumps(evidence), "now": now,
        })
    return get_paydown_settings(scope_key=scope_key, engine=db_engine)


def get_cash_floor_cents(*, scope_key: str = DEFAULT_SCOPE_KEY, engine=None) -> int:
    return int(get_finance_settings(scope_key=scope_key, engine=engine)["cash_floor_cents"])


def get_cash_floor_health(*, scope_key: str = DEFAULT_SCOPE_KEY, engine=None) -> dict:
    """Return an operator-safe diagnostic without inventing a fallback value."""
    try:
        floor_cents = get_cash_floor_cents(scope_key=scope_key, engine=engine)
    except Exception as exc:
        return {
            "available": False,
            "confidence": "low",
            "reason": "configured cash floor could not be loaded",
            "error_type": type(exc).__name__,
        }
    return {
        "available": True,
        "confidence": "confirmed",
        "cash_floor_cents": floor_cents,
    }


def set_cash_floor_cents(
    cash_floor_cents: int,
    *,
    scope_key: str = DEFAULT_SCOPE_KEY,
    actor: str = "operator",
    engine=None,
) -> dict:
    if int(cash_floor_cents) < 0:
        raise ValueError("cash floor cannot be negative")
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.utcnow()
    with db_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO finance_settings (
                scope_key, cash_floor_cents, active_actual_source,
                updated_by, created_at, updated_at
            ) VALUES (
                :scope_key, :cash_floor_cents, 'csv', :actor, :now, :now
            )
            ON CONFLICT(scope_key) DO UPDATE SET
                cash_floor_cents=excluded.cash_floor_cents,
                updated_by=excluded.updated_by,
                updated_at=excluded.updated_at
        """), {
            "scope_key": scope_key,
            "cash_floor_cents": int(cash_floor_cents),
            "actor": actor,
            "now": now,
        })
        conn.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id,
                actor, evidence_json, created_at
            ) VALUES (
                :id, :scope_key, 'cash_floor_updated', 'finance_settings',
                :scope_key, :actor, :evidence, :now
            )
        """), {
            "id": __import__("uuid").uuid4().hex,
            "scope_key": scope_key,
            "actor": actor,
            "evidence": __import__("json").dumps({"cash_floor_cents": int(cash_floor_cents)}),
            "now": now,
        })
    return get_finance_settings(scope_key=scope_key, engine=db_engine)


def resolve_cash_floor_cents(value: int | None, *, scope_key: str = DEFAULT_SCOPE_KEY) -> int:
    """Use an explicit override or require the persisted operator setting.

    Persistence failures must stop trusted calculations. Returning the default
    here could overstate safe-to-commit when the configured reserve is higher.
    """
    if value is not None:
        if int(value) < 0:
            raise ValueError("cash floor cannot be negative")
        return int(value)
    try:
        return get_cash_floor_cents(scope_key=scope_key)
    except RuntimeError as exc:
        # Pure calculation callers can run before application startup. This is
        # not a persistence failure because no database has been configured yet.
        if str(exc).startswith("Database engine not initialized."):
            return DEFAULT_CASH_FLOOR_CENTS
        raise CashFloorUnavailableError(
            "Configured cash floor is unavailable; Finance confidence is low."
        ) from exc
    except Exception as exc:
        raise CashFloorUnavailableError(
            "Configured cash floor is unavailable; Finance confidence is low."
        ) from exc
