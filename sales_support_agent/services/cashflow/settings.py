"""Persisted Finance operator controls."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text

DEFAULT_SCOPE_KEY = "default"
DEFAULT_CASH_FLOOR_CENTS = 1_000_000


class CashFloorUnavailableError(RuntimeError):
    """Raised when the configured reserve cannot be loaded safely."""


def get_finance_settings(*, scope_key: str = DEFAULT_SCOPE_KEY, engine=None) -> dict:
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.utcnow()
    with db_engine.begin() as conn:
        row = conn.execute(text("""
            SELECT scope_key, cash_floor_cents, active_actual_source,
                   updated_by, created_at, updated_at
            FROM finance_settings WHERE scope_key=:scope_key
        """), {"scope_key": scope_key}).fetchone()
        if row is None:
            conn.execute(text("""
                INSERT INTO finance_settings (
                    scope_key, cash_floor_cents, active_actual_source,
                    updated_by, created_at, updated_at
                ) VALUES (
                    :scope_key, :cash_floor_cents, 'csv', 'system', :now, :now
                )
            """), {
                "scope_key": scope_key,
                "cash_floor_cents": DEFAULT_CASH_FLOOR_CENTS,
                "now": now,
            })
            row = conn.execute(text("""
                SELECT scope_key, cash_floor_cents, active_actual_source,
                       updated_by, created_at, updated_at
                FROM finance_settings WHERE scope_key=:scope_key
            """), {"scope_key": scope_key}).one()
    return dict(row._mapping)


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
