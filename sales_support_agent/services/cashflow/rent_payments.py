"""Audited rent-payment reports and Plaid reconciliation."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from typing import Any, Mapping, Sequence
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.services.cashflow.rent_paydown import _as_date, _cents, _same_vendor, _vendor_of


def report_rent_payment(
    *, amount_cents: int, reported_on: date, vendor_key: str,
    actor: str = "operator", scope_key: str = "default", engine=None,
) -> dict[str, Any]:
    """Record what the operator says they sent without manufacturing bank cash."""
    if int(amount_cents) <= 0:
        raise ValueError("Payment amount must be greater than zero")
    if not str(vendor_key or "").strip():
        raise ValueError("Rent payee is required")
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.utcnow()
    report_id = uuid4().hex
    evidence = {
        "amount_cents": int(amount_cents), "reported_on": reported_on.isoformat(),
        "vendor_key": str(vendor_key).strip().lower(), "status": "awaiting_bank",
    }
    with db_engine.begin() as conn:
        existing = conn.execute(text("""
            SELECT id, status FROM finance_rent_payment_reports
            WHERE scope_key=:scope_key AND vendor_key=:vendor_key
              AND amount_cents=:amount_cents AND reported_on=:reported_on
              AND status <> 'voided'
            ORDER BY created_at DESC LIMIT 1
        """), {**evidence, "scope_key": scope_key}).fetchone()
        if existing is not None:
            return {"id": existing.id, **evidence, "status": existing.status, "actor": actor}
        conn.execute(text("""
            INSERT INTO finance_rent_payment_reports (
                id, scope_key, vendor_key, amount_cents, reported_on, status,
                matched_transaction_id, actor, created_at, updated_at
            ) VALUES (
                :id, :scope_key, :vendor_key, :amount_cents, :reported_on,
                'awaiting_bank', '', :actor, :now, :now
            )
        """), {**evidence, "id": report_id, "scope_key": scope_key, "actor": actor, "now": now})
        conn.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id,
                actor, evidence_json, created_at
            ) VALUES (
                :audit_id, :scope_key, 'rent_payment_reported',
                'finance_rent_payment_report', :report_id, :actor, :evidence, :now
            )
        """), {
            "audit_id": uuid4().hex, "scope_key": scope_key, "report_id": report_id,
            "actor": actor, "evidence": json.dumps(evidence), "now": now,
        })
    return {"id": report_id, **evidence, "actor": actor}


def list_rent_payment_reports(*, scope_key: str = "default", engine=None) -> list[dict[str, Any]]:
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    with db_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, scope_key, vendor_key, amount_cents, reported_on, status,
                   matched_transaction_id, actor, created_at, updated_at
            FROM finance_rent_payment_reports
            WHERE scope_key=:scope_key AND status <> 'voided'
            ORDER BY reported_on DESC, created_at DESC
        """), {"scope_key": scope_key}).fetchall()
    return [dict(row._mapping) for row in rows]


def reconcile_rent_payment_reports(
    rows: Sequence[Mapping[str, Any]], *, scope_key: str = "default", as_of: date,
    engine=None,
) -> list[dict[str, Any]]:
    """Match reports to posted bank rows once and advance the saved balance."""
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.utcnow()
    with db_engine.begin() as conn:
        reports = conn.execute(text("""
            SELECT id, vendor_key, amount_cents, reported_on, status
            FROM finance_rent_payment_reports
            WHERE scope_key=:scope_key AND status IN ('awaiting_bank', 'needs_review')
            ORDER BY reported_on, created_at
        """), {"scope_key": scope_key}).fetchall()
        for report_row in reports:
            report = dict(report_row._mapping)
            reported_on = _as_date(report["reported_on"])
            match = None
            for row in rows:
                if str(row.get("event_type") or "").lower() == "inflow":
                    continue
                if str(row.get("status") or "").lower() not in {"posted", "matched", "paid"}:
                    continue
                when = _as_date(row.get("effective_date") or row.get("due_date"))
                if when is None or reported_on is None or abs((when - reported_on).days) > 7:
                    continue
                if _cents(row.get("amount_cents")) != int(report["amount_cents"]):
                    continue
                if not _same_vendor(_vendor_of(row), str(report["vendor_key"])):
                    continue
                match = (row, when)
                break
            if match:
                row, matched_on = match
                transaction_id = str(row.get("source_id") or row.get("id") or row.get("external_id") or "")
                settings = conn.execute(text("""
                    SELECT paydown_balance_cents, paydown_balance_as_of
                    FROM finance_settings WHERE scope_key=:scope_key
                """), {"scope_key": scope_key}).fetchone()
                if settings is not None:
                    new_balance = max(0, int(settings.paydown_balance_cents or 0) - int(report["amount_cents"]))
                    conn.execute(text("""
                        UPDATE finance_settings SET paydown_balance_cents=:balance,
                            paydown_balance_as_of=:matched_on, updated_at=:now
                        WHERE scope_key=:scope_key
                    """), {"balance": new_balance, "matched_on": matched_on, "now": now, "scope_key": scope_key})
                conn.execute(text("""
                    UPDATE finance_rent_payment_reports
                    SET status='bank_confirmed', matched_transaction_id=:transaction_id,
                        updated_at=:now WHERE id=:id AND status <> 'bank_confirmed'
                """), {"transaction_id": transaction_id, "now": now, "id": report["id"]})
                conn.execute(text("""
                    INSERT INTO finance_action_audit (
                        id, scope_key, action_type, entity_type, entity_id,
                        actor, evidence_json, created_at
                    ) VALUES (:id, :scope_key, 'rent_payment_bank_confirmed',
                        'finance_rent_payment_report', :entity_id, 'system', :evidence, :now)
                """), {
                    "id": uuid4().hex, "scope_key": scope_key, "entity_id": report["id"],
                    "evidence": json.dumps({"matched_transaction_id": transaction_id, "matched_on": matched_on.isoformat()}),
                    "now": now,
                })
            elif reported_on is not None and as_of > reported_on + timedelta(days=7):
                conn.execute(text("""
                    UPDATE finance_rent_payment_reports SET status='needs_review', updated_at=:now
                    WHERE id=:id AND status='awaiting_bank'
                """), {"now": now, "id": report["id"]})
    return list_rent_payment_reports(scope_key=scope_key, engine=db_engine)


def void_rent_payment_report(
    report_id: str, *, actor: str = "operator", scope_key: str = "default", engine=None,
) -> bool:
    """Void an unconfirmed report; confirmed bank evidence is immutable here."""
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.utcnow()
    with db_engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE finance_rent_payment_reports SET status='voided', actor=:actor, updated_at=:now
            WHERE id=:id AND scope_key=:scope_key AND status IN ('awaiting_bank', 'needs_review')
        """), {"id": report_id, "scope_key": scope_key, "actor": actor, "now": now})
    return bool(result.rowcount)
