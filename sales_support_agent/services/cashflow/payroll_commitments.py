"""Project HR payroll into Finance without manufacturing bank settlement.

HR establishes what payroll is expected or required. Plaid and explicit
settlement allocations remain the only proof that the money left the bank.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import text

from sales_support_agent.models.database import get_engine, upsert_cash_event

ACTIVE_STATUSES = frozenset({"draft", "processing", "partial", "completed"})


def _external_run_id(row: dict[str, Any]) -> str:
    return str(row.get("base44_id") or row.get("id") or "").strip()


def finance_payroll_id(run_id: str) -> str:
    """Return the stable Finance obligation identity for one HR payroll run."""
    return f"hr-payroll-{run_id}"[:64]


def sync_hr_payroll_commitments(*, actor: str = "system") -> dict[str, int]:
    """Idempotently mirror HR obligations, never HR payment claims.

    The rows are projections/obligations in ``cash_events`` so existing
    settlement allocations can safely link one or many Plaid withdrawals.
    Removing an HR run archives its projection and restores historical-pattern
    eligibility without touching any posted transaction.
    """
    engine = get_engine()
    with engine.connect() as connection:
        runs = [dict(row._mapping) for row in connection.execute(text("""
            SELECT id, base44_id, pay_period_start, pay_period_end, pay_date,
                   status, total_gross_cents, total_net_cents, employee_count,
                   initiated_by, notes
            FROM hr_payroll_runs
            ORDER BY pay_date, id
        """)).fetchall()]

    active_ids: set[str] = set()
    synced = 0
    for run in runs:
        run_id = _external_run_id(run)
        if not run_id:
            continue
        event_id = finance_payroll_id(run_id)
        hr_status = str(run.get("status") or "draft").lower()
        active = hr_status in ACTIVE_STATUSES and run.get("pay_date") is not None
        if active:
            active_ids.add(event_id)
        finance_status = "cancelled" if not active else "planned"
        workflow_status = {
            "draft": "draft",
            "processing": "approved",
            "partial": "partially_paid",
            "completed": "approved",
            "failed": "cancelled",
        }.get(hr_status, "needs_review")
        confidence = "estimated" if hr_status == "draft" else "confirmed"
        gross = max(0, int(run.get("total_gross_cents") or 0))
        net = max(0, int(run.get("total_net_cents") or 0))
        note = (
            f"HR payroll authority. Status: {hr_status}. Gross payroll context: "
            f"{gross} cents. Employees: {max(0, int(run.get('employee_count') or 0))}. "
            "Paid requires allocated posted bank evidence."
        )
        with engine.begin() as connection:
            upsert_cash_event(connection, {
                "id": event_id,
                "source": "hr_payroll",
                "source_id": run_id,
                "record_kind": "obligation",
                "event_type": "outflow",
                "category": "payroll",
                "commitment_type": "payroll",
                "name": "Payroll",
                "vendor_or_customer": "Anata payroll",
                "description": "Upcoming payroll from HR",
                "amount_cents": net,
                "due_date": run.get("pay_date"),
                "status": finance_status,
                "source_status": hr_status,
                "confidence": confidence,
                "workflow_status": workflow_status,
                "pay_priority": "must_pay" if hr_status != "draft" else "review",
                "owner": str(run.get("initiated_by") or ""),
                "created_by": actor or "system",
                "notes": note,
                "preserve_settlement_truth": True,
                "archived_at": None if active else datetime.now(timezone.utc),
            })
            connection.execute(text("""
                UPDATE cash_events SET
                    commitment_type='payroll', workflow_status=:workflow_status,
                    owner=:owner, created_by=:created_by,
                    source_status=:source_status, pay_priority=:pay_priority,
                    archived_at=:archived_at
                WHERE id=:event_id
            """), {
                "workflow_status": workflow_status,
                "owner": str(run.get("initiated_by") or ""),
                "created_by": actor or "system",
                "source_status": hr_status,
                "pay_priority": "must_pay" if hr_status != "draft" else "review",
                "archived_at": None if active else datetime.now(timezone.utc),
                "event_id": event_id,
            })
        synced += 1

    with engine.begin() as connection:
        existing = [str(row[0]) for row in connection.execute(text("""
            SELECT id FROM cash_events
            WHERE source='hr_payroll' AND archived_at IS NULL
        """)).fetchall()]
        stale = [event_id for event_id in existing if event_id not in active_ids]
        if stale:
            params = {f"id_{index}": value for index, value in enumerate(stale)}
            placeholders = ", ".join(f":id_{index}" for index in range(len(stale)))
            params["now"] = datetime.now(timezone.utc)
            connection.execute(text(
                f"UPDATE cash_events SET archived_at=:now, updated_at=:now "
                f"WHERE id IN ({placeholders})"  # noqa: S608
            ), params)
    return {"synced": synced, "archived": len(stale)}


def active_hr_pay_dates() -> list[date]:
    """Dates where HR replaces a historical payroll prediction."""
    with get_engine().connect() as connection:
        values = connection.execute(text("""
            SELECT pay_date FROM hr_payroll_runs
            WHERE status IN ('draft', 'processing', 'partial', 'completed')
              AND pay_date IS NOT NULL
        """)).scalars().all()
    return [value if isinstance(value, date) else date.fromisoformat(str(value)[:10]) for value in values]


__all__ = ["active_hr_pay_dates", "finance_payroll_id", "sync_hr_payroll_commitments"]
