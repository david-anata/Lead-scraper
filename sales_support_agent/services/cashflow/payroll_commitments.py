"""Project HR payroll into Finance without manufacturing bank settlement.

HR establishes what payroll is expected or required. Plaid and explicit
settlement allocations remain the only proof that the money left the bank.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from itertools import combinations
import json
from typing import Any

from sqlalchemy import inspect, text

from sales_support_agent.models.database import get_engine, upsert_cash_event

ACTIVE_STATUSES = frozenset({"draft", "processing", "partial", "completed"})


def _external_run_id(row: dict[str, Any]) -> str:
    return str(row.get("base44_id") or row.get("id") or "").strip()


def finance_payroll_id(run_id: str) -> str:
    """Return the stable Finance obligation identity for one HR payroll run."""
    return f"hr-payroll-{run_id}"[:64]


def _as_date(value: Any) -> date | None:
    """Normalize SQLite strings and native database date values."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def sync_hr_payroll_commitments(*, actor: str = "system") -> dict[str, int]:
    """Idempotently mirror HR obligations, never HR payment claims.

    The rows are projections/obligations in ``cash_events`` so existing
    settlement allocations can safely link one or many Plaid withdrawals.
    Removing an HR run archives its projection and restores historical-pattern
    eligibility without touching any posted transaction.
    """
    engine = get_engine()
    # Finance can be used against an older or narrowly initialized database
    # during restore/cutover checks. HR payroll is an additive source; its
    # absence must not break bank imports or other Finance reads.
    if not inspect(engine).has_table("hr_payroll_runs"):
        return {"synced": 0, "archived": 0}
    with engine.connect() as connection:
        runs = [dict(row._mapping) for row in connection.execute(text("""
            SELECT id, base44_id, pay_period_start, pay_period_end, pay_date,
                   status, total_gross_cents, total_net_cents, employee_count,
                   initiated_by, notes
            FROM hr_payroll_runs
            ORDER BY pay_date, id
        """)).fetchall()]
        existing_rows = [dict(row._mapping) for row in connection.execute(text("""
            SELECT id, source_id, amount_cents, due_date, status, source_status,
                   confidence, workflow_status, pay_priority, owner, archived_at
            FROM cash_events WHERE source='hr_payroll'
        """)).fetchall()]
    existing_by_id = {str(row["id"]): row for row in existing_rows}

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
        # ``upsert_cash_event`` canonicalizes planned obligations to pending.
        # Compare against that stored value so a stable read stays read-only.
        finance_status = "cancelled" if not active else "pending"
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
        current = existing_by_id.get(event_id)
        if active and str((current or {}).get("workflow_status") or "") in {
            "paid", "partially_paid",
        }:
            # A later HR read must never erase settlement truth established by
            # posted bank allocations.
            workflow_status = str(current["workflow_status"])
        desired = {
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
            }
        unchanged = bool(current) and all((
            str(current.get("source_id") or "") == run_id,
            int(current.get("amount_cents") or 0) == net,
            _as_date(current.get("due_date")) == _as_date(run.get("pay_date")),
            str(current.get("status") or "") == finance_status,
            str(current.get("source_status") or "") == hr_status,
            str(current.get("confidence") or "") == confidence,
            str(current.get("workflow_status") or "") == workflow_status,
            str(current.get("pay_priority") or "") == desired["pay_priority"],
            str(current.get("owner") or "") == desired["owner"],
            bool(current.get("archived_at")) == (not active),
        ))
        if unchanged:
            synced += 1
            continue
        with engine.begin() as connection:
            upsert_cash_event(connection, desired)
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

    existing_active = [
        str(row["id"]) for row in existing_rows if not row.get("archived_at")
    ]
    stale = [event_id for event_id in existing_active if event_id not in active_ids]
    if stale:
        with engine.begin() as connection:
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
    return [parsed for value in values if (parsed := _as_date(value)) is not None]


def reconcile_hr_payroll(*, actor: str = "plaid-sync") -> dict[str, Any]:
    """Allocate only one unambiguous exact set of posted payroll withdrawals."""
    from sales_support_agent.services.cashflow.settlements import create_settlement_allocation

    sync_hr_payroll_commitments(actor=actor)
    engine = get_engine()
    with engine.connect() as connection:
        obligations = [dict(row._mapping) for row in connection.execute(text("""
            SELECT event.id, event.source_id, event.amount_cents, event.due_date,
                   event.source_status,
                   COALESCE(SUM(allocation.amount_cents), 0) AS allocated_cents
            FROM cash_events AS event
            LEFT JOIN settlement_allocations AS allocation
              ON allocation.obligation_event_id=event.id
             AND allocation.reversed_allocation_id IS NULL
             AND NOT EXISTS (
               SELECT 1 FROM settlement_allocations AS reversal
               WHERE reversal.reversed_allocation_id=allocation.id
             )
            WHERE event.source='hr_payroll' AND event.archived_at IS NULL
              AND event.source_status IN ('processing','partial','completed')
            GROUP BY event.id, event.source_id, event.amount_cents,
                     event.due_date, event.source_status
        """)).fetchall()]
        transactions = [dict(row._mapping) for row in connection.execute(text("""
            SELECT event.id, event.amount_cents,
                   COALESCE(event.effective_date, event.due_date) AS paid_on,
                   event.name, event.description, event.vendor_or_customer,
                   COALESCE(SUM(allocation.amount_cents), 0) AS allocated_cents
            FROM cash_events AS event
            LEFT JOIN settlement_allocations AS allocation
              ON allocation.transaction_event_id=event.id
             AND allocation.reversed_allocation_id IS NULL
             AND NOT EXISTS (
               SELECT 1 FROM settlement_allocations AS reversal
               WHERE reversal.reversed_allocation_id=allocation.id
             )
            WHERE event.source='plaid' AND event.record_kind='transaction'
              AND event.event_type='outflow' AND event.status IN ('posted','matched')
              AND (
                event.category='payroll' OR LOWER(COALESCE(event.name,'')) LIKE '%payroll%'
                OR LOWER(COALESCE(event.description,'')) LIKE '%payroll%'
                OR LOWER(COALESCE(event.vendor_or_customer,'')) LIKE '%payroll%'
              )
            GROUP BY event.id, event.amount_cents, event.effective_date,
                     event.due_date, event.name, event.description,
                     event.vendor_or_customer
        """)).fetchall()]

    confirmed = 0
    reviews: list[dict[str, Any]] = []
    used_ids: set[str] = set()
    for obligation in obligations:
        remaining = max(0, int(obligation["amount_cents"] or 0) - int(obligation["allocated_cents"] or 0))
        if remaining == 0:
            continue
        due_day = _as_date(obligation["due_date"])
        candidates = []
        for transaction in transactions:
            if str(transaction["id"]) in used_ids:
                continue
            paid_day = _as_date(transaction["paid_on"])
            available = max(0, int(transaction["amount_cents"] or 0) - int(transaction["allocated_cents"] or 0))
            if due_day and paid_day and abs((paid_day - due_day).days) <= 5 and available > 0:
                candidates.append({**transaction, "available_cents": available, "paid_day": paid_day})
        # Keep matching deterministic and bounded. More than twelve nearby
        # withdrawals is itself review territory, not a safe automatic match.
        candidates.sort(key=lambda item: (item["paid_day"], str(item["id"])))
        candidates = candidates[:12]
        exact_sets = [
            group for size in range(1, min(len(candidates), 12) + 1)
            for group in combinations(candidates, size)
            if sum(int(item["available_cents"]) for item in group) == remaining
        ]
        if len(exact_sets) != 1:
            candidate_total = sum(int(item["available_cents"]) for item in candidates)
            if candidates:
                review = {
                    "payroll_run_id": str(obligation["source_id"]),
                    "expected_cents": remaining, "posted_candidate_cents": candidate_total,
                    "variance_cents": candidate_total - remaining,
                    "reason": (
                        "More than one payroll match is possible. Review the posted withdrawals."
                        if len(exact_sets) > 1 else
                        "HR payroll and posted bank withdrawals do not agree yet."
                    ),
                }
                reviews.append(review)
                with engine.begin() as connection:
                    connection.execute(text("""
                        UPDATE cash_events SET
                            match_status='review',
                            match_candidates_json=:candidates,
                            workflow_status='needs_review',
                            updated_at=:now
                        WHERE id=:id
                    """), {
                        "id": obligation["id"],
                        "candidates": json.dumps({
                            "reason": review["reason"],
                            "expected_cents": remaining,
                            "posted_candidate_cents": candidate_total,
                            "variance_cents": review["variance_cents"],
                            "transaction_ids": [str(item["id"]) for item in candidates],
                        }),
                        "now": datetime.now(timezone.utc),
                    })
            continue
        for transaction in exact_sets[0]:
            create_settlement_allocation(
                obligation_event_id=str(obligation["id"]),
                transaction_event_id=str(transaction["id"]),
                amount_cents=int(transaction["available_cents"]),
                allocation_date=transaction["paid_day"], source="plaid",
                confidence="confirmed",
                idempotency_key=f"hr-payroll:{obligation['id']}:{transaction['id']}",
                notes="Exact HR net payroll matched to posted Plaid withdrawal.",
            )
            used_ids.add(str(transaction["id"]))
            confirmed += 1
        with engine.begin() as connection:
            connection.execute(text("""
                UPDATE cash_events SET match_status='', match_candidates_json='[]',
                    workflow_status='paid', updated_at=:now
                WHERE id=:id
            """), {"id": obligation["id"], "now": datetime.now(timezone.utc)})
            already = connection.execute(text("""
                SELECT 1 FROM finance_action_audit
                WHERE action_type='hr_payroll_plaid_matched' AND entity_id=:id
                LIMIT 1
            """), {"id": obligation["id"]}).fetchone()
            if not already:
                connection.execute(text("""
                    INSERT INTO finance_action_audit (
                        id, scope_key, action_type, entity_type, entity_id,
                        actor, evidence_json, created_at
                    ) VALUES (
                        :id, 'default', 'hr_payroll_plaid_matched',
                        'hr_payroll_run', :entity_id, :actor, :evidence, :now
                    )
                """), {
                    "id": f"payroll-match-{obligation['id']}"[:64],
                    "entity_id": obligation["id"], "actor": actor,
                    "evidence": json.dumps({"allocation_count": len(exact_sets[0]), "amount_cents": remaining}),
                    "now": datetime.now(timezone.utc),
                })
    return {"confirmed_allocations": confirmed, "review_count": len(reviews), "reviews": reviews}


__all__ = [
    "active_hr_pay_dates", "finance_payroll_id", "reconcile_hr_payroll",
    "sync_hr_payroll_commitments",
]
