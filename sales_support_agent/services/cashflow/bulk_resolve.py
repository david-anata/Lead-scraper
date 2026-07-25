"""Clear the review backlog in batches instead of one item at a time.

Two actions are offered, both archive-style and fully reversible:

* ``write_off``        - a lost cause; the obligation stops counting as spend.
* ``no_action_needed`` - it was never a real obligation (junk or duplicate).

Nothing is ever deleted. Every batch records each item's prior state so undo is
exact, writes an audit entry, and requires a reason. Protected commitments
(payroll, tax, debt) are excluded from bulk actions on purpose: writing off
payroll or tax by accident is not a recoverable mistake.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

PROTECTED_TYPES = {"payroll", "tax", "debt"}
BULK_ACTIONS = {
    "write_off": "written_off",
    "no_action_needed": "cancelled",
}
ACTION_LABELS = {
    "write_off": "lost cause / written off",
    "no_action_needed": "not a real obligation",
}
REASON_LABELS = {
    "missing settlement evidence": "No matching bank payment found yet",
    "ambiguous match": "More than one possible bank match",
    "source conflict": "Source figures disagree",
    "missing from ClickUp source": "No longer present in ClickUp",
    "ClickUp completion lacks settlement evidence": "Marked done, no bank proof",
    "missing amount": "Missing amount",
    "missing date": "Missing date",
    "stale source evidence": "Source not refreshed recently",
}


def list_review_items(*, as_of: Optional[date] = None) -> dict[str, Any]:
    """Return the blocked obligations grouped by reason, newest issues first."""
    from sales_support_agent.services.cashflow.control import classify_payable_issues
    from sales_support_agent.services.cashflow.obligations import list_obligations

    as_of = as_of or date.today()
    rows = list_obligations(limit=5000)
    by_id = {str(row.get("id")): row for row in rows}
    issues = classify_payable_issues(rows, as_of=as_of)

    groups: dict[str, list[dict[str, Any]]] = {}
    total = 0
    for event_id, reason in issues:
        row = by_id.get(event_id)
        if row is None:
            continue
        protected = str(row.get("commitment_type") or "").lower() in PROTECTED_TYPES
        groups.setdefault(reason, []).append({
            "id": event_id,
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Obligation"),
            "amount_cents": int(row.get("amount_cents") or 0),
            "due_date": str(row.get("due_date") or "")[:10],
            "protected": protected,
            "workflow_status": str(row.get("workflow_status") or ""),
        })
        total += 1

    ordered = sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True)
    return {
        "total": total,
        "groups": [
            {
                "reason": reason,
                "label": REASON_LABELS.get(reason, reason),
                "items": items,
                "count": len(items),
                "amount_cents": sum(item["amount_cents"] for item in items),
                "actionable_count": sum(1 for item in items if not item["protected"]),
            }
            for reason, items in ordered
        ],
    }


def preview_bulk_action(event_ids: list[str], action: str) -> dict[str, Any]:
    """Describe exactly what a bulk action would change. Writes nothing."""
    if action not in BULK_ACTIONS:
        raise ValueError("Unsupported bulk action")
    ids = [str(i) for i in event_ids if str(i).strip()]
    if not ids:
        return {"action": action, "eligible": [], "skipped_protected": [],
                "eligible_count": 0, "amount_cents": 0, "skipped_count": 0}

    placeholders = ",".join(f":id_{index}" for index, _ in enumerate(ids))
    params = {f"id_{index}": value for index, value in enumerate(ids)}
    with get_engine().connect() as connection:
        rows = connection.execute(text(f"""
            SELECT id, name, vendor_or_customer, amount_cents, commitment_type,
                   workflow_status, archived_at
            FROM cash_events WHERE id IN ({placeholders})
        """), params).fetchall()  # noqa: S608 - placeholders are generated, values bound

    eligible: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw._mapping)
        entry = {
            "id": str(row["id"]),
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Obligation"),
            "amount_cents": int(row.get("amount_cents") or 0),
        }
        if str(row.get("commitment_type") or "").lower() in PROTECTED_TYPES:
            entry["why_skipped"] = "protected (payroll, tax, or debt)"
            skipped.append(entry)
        elif row.get("archived_at"):
            entry["why_skipped"] = "already resolved"
            skipped.append(entry)
        else:
            eligible.append(entry)

    return {
        "action": action,
        "action_label": ACTION_LABELS[action],
        "eligible": eligible,
        "eligible_count": len(eligible),
        "amount_cents": sum(item["amount_cents"] for item in eligible),
        "skipped_protected": skipped,
        "skipped_count": len(skipped),
    }


def apply_bulk_action(
    event_ids: list[str], action: str, *, reason: str, actor: str = "system",
) -> dict[str, Any]:
    """Apply a bulk action, recording prior state so it can be undone."""
    if action not in BULK_ACTIONS:
        raise ValueError("Unsupported bulk action")
    reason = str(reason or "").strip()
    if not reason:
        raise ValueError("A reason is required")

    preview = preview_bulk_action(event_ids, action)
    eligible = preview["eligible"]
    if not eligible:
        return {"batch_id": "", "applied": 0, "skipped": preview["skipped_count"]}

    target_status = BULK_ACTIONS[action]
    batch_id = str(uuid4())
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        connection.execute(text("""
            INSERT INTO finance_bulk_batches (
                id, scope_key, action, reason, actor, item_count, amount_cents, created_at
            ) VALUES (:id, 'default', :action, :reason, :actor, :count, :amount, :now)
        """), {"id": batch_id, "action": action, "reason": reason,
               "actor": actor or "system", "count": len(eligible),
               "amount": preview["amount_cents"], "now": now})

        for item in eligible:
            current = connection.execute(text(
                "SELECT workflow_status, archived_at FROM cash_events WHERE id=:id"
            ), {"id": item["id"]}).fetchone()
            previous_status = str(current._mapping.get("workflow_status") or "") if current else ""
            previous_archived = current._mapping.get("archived_at") if current else None
            connection.execute(text("""
                INSERT INTO finance_bulk_batch_items (
                    id, batch_id, event_id, previous_workflow_status,
                    previous_archived_at, amount_cents, created_at
                ) VALUES (:id, :batch, :event, :prev_status, :prev_archived, :amount, :now)
            """), {"id": str(uuid4()), "batch": batch_id, "event": item["id"],
                   "prev_status": previous_status, "prev_archived": previous_archived,
                   "amount": item["amount_cents"], "now": now})
            connection.execute(text("""
                UPDATE cash_events
                SET workflow_status=:status, archived_at=:now, updated_at=:now
                WHERE id=:id
            """), {"status": target_status, "now": now, "id": item["id"]})

        connection.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id, actor,
                evidence_json, created_at
            ) VALUES (
                :audit_id, 'default', :action_type, 'finance_bulk_batch', :batch_id,
                :actor, :evidence, :now
            )
        """), {"audit_id": str(uuid4()), "action_type": f"bulk_{action}",
               "batch_id": batch_id, "actor": actor or "system", "now": now,
               "evidence": json.dumps({
                   "reason": reason,
                   "item_count": len(eligible),
                   "amount_cents": preview["amount_cents"],
                   "skipped_count": preview["skipped_count"],
               })})

    return {
        "batch_id": batch_id,
        "applied": len(eligible),
        "skipped": preview["skipped_count"],
        "amount_cents": preview["amount_cents"],
    }


def latest_batch() -> Optional[dict[str, Any]]:
    with get_engine().connect() as connection:
        row = connection.execute(text("""
            SELECT * FROM finance_bulk_batches
            WHERE undone_at IS NULL AND item_count > 0
            ORDER BY created_at DESC LIMIT 1
        """)).fetchone()
    return dict(row._mapping) if row else None


def undo_batch(batch_id: str, *, actor: str = "system") -> dict[str, Any]:
    """Restore every obligation in a batch to its exact prior state."""
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        items = [
            dict(row._mapping) for row in connection.execute(text(
                "SELECT * FROM finance_bulk_batch_items WHERE batch_id=:batch"
            ), {"batch": batch_id}).fetchall()
        ]
        for item in items:
            connection.execute(text("""
                UPDATE cash_events
                SET workflow_status=:status, archived_at=:archived, updated_at=:now
                WHERE id=:id
            """), {"status": item.get("previous_workflow_status") or "",
                   "archived": item.get("previous_archived_at"),
                   "now": now, "id": str(item["event_id"])})
        connection.execute(
            text("UPDATE finance_bulk_batches SET undone_at=:now WHERE id=:id"),
            {"now": now, "id": batch_id},
        )
        connection.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id, actor,
                evidence_json, created_at
            ) VALUES (
                :audit_id, 'default', 'bulk_undo', 'finance_bulk_batch', :batch_id,
                :actor, :evidence, :now
            )
        """), {"audit_id": str(uuid4()), "batch_id": batch_id, "actor": actor or "system",
               "evidence": json.dumps({"restored": len(items)}), "now": now})
    return {"restored": len(items)}
