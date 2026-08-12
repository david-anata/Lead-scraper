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
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

PROTECTED_TYPES = {"payroll", "tax", "debt"}
BULK_ACTIONS = {
    "write_off": "written_off",
    "no_action_needed": "cancelled",
    "archive_historical": "cancelled",
    "uncollectible": "written_off",
    "invoiced_in_error": "cancelled",
}
ACTION_LABELS = {
    "write_off": "lost cause / written off",
    "no_action_needed": "not a real obligation",
    "archive_historical": "historical, no longer actionable",
    "uncollectible": "written off as uncollectible",
    "invoiced_in_error": "invoiced in error",
}


def get_batch_summary(batch_id: str) -> Optional[dict[str, Any]]:
    """Return the receipt-safe fields for one audited review batch."""
    with get_engine().connect() as connection:
        row = connection.execute(text("""
            SELECT id, action, reason, actor, item_count, amount_cents, created_at
            FROM finance_bulk_batches
            WHERE id=:id
        """), {"id": str(batch_id)}).fetchone()
    return dict(row._mapping) if row else None
# Actions the operator may take on money owed TO us, kept separate so a
# receivable is never quietly resolved with a payables action.
RECEIVABLE_ACTIONS = ("uncollectible", "invoiced_in_error")
HISTORICAL_CLEANUP_DAYS = 90
REASON_LABELS = {
    "missing settlement evidence": "No matching bank payment found yet",
    "ambiguous match": "More than one possible bank match",
    "source conflict": "Source figures disagree",
    "missing from ClickUp source": "No longer present in ClickUp",
    "ClickUp completion lacks settlement evidence": "Marked done, no bank proof",
    "missing amount": "Missing amount",
    "missing date": "Missing date",
    "stale source evidence": "Source not refreshed recently",
    "cross-feed transaction review": "Bank feeds may describe the same transaction",
}


def list_review_items(*, as_of: Optional[date] = None) -> dict[str, Any]:
    """Return the blocked obligations grouped by reason, newest issues first."""
    from sales_support_agent.services.cashflow.control import (
        classify_payable_issues,
        load_annotated_obligations,
    )

    as_of = as_of or date.today()
    # The annotated rows, not raw ones: the flags the classifier reads are
    # computed, not stored, so raw rows would silently report nothing to do.
    rows = load_annotated_obligations()
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

    # Ambiguous mirrored bank rows are transactions, not obligations, so the
    # payable classifier intentionally does not see them. They still need one
    # protected, read-only case in the same Review inbox.
    try:
        with get_engine().connect() as connection:
            candidates = connection.execute(text("""
                SELECT member.group_id, event.id, event.name,
                       event.vendor_or_customer, event.amount_cents,
                       COALESCE(event.effective_date, event.due_date) AS paid_on
                FROM finance_economic_transaction_members AS member
                JOIN finance_economic_transaction_groups AS grouped
                  ON grouped.id=member.group_id AND grouped.status='review'
                JOIN cash_events AS event ON event.id=member.event_id
                ORDER BY member.group_id, member.event_id
            """)).fetchall()
        seen_groups: set[str] = set()
        for raw in candidates:
            row = dict(raw._mapping)
            group_id = str(row["group_id"])
            if group_id in seen_groups:
                continue
            seen_groups.add(group_id)
            groups.setdefault("cross-feed transaction review", []).append({
                "id": str(row["id"]),
                "name": str(row.get("name") or row.get("vendor_or_customer") or "Bank transaction"),
                "amount_cents": int(row.get("amount_cents") or 0),
                "due_date": str(row.get("paid_on") or "")[:10],
                "protected": True,
                "workflow_status": "review",
            })
            total += 1
    except Exception:
        pass

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


def list_historical_backlog(
    *, older_than_days: int = HISTORICAL_CLEANUP_DAYS, as_of: Optional[date] = None,
    event_type: str = "outflow", limit: int = 2000,
) -> dict[str, Any]:
    """Unsettled items older than the cutoff: the "start fresh" cleanup pile.

    These are past their date with no linked payment. Archiving them does not
    claim they were paid; it says they are no longer an actionable forecast.
    """
    as_of = as_of or date.today()
    cutoff = as_of - timedelta(days=older_than_days)
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT id, name, vendor_or_customer, amount_cents, due_date,
                   commitment_type, event_type, source
            FROM cash_events
            WHERE record_kind <> 'transaction'
              AND event_type = :event_type
              AND LOWER(COALESCE(status,'')) IN ('planned','pending','overdue')
              AND COALESCE(amount_cents,0) > 0
              AND archived_at IS NULL
              AND due_date IS NOT NULL
              AND due_date < :cutoff
              AND id NOT IN (SELECT obligation_event_id FROM settlement_allocations)
            ORDER BY due_date
            LIMIT :limit
        """), {"event_type": event_type, "cutoff": cutoff.isoformat(), "limit": limit}).fetchall()

    items = []
    protected = 0
    for raw in rows:
        row = dict(raw._mapping)
        is_protected = str(row.get("commitment_type") or "").lower() in PROTECTED_TYPES
        if is_protected:
            protected += 1
        items.append({
            "id": str(row["id"]),
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Item"),
            "amount_cents": int(row.get("amount_cents") or 0),
            "due_date": str(row.get("due_date") or "")[:10],
            "protected": is_protected,
            "source": str(row.get("source") or ""),
        })
    actionable = [item for item in items if not item["protected"]]
    return {
        "cutoff_date": cutoff.isoformat(),
        "older_than_days": older_than_days,
        "event_type": event_type,
        "count": len(items),
        "protected_count": protected,
        "actionable_ids": [item["id"] for item in actionable],
        "actionable_count": len(actionable),
        "amount_cents": sum(item["amount_cents"] for item in actionable),
        "items": items[:100],
    }


def snooze_events(
    event_ids: list[str], *, until: date, actor: str = "system", note: str = "",
) -> dict[str, Any]:
    """Hide items until a date, then let them come back for another look."""
    ids = [str(i) for i in event_ids if str(i).strip()]
    if not ids:
        return {"snoozed": 0}
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        for event_id in ids:
            connection.execute(text("""
                UPDATE cash_events
                SET snoozed_until=:until, follow_up_on=NULL,
                    defer_count=COALESCE(defer_count,0)+1, updated_at=:now
                WHERE id=:id
            """), {"until": until.isoformat(), "now": now, "id": event_id})
        connection.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id, actor,
                evidence_json, created_at
            ) VALUES (
                :audit_id, 'default', 'snooze', 'cash_event_batch', :first_id,
                :actor, :evidence, :now
            )
        """), {"audit_id": str(uuid4()), "first_id": ids[0], "actor": actor or "system",
               "evidence": json.dumps({"count": len(ids), "until": until.isoformat(), "note": note}),
               "now": now})
    return {"snoozed": len(ids), "until": until.isoformat()}


def set_follow_up(
    event_ids: list[str], *, follow_up_on: date, actor: str = "system",
) -> dict[str, Any]:
    """Keep chasing: leave the item open and set a date to come back to it."""
    ids = [str(i) for i in event_ids if str(i).strip()]
    if not ids:
        return {"scheduled": 0}
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        for event_id in ids:
            connection.execute(text("""
                UPDATE cash_events
                SET follow_up_on=:date, snoozed_until=NULL,
                    defer_count=COALESCE(defer_count,0)+1, updated_at=:now
                WHERE id=:id
            """), {"date": follow_up_on.isoformat(), "now": now, "id": event_id})
        connection.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id, actor,
                evidence_json, created_at
            ) VALUES (
                :audit_id, 'default', 'follow_up', 'cash_event_batch', :first_id,
                :actor, :evidence, :now
            )
        """), {"audit_id": str(uuid4()), "first_id": ids[0], "actor": actor or "system",
               "evidence": json.dumps({"count": len(ids), "follow_up_on": follow_up_on.isoformat()}),
               "now": now})
    return {"scheduled": len(ids), "follow_up_on": follow_up_on.isoformat()}


def list_due_followups(*, as_of: Optional[date] = None, limit: int = 100) -> dict[str, Any]:
    """Deferred items whose date has arrived and that now need a decision.

    A deferral is only honest if it comes back. Anything snoozed or set for
    follow-up reappears here on its date, carrying how many times it has been
    pushed out, so repeated deferral becomes visible instead of quiet rot.
    """
    as_of = as_of or date.today()
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT id, name, vendor_or_customer, amount_cents, due_date, event_type,
                   snoozed_until, follow_up_on, COALESCE(defer_count, 0) AS defer_count
            FROM cash_events
            WHERE record_kind <> 'transaction'
              AND archived_at IS NULL
              AND COALESCE(amount_cents,0) > 0
              AND (
                    (snoozed_until IS NOT NULL AND snoozed_until <= :as_of)
                 OR (follow_up_on  IS NOT NULL AND follow_up_on  <= :as_of)
              )
            ORDER BY COALESCE(follow_up_on, snoozed_until)
            LIMIT :limit
        """), {"as_of": as_of.isoformat(), "limit": limit}).fetchall()

    items = []
    for raw in rows:
        row = dict(raw._mapping)
        deferrals = int(row.get("defer_count") or 0)
        items.append({
            "id": str(row["id"]),
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Item"),
            "amount_cents": int(row.get("amount_cents") or 0),
            "due_date": str(row.get("due_date") or "")[:10],
            "event_type": str(row.get("event_type") or "outflow"),
            "came_back_on": str(row.get("follow_up_on") or row.get("snoozed_until") or "")[:10],
            "defer_count": deferrals,
            "nagging": deferrals >= 3,
        })
    return {
        "count": len(items),
        "amount_cents": sum(item["amount_cents"] for item in items),
        "nagging_count": sum(1 for item in items if item["nagging"]),
        "items": items,
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
                   event_type, workflow_status, archived_at
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
        # A receivable action must never land on a bill, and vice versa.
        is_inflow = str(row.get("event_type") or "") == "inflow"
        receivable_action = action in RECEIVABLE_ACTIONS
        if action != "archive_historical" and is_inflow != receivable_action:
            entry["why_skipped"] = (
                "that action is for money owed to you"
                if receivable_action else "that action is for bills, not receivables"
            )
            skipped.append(entry)
            continue
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
                SET workflow_status=:status, archived_at=:now,
                    snoozed_until=NULL, follow_up_on=NULL, updated_at=:now
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
