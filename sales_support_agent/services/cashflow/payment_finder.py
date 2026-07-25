"""Find the bank payment that actually settled an overdue bill.

The automatic matcher is deliberately strict: the payment must be within a week
of the due date and the vendor name has to be recognizable. A bill paid by check
weeks late, with a bank descriptor that names no vendor, can therefore never be
matched automatically, so it sits overdue forever and keeps inflating what looks
"required out".

This module is the operator-driven counterpart. It searches every bank source
(not just one), ignores the vendor name, and uses a wide date window, because a
human confirms each link. It proposes only; linking reuses the same audited,
undoable allocation path as the automatic matcher.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

BANK_SOURCES = ("plaid", "qbo_bank", "csv", "bank_csv")
DEFAULT_DAY_WINDOW = 45
DEFAULT_AMOUNT_TOLERANCE_BPS = 200  # 2%


def _as_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def find_payment_candidates(
    obligation_id: str,
    *,
    day_window: int = DEFAULT_DAY_WINDOW,
    amount_tolerance_bps: int = DEFAULT_AMOUNT_TOLERANCE_BPS,
    limit: int = 10,
) -> dict[str, Any]:
    """Return posted bank payments that could have settled this obligation.

    Ranked by amount closeness first, then date closeness. Vendor names are
    ignored on purpose: the whole point is to catch payments whose descriptor
    does not name the vendor.
    """
    with get_engine().connect() as connection:
        row = connection.execute(text("""
            SELECT id, name, vendor_or_customer, amount_cents, event_type,
                   due_date, effective_date, record_kind
            FROM cash_events WHERE id=:id
        """), {"id": obligation_id}).fetchone()
        if row is None:
            raise ValueError("obligation not found")
        obligation = dict(row._mapping)

        if obligation.get("record_kind") == "transaction":
            raise ValueError("that is a bank payment, not a bill")

        target = int(obligation.get("amount_cents") or 0)
        if target <= 0:
            return {"obligation": obligation, "candidates": [], "reason": "the bill has no amount"}

        due = _as_date(obligation.get("due_date")) or _as_date(obligation.get("effective_date"))
        tolerance = max(1, target * amount_tolerance_bps // 10_000)
        low, high = target - tolerance, target + tolerance

        placeholders = ",".join(f"'{source}'" for source in BANK_SOURCES)
        rows = connection.execute(text(f"""
            SELECT id, name, vendor_or_customer, amount_cents, source,
                   COALESCE(effective_date, due_date) AS paid_on
            FROM cash_events
            WHERE record_kind='transaction'
              AND event_type=:event_type
              AND LOWER(COALESCE(source,'')) IN ({placeholders})
              AND LOWER(COALESCE(status,'')) = 'posted'
              AND amount_cents BETWEEN :low AND :high
              AND id NOT IN (
                  SELECT transaction_event_id FROM settlement_allocations
                  WHERE transaction_event_id IS NOT NULL
              )
        """), {  # noqa: S608 - sources are a fixed internal allowlist
            "event_type": obligation.get("event_type") or "outflow",
            "low": low, "high": high,
        }).fetchall()

    candidates = []
    for raw in rows:
        candidate = dict(raw._mapping)
        paid_on = _as_date(candidate.get("paid_on"))
        day_gap = abs((paid_on - due).days) if (paid_on and due) else None
        if day_gap is not None and day_gap > day_window:
            continue
        amount = int(candidate.get("amount_cents") or 0)
        candidates.append({
            "transaction_id": str(candidate["id"]),
            "name": str(candidate.get("name") or candidate.get("vendor_or_customer") or "Payment"),
            "amount_cents": amount,
            "amount_gap_cents": abs(amount - target),
            "paid_on": paid_on.isoformat() if paid_on else "",
            "day_gap": day_gap,
            "source": str(candidate.get("source") or ""),
            "exact_amount": amount == target,
        })

    candidates.sort(key=lambda item: (
        item["amount_gap_cents"],
        item["day_gap"] if item["day_gap"] is not None else 10_000,
    ))
    return {
        "obligation": {
            "id": str(obligation["id"]),
            "name": str(obligation.get("name") or obligation.get("vendor_or_customer") or "Bill"),
            "amount_cents": target,
            "due_date": due.isoformat() if due else "",
        },
        "candidates": candidates[:limit],
        "day_window": day_window,
    }


def find_overdue_needing_payment(
    *, as_of: Optional[date] = None, min_days_overdue: int = 1, limit: int = 50,
) -> list[dict[str, Any]]:
    """Overdue bills that have no settlement yet, worst-overdue first.

    These are the ones inflating "required out": either they were paid by a
    route the matcher cannot see, or they genuinely never got paid.
    """
    as_of = as_of or date.today()
    cutoff = as_of - timedelta(days=min_days_overdue)
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT event.id, event.name, event.vendor_or_customer, event.amount_cents,
                   event.due_date, event.commitment_type, event.source
            FROM cash_events AS event
            WHERE event.record_kind <> 'transaction'
              AND event.event_type='outflow'
              AND LOWER(COALESCE(event.status,'')) IN ('planned','pending','overdue')
              AND COALESCE(event.amount_cents,0) > 0
              AND event.archived_at IS NULL
              AND event.due_date IS NOT NULL
              AND event.due_date <= :cutoff
              AND event.id NOT IN (
                  SELECT obligation_event_id FROM settlement_allocations
              )
            ORDER BY event.due_date
            LIMIT :limit
        """), {"cutoff": cutoff.isoformat(), "limit": limit}).fetchall()

    items = []
    for raw in rows:
        row = dict(raw._mapping)
        due = _as_date(row.get("due_date"))
        items.append({
            "id": str(row["id"]),
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Bill"),
            "amount_cents": int(row.get("amount_cents") or 0),
            "due_date": due.isoformat() if due else "",
            "days_overdue": (as_of - due).days if due else 0,
            "commitment_type": str(row.get("commitment_type") or ""),
            "source": str(row.get("source") or ""),
        })
    return items


def overdue_summary(*, as_of: Optional[date] = None) -> dict[str, Any]:
    """How much of the required-out figure is actually an overdue backlog."""
    items = find_overdue_needing_payment(as_of=as_of, limit=1000)
    return {
        "count": len(items),
        "amount_cents": sum(item["amount_cents"] for item in items),
        "oldest_days": max((item["days_overdue"] for item in items), default=0),
    }
