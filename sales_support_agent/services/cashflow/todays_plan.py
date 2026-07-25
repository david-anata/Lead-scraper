"""Today's plan: what to pay, in what order, and whether checking covers it.

Reads open payables from the ledger, orders them, and walks a running total
against spendable checking to flag the first bill that is not covered and the
exact amount to move from savings. It only reads and advises; it never moves
money or changes a bill.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

OPEN_STATUSES = {"planned", "pending", "overdue"}
_PRIORITY_RANK = {"critical": 0, "required": 1, "review": 2, "flexible": 3}
_FAR_FUTURE = date(9999, 12, 31)


def _due_date(row: dict[str, Any]) -> Optional[date]:
    raw = row.get("due_date") or row.get("effective_date")
    if not raw:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def move_in_pay_order(event_id: str, direction: str) -> dict[str, Any]:
    """Move one bill up or down in the pay order and persist the whole order.

    The current on-screen order is written down first, so a single move never
    reshuffles anything else. Advice only: this changes the suggested order,
    never a payment.
    """
    from datetime import datetime, timezone

    from sqlalchemy import text

    from sales_support_agent.models.database import get_engine

    direction = str(direction or "").lower()
    if direction not in {"up", "down"}:
        raise ValueError("direction must be up or down")

    plan = build_todays_plan()
    ids = [item["id"] for item in plan["items"]]
    if event_id not in ids:
        raise ValueError("that bill is not in the current plan")

    index = ids.index(event_id)
    swap_with = index - 1 if direction == "up" else index + 1
    if swap_with < 0 or swap_with >= len(ids):
        return {"moved": False, "order": ids}
    ids[index], ids[swap_with] = ids[swap_with], ids[index]

    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        for position, row_id in enumerate(ids, start=1):
            connection.execute(text(
                "UPDATE cash_events SET manual_pay_order=:pos, updated_at=:now WHERE id=:id"
            ), {"pos": position, "now": now, "id": row_id})
    return {"moved": True, "order": ids}


def clear_manual_pay_order() -> int:
    """Drop the hand-set order and go back to the automatic one."""
    from datetime import datetime, timezone

    from sqlalchemy import text

    from sales_support_agent.models.database import get_engine

    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        result = connection.execute(text(
            "UPDATE cash_events SET manual_pay_order=NULL, updated_at=:now "
            "WHERE manual_pay_order IS NOT NULL"
        ), {"now": now})
    return int(result.rowcount or 0)


def build_todays_plan(*, order: str = "due", horizon_days: Optional[int] = None) -> dict[str, Any]:
    """Return the ordered pay list plus coverage and savings-shortfall math."""
    from sales_support_agent.services.cashflow.obligations import list_obligations
    from sales_support_agent.services.cashflow.accounts_view import spendable_cash_cents

    order = "priority" if str(order).lower() == "priority" else "due"
    spendable = int(spendable_cash_cents())

    bills: list[dict[str, Any]] = []
    for row in list_obligations(event_type="outflow", limit=1000):
        status = str(row.get("status") or "").lower()
        if status not in OPEN_STATUSES:
            continue
        amount = int(row.get("amount_cents") or 0)
        if amount <= 0:
            continue
        if str(row.get("match_status") or "").lower() == "duplicate":
            continue
        if str(row.get("source_status") or "").lower() == "probable_duplicate":
            continue
        bills.append(row)

    if horizon_days is not None:
        cutoff = date.today() + timedelta(days=horizon_days)
        bills = [b for b in bills if (_due_date(b) or _FAR_FUTURE) <= cutoff]

    # A hand-set order wins over any automatic order. Items the operator has
    # not positioned keep their automatic place behind the positioned ones.
    has_manual = any(b.get("manual_pay_order") is not None for b in bills)
    if has_manual:
        order = "manual"
        bills.sort(key=lambda b: (
            0 if b.get("manual_pay_order") is not None else 1,
            int(b.get("manual_pay_order") or 0),
            _due_date(b) or _FAR_FUTURE,
        ))
    elif order == "priority":
        bills.sort(key=lambda b: (
            _PRIORITY_RANK.get(str(b.get("pay_priority") or "review").lower(), 2),
            _due_date(b) or _FAR_FUTURE,
        ))
    else:
        bills.sort(key=lambda b: (_due_date(b) or _FAR_FUTURE))

    running = 0
    items: list[dict[str, Any]] = []
    first_uncovered_index: Optional[int] = None
    for index, bill in enumerate(bills):
        amount = int(bill.get("amount_cents") or 0)
        running += amount
        covered = running <= spendable
        if not covered and first_uncovered_index is None:
            first_uncovered_index = index
        due = _due_date(bill)
        items.append({
            "id": str(bill.get("id") or ""),
            "name": str(bill.get("name") or bill.get("vendor_or_customer") or "Payment"),
            "vendor_or_customer": str(bill.get("vendor_or_customer") or ""),
            "amount_cents": amount,
            "due_date": due.isoformat() if due else "",
            "status": str(bill.get("status") or ""),
            "pay_priority": str(bill.get("pay_priority") or "review"),
            "covered": covered,
        })

    for index, item in enumerate(items):
        item["position"] = index + 1
        item["is_first"] = index == 0
        item["is_last"] = index == len(items) - 1

    total_due = sum(int(b.get("amount_cents") or 0) for b in bills)
    shortfall = max(0, total_due - spendable)
    return {
        "order": order,
        "spendable_cents": spendable,
        "total_due_cents": total_due,
        "shortfall_cents": shortfall,
        "covered_all": shortfall == 0,
        "first_uncovered_index": first_uncovered_index,
        "items": items,
    }
