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

    if order == "priority":
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
