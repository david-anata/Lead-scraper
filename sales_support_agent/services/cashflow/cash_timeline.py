"""The next 14 days as a running list: what comes in, what goes out, what is left.

Starts from spendable cash today and walks forward day by day, applying each
expected inflow and outflow so the balance after every item is visible. This is
the view that makes a wrong number obvious: if something in the list has already
been paid, it shows up here with its date and amount instead of hiding inside a
single total.

Overdue items are reported separately rather than being folded into the forward
walk, because their date has passed and pretending they land on a future day
would misstate the balance.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

OPEN_STATUSES = {"planned", "pending", "overdue"}


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


def build_cash_timeline(
    *, days: int = 14, as_of: Optional[date] = None,
) -> dict[str, Any]:
    """Return the running-balance timeline plus the overdue backlog summary."""
    from sales_support_agent.services.cashflow.accounts_view import spendable_cash_cents
    from sales_support_agent.services.cashflow.obligations import list_obligations
    from sales_support_agent.services.cashflow.payment_finder import overdue_summary

    as_of = as_of or date.today()
    end = as_of + timedelta(days=days - 1)

    try:
        opening_cents = int(spendable_cash_cents())
    except Exception:
        opening_cents = 0

    rows = list_obligations(limit=5000)
    entries: list[dict[str, Any]] = []
    for row in rows:
        if row.get("record_kind") == "transaction":
            continue
        if str(row.get("status") or "").lower() not in OPEN_STATUSES:
            continue
        if row.get("archived_at"):
            continue
        amount = int(row.get("amount_cents") or 0)
        if amount <= 0:
            continue
        if str(row.get("match_status") or "").lower() == "duplicate":
            continue
        if str(row.get("source_status") or "").lower() == "probable_duplicate":
            continue
        due = _as_date(row.get("due_date")) or _as_date(row.get("effective_date"))
        if due is None or due < as_of or due > end:
            continue
        is_inflow = str(row.get("event_type") or "") == "inflow"
        entries.append({
            "id": str(row.get("id") or ""),
            "date": due.isoformat(),
            "name": str(row.get("name") or row.get("vendor_or_customer") or ("Money in" if is_inflow else "Payment")),
            "vendor": str(row.get("vendor_or_customer") or ""),
            "direction": "in" if is_inflow else "out",
            "amount_cents": amount,
            "confidence": str(row.get("confidence") or ""),
            "source": str(row.get("source") or ""),
        })

    # Outflows before inflows on the same day: the conservative reading, so the
    # balance never looks healthier than it might actually be.
    entries.sort(key=lambda item: (item["date"], 0 if item["direction"] == "out" else 1))

    running = opening_cents
    lowest = opening_cents
    lowest_on = as_of.isoformat()
    total_in = total_out = 0
    for entry in entries:
        if entry["direction"] == "in":
            running += entry["amount_cents"]
            total_in += entry["amount_cents"]
        else:
            running -= entry["amount_cents"]
            total_out += entry["amount_cents"]
        entry["running_cents"] = running
        entry["negative"] = running < 0
        if running < lowest:
            lowest = running
            lowest_on = entry["date"]

    try:
        overdue = overdue_summary(as_of=as_of)
    except Exception:
        overdue = {"count": 0, "amount_cents": 0, "oldest_days": 0}

    return {
        "as_of": as_of.isoformat(),
        "days": days,
        "opening_cents": opening_cents,
        "closing_cents": running,
        "total_in_cents": total_in,
        "total_out_cents": total_out,
        "net_cents": total_in - total_out,
        "lowest_cents": lowest,
        "lowest_on": lowest_on,
        "goes_negative": lowest < 0,
        "entries": entries,
        "overdue": overdue,
    }
