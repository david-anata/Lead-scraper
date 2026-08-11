"""How much of one large recurring bill can be paid, and on which days.

The operator settles rent in several payments across a month and works out each
one by hand. Every number needed to do that already exists: the day by day
calendar of what leaves, the confirmed money arriving, the checking balance and
the cash floor. Nothing had ever put them together.

Why this is not "Safe to commit"
--------------------------------
That figure looks 14 days ahead, so it does not subtract the bills landing in
the back half of the month. Paying rent with it is how a comfortable week turns
into a short one on the 22nd. The walk here runs to the end of the month and
holds one rule: **no proposed payment may take any later day below the floor**,
not merely the day it is paid.

What it deliberately does not do
--------------------------------
It never moves money and nothing in this app can. It counts confirmed money in
only, because money owed but not arrived is a wish. It reserves for unconfirmed
bills as well as certain ones, because under-reserving bounces a payment while
over-reserving only means paying more next week.
"""

from __future__ import annotations

import calendar as _calendar
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Sequence

# Below this a separate instalment is noise rather than a plan.
MATERIAL_INSTALMENT_CENTS = 50_000
# Future calendar entries the operator has no firm bill for.
UNCONFIRMED_KINDS = frozenset({"history_warning"})
# Future calendar entries that represent a real dated obligation.
PLANNED_KINDS = frozenset({"planned", "history_planned"})


def _month_end(day: date) -> date:
    return day.replace(day=_calendar.monthrange(day.year, day.month)[1])


def _as_date(value: Any) -> date | None:
    """A plain date, whatever the database handed back.

    datetime is a subclass of date, so testing for date first returns the
    timestamp untouched and every later comparison against a plain date raises.
    That is what broke this against the live ledger while every local fixture,
    which used plain dates and ISO strings, passed.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _cents(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _vendor_of(row: Mapping[str, Any]) -> str:
    return str(row.get("vendor_or_customer") or row.get("name") or "").strip().lower()


def _same_vendor(left: str, right: str) -> bool:
    """Loose match, because the calendar and the ledger word a payee differently."""
    left, right = left.strip().lower(), right.strip().lower()
    if not left or not right:
        return False
    return left == right or left in right or right in left


def largest_recurring_outflow(rows: Sequence[Mapping[str, Any]], *, as_of: date) -> dict[str, Any]:
    """The vendor this is most worth doing for, when none is named.

    Chosen by what actually left the bank over the last three months rather than
    by a schedule, because a schedule can be stale and the bank cannot.
    """
    since = as_of - timedelta(days=92)
    totals: dict[str, int] = {}
    labels: dict[str, str] = {}
    for row in rows:
        if str(row.get("event_type") or "").lower() == "inflow":
            continue
        if str(row.get("status") or "").lower() not in {"posted", "matched", "paid"}:
            continue
        when = _as_date(row.get("effective_date") or row.get("due_date"))
        if when is None or when < since or when > as_of:
            continue
        vendor = _vendor_of(row)
        if not vendor:
            continue
        totals[vendor] = totals.get(vendor, 0) + _cents(row.get("amount_cents"))
        labels.setdefault(vendor, str(row.get("vendor_or_customer") or row.get("name") or ""))
    if not totals:
        return {}
    vendor = max(totals, key=lambda key: totals[key])
    # Three months of history, so a month is a third of it.
    return {"vendor_key": vendor, "vendor": labels[vendor], "monthly_cents": totals[vendor] // 3}


def _paid_this_month(
    rows: Sequence[Mapping[str, Any]], *, vendor_key: str, as_of: date
) -> int:
    start = as_of.replace(day=1)
    total = 0
    for row in rows:
        if str(row.get("event_type") or "").lower() == "inflow":
            continue
        if str(row.get("status") or "").lower() not in {"posted", "matched", "paid"}:
            continue
        when = _as_date(row.get("effective_date") or row.get("due_date"))
        if when is None or when < start or when > as_of:
            continue
        if _same_vendor(_vendor_of(row), vendor_key):
            total += _cents(row.get("amount_cents"))
    return total


def _paid_after_balance_date(
    rows: Sequence[Mapping[str, Any]], *, vendor_key: str, balance_as_of: date, as_of: date
) -> int:
    """Posted payments after an operator-confirmed balance reduce that balance.

    The starting day is excluded because the entered balance already includes
    everything known on that date. This prevents a same-day bank transaction
    from being subtracted twice.
    """
    total = 0
    for row in rows:
        if str(row.get("event_type") or "").lower() == "inflow":
            continue
        if str(row.get("status") or "").lower() not in {"posted", "matched", "paid"}:
            continue
        when = _as_date(row.get("effective_date") or row.get("due_date"))
        if when is None or when <= balance_as_of or when > as_of:
            continue
        if _same_vendor(_vendor_of(row), vendor_key):
            total += _cents(row.get("amount_cents"))
    return total


def _outgoings_by_day(
    calendar: Mapping[str, Any], *, vendor_key: str, as_of: date, horizon_end: date
) -> tuple[dict[date, int], dict[date, int]]:
    """What leaves each day, and how much of that nobody has confirmed.

    The chosen vendor's own future entries are excluded. Reserving for the rent
    and then proposing to pay the same rent would count it twice and halve every
    instalment.
    """
    outgoing: dict[date, int] = {}
    unconfirmed: dict[date, int] = {}
    for bucket in calendar.get("days") or []:
        when = _as_date(bucket.get("date"))
        if when is None or when <= as_of or when > horizon_end:
            continue
        for event in bucket.get("events") or []:
            kind = str(event.get("kind") or "")
            if kind not in PLANNED_KINDS and kind not in UNCONFIRMED_KINDS:
                continue
            if _same_vendor(_vendor_of(event), vendor_key):
                continue
            amount = _cents(event.get("amount_cents"))
            outgoing[when] = outgoing.get(when, 0) + amount
            if kind in UNCONFIRMED_KINDS:
                unconfirmed[when] = unconfirmed.get(when, 0) + amount
    return outgoing, unconfirmed


def _confirmed_incoming_by_day(
    rows: Sequence[Mapping[str, Any]], *, as_of: date, horizon_end: date
) -> dict[date, int]:
    """Money arriving that somebody has actually confirmed.

    Expected receipts are excluded on purpose. A plan built on money that has not
    been promised is a wish, and the failure mode is a bounced rent payment.
    """
    incoming: dict[date, int] = {}
    for row in rows:
        if str(row.get("event_type") or "").lower() != "inflow":
            continue
        if str(row.get("confidence") or "").lower() != "confirmed":
            continue
        if row.get("trend_inferred"):
            continue
        if str(row.get("status") or "").lower() in {"paid", "matched", "posted", "cancelled"}:
            continue
        when = _as_date(row.get("due_date"))
        if when is None or when <= as_of or when > horizon_end:
            continue
        amount = _cents(row.get("open_amount_cents") or row.get("amount_cents"))
        if amount > 0:
            incoming[when] = incoming.get(when, 0) + amount
    return incoming


def _headroom_by_day(
    *,
    spendable_cents: int,
    outgoing: Mapping[date, int],
    incoming: Mapping[date, int],
    floor_cents: int,
    as_of: date,
    horizon_end: date,
) -> tuple[dict[date, int], dict[date, int]]:
    """Balance after each day, and the most that could be paid on that day.

    The second is the whole point. What can be paid today is limited by the
    worst day still to come, not by today's balance, which is the mistake a 14
    day figure makes.
    """
    balance = int(spendable_cents)
    trajectory: dict[date, int] = {}
    day = as_of
    while day <= horizon_end:
        balance = balance - outgoing.get(day, 0) + incoming.get(day, 0)
        trajectory[day] = balance
        day += timedelta(days=1)

    headroom: dict[date, int] = {}
    worst_ahead = None
    for when in sorted(trajectory, reverse=True):
        worst_ahead = trajectory[when] if worst_ahead is None else min(worst_ahead, trajectory[when])
        headroom[when] = max(0, worst_ahead - int(floor_cents))
    return trajectory, headroom


def _reason_for(
    when: date, *, as_of: date, incoming: Mapping[date, int], outgoing: Mapping[date, int]
) -> str:
    if when == as_of:
        return "leaves your cushion untouched"
    arrived = incoming.get(when, 0)
    if arrived:
        return "after money due that day arrives"
    cleared = sum(outgoing.get(as_of + timedelta(days=offset), 0)
                  for offset in range(1, (when - as_of).days + 1))
    if cleared:
        return "once the bills before it have cleared"
    return "next point there is room"


def build_paydown_plan(
    *,
    calendar: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    spendable_cents: int,
    reserve_cents: int,
    floor_cents: int,
    vendor_key: str = "",
    vendor_label: str = "",
    monthly_cents: int = 0,
    authoritative_balance_cents: int | None = None,
    balance_as_of: date | None = None,
    emergency_floor_cents: int = 0,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Return dated instalments that never breach the floor on any later day."""
    as_of = as_of or date.today()
    horizon_end = _month_end(as_of)

    if not vendor_key:
        chosen = largest_recurring_outflow(rows, as_of=as_of)
        if not chosen:
            return {
                "status": "no_vendor",
                "message": "No repeating bill big enough to plan around yet.",
                "instalments": [],
            }
        vendor_key = chosen["vendor_key"]
        vendor_label = vendor_label or chosen["vendor"]
        monthly_cents = monthly_cents or chosen["monthly_cents"]

    paid = _paid_this_month(rows, vendor_key=vendor_key, as_of=as_of)
    paid_since_balance = 0
    if authoritative_balance_cents is not None and balance_as_of is not None:
        paid_since_balance = _paid_after_balance_date(
            rows, vendor_key=vendor_key, balance_as_of=balance_as_of, as_of=as_of
        )
        remaining = max(0, int(authoritative_balance_cents) - paid_since_balance)
        balance_basis = "operator_confirmed"
    else:
        remaining = max(0, int(monthly_cents) - paid)
        balance_basis = "estimated_from_monthly_payments"

    outgoing, unconfirmed = _outgoings_by_day(
        calendar, vendor_key=vendor_key, as_of=as_of, horizon_end=horizon_end
    )
    incoming = _confirmed_incoming_by_day(rows, as_of=as_of, horizon_end=horizon_end)
    _trajectory, headroom = _headroom_by_day(
        spendable_cents=spendable_cents, outgoing=outgoing, incoming=incoming,
        floor_cents=floor_cents, as_of=as_of, horizon_end=horizon_end,
    )

    instalments: list[dict[str, Any]] = []
    committed = 0
    for when in sorted(headroom):
        if committed >= remaining:
            break
        payable = min(headroom[when], remaining) - committed
        if payable <= 0:
            continue
        # A separate line for a small amount is noise, unless it completes the bill.
        if payable < MATERIAL_INSTALMENT_CENTS and committed + payable < remaining:
            continue
        instalments.append({
            "date": when,
            "amount_cents": payable,
            "why": _reason_for(when, as_of=as_of, incoming=incoming, outgoing=outgoing),
        })
        committed += payable

    reserved = sum(outgoing.values())
    unconfirmed_total = sum(unconfirmed.values())
    shortfall = max(0, remaining - committed)
    savings_unlock = min(shortfall, int(reserve_cents))

    return {
        "status": "ok" if instalments else "nothing_spare",
        "vendor": vendor_label or vendor_key.title(),
        "vendor_key": vendor_key,
        "as_of": as_of,
        "month_end": horizon_end,
        "monthly_cents": int(monthly_cents),
        "paid_this_month_cents": paid,
        "paid_since_balance_cents": paid_since_balance,
        "remaining_cents": remaining,
        "balance_basis": balance_basis,
        "balance_as_of": balance_as_of,
        "instalments": instalments,
        "planned_total_cents": committed,
        "shortfall_cents": shortfall,
        "reserved_cents": reserved,
        "unconfirmed_reserved_cents": unconfirmed_total,
        "floor_cents": int(floor_cents),
        "emergency_floor_cents": int(emergency_floor_cents),
        "spendable_cents": int(spendable_cents),
        "savings_available_cents": int(reserve_cents),
        "savings_would_unlock_cents": savings_unlock,
    }


def load_paydown_plan(
    *, rows: Iterable[Mapping[str, Any]] | None = None, as_of: date | None = None
) -> dict[str, Any]:
    """Convenience loader. Pass ``rows`` to share one ledger read with the page."""
    from sales_support_agent.services.cashflow.accounts_view import load_accounts_overview
    from sales_support_agent.services.cashflow.cash_calendar import (
        build_cash_calendar,
        load_cash_calendar,
    )
    from sales_support_agent.services.cashflow.obligations import list_obligations
    from sales_support_agent.services.cashflow.settings import get_paydown_settings

    today = as_of or date.today()
    ledger = list(rows) if rows is not None else list_obligations(limit=10_000)
    # The walk needs to reach month end, which is further than the calendar page
    # itself shows.
    horizon = max(1, (_month_end(today) - today).days + 1)
    try:
        calendar = build_cash_calendar(ledger, as_of=today, future_days=horizon)
    except Exception:
        calendar = load_cash_calendar(as_of=today)
    accounts = load_accounts_overview()
    configured = get_paydown_settings()
    return build_paydown_plan(
        calendar=calendar,
        rows=ledger,
        spendable_cents=int(accounts.get("spendable_cents") or 0),
        reserve_cents=int(accounts.get("reserve_cents") or 0),
        floor_cents=int(configured["cash_goal_cents"]),
        emergency_floor_cents=int(configured["emergency_floor_cents"]),
        vendor_key=str(configured["vendor_key"]),
        vendor_label=str(configured["vendor_label"]),
        monthly_cents=int(configured["monthly_cents"]),
        authoritative_balance_cents=int(configured["balance_cents"]),
        balance_as_of=_as_date(configured["balance_as_of"]),
        as_of=today,
    )
