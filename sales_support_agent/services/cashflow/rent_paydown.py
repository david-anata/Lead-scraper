"""How much of one large recurring bill can be paid, and on which days.

The operator settles rent in several payments across a month and works out each
one by hand. Every number needed to do that already exists: the day by day
calendar of what leaves, the confirmed money arriving, the checking balance and
the cash floor. Nothing had ever put them together.

The decision window runs through the end of next calendar week. Required and
confirmed bills reduce the envelope. Historical warnings stay visible as risk,
but do not silently turn into money that must be reserved.

What it deliberately does not do
--------------------------------
It never moves money and nothing in this app can. Future receivables do not fund
the pay-now amount, because money owed but not arrived is not cash.
"""

from __future__ import annotations

import calendar as _calendar
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Sequence

# Below this a separate instalment is noise rather than a plan.
MATERIAL_INSTALMENT_CENTS = 50_000
MAX_CASH_BALANCE_AGE_DAYS = 1
# Future calendar entries the operator has no firm bill for.
UNCONFIRMED_KINDS = frozenset({"history_warning"})
# Future calendar entries that represent a real dated obligation.
PLANNED_KINDS = frozenset({"planned", "history_planned"})


def _month_end(day: date) -> date:
    return day.replace(day=_calendar.monthrange(day.year, day.month)[1])


def _end_of_next_week(day: date) -> date:
    """Sunday at the end of the calendar week after ``day``."""
    return day + timedelta(days=(6 - day.weekday()) + 7)


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


def _cash_balance_is_fresh(balance_as_of: Any, *, today: date) -> bool:
    """Accept today or yesterday; older or undated cash cannot support advice."""
    balance_day = _as_date(balance_as_of)
    return bool(
        balance_day is not None
        and balance_day <= today
        and (today - balance_day).days <= MAX_CASH_BALANCE_AGE_DAYS
    )


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
) -> tuple[dict[date, int], dict[date, int], int]:
    """What leaves each day, and how much of that nobody has confirmed.

    The chosen vendor's own future entries are excluded. Reserving for the rent
    and then proposing to pay the same rent would count it twice and halve every
    instalment.
    """
    outgoing: dict[date, int] = {}
    unconfirmed: dict[date, int] = {}
    excluded_vendor_cents = max(0, int(calendar.get("suppressed_rent_cents") or 0))
    for bucket in calendar.get("days") or []:
        when = _as_date(bucket.get("date"))
        if when is None or when < as_of or when > horizon_end:
            continue
        for event in bucket.get("events") or []:
            kind = str(event.get("kind") or "")
            if kind not in PLANNED_KINDS and kind not in UNCONFIRMED_KINDS:
                continue
            if _same_vendor(_vendor_of(event), vendor_key):
                excluded_vendor_cents += _cents(event.get("amount_cents"))
                continue
            amount = _cents(event.get("amount_cents"))
            if kind in UNCONFIRMED_KINDS:
                unconfirmed[when] = unconfirmed.get(when, 0) + amount
            else:
                outgoing[when] = outgoing.get(when, 0) + amount
    return outgoing, unconfirmed, excluded_vendor_cents


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
    cash_goal_cents: int | None = None,
    vendor_key: str = "",
    vendor_label: str = "",
    monthly_cents: int = 0,
    authoritative_balance_cents: int | None = None,
    balance_as_of: date | None = None,
    emergency_floor_cents: int = 0,
    pending_reports: Sequence[Mapping[str, Any]] = (),
    as_of: date | None = None,
) -> dict[str, Any]:
    """Return dated instalments that never breach the floor on any later day."""
    as_of = as_of or date.today()
    horizon_end = _end_of_next_week(as_of)

    calculation_id = str(calendar.get("calculation_id") or "")
    calendar_end = _as_date(calendar.get("end"))
    if (
        str(calendar.get("history_status") or "ready") != "ready"
        or (calendar_end is not None and calendar_end < horizon_end)
    ):
        return {
            "status": "paused",
            "message": "Rent recommendation paused because not all upcoming expenses were included.",
            "reason": "Recurring expense history or the next-week calendar is unavailable.",
            "calculation_id": calculation_id,
            "instalments": [],
        }

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

    # Plaid and QuickBooks may mirror the same bank withdrawal. Use the same
    # canonical-source rule as the calendar so "sent" cannot double-count it.
    from sales_support_agent.services.cashflow.budgeting import _canonical_transactions

    _payment_source, canonical_payments = _canonical_transactions(rows, as_of=as_of)
    payment_rows = canonical_payments or list(rows)
    paid = _paid_this_month(payment_rows, vendor_key=vendor_key, as_of=as_of)
    paid_since_balance = 0
    if authoritative_balance_cents is not None and balance_as_of is not None:
        paid_since_balance = _paid_after_balance_date(
            payment_rows, vendor_key=vendor_key, balance_as_of=balance_as_of, as_of=as_of
        )
        remaining = max(0, int(authoritative_balance_cents) - paid_since_balance)
        balance_basis = "operator_confirmed"
    else:
        remaining = max(0, int(monthly_cents) - paid)
        balance_basis = "estimated_from_monthly_payments"

    outgoing, unconfirmed, excluded_vendor_cents = _outgoings_by_day(
        calendar, vendor_key=vendor_key, as_of=as_of, horizon_end=horizon_end
    )
    # Pay-now advice never relies on future receivables. Posted cash will raise
    # the next recommendation after it actually arrives.
    reserved = sum(outgoing.values())
    unconfirmed_total = sum(unconfirmed.values())
    pending_total = sum(
        int(item.get("amount_cents") or 0)
        for item in pending_reports
        if str(item.get("status") or "") in {"awaiting_bank", "needs_review"}
    )
    maximum = max(0, int(spendable_cents) - reserved - int(floor_cents) - pending_total)
    maximum = min(maximum, remaining)
    recommended = ((maximum * 90 // 100) // 10_000) * 10_000
    if recommended < MATERIAL_INSTALMENT_CENTS:
        recommended = 0
    instalments = ([{
        "date": as_of, "amount_cents": recommended,
        "why": "after protecting confirmed bills through next week",
    }] if recommended else [])
    shortfall = max(0, remaining - recommended - pending_total)
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
        "planned_total_cents": recommended,
        "maximum_payment_cents": maximum,
        "cushion_cents": max(0, maximum - recommended),
        "protection_start": as_of,
        "protection_end": horizon_end,
        "pending_payment_cents": pending_total,
        "pending_reports": list(pending_reports),
        "shortfall_cents": shortfall,
        "reserved_cents": reserved,
        "unconfirmed_reserved_cents": unconfirmed_total,
        "excluded_vendor_cents": excluded_vendor_cents,
        "floor_cents": int(floor_cents),
        "cash_goal_cents": int(cash_goal_cents if cash_goal_cents is not None else floor_cents),
        "emergency_floor_cents": int(emergency_floor_cents),
        "spendable_cents": int(spendable_cents),
        "savings_available_cents": int(reserve_cents),
        "savings_would_unlock_cents": savings_unlock,
        "calculation_id": calculation_id,
        "source_as_of": calendar.get("as_of"),
    }


def load_paydown_plan(
    *, rows: Iterable[Mapping[str, Any]] | None = None,
    calendar: Mapping[str, Any] | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Convenience loader. Pass ``rows`` to share one ledger read with the page."""
    from sales_support_agent.services.cashflow.accounts_view import load_accounts_overview
    from sales_support_agent.services.cashflow.cash_calendar import load_cash_calendar
    from sales_support_agent.services.cashflow.obligations import list_obligations
    from sales_support_agent.services.cashflow.settings import get_paydown_settings

    today = as_of or date.today()
    ledger = list(rows) if rows is not None else list_obligations(limit=10_000)
    # The walk needs to reach month end, which is further than the calendar page
    # itself shows.
    horizon = max(0, (_month_end(today) - today).days)
    if calendar is None:
        calendar = load_cash_calendar(as_of=today, rows=ledger, future_days=horizon)
    accounts = load_accounts_overview()
    configured = get_paydown_settings()
    if not _cash_balance_is_fresh(accounts.get("as_of"), today=today):
        return {
            "status": "paused",
            "message": "Rent recommendation paused because the bank balance is stale.",
            "reason": "Refresh Plaid accounts before deciding what to pay.",
            "calculation_id": str(calendar.get("calculation_id") or ""),
            "instalments": [],
        }
    from sales_support_agent.services.cashflow.rent_payments import reconcile_rent_payment_reports
    pending_reports = reconcile_rent_payment_reports(ledger, as_of=today)
    # Reconciliation may advance the authoritative saved balance.
    configured = get_paydown_settings()
    return build_paydown_plan(
        calendar=calendar,
        rows=ledger,
        spendable_cents=int(accounts.get("spendable_cents") or 0),
        reserve_cents=int(accounts.get("reserve_cents") or 0),
        # The goal is aspirational. Only the emergency floor constrains what
        # can safely be proposed for rent after committed payments.
        floor_cents=int(configured["emergency_floor_cents"]),
        cash_goal_cents=int(configured["cash_goal_cents"]),
        emergency_floor_cents=int(configured["emergency_floor_cents"]),
        pending_reports=pending_reports,
        vendor_key=str(configured["vendor_key"]),
        vendor_label=str(configured["vendor_label"]),
        monthly_cents=int(configured["monthly_cents"]),
        authoritative_balance_cents=int(configured["balance_cents"]),
        balance_as_of=_as_date(configured["balance_as_of"]),
        as_of=today,
    )
