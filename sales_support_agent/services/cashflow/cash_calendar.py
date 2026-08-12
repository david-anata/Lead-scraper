"""Evidence-safe daily expense calendar for the owner cash workflow.

The calendar joins three different truths without blurring them:

* posted bank outflows for the prior seven days and today;
* open, planned obligations due today or in the next fourteen days; and
* recurring bill patterns inferred from bank history.

Historical patterns are warnings only.  They are never added to planned cash
or described as confirmed bills until the operator explicitly tracks them.
"""

from __future__ import annotations

import html
import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from sales_support_agent.services.cashflow.budgeting import _canonical_transactions
from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.overview import _money, _page_shell

PAST_DAYS = 7
FUTURE_DAYS = 14
_OPEN_STATUSES = {"planned", "pending", "overdue", "completed"}
_PROTECTED_CATEGORIES = {
    "payroll", "tax", "debt", "rent", "insurance", "utilities", "manual_check"
}


def _operator_today() -> date:
    """Finance operates on the owner's Denver business day, not server UTC."""
    return datetime.now(ZoneInfo("America/Denver")).date()


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value or "")[:10])
    except (TypeError, ValueError):
        return None


def _name(row: Mapping[str, Any], fallback: str = "Expense") -> str:
    return str(
        row.get("friendly_name")
        or row.get("vendor_or_customer")
        or row.get("name")
        or fallback
    ).strip() or fallback


def _category(row: Mapping[str, Any]) -> str:
    return str(row.get("commitment_type") or row.get("category") or "other").lower()


def _same_historical_charge(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """Whether two projections are the same charge under different descriptors."""
    from sales_support_agent.services.cashflow.bill_patterns import bill_merchant_key

    left_due = _as_date(left.get("date") or left.get("due_date") or left.get("expected_date"))
    right_due = _as_date(right.get("date") or right.get("due_date") or right.get("expected_date"))
    if left_due is None or left_due != right_due:
        return False
    if int(left.get("amount_cents") or 0) != int(right.get("amount_cents") or 0):
        return False
    left_key = "".join(ch for ch in bill_merchant_key(_name(left)).lower() if ch.isalnum())
    right_key = "".join(ch for ch in bill_merchant_key(_name(right)).lower() if ch.isalnum())
    return bool(
        len(left_key) >= 5
        and len(right_key) >= 5
        and (left_key in right_key or right_key in left_key)
    )


def _active_allocation_maps(
    allocations: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    by_transaction: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_obligation: dict[str, int] = defaultdict(int)
    for source in allocations:
        row = dict(source)
        transaction_id = str(row.get("transaction_event_id") or "")
        obligation_id = str(row.get("obligation_event_id") or "")
        amount = max(0, int(row.get("amount_cents") or 0))
        if transaction_id:
            by_transaction[transaction_id].append(row)
        if obligation_id:
            by_obligation[obligation_id] += amount
    return dict(by_transaction), dict(by_obligation)


def _posted_history_match(
    row: Mapping[str, Any],
    *,
    occurred: date,
    patterns: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Find strong recurring-history evidence for one posted transaction.

    This is a classification, not a settlement. It requires the exact posted
    date and amount to appear in a very-likely recurring series, so a merely
    similar charge cannot disappear from the owner's review list.
    """
    from sales_support_agent.services.cashflow.bill_patterns import bill_merchant_key

    transaction_key = bill_merchant_key(_name(row))
    amount = int(row.get("amount_cents") or 0)
    if not transaction_key or amount <= 0:
        return None
    for source in patterns:
        pattern = dict(source)
        if int(pattern.get("confidence_bps") or 0) < 7_500:
            continue
        if pattern.get("paid_in_pieces"):
            # A monthly aggregate cannot prove which individual instalment was
            # expected, so leave those payments in review.
            continue
        known_keys = {
            str(pattern.get("merchant_key") or ""),
            bill_merchant_key(str(pattern.get("vendor") or "")),
        }
        evidence = list(pattern.get("evidence") or [])
        known_keys.update(
            bill_merchant_key(str(item.get("raw_descriptor") or ""))
            for item in evidence if isinstance(item, Mapping)
        )
        if transaction_key not in known_keys:
            continue
        for item in evidence:
            if not isinstance(item, Mapping):
                continue
            if (
                _as_date(item.get("due_date")) == occurred
                and int(item.get("amount_cents") or 0) == amount
            ):
                return pattern
    return None


def _day_label(day: date, as_of: date) -> tuple[str, str]:
    readable = day.strftime("%A, %b %d").replace(" 0", " ")
    if day == as_of:
        return "Today", readable
    if day == as_of - timedelta(days=1):
        return "Yesterday", readable
    if day == as_of + timedelta(days=1):
        return "Tomorrow", readable
    return day.strftime("%A"), day.strftime("%b %d").replace(" 0", " ")


def _date_label(day: date, *, weekday: str = "%A", month: str = "") -> str:
    """Format a date without platform-specific ``strftime`` directives."""
    parts = [day.strftime(weekday), str(day.day)]
    if month:
        parts.append(day.strftime(month))
    return " ".join(parts)


def _week_label(start: date, end: date, as_of: date) -> str:
    if start <= as_of <= end:
        return "This week"
    if start > as_of:
        return "Next week" if start <= as_of + timedelta(days=7) else "Upcoming week"
    return "Previous week"


def build_cash_calendar(
    rows: Iterable[Mapping[str, Any]],
    *,
    allocations: Sequence[Mapping[str, Any]] = (),
    historical_events: Sequence[Mapping[str, Any]] = (),
    historical_patterns: Sequence[Mapping[str, Any]] = (),
    as_of: date | None = None,
    past_days: int = PAST_DAYS,
    future_days: int = FUTURE_DAYS,
    history_status: str = "ready",
) -> dict[str, Any]:
    """Build one daily view while preserving each item's evidence class."""
    today = as_of or _operator_today()
    past_days = max(1, int(past_days))
    future_days = max(0, int(future_days))
    start = today - timedelta(days=past_days)
    end = today + timedelta(days=future_days)
    source_rows = [dict(row) for row in rows]
    by_transaction, settled_by_obligation = _active_allocation_maps(allocations)

    days: dict[str, dict[str, Any]] = {}
    for offset in range(-past_days, future_days + 1):
        day = today + timedelta(days=offset)
        label, date_label = _day_label(day, today)
        days[day.isoformat()] = {
            "date": day.isoformat(),
            "label": label,
            "date_label": date_label,
            "period": "today" if offset == 0 else "past" if offset < 0 else "future",
            "events": [],
            "posted_cents": 0,
            "planned_cents": 0,
            "warning_cents": 0,
        }

    source, actuals = _canonical_transactions(source_rows, as_of=today)
    for row in actuals:
        occurred = _as_date(row.get("_budget_date"))
        if occurred is None or occurred < start or occurred > today:
            continue
        amount = max(0, int(row.get("amount_cents") or 0))
        transaction_id = str(row.get("id") or row.get("source_id") or "")
        matches = by_transaction.get(transaction_id, [])
        planned = bool(matches)
        history_match = None if planned else _posted_history_match(
            row, occurred=occurred, patterns=historical_patterns,
        )
        expected = history_match is not None
        matched_name = next(
            (str(match.get("obligation_name") or "").strip() for match in matches
             if str(match.get("obligation_name") or "").strip()),
            "",
        )
        event = {
            "id": transaction_id,
            "kind": (
                "posted_planned" if planned
                else "posted_expected" if expected
                else "posted_unplanned"
            ),
            "state_label": (
                "Paid · matched to plan" if planned
                else "Paid · expected from history" if expected
                else "Paid · not in plan"
            ),
            "payment_status": "paid",
            "name": _name(row),
            "amount_cents": amount,
            "category": _category(row),
            "evidence": (
                f"Matched to {matched_name}" if matched_name
                else "Matched to a planned bill" if planned
                else (
                    f"Recognized from {history_match.get('occurrences')} prior recurring payments"
                    if expected else "No plan or strong recurring history is linked to this posted charge"
                )
            ),
            "href": (
                "/admin/finances/review" if planned
                else "/admin/finances/whats-coming" if expected
                else "/admin/finances/budget"
            ),
            "action_label": (
                "See match" if planned
                else "See recurring evidence" if expected
                else "Review for savings"
            ),
            "protected": _category(row) in _PROTECTED_CATEGORIES,
        }
        bucket = days[occurred.isoformat()]
        bucket["events"].append(event)
        bucket["posted_cents"] += amount

    for row in source_rows:
        if str(row.get("record_kind") or "") == "transaction":
            continue
        if str(row.get("event_type") or "").lower() != "outflow":
            continue
        if str(row.get("status") or "").lower() not in _OPEN_STATUSES:
            continue
        if row.get("archived_at"):
            continue
        if str(row.get("match_status") or "").lower() == "duplicate":
            continue
        if str(row.get("source_status") or "").lower() == "probable_duplicate":
            continue
        due = _as_date(row.get("due_date")) or _as_date(row.get("effective_date"))
        if due is None or due < today or due > end:
            continue
        face = max(0, int(row.get("amount_cents") or 0))
        paid_amount = min(face, settled_by_obligation.get(str(row.get("id") or ""), 0))
        open_amount = max(0, face - paid_amount)
        if open_amount <= 0:
            continue
        category = _category(row)
        is_draft_payroll = (
            str(row.get("source") or "").lower() == "hr_payroll"
            and str(row.get("source_status") or "").lower() == "draft"
        )
        event = {
            "id": str(row.get("id") or ""),
            "kind": "history_warning" if is_draft_payroll else "planned",
            "state_label": (
                "Expected · HR draft" if is_draft_payroll
                else "Partially paid · balance due" if paid_amount else "Unpaid · planned"
            ),
            "payment_status": (
                "unconfirmed" if is_draft_payroll
                else "partially_paid" if paid_amount else "unpaid"
            ),
            "name": _name(row, "Planned payment"),
            "amount_cents": open_amount,
            "category": category,
            "evidence": (
                "HR draft using net payroll; not required until processing"
                if is_draft_payroll
                else f"{_money(paid_amount)} paid; {_money(open_amount)} remains"
                if paid_amount else "Known bill or schedule; no posted payment is matched"
            ),
            "href": "/admin/finances/review",
            "action_label": "Review plan",
            "protected": category in _PROTECTED_CATEGORIES,
        }
        bucket = days[due.isoformat()]
        bucket["events"].append(event)
        bucket["warning_cents" if is_draft_payroll else "planned_cents"] += open_amount

    historical_rows = [dict(item) for item in historical_events]
    confirmed_history = [item for item in historical_rows if item.get("confirmed")]
    for row in historical_rows:
        due = _as_date(row.get("date") or row.get("due_date") or row.get("expected_date"))
        if due is None or due < today or due > end:
            continue
        amount = max(0, int(row.get("amount_cents") or 0))
        if amount <= 0:
            continue
        confirmed = bool(row.get("confirmed"))
        if not confirmed and any(
            _same_historical_charge(row, tracked) for tracked in confirmed_history
        ):
            continue
        kind = "history_planned" if confirmed else "history_warning"
        event = {
            "id": str(row.get("id") or row.get("pattern_key") or ""),
            "kind": kind,
            "state_label": "Unpaid · planned from history" if confirmed else "Unconfirmed · likely from history",
            "payment_status": "unpaid" if confirmed else "unconfirmed",
            "name": _name(row, "Possible recurring expense"),
            "amount_cents": amount,
            "category": _category(row),
            "evidence": str(row.get("evidence") or "Recurring bank pattern; not a confirmed bill"),
            "href": "/admin/finances/whats-coming",
            "action_label": "Review pattern",
            "protected": _category(row) in _PROTECTED_CATEGORIES,
        }
        bucket = days[due.isoformat()]
        bucket["events"].append(event)
        bucket["planned_cents" if confirmed else "warning_cents"] += amount

    ordered_days = list(days.values())
    priority = {"posted_unplanned": 0, "history_warning": 1, "planned": 2,
                "history_planned": 3, "posted_expected": 4, "posted_planned": 5}
    for bucket in ordered_days:
        bucket["events"].sort(
            key=lambda item: (priority.get(str(item["kind"]), 9), -int(item["amount_cents"]), str(item["name"]))
        )

    weeks: list[dict[str, Any]] = []
    week_start = start - timedelta(days=start.weekday())
    while week_start <= end:
        week_end = week_start + timedelta(days=6)
        included = [
            bucket for bucket in ordered_days
            if week_start <= date.fromisoformat(str(bucket["date"])) <= week_end
        ]
        events = [event for bucket in included for event in bucket["events"]]
        weeks.append({
            "start": week_start.isoformat(),
            "end": week_end.isoformat(),
            "label": _week_label(week_start, week_end, today),
            "date_label": (
                f"{week_start.strftime('%b %d').replace(' 0', ' ')}–"
                f"{week_end.strftime('%b %d').replace(' 0', ' ')}"
            ),
            "paid_cents": sum(
                int(event["amount_cents"]) for event in events
                if event.get("payment_status") == "paid"
            ),
            "unpaid_cents": sum(
                int(event["amount_cents"]) for event in events
                if event.get("payment_status") in {"unpaid", "partially_paid"}
            ),
            "possible_cents": sum(
                int(event["amount_cents"]) for event in events
                if event.get("payment_status") == "unconfirmed"
            ),
            "paid_count": sum(1 for event in events if event.get("payment_status") == "paid"),
            "unpaid_count": sum(
                1 for event in events
                if event.get("payment_status") in {"unpaid", "partially_paid"}
            ),
            "possible_count": sum(
                1 for event in events if event.get("payment_status") == "unconfirmed"
            ),
        })
        week_start += timedelta(days=7)

    past_events = [event for bucket in ordered_days if bucket["period"] in {"past", "today"}
                   for event in bucket["events"] if str(event["kind"]).startswith("posted_")]
    future_events = [event for bucket in ordered_days if bucket["period"] in {"today", "future"}
                     for event in bucket["events"]]
    snapshot_basis = {
        "as_of": today.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "actual_source": source,
        "days": ordered_days,
    }
    calculation_id = hashlib.sha256(
        json.dumps(snapshot_basis, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:16]
    return {
        "status": "ready",
        "as_of": today.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "past_days": past_days,
        "future_days": future_days,
        "actual_source": source,
        "history_status": history_status,
        "calculation_id": calculation_id,
        "days": ordered_days,
        "weeks": weeks,
        "totals": {
            "posted_cents": sum(int(item["amount_cents"]) for item in past_events),
            "unplanned_posted_cents": sum(
                int(item["amount_cents"]) for item in past_events if item["kind"] == "posted_unplanned"
            ),
            "expected_posted_cents": sum(
                int(item["amount_cents"]) for item in past_events if item["kind"] == "posted_expected"
            ),
            "planned_cents": sum(
                int(item["amount_cents"]) for item in future_events
                if item["kind"] in {"planned", "history_planned"}
            ),
            "warning_cents": sum(
                int(item["amount_cents"]) for item in future_events if item["kind"] == "history_warning"
            ),
            "unplanned_count": sum(1 for item in past_events if item["kind"] == "posted_unplanned"),
            "expected_count": sum(1 for item in past_events if item["kind"] == "posted_expected"),
            "warning_count": sum(1 for item in future_events if item["kind"] == "history_warning"),
        },
    }


def _load_active_allocations() -> list[dict[str, Any]]:
    from sqlalchemy import text
    from sales_support_agent.models.database import get_engine

    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT allocation.obligation_event_id, allocation.transaction_event_id,
                   allocation.amount_cents, obligation.name AS obligation_name
            FROM settlement_allocations AS allocation
            JOIN cash_events AS obligation ON obligation.id = allocation.obligation_event_id
            WHERE allocation.reversed_allocation_id IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM settlement_allocations AS reversal
                WHERE reversal.reversed_allocation_id = allocation.id
              )
        """)).fetchall()
    return [dict(row._mapping) for row in rows]


def _historical_data(
    *, as_of: date, future_days: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    from sales_support_agent.services.cashflow.bill_patterns import (
        _occurrences_in_window,
        confirmed_bill_projections,
        list_bill_patterns,
    )
    from sales_support_agent.services.cashflow.payroll_commitments import active_hr_pay_dates

    try:
        hr_pay_dates = active_hr_pay_dates()
    except Exception:
        hr_pay_dates = []

    def covered_by_hr_payroll(item: Mapping[str, Any], due: date) -> bool:
        category = str(item.get("category") or "").lower()
        vendor = str(item.get("vendor") or item.get("name") or "").lower()
        is_payroll = category == "payroll" or "payroll" in vendor
        return is_payroll and any(abs((due - pay_date).days) <= 7 for pay_date in hr_pay_dates)

    events: list[dict[str, Any]] = []
    for projection in confirmed_bill_projections(as_of=as_of, horizon_days=future_days):
        projection_due = _as_date(projection.get("due_date"))
        if projection_due and covered_by_hr_payroll(projection, projection_due):
            continue
        events.append({
            **projection,
            "date": projection.get("due_date"),
            "confirmed": True,
            "evidence": "You previously chose to track this recurring bank pattern",
        })
    listing = list_bill_patterns(as_of=as_of)
    horizon_end = as_of + timedelta(days=future_days)
    for pattern in listing.get("patterns") or []:
        if pattern.get("decision"):
            continue
        for due in _occurrences_in_window(pattern, as_of=as_of, horizon_end=horizon_end):
            if covered_by_hr_payroll(pattern, due):
                continue
            events.append({
                "id": f"history-warning-{pattern['pattern_key']}-{due.isoformat()}",
                "pattern_key": pattern["pattern_key"],
                "date": due,
                "name": pattern.get("vendor") or "Possible recurring expense",
                "vendor_or_customer": pattern.get("vendor") or "",
                "amount_cents": int(pattern.get("amount_cents") or 0),
                "category": pattern.get("category") or "other",
                "confirmed": False,
                "evidence": f"{pattern.get('confidence_label') or 'Possible'} recurring pattern from posted bank history",
            })
    patterns = [
        dict(pattern)
        for pattern in [*(listing.get("patterns") or []), *(listing.get("tracked") or [])]
    ]
    return events, patterns


def load_cash_calendar(
    *, as_of: date | None = None, rows: Sequence[Mapping[str, Any]] | None = None,
    future_days: int = FUTURE_DAYS,
) -> dict[str, Any]:
    """Load the live ledger, settlement evidence, and bank-pattern warnings.

    Pass ``rows`` when something else on the same page has already read the
    ledger. Reading ten thousand rows twice for one page is the round-trip cost
    that makes this section feel slow.
    """
    from sales_support_agent.services.cashflow.obligations import list_obligations

    today = as_of or _operator_today()
    rows = list(rows) if rows is not None else list_obligations(limit=10_000)
    try:
        from sales_support_agent.services.cashflow.vendors import (
            list_vendors_with_progress,
            preview_agreement_obligations,
        )
        for vendor in list_vendors_with_progress():
            rows.extend(preview_agreement_obligations(
                vendor, as_of=today, horizon_days=future_days,
            ))
    except Exception:
        # Vendor terms are supporting forecast evidence. They must not hide
        # posted bank truth when the agreement store is temporarily unavailable.
        pass
    # Pattern analysis is advisory. A temporary issue there must not hide the
    # posted bank truth or known obligations from the owner.
    try:
        allocations = _load_active_allocations()
    except Exception:
        allocations = []
    try:
        history, patterns = _historical_data(as_of=today, future_days=future_days)
        history_status = "ready"
    except Exception:
        history, patterns = [], []
        history_status = "unavailable"
    return build_cash_calendar(
        rows,
        allocations=allocations,
        historical_events=history,
        historical_patterns=patterns,
        as_of=today,
        future_days=future_days,
        history_status=history_status,
    )


def overlay_paydown_proposals(
    calendar: Mapping[str, Any], plan: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return the shared calendar with advisory rent proposals visible.

    The plan must be calculated first.  This one-way overlay prevents a rent
    proposal from feeding back into its own affordability calculation.
    """
    result = {
        **dict(calendar),
        "days": [{**dict(bucket), "events": [dict(item) for item in bucket.get("events") or []]}
                 for bucket in calendar.get("days") or []],
        "weeks": [dict(week) for week in calendar.get("weeks") or []],
    }
    if not plan or plan.get("status") not in {"ok", "nothing_spare"}:
        return result
    if str(plan.get("calculation_id") or "") != str(calendar.get("calculation_id") or ""):
        return result

    day_map = {str(bucket.get("date") or ""): bucket for bucket in result["days"]}
    for item in plan.get("instalments") or []:
        when = _as_date(item.get("date"))
        bucket = day_map.get(when.isoformat() if when else "")
        amount = max(0, int(item.get("amount_cents") or 0))
        if bucket is None or amount <= 0:
            continue
        bucket["events"].append({
            "id": f"proposed-rent-{when.isoformat()}",
            "kind": "proposed_rent",
            "state_label": "Proposed · not scheduled",
            "payment_status": "unconfirmed",
            "name": f"{plan.get('vendor') or 'Rent'} — proposed rent payment",
            "vendor_or_customer": str(plan.get("vendor") or ""),
            "amount_cents": amount,
            "category": "rent",
            "evidence": "Calculated after other planned and possible expenses",
            "href": "#cash-calendar-paydown-title",
            "action_label": "See rent plan",
            "protected": True,
            "advisory": True,
        })
        bucket["warning_cents"] = int(bucket.get("warning_cents") or 0) + amount

        for week in result["weeks"]:
            start, end = _as_date(week.get("start")), _as_date(week.get("end"))
            if start and end and start <= when <= end:
                week["possible_cents"] = int(week.get("possible_cents") or 0) + amount
                week["possible_count"] = int(week.get("possible_count") or 0) + 1
                week["proposed_rent_cents"] = int(week.get("proposed_rent_cents") or 0) + amount
                break
    return result


def _event_html(event: Mapping[str, Any]) -> str:
    kind = html.escape(str(event.get("kind") or "planned"), quote=True)
    payment_status = html.escape(str(event.get("payment_status") or "unconfirmed"), quote=True)
    protected = bool(event.get("protected"))
    event_id = str(event.get("id") or "").strip()
    is_canonical = bool(
        event_id
        and not str(event.get("kind") or "").startswith("history_")
        and not event.get("advisory")
    )
    selection = (
        f'<label class="cash-calendar-select"><input type="checkbox" data-calendar-select '
        f'data-object-id="{html.escape(event_id, quote=True)}" data-amount-cents="{int(event.get("amount_cents") or 0)}" '
        f'{"disabled" if protected else ""}><span class="sr-only">Select {html.escape(str(event.get("name") or "expense"))}</span></label>'
        if is_canonical else ""
    )
    detail_button = (
        f'<button type="button" class="cash-calendar-detail" data-finance-object-open '
        f'data-finance-object-type="cash_event" data-finance-object-id="{html.escape(event_id, quote=True)}">Details</button>'
        if is_canonical else ""
    )
    return f"""
      <li class="cash-calendar-event cash-calendar-event--{kind}" data-calendar-kind="{kind}" data-payment-status="{payment_status}">
        {selection}
        <div class="cash-calendar-event__main">
          <span class="cash-calendar-state cash-calendar-state--{kind}">{html.escape(str(event.get('state_label') or 'Expense'))}</span>
          <strong>{html.escape(str(event.get('name') or 'Expense'))}</strong>
          <p>{html.escape(str(event.get('evidence') or 'Evidence unavailable'))}{' · Protected cost' if protected else ''}</p>
        </div>
        <div class="cash-calendar-event__amount">
          <strong>{_money(int(event.get('amount_cents') or 0))}</strong>
          {detail_button}
          <a href="{html.escape(str(event.get('href') or '/admin/finances/review'), quote=True)}">{html.escape(str(event.get('action_label') or 'Review'))}</a>
        </div>
      </li>"""


def _charge_link(week: Mapping[str, Any], state: str, cents: int, note: str) -> str:
    """A total you can open. Zero is not a link: there is nothing behind it."""
    figure = f"<strong>{_money(cents)}</strong><small>{html.escape(note)}</small>"
    if cents <= 0:
        return figure
    start = html.escape(str(week.get("start") or ""), quote=True)
    return (
        f'<a class="cash-calendar-weekly__open" '
        f'href="/admin/finances/calendar/charges?week={start}&amp;state={state}" '
        f'title="See the charges behind this">{figure}</a>'
    )


def _next_week_headline(calendar: Mapping[str, Any]) -> str:
    """One answer to "what leaves next week" so the table need not be read.

    The two figures stay apart. A bill nobody has confirmed is not the same kind
    of fact as one that is dated and owed, and adding them would hide which is
    which at exactly the moment the operator is deciding what to pay.
    """
    week = next(
        (item for item in calendar.get("weeks") or []
         if str(item.get("label") or "") == "Next week"),
        None,
    )
    if week is None:
        return ""

    days = {str(bucket.get("date")): bucket for bucket in calendar.get("days") or []}
    heaviest_label, heaviest_cents = "", 0
    day = _as_date_or_none(week.get("start"))
    end = _as_date_or_none(week.get("end"))
    while day is not None and end is not None and day <= end:
        bucket = days.get(day.isoformat())
        if bucket:
            amount = int(bucket.get("planned_cents") or 0)
            if amount > heaviest_cents:
                heaviest_cents = amount
                heaviest_label = _date_label(day)
        day += timedelta(days=1)

    unpaid = int(week.get("unpaid_cents") or 0)
    possible = int(week.get("possible_cents") or 0)
    heaviest = (
        f"<p class=\"cash-calendar-next__heaviest\">Heaviest day: "
        f"{html.escape(heaviest_label)}, {_money(heaviest_cents)}</p>"
        if heaviest_cents else ""
    )
    return f"""
      <section class="cash-calendar-next" aria-labelledby="cash-calendar-next-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Next week</p>
        <h2 id="cash-calendar-next-title">{html.escape(str(week.get('date_label') or ''))}</h2></div></div>
        <div class="cash-calendar-next__figures">
          <div>
            <span>Leaving your account</span>
            <strong class="amount-out">{_money(unpaid)}</strong>
            <small>{int(week.get('unpaid_count') or 0)} dated and owed</small>
          </div>
          <div>
            <span>Possibly also</span>
            <strong>{_money(possible)}</strong>
            <small>{int(week.get('possible_count') or 0)} nobody has confirmed</small>
          </div>
        </div>
        {heaviest}
      </section>"""


def _as_date_or_none(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _paydown_block(plan: Mapping[str, Any] | None) -> str:
    """What can go toward the biggest repeating bill, and when.

    Dated on purpose. A figure with no date attached leaves the operator doing
    the same arithmetic again nine days later, which is the job this removes.
    """
    # A section that silently disappears is indistinguishable from one that was
    # never built, which is how features in this app have gone missing before.
    # Say what happened instead.
    if plan is None or plan.get("status") == "failed":
        reason = str((plan or {}).get("reason") or "")
        note = (
            f'<p class="metric-note">Reference: {html.escape(reason)}</p>' if reason else ""
        )
        return f"""
      <section class="cash-calendar-paydown">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Paying down</p>
        <h2>Could not work this out</h2></div></div>
        <p class="finance-plan-short">Your calendar above is unaffected. Nothing was
        changed. If this keeps happening it is worth a look.</p>
        {note}
      </section>"""
    if plan.get("status") == "no_vendor":
        return """
      <section class="cash-calendar-paydown">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Paying down</p>
        <h2>No repeating bill to plan around yet</h2></div></div>
        <p class="metric-note">This needs a few months of payments to the same
        supplier before it can suggest anything.</p>
      </section>"""
    if plan.get("status") == "paused":
        return f"""
      <section class="cash-calendar-paydown cash-calendar-paydown--paused" role="alert">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Paying down</p>
        <h2>Rent recommendation paused</h2></div></div>
        <p class="finance-plan-short">{html.escape(str(plan.get('message') or 'Not all upcoming expenses were included.'))}</p>
        <p class="metric-note">{html.escape(str(plan.get('reason') or 'Refresh the calendar sources before deciding what to pay.'))}</p>
      </section>"""

    vendor = html.escape(str(plan.get("vendor") or "this bill"))
    monthly = int(plan.get("monthly_cents") or 0)
    paid = int(plan.get("paid_this_month_cents") or 0)
    remaining = int(plan.get("remaining_cents") or 0)

    if remaining <= 0:
        return f"""
      <section class="cash-calendar-paydown">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Paying down</p>
        <h2>{vendor}</h2></div></div>
        <p class="finance-plan-ok">Nothing left to pay this month. You have sent
        {_money(paid)} of about {_money(monthly)}.</p>
      </section>"""

    reserved = int(plan.get("reserved_cents") or 0)
    unconfirmed = int(plan.get("unconfirmed_reserved_cents") or 0)
    savings = int(plan.get("savings_would_unlock_cents") or 0)
    savings_line = (
        '<p class="cash-calendar-paydown__savings">Last-resort option only: '
        f"{_money(savings)} from protected reserves, including TAX, would let you send "
        "that much more. It is not included in the recommendation.</p>"
        if savings > 0 else ""
    )

    if not plan.get("instalments"):
        planned_reserved = max(0, reserved - unconfirmed)
        excluded_vendor = int(plan.get("excluded_vendor_cents") or 0)
        balance_as_of = _as_date_or_none(plan.get("balance_as_of"))
        balance_note = (
            f"Balance confirmed by you on {balance_as_of.strftime('%b')} {balance_as_of.day}."
            if balance_as_of else "Balance is based on the saved rent facts."
        )
        excluded_note = (
            '<p class="metric-note">The Calendar&#39;s '
            f"{_money(excluded_vendor)} {vendor} estimate is not added again because your "
            f"confirmed {_money(remaining)} balance replaces it.</p>"
            if excluded_vendor else ""
        )
        return f"""
      <section class="cash-calendar-paydown">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Paying down</p>
        <h2>{vendor}</h2></div></div>
        <p class="finance-plan-short"><strong>No rent payment is recommended yet.</strong>
        Nothing spare this month: the expenses below use your cash while protecting your
        {_money(int(plan.get('floor_cents') or 0))} cash goal.</p>
        <p class="cash-calendar-paydown__lead">Monthly rent {_money(monthly)}. Plaid confirms
        {_money(paid)} sent this month. Remaining {_money(remaining)}.</p>
        <p class="metric-note">{html.escape(balance_note)}</p>
        <p class="cash-calendar-paydown__note"><strong>Other expenses reserved before rent:</strong>
        {_money(planned_reserved)} planned and {_money(unconfirmed)} possible,
        {_money(reserved)} total.</p>
        {excluded_note}
        <p><a class="btn btn-secondary btn-sm" href="/admin/finances/collections">See who owes you</a></p>
        {savings_line}
      </section>"""

    rows = "".join(
        f"<tr><th scope=\"row\">{html.escape(_instalment_when(item.get('date')))}</th>"
        f"<td class=\"amount-out\">{_money(int(item.get('amount_cents') or 0))}</td>"
        f"<td>{html.escape(str(item.get('why') or ''))}</td></tr>"
        for item in plan["instalments"]
    )
    total = int(plan.get("planned_total_cents") or 0)
    shortfall = int(plan.get("shortfall_cents") or 0)
    balance_as_of = _as_date_or_none(plan.get("balance_as_of"))
    basis = str(plan.get("balance_basis") or "")
    balance_date_label = (
        f"{balance_as_of.strftime('%b')} {balance_as_of.day}, {balance_as_of.year}"
        if balance_as_of else "the saved date"
    )
    basis_copy = (
        f"Confirmed by you as of {html.escape(balance_date_label)}. "
        "Plaid payments posted after that date reduce it automatically."
        if basis == "operator_confirmed" else
        "Estimated from the monthly amount and posted payments."
    )
    shortfall_line = (
        f'<p class="cash-calendar-paydown__note">{_money(shortfall)} of it has nowhere '
        "to come from this month on current figures.</p>"
        if shortfall > 0 else ""
    )
    return f"""
      <section class="cash-calendar-paydown" aria-labelledby="cash-calendar-paydown-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Paying down</p>
        <h2 id="cash-calendar-paydown-title">{vendor}</h2></div></div>
        <p class="cash-calendar-paydown__lead">About {_money(monthly)} this month. You have sent
        {_money(paid)}. Remaining {_money(remaining)}.</p>
        <p class="metric-note"><strong>Balance source:</strong> {basis_copy}</p>
        <table>
          <thead><tr><th scope="col">When</th><th scope="col">Amount</th><th scope="col">Why then</th></tr></thead>
          <tbody>{rows}</tbody>
          <tfoot><tr><th scope="row">Total</th><td class="amount-out">{_money(total)}</td><td></td></tr></tfoot>
        </table>
        <p class="cash-calendar-paydown__note">Reserved for the rest of the month:
        {_money(reserved)}, of which {_money(unconfirmed)} is not confirmed. If those
        possible expenses do not occur you can send more, sooner.</p>
        <p class="cash-calendar-paydown__note">Normal recommendations protect your
        {_money(int(plan.get('floor_cents') or 0))} cash goal. Your emergency floor is
        {_money(int(plan.get('emergency_floor_cents') or 0))}. TAX and other reserves are
        shown only as a last resort and are never included automatically.</p>
        {shortfall_line}
        {savings_line}
        <details class="cash-calendar-paydown__settings">
          <summary>Update the rent balance</summary>
          <form method="post" action="/admin/finances/calendar/paydown-settings">
            <label>Payee<input name="vendor_label" value="{vendor}" required></label>
            <label>Monthly rent<input name="monthly_amount" inputmode="decimal" value="{monthly / 100:.2f}" required></label>
            <label>Amount owed now<input name="balance_amount" inputmode="decimal" value="{remaining / 100:.2f}" required></label>
            <label>Balance confirmed on<input name="balance_as_of" type="date" value="{balance_as_of.isoformat() if balance_as_of else ''}" required></label>
            <label>Cash goal<input name="cash_goal" inputmode="decimal" value="{int(plan.get('floor_cents') or 0) / 100:.2f}" required></label>
            <button class="btn btn-secondary btn-sm" type="submit">Save payoff facts</button>
          </form>
        </details>
      </section>"""
def _instalment_when(value: Any) -> str:
    when = _as_date_or_none(value) if not isinstance(value, date) else value
    if when is None:
        return "Later"
    return "Today" if when == _operator_today() else _date_label(
        when, weekday="%a", month="%b"
    )


def render_cash_calendar_page(
    calendar: Mapping[str, Any], *, flash: str = "", paydown: Mapping[str, Any] | None = None
) -> str:
    """Render the 7-day history, today, and 14-day forward expense view."""
    if calendar.get("status") != "ready":
        body = f"""<div class="money-brief">{render_finance_nav('calendar', counts={})}
        <div class="money-empty"><h1>The cash calendar could not load</h1>
        <p>Nothing was changed. Refresh the connected accounts, then try again.</p>
        <a class="btn btn-primary" href="/admin/finances/accounts">Check accounts</a></div></div>"""
        return _page_shell("Cash calendar", "calendar", body, flash=flash)

    totals = calendar.get("totals") or {}
    week_rows = []
    for week in calendar.get("weeks") or []:
        proposed_rent = int(week.get("proposed_rent_cents") or 0)
        possible_note = f"{int(week.get('possible_count') or 0)} unconfirmed"
        if proposed_rent:
            possible_note += f" · Includes {_money(proposed_rent)} proposed rent"
        week_rows.append(f"""
          <tr>
            <th scope="row"><strong>{html.escape(str(week.get('label') or 'Week'))}</strong><small>{html.escape(str(week.get('date_label') or ''))}</small></th>
            <td>{_charge_link(week, 'paid', int(week.get('paid_cents') or 0), f"{int(week.get('paid_count') or 0)} paid")}</td>
            <td>{_charge_link(week, 'unpaid', int(week.get('unpaid_cents') or 0), f"{int(week.get('unpaid_count') or 0)} still due")}</td>
            <td>{_charge_link(week, 'possible', int(week.get('possible_cents') or 0), possible_note)}</td>
          </tr>""")
    day_rows: list[str] = []
    day_buttons: list[str] = []
    for day in calendar.get("days") or []:
        events = list(day.get("events") or [])
        event_rows = "".join(_event_html(event) for event in events)
        summary_parts = []
        if int(day.get("posted_cents") or 0):
            summary_parts.append(f"Posted {_money(int(day['posted_cents']))}")
        if int(day.get("planned_cents") or 0):
            summary_parts.append(f"Planned {_money(int(day['planned_cents']))}")
        if int(day.get("warning_cents") or 0):
            summary_parts.append(f"Possible {_money(int(day['warning_cents']))}")
        summary = " · ".join(summary_parts) or "No expenses found"
        period = html.escape(str(day.get("period") or "future"), quote=True)
        selected = period == "today"
        day_buttons.append(f"""
          <button type="button" class="cash-calendar-date{' is-selected' if selected else ''}"
            data-calendar-date="{html.escape(str(day.get('date') or ''), quote=True)}"
            aria-pressed="{'true' if selected else 'false'}">
            <span>{html.escape(str(day.get('label') or 'Day'))}</span>
            <strong>{html.escape(str(day.get('date_label') or day.get('date') or ''))}</strong>
            <small>{html.escape(summary)}</small>
          </button>""")
        day_rows.append(f"""
          <article class="cash-calendar-day cash-calendar-day--{period}{' is-selected' if selected else ''}"
            data-calendar-day data-calendar-date-panel="{html.escape(str(day.get('date') or ''), quote=True)}"
            data-calendar-period="{period}">
            <header><div><span>{html.escape(str(day.get('label') or 'Day'))}</span>
            <strong>{html.escape(str(day.get('date_label') or day.get('date') or ''))}</strong></div>
            <div><p>{html.escape(summary)}</p><button type="button" class="cash-calendar-select-day" data-calendar-select-day>Select this day</button></div></header>
            <ul>{event_rows or '<li class="cash-calendar-empty">Nothing is posted or expected for this day.</li>'}</ul>
          </article>""")

    body = f"""
    <div class="money-brief cash-calendar-page">
      {render_finance_nav('calendar', counts={})}
      <header class="money-page-header"><div><p class="finance-eyebrow">Cash calendar</p>
      <h1>See expenses before they surprise you</h1>
      <p class="money-page-subtitle">Seven days of posted spending and every remaining day this month, separated into planned costs, historical warnings, and proposed rent.</p></div>
      <div class="money-page-status"><span class="money-status money-status--ready">Read-only</span>
      <span>Posted source: {html.escape(str(calendar.get('actual_source') or 'unavailable').title())} · As of {html.escape(str(calendar.get('as_of') or 'today'))}</span></div></header>

      <section class="cash-calendar-summary" aria-label="Expense calendar summary">
        <article><span>Posted in the last 7 days</span><strong>{_money(int(totals.get('posted_cents') or 0))}</strong></article>
        <article class="cash-calendar-summary__expected"><span>Recognized automatically</span><strong>{_money(int(totals.get('expected_posted_cents') or 0))}</strong><small>{int(totals.get('expected_count') or 0)} recurring charge(s)</small></article>
        <article class="cash-calendar-summary__attention"><span>Still needs review</span><strong>{_money(int(totals.get('unplanned_posted_cents') or 0))}</strong><small>{int(totals.get('unplanned_count') or 0)} charge(s)</small></article>
        <article><span>Planned through month-end</span><strong>{_money(int(totals.get('planned_cents') or 0))}</strong></article>
        <article class="cash-calendar-summary__warning"><span>Possible from history</span><strong>{_money(int(totals.get('warning_cents') or 0))}</strong><small>{int(totals.get('warning_count') or 0)} warning(s), not counted as required</small></article>
      </section>

      <aside class="cash-calendar-legend" aria-label="Calendar status meanings">
        <strong>How to read this</strong>
        <span><i class="is-posted"></i>Paid means the bank confirms it left.</span>
        <span><i class="is-planned"></i>Unpaid means a known balance still needs payment.</span>
        <span><i class="is-warning"></i>Unconfirmed is an early warning, not a bill or payment.</span>
      </aside>

      {_next_week_headline(calendar)}
      {_paydown_block(paydown)}

      <section class="cash-calendar-weekly" aria-labelledby="cash-calendar-weekly-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Weekly roll-up</p>
        <h2 id="cash-calendar-weekly-title">What left, what is due, and what is only possible</h2></div></div>
        <p class="cash-calendar-weekly__note">Columns stay separate. Possible expenses are not added to the unpaid total.</p>
        <div class="cash-calendar-weekly__scroll">
          <table>
            <thead><tr><th scope="col">Week</th><th scope="col">Paid from bank</th><th scope="col">Unpaid planned</th><th scope="col">Possible · unconfirmed</th></tr></thead>
            <tbody>{''.join(week_rows)}</tbody>
          </table>
        </div>
      </section>

      <section class="cash-calendar-workspace" aria-labelledby="cash-calendar-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Daily drill-down</p>
        <h2 id="cash-calendar-title">Past 7 days · today · through month-end</h2></div>
        <span class="money-section-state">{len(calendar.get('days') or [])} days</span></div>
        <div class="cash-calendar-filters" role="group" aria-label="Filter expense calendar">
          <button type="button" class="is-active" data-calendar-filter="all" aria-pressed="true">All expenses</button>
          <button type="button" data-calendar-filter="attention" aria-pressed="false">Needs attention</button>
          <button type="button" data-calendar-filter="planned" aria-pressed="false">Planned</button>
          <button type="button" data-calendar-filter="posted" aria-pressed="false">Already posted</button>
        </div>
        <p class="cash-calendar-result" data-calendar-result>Showing Today. Choose another day to drill down.</p>
        <div class="cash-calendar-browser">
          <nav class="cash-calendar-dates" aria-label="Choose a day">{''.join(day_buttons)}</nav>
          <div class="cash-calendar-days">{''.join(day_rows)}</div>
        </div>
        <div class="finance-batch-bar" data-calendar-batch-bar hidden>
          <div><strong data-calendar-selected-count>0 selected</strong><span data-calendar-selected-value>$0</span></div>
          <div class="finance-batch-bar__actions" role="group" aria-label="Stage action for selected expenses">
            <button type="button" data-calendar-batch-action="needed">Needed</button>
            <button type="button" data-calendar-batch-action="unknown">Unknown</button>
            <button type="button" data-calendar-batch-action="investigate">Investigate</button>
            <button type="button" data-calendar-batch-action="waste">Waste</button>
            <button type="button" data-calendar-clear>Clear selection</button>
            <button type="button" class="is-primary" data-calendar-review>Review and save</button>
          </div>
        </div>
      </section>

      <footer class="money-proof-note"><strong>What the calendar does not assume</strong>
      <p>Paid requires a posted bank withdrawal. Unpaid shows a known remaining balance. Historical warnings do not reduce projected cash and do not become planned bills until you confirm them. This page never moves money, cancels a vendor, runs payroll, or edits QuickBooks.</p></footer>
    </div>
    <script>
    (() => {{
      const filterButtons = [...document.querySelectorAll('[data-calendar-filter]')];
      const dateButtons = [...document.querySelectorAll('[data-calendar-date]')];
      const days = [...document.querySelectorAll('[data-calendar-day]')];
      const result = document.querySelector('[data-calendar-result]');
      const selections = [...document.querySelectorAll('[data-calendar-select]')];
      const batchBar = document.querySelector('[data-calendar-batch-bar]');
      const selectedCount = document.querySelector('[data-calendar-selected-count]');
      const selectedValue = document.querySelector('[data-calendar-selected-value]');
      const selected = () => selections.filter(input => input.checked && !input.disabled);
      const updateSelection = () => {{
        const chosen = selected();
        const cents = chosen.reduce((total, input) => total + Math.abs(Number(input.dataset.amountCents || 0)), 0);
        if (batchBar) batchBar.hidden = chosen.length === 0;
        if (selectedCount) selectedCount.textContent = `${{chosen.length}} selected`;
        if (selectedValue) selectedValue.textContent = new Intl.NumberFormat('en-US', {{style: 'currency', currency: 'USD'}}).format(cents / 100);
      }};
      const matches = (event, wanted) => wanted === 'all'
        || (wanted === 'attention' && ['posted_unplanned', 'history_warning'].includes(event.dataset.calendarKind))
        || (wanted === 'planned' && ['planned', 'history_planned'].includes(event.dataset.calendarKind))
        || (wanted === 'posted' && ['posted_planned', 'posted_expected', 'posted_unplanned'].includes(event.dataset.calendarKind));
      const selectDay = date => {{
        const chosen = days.find(day => day.dataset.calendarDatePanel === date && !day.hidden);
        if (!chosen) return;
        days.forEach(day => day.classList.toggle('is-selected', day === chosen));
        dateButtons.forEach(button => {{
          const active = button.dataset.calendarDate === date;
          button.classList.toggle('is-selected', active);
          button.setAttribute('aria-pressed', active ? 'true' : 'false');
        }});
        if (result) result.textContent = `Showing ${{chosen.querySelector('header span')?.textContent || 'selected day'}}. Choose another day to drill down.`;
      }};
      dateButtons.forEach(button => button.addEventListener('click', () => selectDay(button.dataset.calendarDate)));
      selections.forEach(input => input.addEventListener('change', updateSelection));
      document.querySelectorAll('[data-calendar-select-day]').forEach(button => button.addEventListener('click', () => {{
        const day = button.closest('[data-calendar-day]');
        [...day.querySelectorAll('[data-calendar-select]:not(:disabled)')].filter(input => !input.closest('[data-calendar-kind]').hidden).forEach(input => {{ input.checked = true; }});
        updateSelection();
      }}));
      document.querySelector('[data-calendar-clear]')?.addEventListener('click', () => {{ selections.forEach(input => {{ input.checked = false; }}); updateSelection(); }});
      document.querySelectorAll('[data-calendar-batch-action]').forEach(button => button.addEventListener('click', () => {{
        const value = button.dataset.calendarBatchAction;
        selected().forEach(input => window.FinanceWorkspace?.stage({{object_type: 'cash_event', object_id: input.dataset.objectId, action: 'set_savings_state', value}}));
        button.closest('.finance-batch-bar')?.querySelectorAll('[data-calendar-batch-action]').forEach(item => item.classList.toggle('is-selected', item === button));
      }}));
      document.querySelector('[data-calendar-review]')?.addEventListener('click', () => window.FinanceWorkspace?.reviewAndSave());
      filterButtons.forEach(button => button.addEventListener('click', () => {{
        const wanted = button.dataset.calendarFilter;
        let visibleEvents = 0;
        days.forEach(day => {{
          const events = [...day.querySelectorAll('[data-calendar-kind]')];
          events.forEach(event => {{
            const visible = matches(event, wanted);
            event.hidden = !visible;
            if (visible) visibleEvents += 1;
          }});
          day.hidden = wanted !== 'all' && !events.some(event => !event.hidden);
        }});
        dateButtons.forEach(dateButton => {{
          const panel = days.find(day => day.dataset.calendarDatePanel === dateButton.dataset.calendarDate);
          dateButton.hidden = Boolean(panel?.hidden);
        }});
        filterButtons.forEach(item => {{
          const active = item === button;
          item.classList.toggle('is-active', active);
          item.setAttribute('aria-pressed', active ? 'true' : 'false');
        }});
        const selected = days.find(day => day.classList.contains('is-selected') && !day.hidden);
        const fallback = dateButtons.find(dateButton => !dateButton.hidden);
        if (selected) selectDay(selected.dataset.calendarDatePanel);
        else if (fallback) selectDay(fallback.dataset.calendarDate);
        if (result && wanted !== 'all') result.textContent += ` ${{visibleEvents}} matching expense${{visibleEvents === 1 ? '' : 's'}} across the window.`;
      }}));
    }})();
    </script>"""
    return _page_shell("Cash calendar", "calendar", body, flash=flash)


__all__ = ["build_cash_calendar", "load_cash_calendar", "render_cash_calendar_page"]
