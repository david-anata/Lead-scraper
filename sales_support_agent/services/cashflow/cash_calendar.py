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
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Sequence

from sales_support_agent.services.cashflow.budgeting import _canonical_transactions
from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.overview import _money, _page_shell

PAST_DAYS = 7
FUTURE_DAYS = 14
_OPEN_STATUSES = {"planned", "pending", "overdue", "completed"}
_PROTECTED_CATEGORIES = {
    "payroll", "tax", "debt", "rent", "insurance", "utilities", "manual_check"
}


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


def _day_label(day: date, as_of: date) -> tuple[str, str]:
    readable = day.strftime("%A, %b %d").replace(" 0", " ")
    if day == as_of:
        return "Today", readable
    if day == as_of - timedelta(days=1):
        return "Yesterday", readable
    if day == as_of + timedelta(days=1):
        return "Tomorrow", readable
    return day.strftime("%A"), day.strftime("%b %d").replace(" 0", " ")


def build_cash_calendar(
    rows: Iterable[Mapping[str, Any]],
    *,
    allocations: Sequence[Mapping[str, Any]] = (),
    historical_events: Sequence[Mapping[str, Any]] = (),
    as_of: date | None = None,
    past_days: int = PAST_DAYS,
    future_days: int = FUTURE_DAYS,
) -> dict[str, Any]:
    """Build one daily view while preserving each item's evidence class."""
    today = as_of or date.today()
    past_days = max(1, int(past_days))
    future_days = max(1, int(future_days))
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
        matched_name = next(
            (str(match.get("obligation_name") or "").strip() for match in matches
             if str(match.get("obligation_name") or "").strip()),
            "",
        )
        event = {
            "id": transaction_id,
            "kind": "posted_planned" if planned else "posted_unplanned",
            "state_label": "Posted · planned" if planned else "Posted · not in plan",
            "name": _name(row),
            "amount_cents": amount,
            "category": _category(row),
            "evidence": (
                f"Matched to {matched_name}" if matched_name
                else "Matched to a planned bill" if planned
                else "No planned bill is linked to this posted charge"
            ),
            "href": "/admin/finances/review" if planned else "/admin/finances/budget",
            "action_label": "See match" if planned else "Review for savings",
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
        open_amount = max(0, face - settled_by_obligation.get(str(row.get("id") or ""), 0))
        if open_amount <= 0:
            continue
        category = _category(row)
        event = {
            "id": str(row.get("id") or ""),
            "kind": "planned",
            "state_label": "Planned",
            "name": _name(row, "Planned payment"),
            "amount_cents": open_amount,
            "category": category,
            "evidence": "Known bill or schedule",
            "href": "/admin/finances/review",
            "action_label": "Review plan",
            "protected": category in _PROTECTED_CATEGORIES,
        }
        bucket = days[due.isoformat()]
        bucket["events"].append(event)
        bucket["planned_cents"] += open_amount

    for source_event in historical_events:
        row = dict(source_event)
        due = _as_date(row.get("date") or row.get("due_date") or row.get("expected_date"))
        if due is None or due < today or due > end:
            continue
        amount = max(0, int(row.get("amount_cents") or 0))
        if amount <= 0:
            continue
        confirmed = bool(row.get("confirmed"))
        kind = "history_planned" if confirmed else "history_warning"
        event = {
            "id": str(row.get("id") or row.get("pattern_key") or ""),
            "kind": kind,
            "state_label": "Planned from history" if confirmed else "Likely from history · not planned",
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
                "history_planned": 3, "posted_planned": 4}
    for bucket in ordered_days:
        bucket["events"].sort(
            key=lambda item: (priority.get(str(item["kind"]), 9), -int(item["amount_cents"]), str(item["name"]))
        )

    past_events = [event for bucket in ordered_days if bucket["period"] in {"past", "today"}
                   for event in bucket["events"] if str(event["kind"]).startswith("posted_")]
    future_events = [event for bucket in ordered_days if bucket["period"] in {"today", "future"}
                     for event in bucket["events"]]
    return {
        "status": "ready",
        "as_of": today.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "past_days": past_days,
        "future_days": future_days,
        "actual_source": source,
        "days": ordered_days,
        "totals": {
            "posted_cents": sum(int(item["amount_cents"]) for item in past_events),
            "unplanned_posted_cents": sum(
                int(item["amount_cents"]) for item in past_events if item["kind"] == "posted_unplanned"
            ),
            "planned_cents": sum(
                int(item["amount_cents"]) for item in future_events
                if item["kind"] in {"planned", "history_planned"}
            ),
            "warning_cents": sum(
                int(item["amount_cents"]) for item in future_events if item["kind"] == "history_warning"
            ),
            "unplanned_count": sum(1 for item in past_events if item["kind"] == "posted_unplanned"),
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


def _historical_events(*, as_of: date, future_days: int) -> list[dict[str, Any]]:
    from sales_support_agent.services.cashflow.bill_patterns import (
        _occurrences_in_window,
        confirmed_bill_projections,
        list_bill_patterns,
    )

    events: list[dict[str, Any]] = []
    for projection in confirmed_bill_projections(as_of=as_of, horizon_days=future_days):
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
    return events


def load_cash_calendar(*, as_of: date | None = None) -> dict[str, Any]:
    """Load the live ledger, settlement evidence, and bank-pattern warnings."""
    from sales_support_agent.services.cashflow.obligations import list_obligations

    today = as_of or date.today()
    rows = list_obligations(limit=10_000)
    # Pattern analysis is advisory. A temporary issue there must not hide the
    # posted bank truth or known obligations from the owner.
    try:
        allocations = _load_active_allocations()
    except Exception:
        allocations = []
    try:
        history = _historical_events(as_of=today, future_days=FUTURE_DAYS)
    except Exception:
        history = []
    return build_cash_calendar(
        rows, allocations=allocations, historical_events=history, as_of=today
    )


def _event_html(event: Mapping[str, Any]) -> str:
    kind = html.escape(str(event.get("kind") or "planned"), quote=True)
    protected = bool(event.get("protected"))
    return f"""
      <li class="cash-calendar-event cash-calendar-event--{kind}" data-calendar-kind="{kind}">
        <div class="cash-calendar-event__main">
          <span class="cash-calendar-state cash-calendar-state--{kind}">{html.escape(str(event.get('state_label') or 'Expense'))}</span>
          <strong>{html.escape(str(event.get('name') or 'Expense'))}</strong>
          <p>{html.escape(str(event.get('evidence') or 'Evidence unavailable'))}{' · Protected cost' if protected else ''}</p>
        </div>
        <div class="cash-calendar-event__amount">
          <strong>{_money(int(event.get('amount_cents') or 0))}</strong>
          <a href="{html.escape(str(event.get('href') or '/admin/finances/review'), quote=True)}">{html.escape(str(event.get('action_label') or 'Review'))}</a>
        </div>
      </li>"""


def render_cash_calendar_page(calendar: Mapping[str, Any], *, flash: str = "") -> str:
    """Render the 7-day history, today, and 14-day forward expense view."""
    if calendar.get("status") != "ready":
        body = f"""<div class="money-brief">{render_finance_nav('calendar', counts={})}
        <div class="money-empty"><h1>The cash calendar could not load</h1>
        <p>Nothing was changed. Refresh the connected accounts, then try again.</p>
        <a class="btn btn-primary" href="/admin/finances/accounts">Check accounts</a></div></div>"""
        return _page_shell("Cash calendar", "calendar", body, flash=flash)

    totals = calendar.get("totals") or {}
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
            <p>{html.escape(summary)}</p></header>
            <ul>{event_rows or '<li class="cash-calendar-empty">Nothing is posted or expected for this day.</li>'}</ul>
          </article>""")

    body = f"""
    <div class="money-brief cash-calendar-page">
      {render_finance_nav('calendar', counts={})}
      <header class="money-page-header"><div><p class="finance-eyebrow">Cash calendar</p>
      <h1>See expenses before they surprise you</h1>
      <p class="money-page-subtitle">Seven days of posted spending, today, and the next fourteen days—separated into planned costs and historical warnings.</p></div>
      <div class="money-page-status"><span class="money-status money-status--ready">Read-only</span>
      <span>Posted source: {html.escape(str(calendar.get('actual_source') or 'unavailable').title())} · As of {html.escape(str(calendar.get('as_of') or 'today'))}</span></div></header>

      <section class="cash-calendar-summary" aria-label="Expense calendar summary">
        <article><span>Posted in the last 7 days</span><strong>{_money(int(totals.get('posted_cents') or 0))}</strong></article>
        <article class="cash-calendar-summary__attention"><span>Posted but not in plan</span><strong>{_money(int(totals.get('unplanned_posted_cents') or 0))}</strong><small>{int(totals.get('unplanned_count') or 0)} charge(s)</small></article>
        <article><span>Planned next 14 days</span><strong>{_money(int(totals.get('planned_cents') or 0))}</strong></article>
        <article class="cash-calendar-summary__warning"><span>Possible from history</span><strong>{_money(int(totals.get('warning_cents') or 0))}</strong><small>{int(totals.get('warning_count') or 0)} warning(s), not counted as required</small></article>
      </section>

      <aside class="cash-calendar-legend" aria-label="Calendar status meanings">
        <strong>How to read this</strong>
        <span><i class="is-posted"></i>Posted means it left the bank.</span>
        <span><i class="is-planned"></i>Planned means a known bill or schedule exists.</span>
        <span><i class="is-warning"></i>Likely from history is an early warning, not a confirmed bill.</span>
      </aside>

      <section class="cash-calendar-workspace" aria-labelledby="cash-calendar-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Daily drill-down</p>
        <h2 id="cash-calendar-title">Past 7 days · today · next 14 days</h2></div>
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
      </section>

      <footer class="money-proof-note"><strong>What the calendar does not assume</strong>
      <p>Historical warnings do not reduce projected cash and do not become planned bills until you confirm them. This page never moves money, cancels a vendor, runs payroll, or edits QuickBooks.</p></footer>
    </div>
    <script>
    (() => {{
      const filterButtons = [...document.querySelectorAll('[data-calendar-filter]')];
      const dateButtons = [...document.querySelectorAll('[data-calendar-date]')];
      const days = [...document.querySelectorAll('[data-calendar-day]')];
      const result = document.querySelector('[data-calendar-result]');
      const matches = (event, wanted) => wanted === 'all'
        || (wanted === 'attention' && ['posted_unplanned', 'history_warning'].includes(event.dataset.calendarKind))
        || (wanted === 'planned' && ['planned', 'history_planned'].includes(event.dataset.calendarKind))
        || (wanted === 'posted' && ['posted_planned', 'posted_unplanned'].includes(event.dataset.calendarKind));
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
