"""The individual charges behind one week's total, and what each one is.

The weekly roll-up answers "how much" and refuses to answer "of what". Every
charge is already in the calendar, grouped by day; this regroups the same data by
week and by evidence state so a total can be opened and interrogated.

Two rules it holds
------------------
The listed charges must add up to exactly the figure that was clicked. Two
numbers describing the same money is the shape that has produced every wrong
answer in this section, so it is asserted rather than assumed.

Saying a charge is monthly is the same act as tracking it on the What is coming
page, and it writes to the same place. A separate cadence flag living only here
would give two sources of truth about one charge.
"""

from __future__ import annotations

import html
from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

# The three columns of the weekly roll-up, and the event states behind each.
STATE_FILTERS: dict[str, tuple[str, frozenset[str]]] = {
    "paid": ("paid", frozenset({"paid"})),
    "unpaid": ("still due", frozenset({"unpaid", "partially_paid"})),
    "possible": ("possible", frozenset({"unconfirmed"})),
}
# What the operator can say a charge is. "One-time" keeps watching the supplier;
# "not a bill" stops suggesting them at all. David asked for both, distinctly.
CADENCE_CHOICES = (
    ("monthly", "Monthly"),
    ("weekly", "Weekly"),
    ("one_time", "One-time"),
    ("not_a_bill", "Not a bill"),
)
VALID_CADENCES = frozenset(key for key, _label in CADENCE_CHOICES)


def _as_date(value: Any) -> date | None:
    """datetime first: it subclasses date, so the other order returns a timestamp."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def build_charge_drilldown(
    calendar: Mapping[str, Any],
    *,
    week_start: date,
    state: str,
    patterns: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """The charges in one week and one evidence state, newest date first."""
    if state not in STATE_FILTERS:
        raise ValueError("choose paid, unpaid or possible")
    label, statuses = STATE_FILTERS[state]
    week_end = week_start + timedelta(days=6)

    known = _pattern_index(patterns)
    charges: list[dict[str, Any]] = []
    for bucket in calendar.get("days") or []:
        when = _as_date(bucket.get("date"))
        if when is None or when < week_start or when > week_end:
            continue
        for event in bucket.get("events") or []:
            if str(event.get("payment_status") or "") not in statuses:
                continue
            vendor = str(event.get("name") or "Unknown")
            charges.append({
                "date": when,
                "vendor": vendor,
                "amount_cents": int(event.get("amount_cents") or 0),
                "category": str(event.get("category") or ""),
                "state_label": str(event.get("state_label") or ""),
                "href": str(event.get("href") or ""),
                "protected": bool(event.get("protected")),
                **_what_we_know(vendor, known),
            })

    charges.sort(key=lambda item: (item["date"], -item["amount_cents"]))
    return {
        "week_start": week_start,
        "week_end": week_end,
        "state": state,
        "state_label": label,
        "charges": charges,
        "total_cents": sum(item["amount_cents"] for item in charges),
        "count": len(charges),
    }


def _pattern_index(patterns: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    index: dict[str, Mapping[str, Any]] = {}
    for pattern in patterns:
        key = str(pattern.get("merchant_key") or "")
        if key:
            index[key] = pattern
    return index


def _what_we_know(vendor: str, known: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """What the app already believes about this supplier, so the operator is
    correcting a guess rather than answering from nothing."""
    from sales_support_agent.services.cashflow.bill_patterns import (
        bill_merchant_key,
        bill_pattern_key,
    )

    merchant = bill_merchant_key(vendor)
    pattern = known.get(merchant)
    if pattern is None:
        return {
            "merchant_key": merchant,
            "pattern_key": bill_pattern_key(merchant) if merchant else "",
            "known": "",
            "decision": "",
            "has_pattern": False,
        }
    return {
        "merchant_key": merchant,
        "pattern_key": str(pattern.get("pattern_key") or ""),
        "known": str(pattern.get("why") or ""),
        "decision": str(pattern.get("decision") or ""),
        "has_pattern": True,
    }


def cadence_to_decision(cadence: str) -> str:
    """Map the operator's answer onto the decision the bill queue already stores.

    Monthly and weekly both mean "this is a real recurring bill", which is what
    tracking one does. One-time and not a bill both stop it counting, and are
    kept apart because they mean different things next month: one keeps watching
    the supplier, the other stops suggesting them.
    """
    if cadence in {"monthly", "weekly"}:
        return "track"
    if cadence == "one_time":
        return "snooze"
    if cadence == "not_a_bill":
        return "not_a_bill"
    raise ValueError("choose monthly, weekly, one-time or not a bill")


def render_charge_panel(view: Mapping[str, Any], *, action: str) -> str:
    """The panel body. Rendered server side so it works without JavaScript."""
    from sales_support_agent.services.cashflow.overview import _money

    charges = list(view.get("charges") or [])
    heading = (
        f"{_week_words(view)} , {view.get('state_label')}"
        .replace(" ,", ",")
    )
    if not charges:
        return (
            '<section class="charge-panel"><h2>' + html.escape(heading) + "</h2>"
            '<p class="finance-plan-ok">Nothing in this week is '
            + html.escape(str(view.get("state_label") or "")) + ".</p></section>"
        )

    rows = "".join(_charge_row(charge, action=action) for charge in charges)
    return f"""
    <section class="charge-panel" aria-labelledby="charge-panel-title">
      <h2 id="charge-panel-title">{html.escape(heading)}</h2>
      <p class="metric-note">{int(view.get('count') or 0)} charge(s),
      {_money(int(view.get('total_cents') or 0))} in total. Saying what a charge
      is here is the same as tracking it under What is coming.</p>
      <div class="charge-panel__list">{rows}</div>
      <p class="charge-panel__total"><strong>{int(view.get('count') or 0)} charge(s)</strong>
      <strong>{_money(int(view.get('total_cents') or 0))}</strong></p>
    </section>"""


def _week_words(view: Mapping[str, Any]) -> str:
    start, end = view.get("week_start"), view.get("week_end")
    if not isinstance(start, date) or not isinstance(end, date):
        return "This week"
    return (
        f"{start.strftime('%b %d').replace(' 0', ' ')} to "
        f"{end.strftime('%b %d').replace(' 0', ' ')}"
    )


def _charge_row(charge: Mapping[str, Any], *, action: str) -> str:
    from sales_support_agent.services.cashflow.overview import _money

    when = charge.get("date")
    day = when.strftime("%a %d") if isinstance(when, date) else ""
    vendor = html.escape(str(charge.get("vendor") or ""))
    known = str(charge.get("known") or "")
    note = (
        f'<p class="metric-note">{html.escape(known)}</p>' if known
        else '<p class="metric-note">No pattern found yet.</p>'
    )
    decided = str(charge.get("decision") or "")
    settled = (
        f'<p class="metric-note">Already answered: {html.escape(decided)}</p>'
        if decided else ""
    )
    key = html.escape(str(charge.get("pattern_key") or ""), quote=True)
    buttons = "".join(
        f'<button type="submit" name="cadence" value="{value}" '
        f'class="btn btn-secondary btn-sm">{label}</button>'
        for value, label in CADENCE_CHOICES
    ) if key else (
        '<span class="metric-note">Cannot be named yet: the bank wording gives '
        "no supplier to attach an answer to.</span>"
    )
    form = f"""
      <form method="post" action="{action}" class="charge-panel__actions">
        <input type="hidden" name="pattern_key" value="{key}">
        <input type="hidden" name="merchant_key" value="{html.escape(str(charge.get('merchant_key') or ''), quote=True)}">
        <input type="hidden" name="vendor" value="{vendor}">
        {buttons}
      </form>""" if key else f'<div class="charge-panel__actions">{buttons}</div>'
    return f"""
      <article class="charge-panel__row">
        <div class="charge-panel__head">
          <div><strong>{vendor}</strong><small>{html.escape(day)}</small></div>
          <strong class="amount-out">{_money(int(charge.get('amount_cents') or 0))}</strong>
        </div>
        {note}{settled}{form}
      </article>"""
