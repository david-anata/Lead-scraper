"""What is coming: the bills the bank history says are probably on their way.

The Schedules page only knows the bills somebody typed in. ``bill_patterns``
reads the posted bank history and works out the rest. This page is where each
one gets an answer, because silence is not consent: only a bill the operator
tracks reaches the forecast, and nothing here writes a cash event.
"""

from __future__ import annotations

import html
from datetime import date
from typing import Any, Mapping, Sequence

from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.overview import _money, _page_shell

# One wording for how often a bill lands, shared with the Schedules page so
# there is no second copy of the words to drift.
from sales_support_agent.services.cashflow.recurring import _FREQUENCY_WORDS

DECIDE_ACTION = "/admin/finances/whats-coming/decide"
# The navigation key this page owns, so its own tab is the one lit up.
NAV_KEY = "whats_coming"
PAGE_PATH = "/admin/finances/whats-coming"

# How many past payments to show as evidence per bill. list_bill_patterns
# already trims its evidence list; this only guards against a longer one.
_EVIDENCE_SHOWN = 6


def _frequency_words(frequency: Any) -> str:
    key = str(frequency or "").strip().lower()
    return _FREQUENCY_WORDS.get(key, key or "Not sure how often")


def _day(value: Any) -> str:
    """A date in words. Anything unreadable says so rather than showing None."""
    if isinstance(value, date):
        return value.strftime("%b %d, %Y")
    try:
        return date.fromisoformat(str(value)[:10]).strftime("%b %d, %Y")
    except ValueError:
        return "not sure yet"


def _short_day(value: Any) -> str:
    if isinstance(value, date):
        return value.strftime("%b %d")
    try:
        return date.fromisoformat(str(value)[:10]).strftime("%b %d")
    except ValueError:
        return "date unknown"


def _evidence_line(evidence: Sequence[Mapping[str, Any]]) -> str:
    """The payments this prediction is built on, so it can be checked."""
    if not evidence:
        return ""
    parts = [
        html.escape(f"{_money(int(row.get('amount_cents') or 0))} on {_short_day(row.get('due_date'))}")
        for row in list(evidence)[:_EVIDENCE_SHOWN]
    ]
    return (
        '<p class="metric-note" style="margin:8px 0 0">Past payments: '
        + " &middot; ".join(parts)
        + "</p>"
    )


def _summary_line(counts: Mapping[str, Any]) -> str:
    """How much is waiting for an answer and what it costs, in one sentence."""
    waiting = int(counts.get("unreviewed") or 0)
    confirmed = int(counts.get("confirmed") or 0)
    tracked = int(counts.get("tracked") or 0)
    monthly = int(counts.get("monthly_cost_cents") or 0)

    sentences: list[str] = []
    if waiting:
        bills = "bill needs" if waiting == 1 else "bills need"
        sentences.append(f"{waiting} {bills} an answer.")
    else:
        sentences.append("Everything found in your bank history has an answer.")
    if confirmed:
        counted = "it is" if confirmed == 1 else "they are"
        sentences.append(f"{confirmed} you already track, counting from the date each one is next due.")
    if monthly:
        sentences.append(f"Together these run about {_money(monthly)} a month.")
    if tracked:
        already = "one" if tracked == 1 else "ones"
        sentences.append(f"{tracked} {already} you already have a schedule for are tucked away below.")
    return " ".join(sentences)


def _decision_note(decision: str, next_due: Any = None) -> str:
    """What the last answer did, said as an outcome rather than a state.

    It used to promise "counts in your next 14 and 30 days" for every tracked
    bill. A bill due in four weeks does not show up in a fortnight, so the
    operator clicked, watched the 14 day figure stay put, and reasonably
    concluded nothing had happened. Now it says which window it lands in.
    """
    if decision != "track":
        return ""
    days = _days_away(next_due)
    if days is None:
        where = "it counts once its date is known"
    elif days <= 14:
        where = "it counts in your next 14 days"
    elif days <= 30:
        where = (
            "the next one is more than a fortnight out, so it shows in your 30 day "
            "view and not the 14 day one yet"
        )
    else:
        where = (
            f"the next one is about {days} days out, so it is beyond both your 14 and "
            "30 day views for now"
        )
    return f'<p class="metric-note" style="margin:8px 0 0">You track this one, and {where}.</p>'


def _days_away(next_due: Any) -> int | None:
    if not isinstance(next_due, date):
        return None
    return (next_due - date.today()).days


def _pattern_card(pattern: Mapping[str, Any]) -> str:
    vendor = html.escape(str(pattern.get("vendor") or "Someone we could not name"))
    amount = html.escape(_money(int(pattern.get("amount_cents") or 0)))
    how_often = html.escape(_frequency_words(pattern.get("frequency")))
    when = html.escape(_day(pattern.get("next_due")))
    label = html.escape(str(pattern.get("confidence_label") or "Possible"))
    why = html.escape(str(pattern.get("why") or ""))
    key = html.escape(str(pattern.get("pattern_key") or ""), quote=True)
    decision = str(pattern.get("decision") or "")

    return f"""
    <div class="card">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:14px">
        <div>
          <h2 style="margin:0">{vendor}</h2>
          <p style="margin:4px 0 0;font-size:13px">{label}. Next one around {when}.</p>
        </div>
        <div style="text-align:right;white-space:nowrap">
          <strong class="amount-out">{amount}</strong>
          <div class="metric-note">{how_often}</div>
        </div>
      </div>
      <p style="margin:8px 0 0;font-size:13px;color:#6b7a8d">Why we think so: {why}.</p>
      {_evidence_line(pattern.get("evidence") or [])}
      {_decision_note(decision, pattern.get("next_due"))}
      <form method="post" action="{DECIDE_ACTION}" class="action-row" style="margin:12px 0 0">
        <input type="hidden" name="pattern_key" value="{key}">
        <input type="hidden" name="return_to" value="{PAGE_PATH}">
        <button type="submit" name="decision" value="track" class="btn btn-primary">Track this</button>
        <button type="submit" name="decision" value="not_a_bill" class="btn btn-secondary">Not a bill</button>
        <button type="submit" name="decision" value="snooze" class="btn btn-secondary">Not now</button>
      </form>
    </div>"""


def _tracked_section(tracked: Sequence[Mapping[str, Any]]) -> str:
    """The ones a schedule already covers, folded away so they stop asking."""
    if not tracked:
        return ""
    rows = "".join(
        "<tr><td>" + html.escape(str(row.get("vendor") or ""))
        + '</td><td class="amount-out">' + html.escape(_money(int(row.get("amount_cents") or 0)))
        + "</td><td>" + html.escape(_frequency_words(row.get("frequency")))
        + "</td><td>" + html.escape(_day(row.get("next_due")))
        + "</td></tr>"
        for row in tracked
    )
    count = len(tracked)
    heading = f"Already on your schedule ({count})"
    return f"""
    <details class="card">
      <summary style="cursor:pointer;font-weight:600">{html.escape(heading)}</summary>
      <p style="font-size:13px;color:#6b7a8d;margin:10px 0">These already have a schedule here,
         so they are in your plan and we are not asking about them again.</p>
      <table class="finance-accounts-table">
        <thead><tr><th>Who to</th><th>Amount</th><th>How often</th><th>Next one</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </details>"""


def _empty_state() -> str:
    return """
    <div class="card">
      <p style="margin:0 0 10px">Nothing to add. Your bank history has no regular payment
         that is missing from your schedules yet.</p>
      <p style="margin:0 0 14px">It takes a few months of the same payment before we will
         say a bill is coming, so a new one shows up here later rather than straight away.</p>
      <div class="action-row" style="margin:0">
        <a href="/admin/finances/recurring" class="btn btn-secondary">Back to schedules</a>
      </div>
    </div>"""


def render_whats_coming_page(*, flash: str = "") -> str:
    """The predicted bills, biggest monthly cost first, each with three answers."""
    from sales_support_agent.services.cashflow.bill_patterns import list_bill_patterns

    try:
        listing = list_bill_patterns()
    except Exception as exc:
        # A page that cannot read the history must say so, not show an empty list
        # that reads as "no bills are coming".
        return _page_shell(
            "What is coming", NAV_KEY,
            render_finance_nav(NAV_KEY)
            + "<h1>What is coming</h1>"
            + '<div class="card"><p style="margin:0">Your bank history could not be read '
            f"just now: {html.escape(str(exc))}</p></div>",
            flash=flash,
        )

    patterns = listing["patterns"]
    tracked = listing["tracked"]
    counts = listing["counts"]

    if not patterns and not tracked:
        body_main = _empty_state()
        summary = (
            "Nothing found yet. Your bank history has no repeating payment missing from "
            "your schedules."
        )
    else:
        body_main = "".join(_pattern_card(pattern) for pattern in patterns) + _tracked_section(tracked)
        summary = _summary_line(counts)
        if not patterns:
            body_main = (
                '<div class="card"><p style="margin:0">Nothing left to answer. Everything your '
                "bank history turned up already has a schedule here.</p></div>" + _tracked_section(tracked)
            )

    body = f"""
    {render_finance_nav(NAV_KEY)}
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
      <h1>What is coming</h1>
      <div class="action-row" style="margin:0">
        <a href="/admin/finances/recurring" class="btn btn-secondary">Back to schedules</a>
      </div>
    </div>
    <p class="page-sub">Bills we found in your own bank history that are not on your schedules
       yet. Nothing counts against your cash until you say to track it.</p>
    <p class="page-sub" style="margin-bottom:10px">{html.escape(summary)}</p>
    {body_main}"""

    return _page_shell("What is coming", NAV_KEY, body, flash=flash)
