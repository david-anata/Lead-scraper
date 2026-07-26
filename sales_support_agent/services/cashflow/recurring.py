"""Schedules: the repeating bills and payments that build the next 14 and 30 days."""

from __future__ import annotations

import html
from datetime import date, datetime
from typing import Any, Mapping

from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.obligations import (
    create_recurring_template,
    delete_recurring_template,
    get_recurring_template,
    list_recurring_templates,
    update_recurring_template,
)
from sales_support_agent.services.cashflow.overview import _dollar, _page_shell

_CATEGORIES_OUT = [
    "debt", "rent", "payroll", "tax", "software", "utilities",
    "supplies", "equipment", "owner_draw", "insurance", "marketing",
    "professional_services", "other",
]
_CATEGORIES_IN = ["revenue", "client_payment", "interest", "other"]

# What one charge works out to in an average month. Every frequency is put on
# this single footing so one monthly total can be added up across all of them.
_MONTHLY_SHARE = {
    "weekly": 52 / 12,
    "biweekly": 26 / 12,
    "monthly": 1.0,
    "quarterly": 1 / 3,
    "annual": 1 / 12,
}

# Only "in pieces" is offered as a choice, but the other values the rest of
# Finance already uses are accepted so an answer set elsewhere survives a save.
_FLEXIBILITY_VALUES = {"unknown", "fixed", "chunkable", "deferrable"}
_FLEXIBILITY_CHOICES = [
    ("unknown", "Not sure yet"),
    ("fixed", "All at once, on the due date"),
    ("chunkable", "In pieces across the month"),
]
_PAUSED_WORDS = {"no", "false", "0", "off", "paused", "pause"}

# The stored frequency values in words, so nothing on the page reads like a field.
_FREQUENCY_CHOICES = [
    ("weekly", "Every week"),
    ("biweekly", "Every two weeks"),
    ("monthly", "Every month"),
    ("quarterly", "Every three months"),
    ("annual", "Once a year"),
]
_FREQUENCY_WORDS = dict(_FREQUENCY_CHOICES)


def _monthly_cents(amount_cents: Any, frequency: Any) -> int:
    """One charge expressed as what it costs in a month, in whole cents."""
    try:
        cents = int(amount_cents or 0)
    except (TypeError, ValueError):
        cents = 0
    share = _MONTHLY_SHARE.get(str(frequency or "").strip().lower(), 1.0)
    return round(cents * share)


def _is_running(template: Mapping[str, Any]) -> bool:
    """A schedule with no answer stored is running: silence must not pause a bill."""
    value = template.get("is_active", True)
    if isinstance(value, str):
        return value.strip().lower() not in _PAUSED_WORDS | {""}
    return bool(value)


def _is_chunkable(template: Mapping[str, Any]) -> bool:
    """Older rows predate this answer, so a missing one only means we do not know."""
    return str(template.get("flexibility") or "").strip().lower() == "chunkable"


def _parse_running(raw: Any) -> bool:
    """A form that says nothing about pausing must leave the bill running."""
    return str(raw if raw is not None else "").strip().lower() not in _PAUSED_WORDS


def _flexibility_value(raw: Any) -> str:
    """Anything unrecognised means we do not know how this one gets paid."""
    value = str(raw if raw is not None else "").strip().lower()
    return value if value in _FLEXIBILITY_VALUES else "unknown"


def _pretty(value: str) -> str:
    return value.replace("_", " ").capitalize()


def _template_form(*, action: str, values: dict | None = None, submit_label: str = "Save") -> str:
    v = values or {}
    today = datetime.utcnow().date().isoformat()

    def choose(field: str, choices: list[tuple[str, str]], current: str) -> str:
        """A select whose stored values stay as they are while the words change."""
        opts = "".join(
            f'<option value="{html.escape(value)}" {"selected" if value == current else ""}>'
            f"{html.escape(label)}</option>"
            for value, label in choices
        )
        return f'<select name="{field}">{opts}</select>'

    def inp(field: str, type_: str = "text", placeholder: str = "") -> str:
        val = html.escape(str(v.get(field, "")), quote=True)
        return f'<input type="{type_}" name="{field}" value="{val}" placeholder="{html.escape(placeholder, quote=True)}">'

    amount_dollars = ""
    if v.get("amount_cents"):
        try:
            amount_dollars = f"{int(v['amount_cents']) / 100:.2f}"
        except (ValueError, TypeError):
            pass

    categories = _CATEGORIES_OUT + [c for c in _CATEGORIES_IN if c not in _CATEGORIES_OUT]
    category_choices = [(c, _pretty(c)) for c in categories]
    current_category = str(v.get("category") or categories[0])
    current_frequency = str(v.get("frequency") or "monthly")
    current_flexibility = _flexibility_value(v.get("flexibility"))

    return f"""
    <form method="post" action="{html.escape(action)}">
      <div class="form-row">
        <div>
          <label>Name</label>
          {inp("name", placeholder="e.g. Office Rent")}
        </div>
        <div>
          <label>Who it goes to</label>
          {inp("vendor_or_customer", placeholder="e.g. Boulder Ranch")}
        </div>
      </div>
      <div class="form-row triple">
        <div>
          <label>Money out or money in</label>
          {choose("event_type", [("outflow", "Money going out"), ("inflow", "Money coming in")],
                  str(v.get("event_type") or "outflow"))}
        </div>
        <div>
          <label>What it is for</label>
          {choose("category", category_choices, current_category)}
        </div>
        <div>
          <label>How often</label>
          {choose("frequency", _FREQUENCY_CHOICES, current_frequency)}
        </div>
      </div>
      <div class="form-row triple">
        <div>
          <label>Amount ($)</label>
          <input type="number" name="amount_dollars" value="{html.escape(amount_dollars, quote=True)}"
                 min="0" step="0.01" placeholder="0.00">
        </div>
        <div>
          <label>Next date it is due</label>
          <input type="date" name="next_due_date" value="{html.escape(str(v.get('next_due_date', today)), quote=True)}">
        </div>
        <div>
          <label>Day of the month (optional)</label>
          <input type="number" name="day_of_month" min="1" max="31"
                 value="{html.escape(str(v.get('day_of_month') or ''), quote=True)}"
                 placeholder="e.g. 15">
        </div>
      </div>
      <div class="form-row">
        <div>
          <label>Running or paused</label>
          {choose("is_active", [("yes", "Running"), ("no", "Paused for now")],
                  "yes" if _is_running(v) else "no")}
          <p class="metric-note">A paused one stays on this page but is left out of your
             next 14 and 30 days.</p>
        </div>
        <div>
          <label>How you pay it</label>
          {choose("flexibility", _FLEXIBILITY_CHOICES, current_flexibility)}
          <p class="metric-note">Pick paying in pieces if you split this one up over the
             month instead of paying it all on one day.</p>
        </div>
      </div>
      <div class="action-row">
        <button type="submit" class="btn btn-primary">{html.escape(submit_label)}</button>
        <a href="/admin/finances/recurring" class="btn btn-secondary">Cancel</a>
      </div>
    </form>"""


def _summary_line(templates: list[dict]) -> str:
    """How many are running and what they cost in a month, in one sentence."""
    running = [t for t in templates if _is_running(t)]
    paused = len(templates) - len(running)

    monthly_out = sum(
        _monthly_cents(t.get("amount_cents"), t.get("frequency"))
        for t in running
        if str(t.get("event_type") or "outflow") != "inflow"
    )
    monthly_in = sum(
        _monthly_cents(t.get("amount_cents"), t.get("frequency"))
        for t in running
        if str(t.get("event_type") or "") == "inflow"
    )

    if running:
        sentences = [f"{len(running)} running, about {_dollar(monthly_out)} a month going out."]
    else:
        sentences = ["Nothing running, so none of this reaches your next 14 and 30 days."]
    if monthly_in:
        sentences.append(f"About {_dollar(monthly_in)} a month coming in.")
    if paused:
        held = "it is" if paused == 1 else "they are"
        sentences.append(f"{paused} paused, so {held} left out.")
    return " ".join(sentences)


def _empty_state() -> str:
    """Nothing to list yet, so point at the two ways to fill the page."""
    return """
    <div class="card">
      <p style="margin:0 0 10px">Nothing repeating here yet. Your next 14 and 30 days stay
         empty until your regular bills and payments are on this page.</p>
      <p style="margin:0 0 14px">Rather not type them all in? Your bank already shows what
         goes out every month, and we can read it back to you.</p>
      <div class="action-row" style="margin:0">
        <a href="/admin/finances/whats-coming" class="btn btn-primary">Find them in my bank history</a>
        <a href="/admin/finances/recurring/new" class="btn btn-secondary">+ Add a bill</a>
      </div>
    </div>"""


def render_recurring_page(*, flash: str = "") -> str:
    templates = list_recurring_templates(active_only=False)

    rows_html = ""
    for t in templates:
        raw = t.get("next_due_date", "")
        try:
            nd = date.fromisoformat(str(raw)[:10]).strftime("%b %d, %Y")
        except ValueError:
            # A row can reach here with no date at all, and "None" means nothing to him.
            nd = str(raw) if raw else "Not set"
        cents = int(t.get("amount_cents") or 0)
        tid = html.escape(str(t.get("id", "")))
        running = _is_running(t)
        active_badge = (
            '<span class="badge badge-ok">Running</span>'
            if running
            else '<span class="badge" style="background:#eee;color:#888">Paused</span>'
        )
        type_cls = "amount-in" if t.get("event_type") == "inflow" else "amount-out"

        # The badge says the state; these say what the state does to his numbers.
        notes = []
        if not running:
            notes.append("Paused, so it is left out of your next 14 and 30 days.")
        if _is_chunkable(t):
            notes.append("You pay this one in pieces across the month.")
        notes_html = (
            f'<div class="metric-note">{html.escape(" ".join(notes))}</div>' if notes else ""
        )

        rows_html += f"""
        <tr>
          <td>{html.escape(str(t.get("name", "")))}{notes_html}</td>
          <td>{html.escape(str(t.get("vendor_or_customer") or "Not set"))}</td>
          <td class="{type_cls}">{_dollar(cents)}</td>
          <td>{html.escape(_FREQUENCY_WORDS.get(str(t.get("frequency") or ""), str(t.get("frequency") or "")))}</td>
          <td>{html.escape(nd)}</td>
          <td>{active_badge}</td>
          <td style="white-space:nowrap">
            <a href="/admin/finances/recurring/{tid}/edit" class="btn btn-secondary btn-sm">Edit</a>
            <form method="post" action="/admin/finances/recurring/{tid}/delete" style="display:inline"
                  onsubmit="return confirm('Remove this from your schedules?')">
              <button type="submit" class="btn btn-secondary btn-sm" style="color:#b93535">Remove</button>
            </form>
          </td>
        </tr>"""

    table_html = f"""
    <p class="page-sub" style="margin-bottom:10px">{html.escape(_summary_line(templates))}</p>
    <div class="card">
      <table>
        <thead>
          <tr><th>Name</th><th>Who it goes to</th><th>Amount</th><th>How often</th>
              <th>Next date</th><th>Running</th><th></th></tr>
        </thead>
        <tbody>
          {rows_html}
        </tbody>
      </table>
    </div>"""

    body = f"""
    {render_finance_nav("schedules")}
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
      <h1>Schedules</h1>
      <div class="action-row" style="margin:0">
        <a href="/admin/finances/recurring/new" class="btn btn-primary">+ Add a bill</a>
        <form method="post" action="/admin/finances/recurring/generate" style="display:inline">
          <button type="submit" class="btn btn-secondary">Fill in what is coming</button>
        </form>
      </div>
    </div>
    <p class="page-sub">Your repeating bills and payments. These are what build your next
       14 and 30 days, so anything missing here is missing from your plan.</p>
    {table_html if templates else _empty_state()}"""

    return _page_shell("Schedules", "schedules", body, flash=flash)


def render_recurring_new_page(*, flash: str = "") -> str:
    body = f"""
    <h1>Add a bill or payment</h1>
    <p class="page-sub">Something that comes round again. Once it is here it shows up in
       your next 14 and 30 days.</p>
    <div class="card">
      {_template_form(action="/admin/finances/recurring/new", submit_label="Save it")}
    </div>"""
    return _page_shell("Add a bill or payment", "schedules", body, flash=flash)


def render_recurring_edit_page(template_id: str, *, flash: str = "") -> str:
    t = get_recurring_template(template_id)
    if t is None:
        return _page_shell(
            "Not found", "schedules",
            '<div class="card"><p>That schedule is not here any more.</p></div>',
        )
    body = f"""
    <h1>Edit this schedule</h1>
    <p class="page-sub">{html.escape(str(t.get("name", template_id)))}</p>
    <div class="card">
      {_template_form(action=f"/admin/finances/recurring/{html.escape(template_id)}/edit", values=t, submit_label="Save changes")}
    </div>"""
    return _page_shell("Edit this schedule", "schedules", body, flash=flash)


def parse_template_form(form: dict) -> dict:
    amount_dollars = str(form.get("amount_dollars", "0") or "0").strip().replace(",", "")
    try:
        from decimal import Decimal
        amount_cents = int(Decimal(amount_dollars) * 100)
    except Exception:
        amount_cents = 0

    raw_date = str(form.get("next_due_date", "") or "").strip()
    try:
        next_due = date.fromisoformat(raw_date)
    except ValueError:
        next_due = datetime.utcnow().date()

    day_of_month = None
    raw_dom = str(form.get("day_of_month", "") or "").strip()
    if raw_dom.isdigit():
        day_of_month = int(raw_dom)

    return {
        "name": str(form.get("name", "") or "").strip(),
        "vendor_or_customer": str(form.get("vendor_or_customer", "") or "").strip(),
        "event_type": str(form.get("event_type", "outflow") or "outflow"),
        "category": str(form.get("category", "other") or "other"),
        "amount_cents": amount_cents,
        "frequency": str(form.get("frequency", "monthly") or "monthly"),
        "next_due_date": next_due,
        "day_of_month": day_of_month,
        "is_active": _parse_running(form.get("is_active")),
        "flexibility": _flexibility_value(form.get("flexibility")),
    }
