"""Recurring templates management page."""

from __future__ import annotations

import html
from datetime import date, datetime

from sales_support_agent.services.cashflow.obligations import (
    create_recurring_template,
    delete_recurring_template,
    get_recurring_template,
    list_recurring_templates,
    update_recurring_template,
)
from sales_support_agent.services.cashflow.overview import _dollar, _page_shell

_FREQUENCIES = ["weekly", "biweekly", "monthly", "quarterly", "annual"]
_CATEGORIES_OUT = [
    "debt", "rent", "payroll", "tax", "software", "utilities",
    "supplies", "equipment", "owner_draw", "insurance", "marketing",
    "professional_services", "other",
]
_CATEGORIES_IN = ["revenue", "client_payment", "interest", "other"]


def _template_form(*, action: str, values: dict | None = None, submit_label: str = "Save") -> str:
    v = values or {}
    today = datetime.utcnow().date().isoformat()

    def sel(field: str, options: list[str]) -> str:
        cur = v.get(field, options[0])
        opts = "".join(
            f'<option value="{html.escape(o)}" {"selected" if o == cur else ""}>{html.escape(o)}</option>'
            for o in options
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

    return f"""
    <form method="post" action="{html.escape(action)}">
      <div class="form-row">
        <div>
          <label>Name</label>
          {inp("name", placeholder="e.g. Office Rent")}
        </div>
        <div>
          <label>Vendor / Customer</label>
          {inp("vendor_or_customer", placeholder="e.g. Boulder Ranch")}
        </div>
      </div>
      <div class="form-row triple">
        <div>
          <label>Type</label>
          {sel("event_type", ["outflow", "inflow"])}
        </div>
        <div>
          <label>Category</label>
          {sel("category", categories)}
        </div>
        <div>
          <label>Frequency</label>
          {sel("frequency", _FREQUENCIES)}
        </div>
      </div>
      <div class="form-row triple">
        <div>
          <label>Amount ($)</label>
          <input type="number" name="amount_dollars" value="{html.escape(amount_dollars, quote=True)}"
                 min="0" step="0.01" placeholder="0.00">
        </div>
        <div>
          <label>Next Due Date</label>
          <input type="date" name="next_due_date" value="{html.escape(str(v.get('next_due_date', today)), quote=True)}">
        </div>
        <div>
          <label>Day of Month (optional)</label>
          <input type="number" name="day_of_month" min="1" max="31"
                 value="{html.escape(str(v.get('day_of_month') or ''), quote=True)}"
                 placeholder="e.g. 15">
        </div>
      </div>
      <div class="action-row">
        <button type="submit" class="btn btn-primary">{html.escape(submit_label)}</button>
        <a href="/admin/finances/recurring" class="btn btn-secondary">Cancel</a>
      </div>
    </form>"""


def render_recurring_page(*, flash: str = "") -> str:
    templates = list_recurring_templates(active_only=False)

    rows_html = ""
    for t in templates:
        raw = t.get("next_due_date", "")
        try:
            nd = date.fromisoformat(str(raw)[:10]).strftime("%b %d, %Y")
        except ValueError:
            nd = str(raw)
        cents = int(t.get("amount_cents") or 0)
        tid = html.escape(str(t.get("id", "")))
        active = bool(t.get("is_active", True))
        active_badge = (
            '<span class="badge badge-ok">Active</span>'
            if active
            else '<span class="badge" style="background:#eee;color:#888">Paused</span>'
        )
        type_cls = "amount-in" if t.get("event_type") == "inflow" else "amount-out"

        rows_html += f"""
        <tr>
          <td>{html.escape(str(t.get("name", "")))}</td>
          <td>{html.escape(str(t.get("vendor_or_customer", "—")))}</td>
          <td class="{type_cls}">{_dollar(cents)}</td>
          <td>{html.escape(str(t.get("frequency", "")))}</td>
          <td>{html.escape(nd)}</td>
          <td>{active_badge}</td>
          <td style="white-space:nowrap">
            <a href="/admin/finances/recurring/{tid}/edit" class="btn btn-secondary btn-sm">Edit</a>
            <form method="post" action="/admin/finances/recurring/{tid}/delete" style="display:inline"
                  onsubmit="return confirm('Delete this template?')">
              <button type="submit" class="btn btn-secondary btn-sm" style="color:#b93535">Delete</button>
            </form>
          </td>
        </tr>"""

    body = f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
      <h1>Recurring Templates</h1>
      <div class="action-row" style="margin:0">
        <a href="/admin/finances/recurring/new" class="btn btn-primary">+ Add Template</a>
        <form method="post" action="/admin/finances/recurring/generate" style="display:inline">
          <button type="submit" class="btn btn-secondary">Generate Upcoming</button>
        </form>
      </div>
    </div>
    <p class="page-sub">Manage recurring obligations — click "Generate Upcoming" to create events for the next 90 days</p>
    <div class="card">
      <table>
        <thead>
          <tr><th>Name</th><th>Vendor</th><th>Amount</th><th>Frequency</th><th>Next Due</th><th>Status</th><th></th></tr>
        </thead>
        <tbody>
          {rows_html if rows_html else '<tr><td colspan="7" class="empty-state">No recurring templates yet.</td></tr>'}
        </tbody>
      </table>
    </div>"""

    return _page_shell("Recurring Templates", "recurring", body, flash=flash)


def render_recurring_new_page(*, flash: str = "") -> str:
    body = f"""
    <h1>New Recurring Template</h1>
    <p class="page-sub">Create a recurring obligation or income source</p>
    <div class="card">
      {_template_form(action="/admin/finances/recurring/new", submit_label="Create Template")}
    </div>"""
    return _page_shell("New Recurring Template", "recurring", body, flash=flash)


def render_recurring_edit_page(template_id: str, *, flash: str = "") -> str:
    t = get_recurring_template(template_id)
    if t is None:
        return _page_shell("Not Found", "recurring", '<div class="card"><p>Template not found.</p></div>')
    body = f"""
    <h1>Edit Recurring Template</h1>
    <p class="page-sub">{html.escape(str(t.get("name", template_id)))}</p>
    <div class="card">
      {_template_form(action=f"/admin/finances/recurring/{html.escape(template_id)}/edit", values=t, submit_label="Save Changes")}
    </div>"""
    return _page_shell("Edit Template", "recurring", body, flash=flash)


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
    }
