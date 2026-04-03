"""Accounts Receivable — list, add, edit, delete inflow obligations."""

from __future__ import annotations

import html
from datetime import date, datetime

from sales_support_agent.services.cashflow.obligations import get_obligation, list_obligations
from sales_support_agent.services.cashflow.overview import _dollar, _page_shell

_CATEGORIES = [
    "revenue", "client_payment", "refund", "transfer", "interest", "other",
]

_STATUSES = ["planned", "pending", "paid", "cancelled"]


def _ar_form(*, action: str, values: dict | None = None, submit_label: str = "Save") -> str:
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

    return f"""
    <form method="post" action="{html.escape(action)}">
      <div class="form-row">
        <div>
          <label>Description</label>
          {inp("name", placeholder="e.g. Client — April invoice")}
        </div>
        <div>
          <label>Customer / Source</label>
          {inp("vendor_or_customer", placeholder="e.g. Acme Corp")}
        </div>
      </div>
      <div class="form-row triple">
        <div>
          <label>Expected Amount ($)</label>
          <input type="number" name="amount_dollars" value="{html.escape(amount_dollars, quote=True)}"
                 min="0" step="0.01" placeholder="0.00">
        </div>
        <div>
          <label>Expected Date</label>
          <input type="date" name="due_date" value="{html.escape(str(v.get('due_date', today)), quote=True)}">
        </div>
        <div>
          <label>Status</label>
          {sel("status", _STATUSES)}
        </div>
      </div>
      <div class="form-row">
        <div>
          <label>Category</label>
          {sel("category", _CATEGORIES)}
        </div>
        <div>
          <label>Confidence</label>
          {sel("confidence", ["confirmed", "estimated"])}
        </div>
      </div>
      <div class="form-row single">
        <div>
          <label>Notes</label>
          <textarea name="notes" rows="2" placeholder="Optional">{html.escape(str(v.get('notes', '')))}</textarea>
        </div>
      </div>
      <div class="action-row">
        <button type="submit" class="btn btn-primary">{html.escape(submit_label)}</button>
        <a href="/admin/finances/ar" class="btn btn-secondary">Cancel</a>
      </div>
    </form>"""


def render_expected_ar_page(*, flash: str = "") -> str:
    rows = list_obligations(event_type="inflow", limit=500)
    active = [r for r in rows if r.get("status") not in ("paid", "cancelled", "matched")]
    today = datetime.utcnow().date()

    def row_html(r: dict) -> str:
        raw_date = r.get("due_date", "")
        try:
            due = date.fromisoformat(str(raw_date)[:10])
            date_s = due.strftime("%b %d, %Y")
            overdue = due < today and r.get("status") not in ("paid", "matched", "cancelled")
        except ValueError:
            date_s = str(raw_date)
            overdue = False

        status = str(r.get("status", "planned"))
        status_cls = f"status-{status}" if status in ("planned", "pending", "overdue", "paid", "matched") else ""
        cents = int(r.get("amount_cents") or 0)
        event_id = html.escape(str(r.get("id", "")))

        return f"""
        <tr>
          <td>{html.escape(date_s)}{"&nbsp;⚠" if overdue else ""}</td>
          <td>{html.escape(str(r.get("name") or r.get("vendor_or_customer") or "—"))}</td>
          <td>{html.escape(str(r.get("category", "")))}</td>
          <td class="amount-in">{_dollar(cents)}</td>
          <td><span class="status-pill {status_cls}">{html.escape(status)}</span></td>
          <td style="white-space:nowrap">
            <a href="/admin/finances/ar/{event_id}/edit" class="btn btn-secondary btn-sm">Edit</a>
            <form method="post" action="/admin/finances/ar/{event_id}/delete" style="display:inline"
                  onsubmit="return confirm('Delete this entry?')">
              <button type="submit" class="btn btn-secondary btn-sm" style="color:#b93535">Delete</button>
            </form>
          </td>
        </tr>"""

    rows_html = "".join(row_html(r) for r in active)
    total = sum(int(r.get("amount_cents") or 0) for r in active)

    body = f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
      <h1>Receivables (AR)</h1>
      <a href="/admin/finances/ar/new" class="btn btn-primary">+ Add Receivable</a>
    </div>
    <p class="page-sub">Expected inflows · {len(active)} open · {_dollar(total)} expected</p>
    <div class="card">
      <table>
        <thead>
          <tr><th>Expected</th><th>Description</th><th>Category</th><th>Amount</th><th>Status</th><th></th></tr>
        </thead>
        <tbody>
          {rows_html if rows_html else '<tr><td colspan="6" class="empty-state">No receivables yet.</td></tr>'}
        </tbody>
      </table>
    </div>"""

    return _page_shell("Receivables (AR)", "ar", body, flash=flash)


def render_ar_new_page(*, flash: str = "") -> str:
    body = f"""
    <h1>Add Receivable</h1>
    <p class="page-sub">Log an expected inflow</p>
    <div class="card">
      {_ar_form(action="/admin/finances/ar/new", submit_label="Add Receivable")}
    </div>"""
    return _page_shell("Add Receivable", "ar", body, flash=flash)


def render_ar_edit_page(event_id: str, *, flash: str = "") -> str:
    row = get_obligation(event_id)
    if row is None:
        return _page_shell("Not Found", "ar", '<div class="card"><p>Entry not found.</p></div>')
    body = f"""
    <h1>Edit Receivable</h1>
    <p class="page-sub">{html.escape(str(row.get("name") or row.get("vendor_or_customer") or event_id))}</p>
    <div class="card">
      {_ar_form(action=f"/admin/finances/ar/{html.escape(event_id)}/edit", values=row, submit_label="Save Changes")}
    </div>"""
    return _page_shell("Edit Receivable", "ar", body, flash=flash)
