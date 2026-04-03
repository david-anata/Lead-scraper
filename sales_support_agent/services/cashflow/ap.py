"""Accounts Payable — list, add, edit, delete outflow obligations."""

from __future__ import annotations

import html
from datetime import date, datetime

from sales_support_agent.services.cashflow.obligations import (
    create_obligation,
    delete_obligation,
    get_obligation,
    list_obligations,
    update_obligation,
)
from sales_support_agent.services.cashflow.overview import _dollar, _page_shell

_CATEGORIES = [
    "debt", "rent", "payroll", "tax", "software", "utilities",
    "supplies", "equipment", "owner_draw", "transfer", "insurance",
    "marketing", "professional_services", "other",
]

_STATUSES = ["planned", "pending", "overdue", "paid", "cancelled"]


def _obligation_form(
    *,
    action: str,
    method: str = "post",
    values: dict | None = None,
    submit_label: str = "Save",
) -> str:
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
        return (
            f'<input type="{type_}" name="{field}" value="{val}"'
            f' placeholder="{html.escape(placeholder, quote=True)}">'
        )

    amount_dollars = ""
    if v.get("amount_cents"):
        try:
            amount_dollars = f"{int(v['amount_cents']) / 100:.2f}"
        except (ValueError, TypeError):
            pass

    return f"""
    <form method="{method}" action="{html.escape(action)}">
      <div class="form-row">
        <div>
          <label>Name / Description</label>
          {inp("name", placeholder="e.g. Fora Financial — May payment")}
        </div>
        <div>
          <label>Vendor</label>
          {inp("vendor_or_customer", placeholder="e.g. Fora Financial")}
        </div>
      </div>
      <div class="form-row triple">
        <div>
          <label>Amount ($)</label>
          <input type="number" name="amount_dollars" value="{html.escape(amount_dollars, quote=True)}"
                 min="0" step="0.01" placeholder="0.00">
        </div>
        <div>
          <label>Due Date</label>
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
          <textarea name="notes" rows="2" placeholder="Optional notes">{html.escape(str(v.get('notes', '')))}</textarea>
        </div>
      </div>
      <div class="action-row">
        <button type="submit" class="btn btn-primary">{html.escape(submit_label)}</button>
        <a href="/admin/finances/ap" class="btn btn-secondary">Cancel</a>
      </div>
    </form>"""


def render_upcoming_ap_page(*, flash: str = "") -> str:
    rows = list_obligations(event_type="outflow", limit=500)
    # Group by status priority
    active = [r for r in rows if r.get("status") not in ("paid", "cancelled", "matched")]
    paid = [r for r in rows if r.get("status") in ("paid", "matched")]

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
        if overdue and status == "planned":
            status = "overdue"
            status_cls = "status-overdue"

        cents = int(r.get("amount_cents") or 0)
        event_id = html.escape(str(r.get("id", "")))

        return f"""
        <tr>
          <td>{html.escape(date_s)}{"&nbsp;⚠" if overdue else ""}</td>
          <td>{html.escape(str(r.get("name") or r.get("vendor_or_customer") or "—"))}</td>
          <td>{html.escape(str(r.get("category", "")))}</td>
          <td class="amount-out">{_dollar(cents)}</td>
          <td><span class="status-pill {status_cls}">{html.escape(status)}</span></td>
          <td style="white-space:nowrap">
            <a href="/admin/finances/ap/{event_id}/edit" class="btn btn-secondary btn-sm">Edit</a>
            <form method="post" action="/admin/finances/ap/{event_id}/delete" style="display:inline"
                  onsubmit="return confirm('Delete this obligation?')">
              <button type="submit" class="btn btn-secondary btn-sm" style="color:#b93535">Delete</button>
            </form>
          </td>
        </tr>"""

    active_rows = "".join(row_html(r) for r in active)
    total_active = sum(int(r.get("amount_cents") or 0) for r in active)

    body = f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
      <h1>Payables (AP)</h1>
      <a href="/admin/finances/ap/new" class="btn btn-primary">+ Add Payable</a>
    </div>
    <p class="page-sub">Manage accounts payable obligations · {len(active)} open · {_dollar(total_active)} total</p>
    <div class="card">
      <table>
        <thead>
          <tr><th>Due</th><th>Description</th><th>Category</th><th>Amount</th><th>Status</th><th></th></tr>
        </thead>
        <tbody>
          {active_rows if active_rows else '<tr><td colspan="6" class="empty-state">No open payables. Add one above.</td></tr>'}
        </tbody>
      </table>
    </div>"""

    return _page_shell("Payables (AP)", "ap", body, flash=flash)


def render_ap_new_page(*, flash: str = "") -> str:
    body = f"""
    <h1>Add Payable</h1>
    <p class="page-sub">Log a new accounts payable obligation</p>
    <div class="card">
      {_obligation_form(action="/admin/finances/ap/new", submit_label="Add Payable")}
    </div>"""
    return _page_shell("Add Payable", "ap", body, flash=flash)


def render_ap_edit_page(event_id: str, *, flash: str = "") -> str:
    row = get_obligation(event_id)
    if row is None:
        return _page_shell("Not Found", "ap", '<div class="card"><p>Obligation not found.</p></div>')
    body = f"""
    <h1>Edit Payable</h1>
    <p class="page-sub">{html.escape(str(row.get("name") or row.get("vendor_or_customer") or event_id))}</p>
    <div class="card">
      {_obligation_form(action=f"/admin/finances/ap/{html.escape(event_id)}/edit", values=row, submit_label="Save Changes")}
    </div>"""
    return _page_shell("Edit Payable", "ap", body, flash=flash)


# ---------------------------------------------------------------------------
# Form processing helpers (called from router)
# ---------------------------------------------------------------------------

def parse_obligation_form(form: dict) -> dict:
    """Convert raw form data to obligation kwargs."""
    amount_dollars = str(form.get("amount_dollars", "0") or "0").strip().replace(",", "")
    try:
        from decimal import Decimal
        amount_cents = int(Decimal(amount_dollars) * 100)
    except Exception:
        amount_cents = 0

    raw_date = str(form.get("due_date", "") or "").strip()
    try:
        due = date.fromisoformat(raw_date)
    except ValueError:
        due = datetime.utcnow().date()

    return {
        "name": str(form.get("name", "") or "").strip(),
        "vendor_or_customer": str(form.get("vendor_or_customer", "") or "").strip(),
        "amount_cents": amount_cents,
        "due_date": due,
        "status": str(form.get("status", "planned") or "planned"),
        "category": str(form.get("category", "other") or "other"),
        "confidence": str(form.get("confidence", "confirmed") or "confirmed"),
        "notes": str(form.get("notes", "") or "").strip(),
    }
