"""Cash Ledger page — daily running balance view."""
from __future__ import annotations

import html
from datetime import date, datetime, timedelta
from typing import Optional

from sales_support_agent.services.cashflow.cashflow_helpers import (
    _dollar,
    _display_name,
    _page_shell,
)
from sales_support_agent.services.cashflow.obligations import list_obligations


def render_ledger_page(
    *,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    filter_type: str = "all",  # "all" | "income" | "expenses"
    flash: str = "",
) -> str:
    """Render the Cash Ledger page with daily running balance."""
    today = datetime.utcnow().date()

    # Default date range: current month
    if not from_date:
        from_date = today.replace(day=1).isoformat()
    if not to_date:
        to_date = today.isoformat()

    # Get starting balance from latest CSV row before from_date
    all_rows = list_obligations(limit=5000)
    csv_rows_before = sorted(
        [r for r in all_rows
         if r.get("source") == "csv"
         and r.get("account_balance_cents") is not None
         and str(r.get("due_date",""))[:10] <= from_date],
        key=lambda r: str(r.get("due_date", ""))
    )

    # Starting balance: from CSV if available, else 0
    if csv_rows_before:
        starting_balance = int(csv_rows_before[-1].get("account_balance_cents") or 0)
    else:
        starting_balance = 0

    # Filter rows to date range
    filtered = [
        r for r in all_rows
        if str(r.get("due_date",""))[:10] >= from_date
        and str(r.get("due_date",""))[:10] <= to_date
    ]

    # Apply income/expense filter
    if filter_type == "income":
        filtered = [r for r in filtered if r.get("event_type") == "inflow"]
    elif filter_type == "expenses":
        filtered = [r for r in filtered if r.get("event_type") == "outflow"]

    # Sort chronologically
    filtered.sort(key=lambda r: (str(r.get("due_date","")), str(r.get("created_at",""))))

    # Build week subtotal buckets
    def get_week_sunday(d: date) -> date:
        return d + timedelta(days=(6 - d.weekday()))

    running = starting_balance
    rows_html = ""
    current_week_end = None
    week_in = week_out = 0

    for row in filtered:
        due_str = str(row.get("due_date",""))[:10]
        try:
            due = date.fromisoformat(due_str)
        except Exception:
            continue

        week_end = get_week_sunday(due)

        # Insert week subtotal row when week changes
        if current_week_end and week_end != current_week_end:
            week_net = week_in - week_out
            net_color = "#16a34a" if week_net >= 0 else "#dc2626"
            rows_html += f"""
            <tr style="background:#f8fafc;color:#6b7280;font-size:0.8rem;font-style:italic">
              <td colspan="4" style="padding:3px 12px">
                Week of {(current_week_end - timedelta(days=6)).strftime("%b %d")}:
                In {_dollar(week_in)} · Out {_dollar(week_out)} ·
                <span style="color:{net_color}">Net {'+' if week_net>=0 else ''}{_dollar(week_net)}</span>
                · Balance {_dollar(running)}
              </td>
              <td colspan="2"></td>
            </tr>"""
            week_in = week_out = 0

        current_week_end = week_end

        is_in = row.get("event_type") == "inflow"
        amt = row.get("amount_cents", 0)
        running += amt if is_in else -amt

        if is_in:
            week_in += amt
        else:
            week_out += amt

        bal_color = "#16a34a" if running >= 0 else "#dc2626"
        ev_id = row.get("id","")
        fname = (row.get("friendly_name") or "").strip()
        notes_val = (row.get("notes") or "").strip()

        # Description cell
        if fname:
            desc_html = f'<span id="disp-{ev_id}">{html.escape(fname)}</span>'
        else:
            raw = _display_name(row)
            desc_html = f'<span id="disp-{ev_id}" style="color:#9ca3af;font-style:italic">⚠ {html.escape(raw)}</span>'

        desc_with_edit = (
            f'<span class="inline-edit-wrap" id="wrap-{ev_id}">'
            f'{desc_html}'
            f'<button class="pencil-btn" onclick="editName(\'{ev_id}\', \'{html.escape(fname)}\', \'friendly_name\')" title="Edit label">✏</button>'
            f'</span>'
        )

        rows_html += f"""
        <tr>
          <td style="white-space:nowrap;color:#374151">{due.strftime("%b %d, %Y")}</td>
          <td style="text-align:right;color:#16a34a;font-weight:500">{"" if not is_in else _dollar(amt)}</td>
          <td style="text-align:right;color:#dc2626;font-weight:500">{"" if is_in else _dollar(amt)}</td>
          <td>{desc_with_edit}</td>
          <td style="text-align:right;font-weight:700;color:{bal_color}">{_dollar(running)}</td>
          <td style="color:#6b7280;font-size:0.85rem">
            <span class="inline-edit-wrap" id="wrap-notes-{ev_id}">
              <span id="disp-notes-{ev_id}">{html.escape(notes_val)}</span>
              <button class="pencil-btn" onclick="editName('{ev_id}-notes', '{html.escape(notes_val)}', 'notes')" title="Edit note">✏</button>
            </span>
          </td>
        </tr>"""

    # Final week subtotal
    if current_week_end and (week_in or week_out):
        week_net = week_in - week_out
        net_color = "#16a34a" if week_net >= 0 else "#dc2626"
        rows_html += f"""
        <tr style="background:#f8fafc;color:#6b7280;font-size:0.8rem;font-style:italic">
          <td colspan="4" style="padding:3px 12px">
            Week of {(current_week_end - timedelta(days=6)).strftime("%b %d")}:
            In {_dollar(week_in)} · Out {_dollar(week_out)} ·
            <span style="color:{net_color}">Net {'+' if week_net>=0 else ''}{_dollar(week_net)}</span>
            · Balance {_dollar(running)}
          </td>
          <td colspan="2"></td>
        </tr>"""

    # Filter toggle style
    def tab_style(t):
        active = "background:#0f172a;color:#fff" if filter_type == t else "background:#f1f5f9;color:#374151"
        return f'style="padding:6px 14px;border-radius:20px;border:none;cursor:pointer;font-size:0.85rem;{active}"'

    body = f"""
    <h1>Cash Ledger</h1>
    <p class="page-sub">Daily running balance · Starting balance: {_dollar(starting_balance)}</p>

    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;flex-wrap:wrap;gap:0.5rem">
        <div style="display:flex;gap:0.5rem">
          <form method="get" action="/admin/finances/ledger" style="display:flex;gap:0.5rem;align-items:center">
            <input type="date" name="from" value="{from_date}" style="padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;font-size:0.85rem">
            <span style="color:#9ca3af">to</span>
            <input type="date" name="to" value="{to_date}" style="padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;font-size:0.85rem">
            <input type="hidden" name="filter" value="{filter_type}">
            <button type="submit" style="padding:4px 12px;background:#0f172a;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:0.85rem">Apply</button>
          </form>
        </div>
        <div style="display:flex;gap:0.25rem;background:#f1f5f9;border-radius:24px;padding:3px">
          <a href="?from={from_date}&to={to_date}&filter=all"><button {tab_style('all')}>All</button></a>
          <a href="?from={from_date}&to={to_date}&filter=income"><button {tab_style('income')}>Income</button></a>
          <a href="?from={from_date}&to={to_date}&filter=expenses"><button {tab_style('expenses')}>Expenses</button></a>
        </div>
        <a href="/admin/finances/ledger/export?from={from_date}&to={to_date}&filter={filter_type}"
           style="font-size:0.8rem;color:#2563EB;text-decoration:none">↓ Export CSV</a>
      </div>

      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th style="text-align:right">Income</th>
            <th style="text-align:right">Expenses</th>
            <th>Description</th>
            <th style="text-align:right">Running Total</th>
            <th>Note</th>
          </tr>
        </thead>
        <tbody>
          {rows_html if rows_html else '<tr><td colspan="6" style="color:#9ca3af;font-style:italic;padding:1rem">No transactions in this date range.</td></tr>'}
        </tbody>
      </table>
    </div>"""

    return _page_shell("Cash Ledger", "ledger", body, flash=flash)
