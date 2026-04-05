"""Weekly Forecast page — 12-week rolling cashflow view."""

from __future__ import annotations

import html
from datetime import date, datetime
from typing import Any

from sales_support_agent.services.cashflow.cashflow_helpers import (
    _dollar,
    _display_name,
    _events_to_dtos,
    _page_shell,
)
from sales_support_agent.services.cashflow.engine import aggregate_weeks, flag_risks
from sales_support_agent.services.cashflow.obligations import list_obligations


def render_weekly_forecast_page(*, flash: str = "") -> str:
    # Auto-expand recurring templates so forecast always shows full 12-week horizon.
    # This is a no-op if all upcoming events already exist (upsert by template+date).
    try:
        from sales_support_agent.services.cashflow.obligations import (
            generate_upcoming_from_templates,
        )
        generate_upcoming_from_templates(horizon_days=90, advance_template=True)
    except Exception as exc:
        import logging as _logging
        _logging.getLogger(__name__).warning("Template expansion failed (forecast page): %s", exc)

    rows = list_obligations(limit=2000)
    events = _events_to_dtos(rows)

    # Latest balance
    balance_cents = 0
    csv_rows = sorted(
        [r for r in rows if r.get("source") == "csv" and r.get("account_balance_cents") is not None],
        key=lambda r: str(r.get("due_date", "")),
        reverse=True,
    )
    if csv_rows:
        balance_cents = int(csv_rows[0]["account_balance_cents"] or 0)

    weeks = aggregate_weeks(events, starting_cash_cents=balance_cents, weeks=12)
    alerts = flag_risks(weeks, events)

    alert_week_starts = {a.week_start for a in alerts if a.week_start}
    critical_weeks = {a.week_start for a in alerts if a.severity == "critical" and a.week_start}

    if not weeks:
        body = """
        <h1>Weekly Forecast</h1>
        <p class="page-sub">12-week cashflow projection</p>
        <div class="card">
          <div class="empty-state">No cashflow data yet. Upload a bank CSV and add obligations to see the forecast.</div>
        </div>"""
        return _page_shell("Weekly Forecast", "forecast", body)

    rows_html = ""
    for w in weeks:
        has_alert = w.week_start in alert_week_starts
        is_critical = w.week_start in critical_weeks
        row_style = ""
        if is_critical:
            row_style = ' style="background:rgba(185,53,53,0.05)"'
        elif has_alert:
            row_style = ' style="background:rgba(185,115,0,0.04)"'

        net_class = "amount-in" if w.net_cents >= 0 else "amount-out"
        end_class = "amount-out" if w.is_negative else ""
        alert_icon = ' ⚠' if has_alert else ''

        rows_html += f"""
        <tr{row_style}>
          <td style="font-weight:600">{html.escape(w.label)}{alert_icon}</td>
          <td class="amount-in">{_dollar(w.inflow_cents)}</td>
          <td class="amount-out">{_dollar(w.outflow_cents)}</td>
          <td class="{net_class}">{_dollar(w.net_cents)}</td>
          <td class="{end_class}" style="font-weight:600">{_dollar(w.ending_cash_cents)}</td>
        </tr>"""

    # Event detail table (next 4 weeks, non-posted)
    today = datetime.utcnow().date()
    near_events = sorted(
        [e for e in events if e.status not in ("posted", "matched", "cancelled", "paid")],
        key=lambda e: e.due_date,
    )[:50]

    event_rows_html = ""
    for e in near_events:
        date_s = e.due_date.strftime("%b %d")
        status_cls = f"status-{e.status}" if e.status in ("planned", "pending", "overdue", "paid", "posted", "matched") else ""
        type_class = "amount-in" if e.event_type == "inflow" else "amount-out"
        event_rows_html += f"""
        <tr>
          <td>{date_s}</td>
          <td>{html.escape(_display_name({"name": e.name, "vendor_or_customer": e.vendor_or_customer, "description": ""}))}</td>
          <td>{html.escape(e.category)}</td>
          <td class="{type_class}">{_dollar(e.amount_cents) if e.event_type == "inflow" else ""}</td>
          <td class="{type_class}">{_dollar(e.amount_cents) if e.event_type == "outflow" else ""}</td>
          <td><span class="status-pill {status_cls}">{html.escape(e.status)}</span></td>
        </tr>"""

    body = f"""
    <h1>Weekly Forecast</h1>
    <p class="page-sub">12-week rolling projection · Starting balance: {_dollar(balance_cents)}</p>
    <div class="card">
      <h2>Week-by-Week Summary</h2>
      <table>
        <thead>
          <tr>
            <th>Week</th>
            <th>Inflows</th>
            <th>Outflows</th>
            <th>Net</th>
            <th>Ending Cash</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    <div class="card">
      <h2>Upcoming Obligations</h2>
      <table>
        <thead>
          <tr><th>Date</th><th>Name</th><th>Category</th><th>In</th><th>Out</th><th>Status</th></tr>
        </thead>
        <tbody>
          {event_rows_html if event_rows_html else '<tr><td colspan="6" class="empty-state">No upcoming obligations.</td></tr>'}
        </tbody>
      </table>
    </div>"""

    return _page_shell("Weekly Forecast", "forecast", body, flash=flash)
