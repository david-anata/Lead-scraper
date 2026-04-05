"""Weekly Forecast page — rolling cashflow view (default 52 weeks / 1 year)."""

from __future__ import annotations

import html
import logging
from datetime import date, datetime, timedelta
from typing import Any

from sales_support_agent.services.cashflow.cashflow_helpers import (
    _dollar,
    _display_name,
    _events_to_dtos,
    _page_shell,
)
from sales_support_agent.services.cashflow.engine import aggregate_weeks, flag_risks
from sales_support_agent.services.cashflow.obligations import list_obligations

logger = logging.getLogger(__name__)

# How far back to look for overdue obligations and CSV balance rows.
_LOOKBACK_DAYS = 180


def _latest_balance_cents() -> int:
    """Return the account balance from the most recent uploaded bank CSV row.

    Runs a targeted, indexed query (source='csv', ORDER BY due_date DESC LIMIT 1)
    instead of fetching all rows and filtering in Python.
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    try:
        with get_engine().connect() as conn:
            row = conn.execute(
                text("""
                    SELECT account_balance_cents FROM cash_events
                    WHERE source = 'csv'
                      AND account_balance_cents IS NOT NULL
                    ORDER BY due_date DESC
                    LIMIT 1
                """)
            ).fetchone()
        return int(row[0]) if row else 0
    except Exception as exc:
        logger.warning("Could not fetch latest balance: %s", exc)
        return 0


def render_weekly_forecast_page(*, flash: str = "") -> str:
    today = datetime.utcnow().date()

    # Targeted balance query — avoids loading all CSV rows
    balance_cents = _latest_balance_cents()

    # Fetch only the date window we need:
    #   - _LOOKBACK_DAYS back: captures overdue planned/pending obligations
    #   - 52 weeks forward:    covers the full forecast horizon
    # Posted/matched CSV rows in this window are excluded below.
    window_start = today - timedelta(days=_LOOKBACK_DAYS)
    rows = list_obligations(from_date=window_start, limit=3000)

    # Exclude already-settled bank rows — they're baked into balance_cents.
    # REGRESSION GUARD: removing this filter causes posted rows to double-count
    # and forecasted outflows to show as $0. See test_cashflow_overview_metrics.py
    # :: TestPostedRowFilterRegression for the explicit regression test.
    forecast_rows = [
        r for r in rows
        if r.get("status") not in ("posted", "matched", "cancelled", "paid")
    ]
    events = _events_to_dtos(forecast_rows)

    weeks = aggregate_weeks(events, starting_cash_cents=balance_cents, weeks=52)
    alerts = flag_risks(weeks, events)

    alert_week_starts = {a.week_start for a in alerts if a.week_start}
    critical_weeks = {a.week_start for a in alerts if a.severity == "critical" and a.week_start}

    if not weeks:
        body = """
        <h1>Weekly Forecast</h1>
        <p class="page-sub">52-week cashflow projection</p>
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
    <p class="page-sub">52-week rolling projection · Starting balance: {_dollar(balance_cents)}</p>
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
