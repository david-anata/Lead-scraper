"""Cashflow Overview — Finance OS landing page."""

from __future__ import annotations

import asyncio
import html
import os
from dataclasses import dataclass as _dc
from datetime import date, datetime, timedelta
from typing import Any

from sales_support_agent.services.cashflow.cashflow_helpers import (
    _dollar,
    _events_to_dtos,
    _page_shell,
    _name_cell,
    _display_name,
    CATEGORY_ORDER,
    _CAT_ORDER_INDEX,
    _CAT_ICON,
)
from sales_support_agent.services.cashflow.engine import (
    EventDTO,
    aggregate_weeks,
    flag_risks,
)
from sales_support_agent.services.cashflow.obligations import list_obligations


def _render_weekly_table(events: list, today) -> str:
    """Build an 8-week rolling cashflow table grouped by category (mirrors ClickUp structure)."""
    week_start = today - timedelta(days=today.weekday())

    weeks_data = []
    for w in range(8):
        wstart = week_start + timedelta(weeks=w)
        wend = wstart + timedelta(days=6)
        week_events = [e for e in events if e.due_date and wstart <= e.due_date <= wend]
        ap_total = sum(e.amount_cents for e in week_events if e.event_type == "outflow")
        ar_total = sum(e.amount_cents for e in week_events if e.event_type == "inflow")
        weeks_data.append({
            "label": f"Week of {wstart.strftime('%b %-d')}",
            "wstart": wstart,
            "wend": wend,
            "events": week_events,
            "ap_total": ap_total,
            "ar_total": ar_total,
            "net": ar_total - ap_total,
        })

    html_parts = []
    for wd in weeks_data:
        net = wd["net"]
        net_s = ("+$" if net >= 0 else "-$") + f"{abs(net)/100:,.0f}"
        net_col = "#0f766e" if net >= 0 else "#b91c1c"

        header = f"""
        <div class="week-header">
          <span>{html.escape(wd['label'])}</span>
          <div class="week-header-totals">
            <span style="color:#7dbaaa">AR {_dollar(wd['ar_total'])}</span>
            <span style="color:#e89090">AP {_dollar(wd['ap_total'])}</span>
            <span style="color:{net_col}">Net {net_s}</span>
          </div>
        </div>"""

        week_events = wd["events"]

        # Group events by category
        by_category: dict[str, list] = {}
        for ev in week_events:
            cat = getattr(ev, "category", "uncategorized") or "uncategorized"
            by_category.setdefault(cat, []).append(ev)

        rows_html = ""
        if by_category:
            # Sort categories by our defined order
            sorted_cats = sorted(
                by_category.keys(),
                key=lambda c: _CAT_ORDER_INDEX.get(c, 99),
            )

            for cat in sorted_cats:
                cat_events = sorted(by_category[cat], key=lambda e: (e.due_date or date.min, e.name))
                cat_total_in  = sum(e.amount_cents for e in cat_events if e.event_type == "inflow")
                cat_total_out = sum(e.amount_cents for e in cat_events if e.event_type == "outflow")
                cat_net = cat_total_in - cat_total_out

                # Find display label
                display_label = cat.replace("_", " ").title()
                for key, label, _ in CATEGORY_ORDER:
                    if key == cat:
                        display_label = label
                        break

                icon = _CAT_ICON.get(cat, "📌")
                subtotal_s = _dollar(cat_total_in if cat_total_in else cat_total_out)
                subtotal_cls = "amount-in" if cat_total_in > 0 and cat_total_out == 0 else "amount-out"
                count = len(cat_events)

                # Category header row
                rows_html += f"""
                <tr class="cat-group-header">
                  <td colspan="3">
                    <span style="font-size:14px;margin-right:6px">{icon}</span>
                    <strong>{html.escape(display_label)}</strong>
                    <span style="color:var(--muted);font-size:11px;margin-left:6px">{count} item{'s' if count != 1 else ''}</span>
                  </td>
                  <td class="{subtotal_cls}" style="font-weight:700">{subtotal_s}</td>
                  <td></td>
                </tr>"""

                # Individual transaction rows
                for ev in cat_events:
                    amount_cls = "amount-out" if ev.event_type == "outflow" else "amount-in"
                    amount_s = _dollar(ev.amount_cents)
                    status_cls = f"status-{ev.status}"
                    vendor = getattr(ev, "vendor_or_customer", "") or ""
                    display_name = ev.name or vendor or "(unnamed)"
                    due_str = ev.due_date.strftime("%-m/%-d") if ev.due_date else ""

                    rows_html += f"""
                    <tr class="cat-item-row">
                      <td style="padding-left:28px;color:var(--muted);font-size:12px">{html.escape(due_str)}</td>
                      <td style="padding-left:6px">
                        <span style="font-size:13px">{html.escape(display_name)}</span>
                        {f'<br><span style="color:var(--muted);font-size:11px">{html.escape(vendor)}</span>' if vendor and vendor != display_name else ''}
                      </td>
                      <td class="{amount_cls}" style="font-size:13px">{amount_s}</td>
                      <td><span class="status-pill {status_cls}" style="font-size:10px">{html.escape(ev.status)}</span></td>
                      <td></td>
                    </tr>"""

            # Net row
            rows_html += f"""
            <tr class="week-net-row">
              <td colspan="3" style="text-align:right">Week Net</td>
              <td colspan="2" style="color:{net_col}">{net_s}</td>
            </tr>"""
        else:
            rows_html = '<tr><td colspan="5" class="week-empty">No transactions this week — upload a bank CSV to populate.</td></tr>'

        body = f"""
        <div class="week-body">
          <table>
            <thead><tr>
              <th>Date</th><th>Description</th><th>Amount</th><th>Status</th><th></th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>"""

        html_parts.append(f'<div class="week-section">{header}{body}</div>')

    return "\n".join(html_parts)


# ---------------------------------------------------------------------------
# Finance overview metrics dataclass and compute function
# ---------------------------------------------------------------------------

@_dc
class FinanceOverviewMetrics:
    balance_cents: int
    balance_class: str          # "negative" or ""
    net_4w_cents: int
    net_class: str              # "negative", "positive", or ""
    upcoming_total_cents: int
    upcoming_count: int
    upcoming_class: str         # "amount-out" if > 0, else ""
    overdue_count: int
    overdue_total_cents: int
    overdue_class: str          # "negative" if overdue > 0, else ""
    critical_count: int
    warning_count: int
    alerts_class: str           # "negative" if critical > 0, else ""
    weeks: list                 # list[WeekSummary]
    ai_text: str


def compute_finance_overview(
    events: list,
    alerts: list,
    weeks: list,
    balance_cents: int,
    *,
    today,
    ai_text: str = "",
) -> FinanceOverviewMetrics:
    balance_class = "negative" if balance_cents < 0 else ""
    net_4w = sum(w.net_cents for w in weeks)
    net_class = "negative" if net_4w < 0 else ("positive" if net_4w > 0 else "")

    soon = today + timedelta(days=14)
    upcoming = [
        e for e in events
        if e.status in ("planned", "pending", "overdue")
        and today <= e.due_date <= soon
        and e.event_type == "outflow"
    ]
    upcoming_total = sum(e.amount_cents for e in upcoming)
    upcoming_class = "amount-out" if upcoming_total > 0 else ""

    overdue = [e for e in events if e.status == "overdue"]
    overdue_total = sum(e.amount_cents for e in overdue)
    overdue_class = "negative" if overdue else ""

    critical_count = sum(1 for a in alerts if a.severity == "critical")
    warning_count = sum(1 for a in alerts if a.severity == "warning")
    alerts_class = "negative" if critical_count else ""

    return FinanceOverviewMetrics(
        balance_cents=balance_cents,
        balance_class=balance_class,
        net_4w_cents=net_4w,
        net_class=net_class,
        upcoming_total_cents=upcoming_total,
        upcoming_count=len(upcoming),
        upcoming_class=upcoming_class,
        overdue_count=len(overdue),
        overdue_total_cents=overdue_total,
        overdue_class=overdue_class,
        critical_count=critical_count,
        warning_count=warning_count,
        alerts_class=alerts_class,
        weeks=weeks,
        ai_text=ai_text,
    )


# ---------------------------------------------------------------------------
# Overview page
# ---------------------------------------------------------------------------

async def render_cashflow_overview_page(*, flash: str = "") -> str:
    # Load all non-cancelled events
    rows = list_obligations(limit=2000)
    events = _events_to_dtos(rows)

    # Latest balance from CSV
    balance_cents = 0
    csv_rows = [r for r in rows if r.get("source") == "csv" and r.get("account_balance_cents") is not None]
    if csv_rows:
        csv_rows_sorted = sorted(
            csv_rows,
            key=lambda r: str(r.get("due_date", "")).ljust(10, "0"),
            reverse=True,
        )
        balance_cents = int(csv_rows_sorted[0]["account_balance_cents"] or 0)

    # 4-week summary
    weeks = aggregate_weeks(events, starting_cash_cents=balance_cents, weeks=4)
    alerts = flag_risks(weeks, events)

    today = datetime.utcnow().date()

    # AI summary
    ai_text = ""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key and weeks:
        from sales_support_agent.services.cashflow.ai_summary import generate_cashflow_summary
        result = await asyncio.to_thread(generate_cashflow_summary, weeks, alerts, balance_cents, api_key=api_key)
        ai_text = result.text

    # Compute metrics using the extracted function
    metrics = compute_finance_overview(
        events,
        alerts,
        weeks,
        balance_cents,
        today=today,
        ai_text=ai_text,
    )

    # Metric cards
    cards_html = f"""
    <div class="card-grid">
      <div class="metric-card">
        <div class="metric-label">Bank Balance</div>
        <div class="metric-value {metrics.balance_class}">{_dollar(metrics.balance_cents)}</div>
        <div class="metric-note">From latest CSV upload</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">4-Week Net</div>
        <div class="metric-value {metrics.net_class}">{_dollar(metrics.net_4w_cents)}</div>
        <div class="metric-note">Forecasted net cashflow</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Due in 14 Days</div>
        <div class="metric-value {metrics.upcoming_class}">{_dollar(metrics.upcoming_total_cents)}</div>
        <div class="metric-note">{metrics.upcoming_count} obligations</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Overdue AP</div>
        <div class="metric-value {metrics.overdue_class}">{metrics.overdue_count}</div>
        <div class="metric-note">{_dollar(metrics.overdue_total_cents)} outstanding</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Risk Alerts</div>
        <div class="metric-value {metrics.alerts_class}">
          {metrics.critical_count + metrics.warning_count}
        </div>
        <div class="metric-note">
          {metrics.critical_count} critical · {metrics.warning_count} warnings
        </div>
      </div>
    </div>"""

    # Top 3 alerts
    alert_rows = ""
    for a in alerts[:3]:
        badge_cls = f"badge-{a.severity}"
        alert_rows += f"""
        <tr>
          <td><span class="badge {badge_cls}">{html.escape(a.severity.upper())}</span></td>
          <td>{html.escape(a.title)}</td>
          <td style="color:#6b7a8d;font-size:12px">{html.escape(a.detail)}</td>
        </tr>"""

    alerts_table = f"""
    <table>
      <thead><tr><th>Level</th><th>Alert</th><th>Detail</th></tr></thead>
      <tbody>{alert_rows if alert_rows else '<tr><td colspan="3" class="empty-state">No active alerts</td></tr>'}</tbody>
    </table>""" if alerts else '<div class="empty-state">No risk alerts. Looking good.</div>'

    ai_block = f'<div class="ai-summary">{html.escape(metrics.ai_text)}</div>' if metrics.ai_text else ""

    # 8-week rolling cashflow table
    weekly_table_html = _render_weekly_table(events, today)

    body = f"""
    <div>
      <p class="eyebrow" style="margin:0 0 10px;text-transform:uppercase;letter-spacing:.18em;font-size:12px;font-weight:800;color:var(--accent);font-family:'Montserrat',sans-serif;">Finance</p>
      <h1>Cash overview.</h1>
      <p class="page-sub" style="margin-top:10px">Cash position · {today.strftime("%B %d, %Y")}</p>
      {ai_block}
    </div>
    {cards_html}
    <div class="card">
      <h2>Risk Alerts</h2>
      <div style="margin-top:14px">{alerts_table}</div>
      <div class="action-row">
        <a href="/admin/finances/alerts" class="btn btn-secondary btn-sm">All Alerts →</a>
      </div>
    </div>
    <div class="card">
      <h2>8-Week Cashflow</h2>
      <div style="margin-top:14px">
        {weekly_table_html if events else '<div class="empty-state">No transactions yet — upload a bank CSV to see your 8-week cashflow view.</div>'}
      </div>
      <div class="action-row">
        <a href="/admin/finances/forecast" class="btn btn-secondary btn-sm">Full Forecast →</a>
      </div>
    </div>
    <div class="action-row" style="margin-top:0">
      <a href="/admin/finances/upload" class="btn btn-primary">Upload Bank CSV</a>
      <a href="/admin/finances/ap/new" class="btn btn-secondary">+ Add Payable</a>
      <a href="/admin/finances/ar/new" class="btn btn-secondary">+ Add Receivable</a>
      <a href="/admin/finances/recurring" class="btn btn-secondary">Recurring Templates</a>
      <form method="post" action="/admin/finances/sync-qbo" style="display:inline;margin:0">
        <button type="submit" class="btn btn-secondary">Sync QBO Invoices</button>
      </form>
    </div>"""

    return _page_shell("Finance Overview", "overview", body, flash=flash)
