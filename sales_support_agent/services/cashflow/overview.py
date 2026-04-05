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


def _build_chart_data(period_weeks: int = 12) -> dict:
    """Build Chart.js dataset for the cashflow chart.

    Performance note: uses a single DB call (list_obligations) for ALL data.
    Forward weeks are projected by Python-bucketing the already-loaded rows
    rather than issuing one get_events_for_range() query per future week.
    """
    today = date.today()

    # Single query for everything — historical CSV rows AND forward obligations.
    all_rows = list_obligations(limit=5000)

    csv_rows = sorted(
        [r for r in all_rows if r.get("source") == "csv" and r.get("account_balance_cents") is not None],
        key=lambda r: str(r.get("due_date", ""))
    )

    starting_balance = int(csv_rows[-1].get("account_balance_cents") or 0) if csv_rows else 0

    # ── Pre-bucket forward obligations by week-start (Monday) ──────────────
    # This replaces the old N+1 loop that called get_events_for_range() once
    # per future week.  One pass here; O(1) lookup below.
    def _row_monday(r: dict) -> date | None:
        raw = r.get("due_date")
        if raw is None:
            return None
        if isinstance(raw, datetime):
            d = raw.date()
        elif isinstance(raw, date):
            d = raw
        elif isinstance(raw, str):
            try:
                d = date.fromisoformat(str(raw)[:10])
            except ValueError:
                return None
        else:
            return None
        return d - timedelta(days=d.weekday())

    from collections import defaultdict
    week_buckets: dict = defaultdict(int)   # monday -> cumulative net_cents for that week
    for r in all_rows:
        if r.get("status") not in ("planned", "overdue", "pending"):
            continue
        mon = _row_monday(r)
        if mon is None or mon <= today - timedelta(days=today.weekday()):
            continue  # skip historical / current week (already in starting_balance)
        amt = int(r.get("amount_cents") or 0)
        delta = amt if r.get("event_type") == "inflow" else -amt
        week_buckets[mon] += delta

    # ── Build weekly labels ─────────────────────────────────────────────────
    monday = today - timedelta(days=today.weekday())
    start_monday = monday - timedelta(weeks=4)  # 4 weeks back

    labels: list = []
    actual_data: list = []
    projected_data: list = []
    cumulative_net = 0  # cents accumulated week-by-week into the future

    for i in range(period_weeks + 4):
        week_date = start_monday + timedelta(weeks=i)
        labels.append(week_date.strftime("%b %d"))

        if week_date <= today:
            week_csv = [r for r in csv_rows if str(r.get("due_date", ""))[:10] <= week_date.isoformat()]
            actual_data.append(int(week_csv[-1].get("account_balance_cents") or 0) / 100 if week_csv else None)
            projected_data.append(None)
        else:
            actual_data.append(None)
            cumulative_net += week_buckets.get(week_date, 0)
            projected_data.append((starting_balance + cumulative_net) / 100)

    return {
        "labels": labels,
        "actual": actual_data,
        "projected": projected_data,
        "threshold": 10000,  # $10,000 default floor
        "starting_balance": starting_balance / 100,
    }


def _render_weekly_table(rows_or_events: list, today, balance_cents: int = 0) -> str:
    """Render the 8-week cashflow table: chronological, one row per transaction."""
    monday = today - timedelta(days=today.weekday())

    # Build 8-week buckets
    weeks = []
    for i in range(8):
        ws = monday + timedelta(weeks=i)
        we = ws + timedelta(days=6)
        weeks.append((ws, we, []))

    # Handle both EventDTO list and raw dict list
    def _get_due(item):
        raw = getattr(item, "due_date", None) or (item.get("due_date") if isinstance(item, dict) else None)
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw.date()
        if isinstance(raw, date):
            return raw
        if isinstance(raw, str):
            try:
                return date.fromisoformat(raw[:10])
            except Exception:
                return None
        return None

    def _get_attr(item, key, default=""):
        if isinstance(item, dict):
            return item.get(key, default)
        return getattr(item, key, default)

    # Assign events to weeks
    for item in rows_or_events:
        due = _get_due(item)
        if not due:
            continue
        due_monday = due - timedelta(days=due.weekday())
        for ws, we, evts in weeks:
            if ws == due_monday:
                evts.append((due, item))
                break

    running = balance_cents
    html_rows = ""

    for ws, we, evts in weeks:
        evts.sort(key=lambda x: x[0])
        week_in = sum(_get_attr(r, "amount_cents", 0) for _, r in evts if _get_attr(r, "event_type") == "inflow")
        week_out = sum(_get_attr(r, "amount_cents", 0) for _, r in evts if _get_attr(r, "event_type") == "outflow")
        week_net = week_in - week_out

        # Week header
        net_col = "#86efac" if week_net >= 0 else "#fca5a5"
        html_rows += f"""
        <tr style="background:#1e293b;color:#fff">
          <td colspan="3" style="font-weight:600;padding:6px 12px">
            Week of {ws.strftime("%b %d")} – {we.strftime("%b %d")}
          </td>
          <td style="text-align:right;color:#86efac">+{_dollar(week_in)}</td>
          <td style="text-align:right;color:#fca5a5">–{_dollar(week_out)}</td>
          <td style="text-align:right;font-weight:600;color:{net_col}">{'+' if week_net >= 0 else ''}{_dollar(week_net)}</td>
        </tr>"""

        if not evts:
            html_rows += '<tr><td colspan="6" style="color:#9ca3af;font-style:italic;padding:4px 12px">No transactions this week</td></tr>'

        for due_date, item in evts:
            running += _get_attr(item, "amount_cents", 0) if _get_attr(item, "event_type") == "inflow" else -_get_attr(item, "amount_cents", 0)
            is_in = _get_attr(item, "event_type") == "inflow"
            amt = _dollar(_get_attr(item, "amount_cents", 0))
            bal_color = "#16a34a" if running >= 0 else "#dc2626"

            # Build row dict for _name_cell
            if isinstance(item, dict):
                row_dict = item
            else:
                row_dict = {
                    "id": getattr(item, "id", ""),
                    "name": getattr(item, "name", ""),
                    "vendor_or_customer": getattr(item, "vendor_or_customer", ""),
                    "description": "",
                    "friendly_name": None,
                }

            html_rows += f"""
            <tr>
              <td style="color:#6b7280;white-space:nowrap">{due_date.strftime("%b %d")}</td>
              <td>{_name_cell(row_dict)}</td>
              <td style="color:#6b7280;font-size:0.8rem">{html.escape(str(_get_attr(item, "category", "")))}</td>
              <td style="text-align:right;color:#16a34a;font-weight:500">{"" if not is_in else amt}</td>
              <td style="text-align:right;color:#dc2626;font-weight:500">{"" if is_in else amt}</td>
              <td style="text-align:right;font-weight:600;color:{bal_color}">{_dollar(running)}</td>
            </tr>"""

    return f"""
    <table style="width:100%">
      <thead>
        <tr>
          <th>Date</th><th>Description</th><th>Category</th>
          <th style="text-align:right">In</th>
          <th style="text-align:right">Out</th>
          <th style="text-align:right">Balance</th>
        </tr>
      </thead>
      <tbody>{html_rows}</tbody>
    </table>"""


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
    # Load all events
    rows = list_obligations(limit=2000)

    # Exclude already-settled rows from the forecast — they're already baked
    # into balance_cents from the latest CSV upload and would double-count.
    forecast_rows = [
        r for r in rows
        if r.get("status") not in ("posted", "matched", "cancelled", "paid")
    ]
    events = _events_to_dtos(forecast_rows)

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
    weekly_table_html = _render_weekly_table(events, today, balance_cents=balance_cents)

    chart_html = """
<!-- Load Chart.js -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>

<div class="card" style="margin-bottom:1rem">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
    <h2 style="margin:0">Cash Flow</h2>
    <div>
      <select id="chart-period" onchange="loadChart(this.value)" style="padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;font-size:0.85rem">
        <option value="4">4 weeks</option>
        <option value="8">8 weeks</option>
        <option value="12" selected>3 months</option>
        <option value="26">6 months</option>
        <option value="52">12 months</option>
      </select>
    </div>
  </div>
  <p style="color:#6b7280;font-size:0.85rem;margin:0 0 0.75rem">Today&#39;s balance: <strong id="chart-balance">loading...</strong></p>
  <div style="height:280px;position:relative">
    <canvas id="cashflowChart"></canvas>
  </div>
</div>

<script>
let cashflowChart = null;
function loadChart(weeks) {
  fetch('/admin/finances/chart-data?weeks=' + weeks)
    .then(r => r.json())
    .then(data => {
      document.getElementById('chart-balance').textContent = '$' + (data.starting_balance || 0).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
      const ctx = document.getElementById('cashflowChart').getContext('2d');
      if (cashflowChart) cashflowChart.destroy();
      cashflowChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: data.labels,
          datasets: [
            {
              label: 'Cash balance',
              data: data.actual,
              borderColor: '#0D9488',
              backgroundColor: 'rgba(13,148,136,0.08)',
              borderWidth: 2,
              tension: 0.3,
              fill: true,
              spanGaps: false,
              pointRadius: 3,
            },
            {
              label: 'Projected balance',
              data: data.projected,
              borderColor: '#2563EB',
              backgroundColor: 'transparent',
              borderWidth: 2,
              borderDash: [6, 3],
              tension: 0.3,
              fill: false,
              spanGaps: false,
              pointRadius: 3,
            },
            {
              label: 'Threshold ($10k)',
              data: data.labels.map(() => data.threshold),
              borderColor: '#9CA3AF',
              backgroundColor: 'transparent',
              borderWidth: 1,
              borderDash: [4, 4],
              pointRadius: 0,
              fill: false,
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: 'index', intersect: false },
          plugins: {
            legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 12 } } },
            tooltip: {
              callbacks: {
                label: ctx => ctx.dataset.label + ': $' + (ctx.parsed.y || 0).toLocaleString('en-US', {minimumFractionDigits:2})
              }
            }
          },
          scales: {
            y: { ticks: { callback: v => '$' + (v/1000).toFixed(0) + 'k' } },
            x: { ticks: { maxTicksLimit: 8 } }
          }
        }
      });
    });
}
document.addEventListener('DOMContentLoaded', () => loadChart(12));
</script>"""

    body = f"""
    <div>
      <p class="eyebrow" style="margin:0 0 10px;text-transform:uppercase;letter-spacing:.18em;font-size:12px;font-weight:800;color:var(--accent);font-family:'Montserrat',sans-serif;">Finance</p>
      <h1>Cash overview.</h1>
      <p class="page-sub" style="margin-top:10px">Cash position · {today.strftime("%B %d, %Y")}</p>
      {ai_block}
    </div>
    {cards_html}
    {chart_html}
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
      <a href="/admin/finances/reconcile" class="btn btn-secondary">Actuals vs Planned</a>
    </div>"""

    return _page_shell("Finance Overview", "overview", body, flash=flash)
