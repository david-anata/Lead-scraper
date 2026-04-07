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


def _build_daily_chart_data(days_back: int = 14, days_forward: int = 42) -> dict:
    """Build day-level chart data for the cashflow bar + balance line chart.

    Returns a dict consumed by the Chart.js mixed chart:
      labels          : list[str] — "Apr 06"
      actual_out      : list[float|None] — negative posted outflows (past only)
      actual_in       : list[float|None] — positive posted inflows (past only)
      planned_out     : list[float|None] — negative planned outflows (future only)
      planned_in      : list[float|None] — positive planned inflows (future only)
      balance_actual  : list[float|None] — actual EOD balance (past only)
      balance_projected: list[float|None] — projected EOD balance (future only)
      tooltips        : list[dict] — per-day event breakdown for hover cards
      cutoff_index    : int — index of today in the lists
      starting_balance: float — current bank balance in dollars
      threshold       : float — safety floor in dollars
    """
    from collections import defaultdict

    today = date.today()
    start = today - timedelta(days=days_back)
    end   = today + timedelta(days=days_forward)

    all_rows = list_obligations(limit=5000)

    # Resolve starting balance (latest CSV row with a known balance)
    balance_cents = 0
    csv_sorted = sorted(
        [r for r in all_rows if r.get("source") == "csv" and r.get("account_balance_cents") is not None],
        key=lambda r: str(r.get("due_date", "")),
    )
    if csv_sorted:
        balance_cents = int(csv_sorted[-1]["account_balance_cents"] or 0)

    # Per-day buckets
    actual_out_day:  dict = defaultdict(int)
    actual_in_day:   dict = defaultdict(int)
    planned_out_day: dict = defaultdict(int)
    planned_in_day:  dict = defaultdict(int)
    tooltip_day:     dict = defaultdict(list)

    for r in all_rows:
        raw = r.get("due_date")
        if raw is None:
            continue
        if isinstance(raw, datetime):
            d = raw.date()
        elif isinstance(raw, date):
            d = raw
        elif isinstance(raw, str):
            try:
                d = date.fromisoformat(str(raw)[:10])
            except ValueError:
                continue
        else:
            continue

        if d < start or d > end:
            continue

        amt    = int(r.get("amount_cents") or 0)
        is_in  = r.get("event_type") == "inflow"
        status = r.get("status", "")

        tooltip_day[d].append({
            "name":     (r.get("name") or r.get("vendor_or_customer") or "")[:40],
            "category": r.get("category") or "other",
            "amount":   amt / 100,
            "dir":      "in" if is_in else "out",
            "status":   status,
        })

        if status in ("posted", "matched") and d <= today:
            (actual_in_day if is_in else actual_out_day)[d] += amt
        elif status in ("planned", "pending", "overdue"):
            (planned_in_day if is_in else planned_out_day)[d] += amt

    # --- Compute actual EOD balances (walk backward from today) -----------
    # Strategy: start_balance is as of close-of-business today.
    # Going back: subtract today's net, then yesterday's, etc.
    actual_bal: dict = {today: balance_cents}
    running = balance_cents
    d = today - timedelta(days=1)
    while d >= start:
        # Un-apply next day's changes to get balance at end of d
        next_d = d + timedelta(days=1)
        day_net = actual_in_day.get(next_d, 0) - actual_out_day.get(next_d, 0)
        running -= day_net
        actual_bal[d] = running
        d -= timedelta(days=1)

    # --- Compute projected EOD balances (walk forward from today) ----------
    proj_bal: dict = {}
    running = balance_cents
    d = today + timedelta(days=1)
    while d <= end:
        running += planned_in_day.get(d, 0) - planned_out_day.get(d, 0)
        proj_bal[d] = running
        d += timedelta(days=1)

    # --- Assemble output arrays -------------------------------------------
    labels           = []
    actual_out_list  = []
    actual_in_list   = []
    planned_out_list = []
    planned_in_list  = []
    bal_actual_list  = []
    bal_proj_list    = []
    tooltips         = []
    cutoff_index     = days_back   # index of "today"

    d = start
    while d <= end:
        labels.append(d.strftime("%b %d"))

        is_past_or_today = d <= today

        actual_out_list.append( -(actual_out_day.get(d, 0) / 100)  if is_past_or_today else None)
        actual_in_list.append(   actual_in_day.get(d, 0) / 100     if is_past_or_today else None)
        planned_out_list.append( -(planned_out_day.get(d, 0) / 100) if not is_past_or_today else None)
        planned_in_list.append(   planned_in_day.get(d, 0) / 100   if not is_past_or_today else None)
        bal_actual_list.append(  actual_bal[d] / 100               if d in actual_bal else None)
        bal_proj_list.append(    proj_bal[d] / 100                  if d in proj_bal else None)

        items = tooltip_day.get(d, [])
        tooltips.append({
            "date":  d.isoformat(),
            "items": items,
        })
        d += timedelta(days=1)

    return {
        "labels":           labels,
        "actual_out":       actual_out_list,
        "actual_in":        actual_in_list,
        "planned_out":      planned_out_list,
        "planned_in":       planned_in_list,
        "balance_actual":   bal_actual_list,
        "balance_projected":bal_proj_list,
        "tooltips":         tooltips,
        "cutoff_index":     cutoff_index,
        "starting_balance": balance_cents / 100,
        "threshold":        10000,
    }


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
    # Budget safeguard fields
    runway_days: int            # days until balance < min floor (default $10k)
    at_risk_dates: list         # list[date] of Monday dates going below floor
    min_balance_cents: int      # floor used for safeguard calc


def compute_cash_runway(
    balance_cents: int,
    all_rows: list,
    *,
    min_balance_cents: int = 1_000_000,  # $10,000 floor
    horizon_days: int = 365,
) -> tuple[int, list[date]]:
    """Calculate how many days until the projected balance breaches min_balance_cents.

    Args:
        balance_cents: Current bank balance in cents.
        all_rows: All cash_event rows (from list_obligations).
        min_balance_cents: Safety floor — default $10,000.
        horizon_days: How far to project (default 365 days).

    Returns:
        (runway_days, at_risk_dates) where:
          runway_days  = days until balance < min_balance_cents (or horizon_days if never).
          at_risk_dates = list of Monday dates where weekly projected balance < min_balance_cents.
    """
    from collections import defaultdict

    today = date.today()
    cutoff = today + timedelta(days=horizon_days)

    # Build daily net buckets from planned/overdue rows
    day_buckets: dict = defaultdict(int)
    for r in all_rows:
        if r.get("status") not in ("planned", "overdue", "pending"):
            continue
        raw = r.get("due_date")
        if raw is None:
            continue
        if isinstance(raw, datetime):
            d = raw.date()
        elif isinstance(raw, date):
            d = raw
        elif isinstance(raw, str):
            try:
                d = date.fromisoformat(str(raw)[:10])
            except ValueError:
                continue
        else:
            continue
        if d <= today or d > cutoff:
            continue
        amt = int(r.get("amount_cents") or 0)
        delta = amt if r.get("event_type") == "inflow" else -amt
        day_buckets[d] += delta

    running = balance_cents
    at_risk_dates: list[date] = []
    runway_days = horizon_days  # assume no breach until proven otherwise

    current = today + timedelta(days=1)
    day_num = 0
    while current <= cutoff:
        running += day_buckets.get(current, 0)
        day_num += 1
        if running < min_balance_cents:
            # Track weekly breach dates (one per week to avoid noise)
            week_monday = current - timedelta(days=current.weekday())
            if not at_risk_dates or at_risk_dates[-1] != week_monday:
                at_risk_dates.append(week_monday)
            if runway_days == horizon_days:
                runway_days = day_num
        current += timedelta(days=1)

    return runway_days, at_risk_dates


def compute_finance_overview(
    events: list,
    alerts: list,
    weeks: list,
    balance_cents: int,
    *,
    today,
    ai_text: str = "",
    all_rows: list | None = None,
    min_balance_cents: int = 1_000_000,
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

    # Budget safeguard — runway and at-risk weeks
    if all_rows is not None:
        runway_days, at_risk_dates = compute_cash_runway(
            balance_cents, all_rows, min_balance_cents=min_balance_cents,
        )
    else:
        runway_days, at_risk_dates = 365, []

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
        runway_days=runway_days,
        at_risk_dates=at_risk_dates,
        min_balance_cents=min_balance_cents,
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

    # Also pull the balance from QBO bank actuals if no CSV balance is available
    if balance_cents == 0:
        qbo_bank_rows = [
            r for r in rows
            if r.get("source") == "qbo_bank"
            and r.get("status") == "posted"
            and r.get("account_balance_cents") is not None
        ]
        if qbo_bank_rows:
            qbo_sorted = sorted(
                qbo_bank_rows,
                key=lambda r: str(r.get("due_date", "")).ljust(10, "0"),
                reverse=True,
            )
            balance_cents = int(qbo_sorted[0]["account_balance_cents"] or 0)

    # Compute metrics using the extracted function
    metrics = compute_finance_overview(
        events,
        alerts,
        weeks,
        balance_cents,
        today=today,
        ai_text=ai_text,
        all_rows=rows,
        min_balance_cents=1_000_000,  # $10,000 floor
    )

    # Runway card formatting
    runway_class = ""
    runway_note = "Until $10k floor"
    if metrics.runway_days < 30:
        runway_class = "negative"
        runway_note = "⚠ Critical — under 30 days"
    elif metrics.runway_days < 60:
        runway_class = "negative"
        runway_note = "⚠ Warning — under 60 days"
    elif metrics.runway_days >= 365:
        runway_note = "12+ months — looking good"
    runway_display = f"{metrics.runway_days}d" if metrics.runway_days < 365 else "365d+"

    balance_note = "From latest CSV upload" if csv_rows else "From QBO bank sync"

    # Metric cards
    cards_html = f"""
    <div class="card-grid">
      <div class="metric-card">
        <div class="metric-label">Bank Balance</div>
        <div class="metric-value {metrics.balance_class}">{_dollar(metrics.balance_cents)}</div>
        <div class="metric-note">{balance_note}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Cash Runway</div>
        <div class="metric-value {runway_class}">{runway_display}</div>
        <div class="metric-note">{runway_note}</div>
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

    # Budget safeguard — going-red banner
    safeguard_html = ""
    if metrics.at_risk_dates:
        first_at_risk = metrics.at_risk_dates[0]
        days_to_risk = (first_at_risk - today).days
        risk_weeks_str = ", ".join(d.strftime("%b %d") for d in metrics.at_risk_dates[:5])
        if len(metrics.at_risk_dates) > 5:
            risk_weeks_str += f" (+{len(metrics.at_risk_dates) - 5} more)"
        banner_color = "#fee2e2" if days_to_risk < 14 else "#fef3c7"
        icon_color = "#dc2626" if days_to_risk < 14 else "#d97706"
        safeguard_html = f"""
    <div style="background:{banner_color};border-left:4px solid {icon_color};border-radius:8px;padding:12px 16px;margin-bottom:1rem">
      <div style="display:flex;align-items:flex-start;gap:10px">
        <span style="font-size:1.4rem">⚠️</span>
        <div>
          <strong style="color:{icon_color}">Budget Safeguard — Balance Going Below $10,000</strong>
          <p style="margin:4px 0 0;font-size:0.875rem;color:#374151">
            Projected balance drops below the $10,000 safety floor starting <strong>week of {first_at_risk.strftime("%B %d")}</strong>
            ({days_to_risk} days away).
            At-risk weeks: {html.escape(risk_weeks_str)}.
            Review your <a href="/admin/finances/forecast" style="color:{icon_color}">forecast</a> to identify which expenses to defer.
          </p>
        </div>
      </div>
    </div>"""
    elif metrics.balance_cents > 0 and metrics.runway_days >= 365:
        safeguard_html = """
    <div style="background:#dcfce7;border-left:4px solid #16a34a;border-radius:8px;padding:10px 16px;margin-bottom:1rem">
      <strong style="color:#16a34a">✓ Budget Safeguard — No Red Weeks in 12-Month Horizon</strong>
    </div>"""

    # Top 3 alerts (including at-risk weeks injected as critical alerts)
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
<!-- Chart.js + date adapter for daily x-axis -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>

<div class="card" style="margin-bottom:1rem">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
    <div>
      <h2 style="margin:0 0 2px">Cash Flow — Daily View</h2>
      <p style="color:#6b7280;font-size:0.8rem;margin:0">
        Last 14 days (actuals) · Next 42 days (forecast) &nbsp;·&nbsp;
        Balance: <strong id="chart-balance" style="color:#0D9488">loading…</strong>
      </p>
    </div>
    <div style="display:flex;gap:8px;align-items:center">
      <span style="font-size:0.75rem;color:#6b7280">
        <span style="display:inline-block;width:10px;height:10px;background:rgba(220,38,38,0.85);border-radius:2px;margin-right:3px"></span>Actual out
        <span style="display:inline-block;width:10px;height:10px;background:rgba(22,163,74,0.85);border-radius:2px;margin:0 3px 0 8px"></span>Actual in
        <span style="display:inline-block;width:10px;height:10px;background:rgba(220,38,38,0.22);border:1px solid rgba(220,38,38,0.5);border-radius:2px;margin:0 3px 0 8px"></span>Planned out
        <span style="display:inline-block;width:10px;height:10px;background:rgba(22,163,74,0.22);border:1px solid rgba(22,163,74,0.5);border-radius:2px;margin:0 3px 0 8px"></span>Planned in
      </span>
    </div>
  </div>
  <div style="height:340px;position:relative">
    <canvas id="cashflowChart"></canvas>
    <!-- Hover detail card — must be inside position:relative container so absolute coords align with caretX/Y -->
    <div id="chart-tooltip-card" style="display:none;position:absolute;z-index:20;background:#fff;border:1px solid #e5e7eb;border-radius:10px;box-shadow:0 4px 18px rgba(0,0,0,0.13);padding:12px 14px;min-width:220px;max-width:300px;font-size:0.82rem;pointer-events:none"></div>
  </div>
</div>

<script>
let _chartData = null;
let cashflowChart = null;

function fmt$(v) {
  return '$' + Math.abs(v || 0).toLocaleString('en-US', {minimumFractionDigits:0, maximumFractionDigits:0});
}
function fmtFull$(v) {
  return '$' + (v || 0).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
}

function loadDailyChart() {
  fetch('/admin/finances/chart-data-daily')
    .then(r => r.json())
    .then(data => {
      _chartData = data;
      document.getElementById('chart-balance').textContent = fmtFull$(data.starting_balance);

      const ctx = document.getElementById('cashflowChart').getContext('2d');
      if (cashflowChart) cashflowChart.destroy();

      // Annotate today with a vertical line via plugin
      const todayLine = {
        id: 'todayLine',
        afterDraw(chart) {
          const idx = data.cutoff_index;
          const meta = chart.getDatasetMeta(0);
          if (!meta.data[idx]) return;
          const x = meta.data[idx].x;
          const {top, bottom} = chart.chartArea;
          const ctx2 = chart.ctx;
          ctx2.save();
          ctx2.beginPath();
          ctx2.strokeStyle = 'rgba(15,23,42,0.25)';
          ctx2.lineWidth = 1.5;
          ctx2.setLineDash([4, 3]);
          ctx2.moveTo(x, top);
          ctx2.lineTo(x, bottom);
          ctx2.stroke();
          ctx2.fillStyle = 'rgba(15,23,42,0.7)';
          ctx2.font = '10px system-ui';
          ctx2.fillText('Today', x + 3, top + 12);
          ctx2.restore();
        }
      };

      cashflowChart = new Chart(ctx, {
        type: 'bar',
        plugins: [todayLine],
        data: {
          labels: data.labels,
          datasets: [
            // ---- ACTUALS (past) ----------------------------------------
            {
              label: 'Actual Expenses',
              data: data.actual_out,      // negative values
              backgroundColor: 'rgba(220,38,38,0.85)',
              borderRadius: 3,
              stack: 'actual',
              order: 2,
            },
            {
              label: 'Actual Income',
              data: data.actual_in,       // positive values
              backgroundColor: 'rgba(22,163,74,0.85)',
              borderRadius: 3,
              stack: 'actual',
              order: 2,
            },
            // ---- PLANNED (future) ----------------------------------------
            {
              label: 'Planned Expenses',
              data: data.planned_out,     // negative values
              backgroundColor: 'rgba(220,38,38,0.22)',
              borderColor: 'rgba(220,38,38,0.5)',
              borderWidth: 1,
              borderRadius: 3,
              stack: 'planned',
              order: 2,
            },
            {
              label: 'Planned Income',
              data: data.planned_in,      // positive values
              backgroundColor: 'rgba(22,163,74,0.22)',
              borderColor: 'rgba(22,163,74,0.5)',
              borderWidth: 1,
              borderRadius: 3,
              stack: 'planned',
              order: 2,
            },
            // ---- BALANCE LINES ------------------------------------------
            {
              type: 'line',
              label: 'Cash Balance',
              data: data.balance_actual,
              borderColor: '#0D9488',
              backgroundColor: 'rgba(13,148,136,0.06)',
              borderWidth: 2.5,
              tension: 0.35,
              fill: true,
              spanGaps: false,
              pointRadius: 2,
              pointHoverRadius: 5,
              yAxisID: 'y1',
              order: 1,
            },
            {
              type: 'line',
              label: 'Projected Balance',
              data: data.balance_projected,
              borderColor: '#2563EB',
              backgroundColor: 'transparent',
              borderWidth: 2,
              borderDash: [6, 3],
              tension: 0.35,
              fill: false,
              spanGaps: false,
              pointRadius: 0,
              pointHoverRadius: 4,
              yAxisID: 'y1',
              order: 1,
            },
            {
              type: 'line',
              label: '$10k Safety Floor',
              data: data.labels.map(() => data.threshold),
              borderColor: 'rgba(156,163,175,0.6)',
              backgroundColor: 'transparent',
              borderWidth: 1,
              borderDash: [3, 4],
              pointRadius: 0,
              yAxisID: 'y1',
              order: 1,
            },
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: 'index', intersect: false },
          plugins: {
            legend: { display: false },
            tooltip: {
              enabled: false,   // we use custom hover card below
              external(context) {
                const card = document.getElementById('chart-tooltip-card');
                if (context.tooltip.opacity === 0) { card.style.display='none'; return; }
                const idx = context.tooltip.dataPoints[0]?.dataIndex;
                if (idx == null || !data.tooltips[idx]) { card.style.display='none'; return; }

                const tip = data.tooltips[idx];
                const isActual = idx <= data.cutoff_index;
                const label = data.labels[idx];

                // Group items by category
                const outItems = tip.items.filter(i => i.dir === 'out').sort((a,b) => b.amount-a.amount);
                const inItems  = tip.items.filter(i => i.dir === 'in').sort((a,b)  => b.amount-a.amount);
                const totalOut = outItems.reduce((s,i) => s+i.amount, 0);
                const totalIn  = inItems.reduce((s,i)  => s+i.amount, 0);

                const rows = (items, color) => items.slice(0,6).map(i =>
                  `<tr><td style="color:#6b7280;padding:1px 6px 1px 0;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${i.name||i.cat}</td>
                   <td style="text-align:right;color:${color};font-weight:600;white-space:nowrap">${fmt$(i.amount)}</td></tr>`
                ).join('') + (items.length>6 ? `<tr><td colspan="2" style="color:#9ca3af;font-size:0.75rem">+${items.length-6} more</td></tr>` : '');

                card.innerHTML = `
                  <div style="font-weight:700;font-size:0.875rem;margin-bottom:6px;border-bottom:1px solid #f3f4f6;padding-bottom:4px">
                    ${label} <span style="font-weight:400;color:#9ca3af;font-size:0.75rem">${isActual?'Actual':'Forecast'}</span>
                  </div>
                  ${outItems.length ? `
                    <div style="color:#dc2626;font-size:0.75rem;font-weight:600;margin-bottom:2px">▼ OUT — ${fmt$(totalOut)}</div>
                    <table style="width:100%;font-size:0.78rem">${rows(outItems,'#dc2626')}</table>` : ''}
                  ${inItems.length ? `
                    <div style="color:#16a34a;font-size:0.75rem;font-weight:600;margin-bottom:2px;margin-top:${outItems.length?'6px':'0'}">▲ IN — ${fmt$(totalIn)}</div>
                    <table style="width:100%;font-size:0.78rem">${rows(inItems,'#16a34a')}</table>` : ''}
                  ${!outItems.length && !inItems.length ? '<div style="color:#9ca3af;font-size:0.8rem">No transactions</div>' : ''}
                `;

                // Position card — caretX/Y are canvas-relative pixels; card is
                // inside the same position:relative container so coords align directly.
                const chartW = context.chart.width;
                const x = context.tooltip.caretX;
                const cardW = 240;
                const left = (x + cardW + 12) > chartW ? x - cardW - 8 : x + 12;
                const top  = Math.max(0, context.tooltip.caretY - 20);
                card.style.left = left + 'px';
                card.style.top  = top + 'px';
                card.style.display = 'block';
              }
            }
          },
          scales: {
            x: {
              stacked: true,
              ticks: {
                maxTicksLimit: 14,
                maxRotation: 45,
                font: { size: 10 },
                color: '#9ca3af',
              },
              grid: { display: false },
            },
            y: {
              // Bar axis — daily flows
              stacked: true,
              position: 'left',
              ticks: {
                callback: v => fmt$(v),
                font: { size: 10 },
                color: '#9ca3af',
                maxTicksLimit: 6,
              },
              grid: { color: 'rgba(0,0,0,0.04)' },
            },
            y1: {
              // Line axis — running balance
              position: 'right',
              ticks: {
                callback: v => '$' + (v/1000).toFixed(0) + 'k',
                font: { size: 10 },
                color: '#0D9488',
                maxTicksLimit: 6,
              },
              grid: { drawOnChartArea: false },
            },
          }
        }
      });
    });
}
document.addEventListener('DOMContentLoaded', loadDailyChart);
</script>"""

    body = f"""
    <div>
      <p class="eyebrow" style="margin:0 0 10px;text-transform:uppercase;letter-spacing:.18em;font-size:12px;font-weight:800;color:var(--accent);font-family:'Montserrat',sans-serif;">Finance</p>
      <h1>Cash overview.</h1>
      <p class="page-sub" style="margin-top:10px">Cash position · {today.strftime("%B %d, %Y")}</p>
      {ai_block}
    </div>
    {safeguard_html}
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
        {weekly_table_html if events else '<div class="empty-state">No planned events yet — sync QuickBooks or add recurring templates to populate the forecast.</div>'}
      </div>
      <div class="action-row">
        <a href="/admin/finances/forecast" class="btn btn-secondary btn-sm">Full Forecast →</a>
      </div>
    </div>
    <div class="action-row" style="margin-top:0">
      <form method="post" action="/admin/finances/sync-qbo" style="display:inline">
        <button type="submit" class="btn btn-primary">⟳ Sync All (QBO + ClickUp)</button>
      </form>
      <a href="/admin/finances/ap/new" class="btn btn-secondary">+ Add Payable</a>
      <a href="/admin/finances/ar/new" class="btn btn-secondary">+ Add Receivable</a>
      <a href="/admin/finances/recurring" class="btn btn-secondary">Recurring Templates</a>
      <a href="/admin/finances/reconcile" class="btn btn-secondary">Actuals vs Planned</a>
      <a href="/admin/finances/upload" class="btn btn-secondary btn-sm" style="opacity:0.6" title="Manual CSV upload (fallback)">Upload CSV</a>
    </div>"""

    return _page_shell("Finance Overview", "overview", body, flash=flash)
