"""Cashflow Overview — Finance OS landing page."""

from __future__ import annotations

import asyncio
import html
import os
from dataclasses import dataclass as _dc
from datetime import date, datetime, timedelta
from typing import Any

from sales_support_agent.services.admin_nav import render_agent_nav, render_agent_nav_styles
from sales_support_agent.services.cashflow.engine import (
    EventDTO,
    aggregate_weeks,
    flag_risks,
)
from sales_support_agent.services.cashflow.obligations import list_obligations


# ---------------------------------------------------------------------------
# Shared CSS
# ---------------------------------------------------------------------------

FINANCE_CSS = """
  :root {
    --anata-ink: #2b3644;
    --anata-ink-soft: #4b5668;
    --anata-sky: #85bbda;
    --anata-sky-deep: #4f84c4;
    --anata-sand: #bfa889;
    --anata-sand-soft: #f9f7f3;
    --anata-paper: #ffffff;
    --anata-line: rgba(43, 54, 68, 0.10);
    --anata-shadow: rgba(43, 54, 68, 0.10);
    --anata-muted: #6b7688;
    --panel: var(--anata-paper);
    --ink: var(--anata-ink);
    --muted: var(--anata-muted);
    --line: var(--anata-line);
    --accent: var(--anata-sky);
    --good: #0f766e;
    --warn: #a16207;
    --bad: #b91c1c;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--anata-sand-soft); color: var(--ink); font-family: "Inter", "Segoe UI", sans-serif; }
  a { color: var(--anata-ink); }
  .shell { max-width: 1180px; margin: 0 auto; padding: 28px 18px 64px; display: grid; gap: 20px; }
  h1, h2, h3, p { margin: 0; }
  h1, h2, h3 { font-family: "Montserrat", sans-serif; color: var(--anata-ink); }
  h1 { font-size: clamp(1.6rem, 3vw, 2.6rem); line-height: 1.05; letter-spacing: -0.02em; font-weight: 800; }
  h2 { font-size: 18px; line-height: 1.2; letter-spacing: -0.01em; font-weight: 700; }
  .page-sub { color: var(--muted); font-size: 14px; line-height: 1.5; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 26px; padding: 24px; box-shadow: 0 18px 40px var(--anata-shadow); }
  .card-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 14px;
  }
  .metric-card {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 22px;
    padding: 18px 20px;
    display: grid;
    gap: 6px;
    box-shadow: 0 8px 20px var(--anata-shadow);
  }
  .metric-label { font-size: 11px; color: var(--muted); font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; font-family: "Montserrat", sans-serif; }
  .metric-value { font-size: 26px; font-weight: 700; font-family: "Montserrat", sans-serif; color: var(--anata-ink); line-height: 1.1; }
  .metric-note { font-size: 12px; color: var(--muted); }
  .metric-value.positive { color: var(--good); }
  .metric-value.negative { color: var(--bad); }
  .badge { display: inline-flex; align-items: center; padding: 3px 9px; border-radius: 999px; font-size: 11px; font-weight: 700; letter-spacing: 0.04em; }
  .badge-critical { background: rgba(185,28,28,.10); color: var(--bad); }
  .badge-warning  { background: rgba(161,98,7,.12); color: var(--warn); }
  .badge-info     { background: rgba(133,187,218,.18); color: var(--anata-sky-deep); }
  .badge-ok       { background: rgba(15,118,110,.10); color: var(--good); }
  .btn {
    display: inline-flex; align-items: center; padding: 10px 20px;
    border-radius: 999px; font-family: "Montserrat", sans-serif;
    font-weight: 700; font-size: 13px; text-decoration: none;
    cursor: pointer; border: 0; transition: box-shadow 120ms, transform 120ms;
  }
  .btn:hover { transform: translateY(-1px); }
  .btn-primary { background: var(--anata-ink); color: #fff; box-shadow: 0 8px 18px rgba(43,54,68,.18); }
  .btn-primary:hover { box-shadow: 0 12px 24px rgba(43,54,68,.22); }
  .btn-secondary { background: #fff; color: var(--anata-ink); border: 1px solid rgba(43,54,68,0.14); box-shadow: 0 4px 10px rgba(43,54,68,.06); }
  .btn-sm { padding: 7px 15px; font-size: 12px; }
  .action-row { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 16px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 10px; border-bottom: 2px solid var(--line); font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); font-family: "Montserrat", sans-serif; font-weight: 700; }
  td { padding: 10px 10px; border-bottom: 1px solid rgba(43,54,68,0.05); vertical-align: middle; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: rgba(133,187,218,.04); }
  .amount-in  { color: var(--good); font-weight: 700; }
  .amount-out { color: var(--bad);  font-weight: 700; }
  .status-pill { display: inline-flex; align-items: center; padding: 3px 10px; border-radius: 999px; font-size: 11px; font-weight: 700; }
  .status-planned  { background: rgba(133,187,218,.18); color: var(--anata-sky-deep); }
  .status-pending  { background: rgba(161,98,7,.12); color: var(--warn); }
  .status-overdue  { background: rgba(185,28,28,.10); color: var(--bad); }
  .status-paid     { background: rgba(15,118,110,.10); color: var(--good); }
  .status-posted   { background: rgba(43,54,68,.08); color: var(--muted); }
  .status-matched  { background: rgba(15,118,110,.10); color: var(--good); }
  .flash-success { background: rgba(15,118,110,.08); color: var(--good); border: 1px solid rgba(15,118,110,.20); border-radius: 16px; padding: 12px 18px; font-size: 14px; }
  .flash-error   { background: rgba(185,28,28,.08); color: var(--bad);  border: 1px solid rgba(185,28,28,.18); border-radius: 16px; padding: 12px 18px; font-size: 14px; }
  .ai-summary { background: rgba(133,187,218,.08); border: 1px solid rgba(133,187,218,.28); border-radius: 18px; padding: 18px 20px; font-size: 14px; line-height: 1.65; color: var(--anata-ink); }
  .week-bar-row { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; font-size: 13px; }
  .week-bar-label { width: 80px; flex-shrink: 0; color: var(--muted); font-size: 12px; font-weight: 600; }
  .week-bar-track { flex: 1; height: 7px; background: rgba(43,54,68,.07); border-radius: 4px; overflow: hidden; }
  .week-bar-fill-in  { height: 100%; background: var(--good); border-radius: 4px; }
  .week-bar-fill-out { height: 100%; background: var(--bad); border-radius: 4px; }
  .week-net { width: 90px; text-align: right; font-weight: 700; font-size: 13px; }
  .subnav { display: flex; gap: 8px; flex-wrap: wrap; }
  .subnav-link {
    display: inline-flex; align-items: center; min-height: 36px; padding: 0 14px;
    border-radius: 999px; font-size: 12px; font-weight: 700; letter-spacing: .02em;
    font-family: "Montserrat", sans-serif; text-decoration: none;
    background: rgba(255,255,255,.62); border: 1px solid rgba(43,54,68,.08);
    color: var(--anata-ink); transition: background 120ms, border-color 120ms, box-shadow 120ms;
  }
  .subnav-link:hover { background: #fff; border-color: rgba(43,54,68,.18); box-shadow: 0 4px 12px rgba(43,54,68,.08); transform: translateY(-1px); }
  .subnav-link.active { background: rgba(133,187,218,.22); border-color: rgba(133,187,218,.52); color: var(--anata-ink); box-shadow: inset 0 0 0 1px rgba(133,187,218,.16); }
  .empty-state { text-align: center; padding: 40px 20px; color: var(--muted); font-size: 14px; }
  .week-section { margin-bottom: 6px; }
  .week-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 10px 16px; background: var(--anata-ink); color: #fff;
    border-radius: 14px 14px 0 0; font-family: "Montserrat", sans-serif;
    font-weight: 800; font-size: 13px; cursor: pointer;
  }
  .week-header-totals { display: flex; gap: 16px; font-size: 12px; font-weight: 700; }
  .week-body { background: var(--panel); border: 1px solid var(--line); border-top: none; border-radius: 0 0 14px 14px; overflow: hidden; }
  .week-body table { margin: 0; }
  .week-body td, .week-body th { padding: 8px 12px; }
  .week-net-row td { background: rgba(43,54,68,.04); font-weight: 700; font-family: "Montserrat", sans-serif; font-size: 13px; }
  .priority-must_pay { background: rgba(185,28,28,.12); color: #b91c1c; }
  .priority-should_pay { background: rgba(161,98,7,.12); color: #a16207; }
  .priority-review { background: rgba(133,187,218,.18); color: var(--anata-sky-deep); }
  .priority-can_hold { background: rgba(15,118,110,.10); color: var(--good); }
  .clickup-link { font-size: 11px; color: var(--muted); text-decoration: none; }
  .clickup-link:hover { color: var(--anata-sky-deep); }
  .week-empty { padding: 12px 16px; color: var(--muted); font-size: 13px; font-style: italic; }
  .cat-group-header td { background: rgba(43,54,68,.03); padding: 8px 12px; border-top: 1px solid rgba(43,54,68,.06); font-size: 13px; }
  .cat-group-header:first-child td { border-top: none; }
  .cat-item-row td { padding: 6px 10px; border-bottom: none; }
  .cat-item-row:last-of-type td { border-bottom: 1px solid rgba(43,54,68,.06); }
  .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 14px; }
  .form-row.single { grid-template-columns: 1fr; }
  .form-row.triple { grid-template-columns: 1fr 1fr 1fr; }
  label { display: block; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 6px; font-family: "Montserrat", sans-serif; }
  input[type=text], input[type=number], input[type=date], select, textarea {
    width: 100%; padding: 10px 14px; border: 1px solid var(--line);
    border-radius: 14px; font-size: 14px; font-family: inherit; color: var(--anata-ink);
    background: #fff; outline: none;
  }
  input:focus, select:focus, textarea:focus { border-color: var(--accent); box-shadow: 0 0 0 3px rgba(133,187,218,.18); }
  .finance-subnav-bar {
    padding: 10px 24px;
    border-bottom: 1px solid rgba(43,54,68,.10);
    background: rgba(249,247,243,.94);
    backdrop-filter: blur(12px);
    position: sticky;
    top: 65px;
    z-index: 15;
    box-shadow: 0 4px 12px rgba(43,54,68,.04);
  }
  .finance-subnav-inner { max-width: 1180px; margin: 0 auto; }
  @media (max-width: 700px) {
    .form-row, .form-row.triple { grid-template-columns: 1fr; }
    .card-grid { grid-template-columns: 1fr 1fr; }
  }
"""


def _page_shell(title: str, active_section: str, body: str, *, flash: str = "") -> str:
    flash_html = ""
    if flash.startswith("ok:"):
        flash_html = f'<div class="flash-success">{html.escape(flash[3:])}</div>'
    elif flash.startswith("err:"):
        flash_html = f'<div class="flash-error">{html.escape(flash[4:])}</div>'

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | {html.escape(title)}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800;900&display=swap" rel="stylesheet">
    <style>
      {render_agent_nav_styles()}
      {FINANCE_CSS}
    </style>
  </head>
  <body>
    {render_agent_nav(active="finance")}
    <div class="finance-subnav-bar">
      <div class="finance-subnav-inner">
        {_finance_subnav(active_section)}
      </div>
    </div>
    <div class="shell">
      {flash_html}
      {body}
    </div>
  </body>
</html>"""


def _finance_subnav(active: str) -> str:
    items = [
        ("Overview", "/admin/finances", "overview"),
        ("Forecast", "/admin/finances/forecast", "forecast"),
        ("Payables (AP)", "/admin/finances/ap", "ap"),
        ("Receivables (AR)", "/admin/finances/ar", "ar"),
        ("Alerts", "/admin/finances/alerts", "alerts"),
        ("Scenario", "/admin/finances/scenario", "scenario"),
        ("Upload CSV", "/admin/finances/upload", "upload"),
        ("Recurring", "/admin/finances/recurring", "recurring"),
    ]
    links = "".join(
        f'<a href="{href}" class="subnav-link{"" if key != active else " active"}">{html.escape(label)}</a>'
        for label, href, key in items
    )
    return f'<nav class="subnav">{links}</nav>'


def _dollar(cents: int) -> str:
    neg = cents < 0
    val = abs(cents) / 100
    s = f"${val:,.0f}"
    return f"-{s}" if neg else s


def _events_to_dtos(rows: list[dict[str, Any]]) -> list[EventDTO]:
    out: list[EventDTO] = []
    for r in rows:
        raw_date = r.get("due_date")
        if isinstance(raw_date, str):
            try:
                due = date.fromisoformat(raw_date[:10])
            except ValueError:
                continue
        elif isinstance(raw_date, datetime):
            due = raw_date.date()
        elif isinstance(raw_date, date):
            due = raw_date
        else:
            continue
        out.append(
            EventDTO(
                id=str(r.get("id", "")),
                source=str(r.get("source", "")),
                event_type=str(r.get("event_type", "outflow")),
                category=str(r.get("category", "other")),
                name=str(r.get("name", "")),
                vendor_or_customer=str(r.get("vendor_or_customer", "")),
                amount_cents=int(r.get("amount_cents") or 0),
                due_date=due,
                status=str(r.get("status", "planned")),
                confidence=str(r.get("confidence", "estimated")),
                matched_to_id=r.get("matched_to_id"),
                recurring_rule=r.get("recurring_rule"),
            )
        )
    return out


# ---------------------------------------------------------------------------
# Weekly table helpers
# ---------------------------------------------------------------------------

# Category display order and labels — mirrors ClickUp AP/AR list structure
CATEGORY_ORDER: list[tuple[str, str, str]] = [
    # (internal_key, display_label, event_type_filter)  "" = show both
    ("revenue",      "Revenue",                "inflow"),
    ("payroll",      "Payroll",                "outflow"),
    ("rent",         "Rent",                   "outflow"),
    ("debt",         "Debt / Loans",           "outflow"),
    ("software",     "Software & SaaS",        "outflow"),
    ("utilities",    "Utilities",              "outflow"),
    ("insurance",    "Insurance",              "outflow"),
    ("credit_card",  "Credit Card Payments",   "outflow"),
    ("tax",          "Tax",                    "outflow"),
    ("supplies",     "Supplies",               "outflow"),
    ("equipment",    "Equipment",              "outflow"),
    ("meals",        "Meals",                  "outflow"),
    ("fees",         "Bank Fees",              "outflow"),
    ("owner_draw",   "Owner Draw",             "outflow"),
    ("transfer",     "Transfers",              ""),
    ("uncategorized","Uncategorized",          ""),
]

_CAT_ORDER_INDEX: dict[str, int] = {cat: i for i, (cat, _, _) in enumerate(CATEGORY_ORDER)}

# Category icon mapping for visual variety
_CAT_ICON: dict[str, str] = {
    "revenue":      "💰",
    "payroll":      "👥",
    "rent":         "🏠",
    "debt":         "🏦",
    "software":     "💻",
    "utilities":    "⚡",
    "insurance":    "🛡️",
    "credit_card":  "💳",
    "tax":          "🧾",
    "supplies":     "📦",
    "equipment":    "🔧",
    "meals":        "🍽️",
    "fees":         "💸",
    "owner_draw":   "👤",
    "transfer":     "↔️",
    "uncategorized":"📌",
}


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
