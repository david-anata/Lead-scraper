"""Risk Alerts page."""

from __future__ import annotations

import html
from datetime import datetime

from sales_support_agent.services.cashflow.engine import aggregate_weeks, flag_risks
from sales_support_agent.services.cashflow.obligations import list_obligations
from sales_support_agent.services.cashflow.overview import _dollar, _events_to_dtos, _page_shell


def render_risk_alerts_page(*, flash: str = "") -> str:
    rows = list_obligations(limit=2000)
    events = _events_to_dtos(rows)

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

    if not alerts:
        body = """
        <h1>Risk Alerts</h1>
        <p class="page-sub">Automated cashflow risk detection</p>
        <div class="card">
          <div class="empty-state" style="color:#2e7d52">
            ✓ No risk alerts detected. Cashflow looks healthy.
          </div>
        </div>"""
        return _page_shell("Risk Alerts", "alerts", body)

    counts = {"critical": 0, "warning": 0, "info": 0}
    for a in alerts:
        counts[a.severity] = counts.get(a.severity, 0) + 1

    alert_rows = ""
    for a in alerts:
        badge_cls = f"badge-{a.severity}"
        week_s = a.week_start.strftime("%b %d") if a.week_start else "—"
        alert_rows += f"""
        <tr>
          <td><span class="badge {badge_cls}">{html.escape(a.severity.upper())}</span></td>
          <td style="font-weight:600">{html.escape(a.title)}</td>
          <td style="font-size:12px;color:#6b7a8d">{html.escape(a.detail)}</td>
          <td style="font-size:12px;color:#6b7a8d">{html.escape(a.alert_type)}</td>
          <td style="font-size:12px">{week_s}</td>
        </tr>"""

    body = f"""
    <h1>Risk Alerts</h1>
    <p class="page-sub">
      {counts.get("critical", 0)} critical ·
      {counts.get("warning", 0)} warnings ·
      {counts.get("info", 0)} info
    </p>
    <div class="card">
      <table>
        <thead>
          <tr><th>Level</th><th>Alert</th><th>Detail</th><th>Type</th><th>Week</th></tr>
        </thead>
        <tbody>{alert_rows}</tbody>
      </table>
    </div>
    <div class="action-row">
      <a href="/admin/finances/forecast" class="btn btn-secondary btn-sm">View Forecast →</a>
      <a href="/admin/finances/scenario" class="btn btn-secondary btn-sm">Run Scenario →</a>
    </div>"""

    return _page_shell("Risk Alerts", "alerts", body, flash=flash)
