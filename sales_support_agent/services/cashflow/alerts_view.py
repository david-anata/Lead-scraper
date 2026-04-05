"""Alerts view page with bulk dismiss support."""
from __future__ import annotations

import html
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

from sales_support_agent.services.cashflow.cashflow_helpers import _dollar, _events_to_dtos, _page_shell
from sales_support_agent.services.cashflow.engine import aggregate_weeks, flag_risks
from sales_support_agent.services.cashflow.obligations import list_obligations


def _get_dismissed_keys() -> set:
    """Load dismissed alert keys from kv_store."""
    try:
        from sales_support_agent.models.database import get_engine
        from sqlalchemy import text
        with get_engine().connect() as conn:
            rows = conn.execute(text("SELECT key FROM kv_store WHERE key LIKE 'alert_dismissed:%'")).fetchall()
        return {row[0] for row in rows}
    except Exception as exc:
        logger.warning("Could not load dismissed alert keys from kv_store: %s", exc)
        return set()


def _get_bulk_dismiss_time():
    """Get the bulk dismiss timestamp if set."""
    try:
        from sales_support_agent.models.database import get_engine
        from sqlalchemy import text
        with get_engine().connect() as conn:
            row = conn.execute(text("SELECT value FROM kv_store WHERE key='alerts_bulk_dismissed_at'")).fetchone()
        if row:
            return datetime.fromisoformat(row[0])
    except Exception as exc:
        logger.warning("Could not load bulk dismiss timestamp from kv_store: %s", exc)
    return None


def render_alerts_view_page(*, flash: str = "", severity_filter: str = "all") -> str:
    """Render the Alerts page with grouping, dismiss buttons, and severity filter."""
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
    all_alerts = flag_risks(weeks, events)

    # Load dismissed state
    dismissed_keys = _get_dismissed_keys()
    bulk_dismiss_time = _get_bulk_dismiss_time()

    # Filter by severity
    if severity_filter != "all":
        filtered_alerts = [a for a in all_alerts if a.severity == severity_filter]
    else:
        filtered_alerts = all_alerts

    counts = {"critical": 0, "warning": 0, "info": 0}
    for a in all_alerts:
        counts[a.severity] = counts.get(a.severity, 0) + 1

    def _alert_id(a) -> str:
        return f"{a.alert_type}:{a.title[:40].replace(' ', '_')}"

    def _is_dismissed(a) -> bool:
        alert_key = f"alert_dismissed:{_alert_id(a)}"
        if alert_key in dismissed_keys:
            return True
        if bulk_dismiss_time:
            # If alert's week is before bulk dismiss time, treat as dismissed
            if a.week_start:
                alert_dt = datetime(a.week_start.year, a.week_start.month, a.week_start.day)
                if alert_dt <= bulk_dismiss_time:
                    return True
        return False

    visible_alerts = [a for a in filtered_alerts if not _is_dismissed(a)]

    # Group by severity
    critical_alerts = [a for a in visible_alerts if a.severity == "critical"]
    warning_alerts = [a for a in visible_alerts if a.severity == "warning"]
    info_alerts = [a for a in visible_alerts if a.severity == "info"]

    def _render_group(alerts_list, group_label, color, bg_color) -> str:
        if not alerts_list:
            return ""
        rows_html = ""
        for a in alerts_list:
            a_id = _alert_id(a)
            week_s = a.week_start.strftime("%b %d") if a.week_start else "—"
            rows_html += f"""
            <tr>
              <td><span class="badge badge-{a.severity}">{html.escape(a.severity.upper())}</span></td>
              <td style="font-weight:600">{html.escape(a.title)}</td>
              <td style="font-size:12px;color:#6b7a8d">{html.escape(a.detail)}</td>
              <td style="font-size:12px;color:#6b7a8d">{html.escape(a.alert_type)}</td>
              <td style="font-size:12px">{week_s}</td>
              <td>
                <form method="post" action="/admin/finances/alerts/dismiss/{html.escape(a_id)}" style="display:inline">
                  <button type="submit" style="padding:3px 8px;background:#f3f4f6;border:1px solid #e5e7eb;border-radius:4px;font-size:0.75rem;cursor:pointer;color:#6b7280">Dismiss</button>
                </form>
              </td>
            </tr>"""
        return f"""
        <div style="margin-bottom:1.5rem">
          <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem">
            <h3 style="margin:0;color:{color}">{group_label}</h3>
            <span style="background:{bg_color};color:{color};padding:2px 8px;border-radius:10px;font-size:0.75rem;font-weight:700">{len(alerts_list)}</span>
          </div>
          <table>
            <thead><tr><th>Level</th><th>Alert</th><th>Detail</th><th>Type</th><th>Week</th><th></th></tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>"""

    # Severity filter tabs
    def stab(sev, label, cnt):
        active = "background:#0f172a;color:#fff" if severity_filter == sev else "background:#f1f5f9;color:#374151"
        return f'<a href="?severity={sev}" style="text-decoration:none"><button style="padding:6px 14px;border-radius:20px;border:none;cursor:pointer;font-size:0.85rem;{active}">{label} ({cnt})</button></a>'

    if not visible_alerts:
        content = '<div class="empty-state" style="color:#2e7d52">✓ No active risk alerts detected. Cashflow looks healthy.</div>'
    else:
        content = (
            _render_group(critical_alerts, "Critical", "#b91c1c", "rgba(185,28,28,.10)") +
            _render_group(warning_alerts, "Warnings", "#a16207", "rgba(161,98,7,.12)") +
            _render_group(info_alerts, "Info", "#4f84c4", "rgba(133,187,218,.18)")
        )

    body = f"""
    <h1>Risk Alerts</h1>
    <p class="page-sub">
      {counts.get("critical", 0)} critical ·
      {counts.get("warning", 0)} warnings ·
      {counts.get("info", 0)} info
    </p>

    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;flex-wrap:wrap;gap:0.5rem">
        <div style="display:flex;gap:0.25rem;background:#f1f5f9;border-radius:24px;padding:3px">
          {stab('all','All',len(all_alerts))}
          {stab('critical','Critical',counts.get('critical',0))}
          {stab('warning','Warning',counts.get('warning',0))}
          {stab('info','Info',counts.get('info',0))}
        </div>
        <form method="post" action="/admin/finances/alerts/dismiss-all" style="display:inline">
          <button type="submit" style="padding:6px 14px;background:#fee2e2;color:#b91c1c;border:none;border-radius:6px;cursor:pointer;font-size:0.85rem">Dismiss All</button>
        </form>
      </div>

      {content}
    </div>

    <div class="action-row">
      <a href="/admin/finances/forecast" class="btn btn-secondary btn-sm">View Forecast →</a>
      <a href="/admin/finances/scenario" class="btn btn-secondary btn-sm">Run Scenario →</a>
    </div>"""

    return _page_shell("Risk Alerts", "alerts", body, flash=flash)
