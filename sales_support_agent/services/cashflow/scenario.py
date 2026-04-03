"""Scenario planner — what-if adjustments to the cashflow forecast."""

from __future__ import annotations

import html
from datetime import date, datetime
from typing import Any

from sales_support_agent.services.cashflow.engine import (
    ScenarioAdjustment,
    aggregate_weeks,
    apply_scenario,
    flag_risks,
)
from sales_support_agent.services.cashflow.obligations import list_obligations
from sales_support_agent.services.cashflow.overview import (
    _dollar,
    _events_to_dtos,
    _page_shell,
)


def render_scenario_page(
    *,
    adjustments: list[dict[str, Any]] | None = None,
    flash: str = "",
) -> str:
    rows = list_obligations(limit=2000)
    base_events = _events_to_dtos(rows)

    balance_cents = 0
    csv_rows = sorted(
        [r for r in rows if r.get("source") == "csv" and r.get("account_balance_cents") is not None],
        key=lambda r: str(r.get("due_date", "")),
        reverse=True,
    )
    if csv_rows:
        balance_cents = int(csv_rows[0]["account_balance_cents"] or 0)

    # Baseline forecast
    base_weeks = aggregate_weeks(base_events, starting_cash_cents=balance_cents, weeks=12)

    # Scenario forecast (apply adjustments if provided)
    adj_objects: list[ScenarioAdjustment] = []
    if adjustments:
        for a in adjustments:
            new_date = None
            raw_d = a.get("new_due_date")
            if raw_d:
                try:
                    new_date = date.fromisoformat(str(raw_d)[:10])
                except ValueError:
                    pass

            new_amount = None
            raw_amt = a.get("new_amount_dollars")
            if raw_amt is not None:
                try:
                    from decimal import Decimal
                    new_amount = int(Decimal(str(raw_amt).replace(",", "")) * 100)
                except Exception:
                    pass

            adj_objects.append(
                ScenarioAdjustment(
                    event_id=str(a.get("event_id", "")),
                    new_amount_cents=new_amount,
                    new_due_date=new_date,
                    remove=bool(a.get("remove", False)),
                )
            )

    scenario_events = apply_scenario(base_events, adj_objects)
    scen_weeks = aggregate_weeks(scenario_events, starting_cash_cents=balance_cents, weeks=12)
    scen_alerts = flag_risks(scen_weeks, scenario_events)

    # Comparison table
    compare_rows = ""
    for bw, sw in zip(base_weeks, scen_weeks):
        base_end = _dollar(bw.ending_cash_cents)
        scen_end = _dollar(sw.ending_cash_cents)
        diff = sw.ending_cash_cents - bw.ending_cash_cents
        diff_cls = "amount-in" if diff >= 0 else "amount-out"
        diff_s = ("+" if diff >= 0 else "") + _dollar(diff)
        flag = " 🔴" if sw.is_negative else (" 🟡" if sw.ending_cash_cents < bw.ending_cash_cents else "")
        compare_rows += f"""
        <tr>
          <td style="font-weight:600">{html.escape(bw.label)}</td>
          <td>{base_end}</td>
          <td style="font-weight:600">{scen_end}{flag}</td>
          <td class="{diff_cls}">{diff_s}</td>
        </tr>"""

    # Event selector for adding adjustments
    upcoming = sorted(
        [e for e in base_events if e.status not in ("paid", "matched", "cancelled")],
        key=lambda e: e.due_date,
    )[:60]

    event_options = '<option value="">— select an obligation —</option>' + "".join(
        f'<option value="{html.escape(e.id)}">'
        f'{e.due_date.strftime("%b %d")} · {html.escape(e.name or e.vendor_or_customer or e.id[:8])}'
        f' ({_dollar(e.amount_cents)})'
        f'</option>'
        for e in upcoming
    )

    scen_alert_pills = ""
    for a in scen_alerts[:5]:
        scen_alert_pills += f'<span class="badge badge-{a.severity}" style="margin-right:6px">{html.escape(a.title)}</span>'
    if not scen_alert_pills:
        scen_alert_pills = '<span class="badge badge-ok">No alerts in scenario</span>'

    body = f"""
    <h1>Scenario Planner</h1>
    <p class="page-sub">Adjust amounts and dates to model what-if situations</p>

    <div class="card">
      <h2>Add Adjustment</h2>
      <form method="post" action="/admin/finances/scenario">
        <div class="form-row">
          <div>
            <label>Obligation</label>
            <select name="event_id">{event_options}</select>
          </div>
          <div>
            <label>New Amount ($) — leave blank to keep</label>
            <input type="number" name="new_amount_dollars" step="0.01" min="0" placeholder="e.g. 1500.00">
          </div>
        </div>
        <div class="form-row">
          <div>
            <label>New Due Date — leave blank to keep</label>
            <input type="date" name="new_due_date">
          </div>
          <div style="display:flex;align-items:flex-end;gap:10px">
            <label style="display:flex;align-items:center;gap:8px;text-transform:none;font-size:14px;cursor:pointer">
              <input type="checkbox" name="remove" value="1">
              Remove from scenario entirely
            </label>
          </div>
        </div>
        <div class="action-row">
          <button type="submit" class="btn btn-primary">Apply Adjustment</button>
          <a href="/admin/finances/scenario" class="btn btn-secondary">Reset</a>
        </div>
      </form>
    </div>

    <div class="card">
      <h2>Scenario vs Baseline</h2>
      <div style="margin-bottom:12px">{scen_alert_pills}</div>
      <table>
        <thead>
          <tr><th>Week</th><th>Baseline Ending</th><th>Scenario Ending</th><th>Difference</th></tr>
        </thead>
        <tbody>
          {compare_rows if compare_rows else '<tr><td colspan="4" class="empty-state">Upload a CSV and add obligations to run scenarios.</td></tr>'}
        </tbody>
      </table>
    </div>"""

    return _page_shell("Scenario Planner", "scenario", body, flash=flash)
