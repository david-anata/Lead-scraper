"""Review page for rebuilding the billing schedule off ClickUp.

Three sections, all pre-ticked with the safe recommendation, none of it applied
until the operator confirms.
"""

from __future__ import annotations

import html
from typing import Any

from sales_support_agent.services.cashflow.overview import _money, _page_shell


def _cadence_label(proposal: dict[str, Any]) -> str:
    frequency = str(proposal["frequency"])
    if frequency in {"weekly", "biweekly"} and proposal.get("weekday"):
        return f"{frequency} on {proposal['weekday']}s"
    if proposal.get("day_of_month"):
        return f"{frequency} on the {proposal['day_of_month']}"
    return frequency


def render_import_page(*, flash: str = "") -> str:
    from sales_support_agent.services.cashflow.schedule_import import build_import_plan

    try:
        plan = build_import_plan()
    except Exception as exc:
        return _page_shell(
            "Rebuild schedule", "review",
            f'<div class="card"><p>The schedule could not be read: {html.escape(str(exc))}</p></div>',
            flash=flash,
        )

    if not plan["schedules"] and not plan["overdue"] and not plan["unscheduled"]:
        return _page_shell(
            "Rebuild schedule", "review",
            '<h1>Rebuild your schedule</h1>'
            '<div class="card"><p style="margin:0">Nothing left to import. '
            'Your schedules already live here.</p></div>',
            flash=flash,
        )

    # --- schedules ---------------------------------------------------------
    schedule_rows = []
    for item in plan["schedules"]:
        key = html.escape(str(item["key"]), quote=True)
        method = "Auto" if item["payment_method"] == "auto" else "Manual"
        method_note = (
            '<span style="color:#1f7a34">Auto</span>' if item["payment_method"] == "auto"
            else '<span style="color:#b8860b">Manual</span>'
        )
        flags = []
        if item["amount_varies"]:
            flags.append("amount varies, using the middle value")
        if item["is_estimate"]:
            flags.append("estimate until HR provides real hours")
        flag_html = (
            '<br><small style="color:#6b7a8d">' + html.escape(" &middot; ".join(flags)) + "</small>"
            if flags else ""
        )
        schedule_rows.append(
            f'<tr><td><input type="checkbox" name="schedule_key" value="{key}" checked></td>'
            + "<td>" + html.escape(str(item["name"])) + flag_html + "</td>"
            + '<td style="text-align:right">' + _money(item["amount_cents"]) + "</td>"
            + "<td>" + html.escape(_cadence_label(item)) + "</td>"
            + "<td>" + method_note + "</td>"
            + "<td>" + html.escape(str(item["next_due"])) + "</td>"
            + f'<td><small>{item["occurrence_count"]} seen</small></td></tr>'
        )
    schedules_html = f"""
    <div class="card">
      <h2>1. Your recurring schedule ({plan['schedule_count']})</h2>
      <p style="font-size:13px;color:#6b7a8d;margin:0 0 10px">
        Found from your existing items. Roughly {_money(plan['monthly_estimate_cents'])} a month.
        Untick anything that is dead. You can edit amounts afterwards on the Recurring page.
      </p>
      <table class="finance-accounts-table">
        <thead><tr><th></th><th>Bill</th><th style="text-align:right">Amount</th>
          <th>How often</th><th>Paid</th><th>Next</th><th></th></tr></thead>
        <tbody>{''.join(schedule_rows) or '<tr><td colspan="7">No recurring series found.</td></tr>'}</tbody>
      </table>
    </div>"""

    # --- overdue, sorted against the bank feed -----------------------------
    overdue_rows = []
    for item in plan["overdue"]:
        item_id = html.escape(str(item["id"]), quote=True)
        if item["looks_paid"]:
            verdict = (
                '<span style="color:#1f7a34">Looks paid</span><br><small>'
                + html.escape(item["match_name"]) + " on " + html.escape(item["match_date"])
                + ("" if item["match_exact"] else " (amount differs)") + "</small>"
            )
            control = f'<input type="checkbox" name="archive_id" value="{item_id}" checked>'
            action = "archive"
        else:
            verdict = '<span style="color:#a12020">No payment found</span><br><small>may be genuinely unpaid</small>'
            control = f'<input type="checkbox" name="keep_id" value="{item_id}" checked>'
            action = "keep"
        overdue_rows.append(
            f'<tr><td>{control}</td>'
            + "<td>" + html.escape(str(item["name"])) + "</td>"
            + '<td style="text-align:right">' + _money(item["amount_cents"]) + "</td>"
            + f'<td>{item["days_overdue"]}d</td>'
            + f"<td>{verdict}</td>"
            + f'<td><small>{action}</small></td></tr>'
        )
    overdue_html = f"""
    <div class="card">
      <h2>2. Overdue items, checked against your bank ({len(plan['overdue'])})</h2>
      <p style="font-size:13px;color:#6b7a8d;margin:0 0 10px">
        {plan['archive_suggested']} look already paid and are ticked to archive.
        {plan['keep_suggested']} have no matching payment and are ticked to keep as real bills.
        Change any of them.
      </p>
      <table class="finance-accounts-table">
        <thead><tr><th></th><th>Bill</th><th style="text-align:right">Amount</th>
          <th>Late</th><th>Bank says</th><th></th></tr></thead>
        <tbody>{''.join(overdue_rows) or '<tr><td colspan="6">Nothing overdue to sort.</td></tr>'}</tbody>
      </table>
    </div>""" if plan["overdue"] else ""

    # --- undated -----------------------------------------------------------
    unscheduled_rows = []
    for item in plan["unscheduled"]:
        item_id = html.escape(str(item["id"]), quote=True)
        unscheduled_rows.append(
            f'<tr><td><input type="checkbox" name="keep_id" value="{item_id}"></td>'
            + "<td>" + html.escape(str(item["name"])) + "</td>"
            + '<td style="text-align:right">' + _money(item["amount_cents"]) + "</td></tr>"
        )
    unscheduled_html = f"""
    <div class="card">
      <h2>3. Items with no date ({plan['unscheduled_count']})</h2>
      <p style="font-size:13px;color:#6b7a8d;margin:0 0 10px">
        These cannot be a schedule without a date. Tick only the ones that are real money.
        Anything left unticked stays behind.
      </p>
      <table class="finance-accounts-table">
        <thead><tr><th></th><th>Item</th><th style="text-align:right">Amount</th></tr></thead>
        <tbody>{''.join(unscheduled_rows)}</tbody>
      </table>
    </div>""" if plan["unscheduled"] else ""

    body = f"""
    <h1>Rebuild your schedule</h1>
    <p class="page-sub">Read from what ClickUp already told us. Nothing is created or archived
       until you confirm at the bottom.</p>
    <form method="post" action="/admin/finances/import/apply"
          onsubmit="return confirm('Create the ticked schedules and archive the rest? This can be undone.');">
      {schedules_html}
      {overdue_html}
      {unscheduled_html}
      <div class="card">
        <p style="margin:0 0 10px;font-size:13px">
          Ticked schedules become native recurring bills and vendors. Items ticked to archive are
          filed away, not deleted, and the batch can be undone.
          <strong>ClickUp keeps running until you turn it off separately</strong>, so you can compare first.
        </p>
        <div class="action-row">
          <a class="btn btn-secondary" href="/admin/finances">Cancel</a>
          <button type="submit" class="btn btn-primary">Create my schedule</button>
        </div>
      </div>
    </form>"""
    return _page_shell("Rebuild schedule", "review", body, flash=flash)
