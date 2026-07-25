"""Review page: clear the blocked-obligation backlog in batches.

Three surfaces: the grouped list with checkboxes, a preview of exactly what a
bulk action would change, and the confirmation result. Nothing is written until
the preview is confirmed with a reason.
"""

from __future__ import annotations

import html
from typing import Any

from sales_support_agent.services.cashflow.overview import _money, _page_shell


def _action_options(selected: str = "write_off") -> str:
    from sales_support_agent.services.cashflow.bulk_resolve import ACTION_LABELS
    return "".join(
        '<option value="' + key + '"' + (" selected" if key == selected else "") + '>'
        + html.escape(label.title()) + '</option>'
        for key, label in ACTION_LABELS.items()
    )


def _render_historical_cleanup() -> str:
    """Start-fresh cleanup: archive everything older than the chosen cutoff."""
    from sales_support_agent.services.cashflow.bulk_resolve import list_historical_backlog

    try:
        out = list_historical_backlog(event_type="outflow")
        inc = list_historical_backlog(event_type="inflow")
    except Exception:
        return ""
    if not out["actionable_count"] and not inc["actionable_count"]:
        return ""

    def _row(label: str, data: dict, event_type: str) -> str:
        if not data["actionable_count"]:
            return ""
        protected_note = (
            f" &middot; {data['protected_count']} protected item(s) will be skipped"
            if data["protected_count"] else ""
        )
        return f"""
        <form method="post" action="/admin/finances/review/cleanup-preview" class="finance-cleanup-row">
          <input type="hidden" name="event_type" value="{event_type}">
          <div>
            <strong>{label}</strong>
            <p style="margin:2px 0 0;font-size:13px;color:#6b7a8d">
              {data['actionable_count']} item(s) worth {_money(data['amount_cents'])},
              dated before {html.escape(data['cutoff_date'])}{protected_note}
            </p>
          </div>
          <div style="display:flex;gap:8px;align-items:center">
            <label style="font-size:12px;color:#6b7a8d">Older than
              <select name="older_than_days">
                <option value="90" selected>90 days</option>
                <option value="60">60 days</option>
                <option value="180">180 days</option>
                <option value="365">1 year</option>
              </select>
            </label>
            <button type="submit" class="btn btn-secondary">Preview cleanup</button>
          </div>
        </form>"""

    return f"""
    <div class="card" style="background:rgba(43,54,68,0.02)">
      <h2>Start fresh: archive old items</h2>
      <p style="font-size:13px;color:#6b7a8d;margin:0 0 10px">
        These are past their date with no linked payment. Archiving does not claim they were paid,
        it says they are no longer an actionable forecast. Reversible, and you see the full list first.
      </p>
      {_row("Old bills (money out)", out, "outflow")}
      {_row("Old receivables (money in)", inc, "inflow")}
    </div>"""


def _render_old_receivables() -> str:
    """Old money owed to us, with the four ways to resolve it."""
    from sales_support_agent.services.cashflow.bulk_resolve import list_historical_backlog

    try:
        data = list_historical_backlog(event_type="inflow", older_than_days=60)
    except Exception:
        return ""
    if not data["items"]:
        return ""

    rows = []
    for item in data["items"][:40]:
        item_id = html.escape(str(item["id"]), quote=True)
        rows.append(
            f'<tr><td><input type="checkbox" name="event_id" value="{item_id}"></td>'
            + "<td>" + html.escape(str(item["name"])) + "</td>"
            + '<td style="text-align:right">' + _money(item["amount_cents"]) + "</td>"
            + "<td>" + html.escape(str(item["due_date"])) + "</td></tr>"
        )

    return f"""
    <h2 style="margin-top:22px">Old money owed to you</h2>
    <p class="page-sub">{data['actionable_count']} item(s) worth {_money(data['amount_cents'])}
       are more than 60 days past due. Tick some, then choose what to do.</p>
    <form method="post" id="receivable-form">
      <div class="card">
        <table class="finance-accounts-table">
          <thead><tr><th></th><th>Customer / invoice</th>
            <th style="text-align:right">Amount</th><th>Due</th></tr></thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
      </div>
      <div class="card">
        <p style="margin:0 0 10px;font-size:13px"><strong>What do you want to do with the ticked items?</strong></p>
        <div class="finance-receivable-actions">
          <button type="submit" formaction="/admin/finances/review/preview" name="action" value="uncollectible"
                  class="btn btn-secondary">Write off as uncollectible</button>
          <button type="submit" formaction="/admin/finances/review/preview" name="action" value="invoiced_in_error"
                  class="btn btn-secondary">Cancel, invoiced in error</button>
        </div>
        <div class="finance-receivable-actions" style="margin-top:10px">
          <label style="font-size:12px;color:#6b7a8d">Snooze until
            <input type="date" name="until"></label>
          <button type="submit" formaction="/admin/finances/review/snooze" class="btn btn-secondary">Snooze</button>
          <label style="font-size:12px;color:#6b7a8d">Keep chasing, follow up on
            <input type="date" name="follow_up_on"></label>
          <button type="submit" formaction="/admin/finances/review/follow-up" class="btn btn-secondary">Keep chasing</button>
        </div>
        <p style="font-size:12px;color:#6b7a8d;margin:10px 0 0">
          Write off and cancel both show you a preview and ask for a reason first. Reminder emails
          for the ones you keep chasing live in the collections panel on the finance page.
        </p>
      </div>
    </form>"""


def _render_overdue_matcher() -> str:
    """Overdue bills with the bank payments that may already have settled them."""
    from sales_support_agent.services.cashflow.payment_finder import (
        find_overdue_needing_payment,
        find_payment_candidates,
    )

    try:
        overdue = find_overdue_needing_payment(limit=25)
    except Exception:
        return ""
    if not overdue:
        return ""

    blocks = []
    for bill in overdue:
        bill_id = html.escape(str(bill["id"]), quote=True)
        try:
            found = find_payment_candidates(bill["id"])
            candidates = found["candidates"]
        except Exception:
            candidates = []

        if candidates:
            rows = []
            for candidate in candidates:
                pair = html.escape(candidate["transaction_id"] + "|" + bill["id"], quote=True)
                gap = candidate["amount_gap_cents"]
                closeness = "exact amount" if candidate["exact_amount"] else "off by " + _money(gap)
                day_gap = candidate["day_gap"]
                timing = (str(day_gap) + " day(s) from the due date") if day_gap is not None else "date unknown"
                rows.append(
                    "<tr><td>" + html.escape(candidate["name"])
                    + "<br><small>" + html.escape(candidate["source"]) + "</small></td>"
                    + '<td style="text-align:right">' + _money(candidate["amount_cents"]) + "</td>"
                    + "<td>" + html.escape(candidate["paid_on"]) + "</td>"
                    + "<td><small>" + closeness + " &middot; " + timing + "</small></td>"
                    + '<td><form method="post" action="/admin/finances/matches/confirm">'
                    + '<input type="hidden" name="pair" value="' + pair + '">'
                    + '<button type="submit" class="btn btn-secondary btn-sm">This paid it</button>'
                    + "</form></td></tr>"
                )
            candidate_html = (
                '<table class="finance-accounts-table" style="margin-top:8px">'
                + "<thead><tr><th>Bank payment</th><th style=\"text-align:right\">Amount</th>"
                + "<th>Paid</th><th>How close</th><th></th></tr></thead>"
                + "<tbody>" + "".join(rows) + "</tbody></table>"
            )
        else:
            candidate_html = (
                '<p style="font-size:13px;color:#6b7a8d;margin:8px 0 0">'
                + "No bank payment near this amount was found, so this one looks genuinely unpaid "
                + "(or was paid from an account that is not connected).</p>"
            )

        blocks.append(f"""
        <details class="card">
          <summary><strong>{html.escape(str(bill['name']))}</strong>
            &nbsp;{_money(bill['amount_cents'])} &middot; due {html.escape(str(bill['due_date']))}
            &middot; {bill['days_overdue']} days overdue
            {'&middot; ' + str(len(candidates)) + ' possible payment(s)' if candidates else '&middot; no match found'}
          </summary>
          {candidate_html}
        </details>""")

    return f"""
    <h2 style="margin-top:22px">Overdue bills: find the payment</h2>
    <p class="page-sub">These have no linked payment yet, which is what inflates "required out".
       If one was already paid, link it here instead of writing it off. Vendor names are ignored
       on purpose so check payments can still be found.</p>
    {''.join(blocks)}"""


def render_review_page(*, flash: str = "") -> str:
    from sales_support_agent.services.cashflow.bulk_resolve import latest_batch, list_review_items

    try:
        data = list_review_items()
    except Exception:
        data = {"total": 0, "groups": []}
    try:
        batch = latest_batch()
    except Exception:
        batch = None

    undo_html = ""
    if batch:
        batch_id = html.escape(str(batch["id"]), quote=True)
        undo_html = f"""
        <div class="card" style="background:rgba(43,54,68,0.02)">
          <form method="post" action="/admin/finances/review/undo/{batch_id}"
                onsubmit="return confirm('Undo that batch and put those items back?');">
            <p style="margin:0 0 8px">Last batch: {int(batch.get('item_count') or 0)} item(s)
               ({_money(int(batch.get('amount_cents') or 0))}) marked
               &quot;{html.escape(str(batch.get('reason') or ''))}&quot;.</p>
            <button type="submit" class="btn btn-secondary">Undo that batch</button>
          </form>
        </div>"""

    if not data["total"]:
        body = f"""
        <h1>Needs review</h1>
        <p class="page-sub">Obligations that are pausing cash decisions</p>
        <div class="card"><p style="margin:0">Nothing needs review. Cash decisions are unblocked.</p></div>
        {undo_html}
        {_render_historical_cleanup()}
        {_render_overdue_matcher()}
        {_render_old_receivables()}"""
        return _page_shell("Needs review", "review", body, flash=flash)

    groups_html = []
    for group in data["groups"]:
        rows = []
        for item in group["items"]:
            item_id = html.escape(str(item["id"]), quote=True)
            if item["protected"]:
                control = '<span title="Protected: payroll, tax, or debt">&#128274;</span>'
            else:
                control = f'<input type="checkbox" name="event_id" value="{item_id}">'
            rows.append(
                "<tr><td>" + control + "</td>"
                + "<td>" + html.escape(str(item["name"])) + "</td>"
                + '<td style="text-align:right">' + _money(item["amount_cents"]) + "</td>"
                + "<td>" + html.escape(str(item["due_date"])) + "</td></tr>"
            )
        groups_html.append(f"""
        <details class="card" open>
          <summary><strong>{html.escape(group['label'])}</strong>
            &nbsp;{group['count']} item(s) &middot; {_money(group['amount_cents'])}
            {'&middot; ' + str(group['count'] - group['actionable_count']) + ' protected' if group['count'] != group['actionable_count'] else ''}
          </summary>
          <table class="finance-accounts-table" style="margin-top:10px">
            <thead><tr><th></th><th>Obligation</th><th style="text-align:right">Amount</th><th>Due</th></tr></thead>
            <tbody>{''.join(rows)}</tbody>
          </table>
        </details>""")

    body = f"""
    <h1>Needs review</h1>
    <p class="page-sub">{data['total']} obligation(s) are pausing cash decisions. Tick items, choose an action, then preview.</p>
    <form method="post" action="/admin/finances/review/preview">
      {''.join(groups_html)}
      <div class="card">
        <div class="form-row">
          <div>
            <label>Action for the ticked items</label>
            <select name="action">{_action_options()}</select>
          </div>
        </div>
        <p style="font-size:12px;color:#6b7a8d;margin:6px 0 10px">
          Locked items (payroll, tax, debt) are never included in a bulk action. Nothing is deleted
          and every batch can be undone.
        </p>
        <div class="action-row">
          <button type="submit" class="btn btn-primary">Preview the change</button>
        </div>
      </div>
    </form>
    {undo_html}
    {_render_historical_cleanup()}
    {_render_overdue_matcher()}
    {_render_old_receivables()}"""
    return _page_shell("Needs review", "review", body, flash=flash)


def render_review_preview(preview: dict[str, Any]) -> str:
    action = html.escape(str(preview["action"]), quote=True)
    hidden = "".join(
        '<input type="hidden" name="event_id" value="' + html.escape(str(item["id"]), quote=True) + '">'
        for item in preview["eligible"]
    )
    listed = "".join(
        "<tr><td>" + html.escape(str(item["name"])) + "</td>"
        + '<td style="text-align:right">' + _money(item["amount_cents"]) + "</td></tr>"
        for item in preview["eligible"]
    )
    skipped_html = ""
    if preview["skipped_count"]:
        skipped_rows = "".join(
            "<li>" + html.escape(str(item["name"])) + " - " + html.escape(str(item.get("why_skipped") or "")) + "</li>"
            for item in preview["skipped_protected"]
        )
        skipped_html = (
            '<div class="card" style="background:rgba(43,54,68,0.02)">'
            + "<h2>" + str(preview["skipped_count"]) + " item(s) will be skipped</h2>"
            + '<ul style="margin:0;font-size:13px">' + skipped_rows + "</ul></div>"
        )

    if not preview["eligible_count"]:
        body = f"""
        <h1>Nothing to change</h1>
        <p class="page-sub">None of the ticked items can take this action.</p>
        {skipped_html}
        <div class="action-row"><a class="btn btn-secondary" href="/admin/finances/review">Back to review</a></div>"""
        return _page_shell("Confirm review action", "review", body)

    body = f"""
    <h1>Confirm: {html.escape(str(preview['action_label']))}</h1>
    <p class="page-sub">Read what changes, give a reason, then confirm. Nothing has changed yet.</p>
    <div class="card">
      <h2>What will change</h2>
      {'<p style="font-size:13px;color:#6b7a8d;margin:0 0 8px">' + html.escape(str(preview.get('cutoff_note'))) + '</p>' if preview.get('cutoff_note') else ''}
      <ul style="font-size:14px">
        <li><strong>{preview['eligible_count']}</strong> obligation(s) will be marked
            &quot;{html.escape(str(preview['action_label']))}&quot; and leave the review queue.</li>
        <li>Total removed from expected spend: <strong>{_money(preview['amount_cents'])}</strong></li>
        <li>Nothing is deleted. Every item stays searchable and this batch can be undone.</li>
      </ul>
      <table class="finance-accounts-table">
        <thead><tr><th>Obligation</th><th style="text-align:right">Amount</th></tr></thead>
        <tbody>{listed}</tbody>
      </table>
    </div>
    {skipped_html}
    <form method="post" action="/admin/finances/review/apply">
      <input type="hidden" name="action" value="{action}">
      {hidden}
      <div class="card">
        <div class="form-row">
          <div>
            <label>Reason (required)</label>
            <input name="reason" required placeholder="e.g. vendor closed, uncollectible">
          </div>
        </div>
        <div class="action-row">
          <a class="btn btn-secondary" href="/admin/finances/review">Cancel</a>
          <button type="submit" class="btn btn-primary">
            Yes, apply to {preview['eligible_count']} item(s)
          </button>
        </div>
      </div>
    </form>"""
    return _page_shell("Confirm review action", "review", body)
