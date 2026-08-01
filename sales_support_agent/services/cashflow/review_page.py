"""Review page: clear the blocked-obligation backlog in batches.

Three surfaces: the grouped list with checkboxes, a preview of exactly what a
bulk action would change, and the confirmation result. Nothing is written until
the preview is confirmed with a reason.
"""

from __future__ import annotations

import html
from typing import Any

from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.overview import _money, _page_shell


def _action_options(selected: str = "write_off") -> str:
    from sales_support_agent.services.cashflow.bulk_resolve import ACTION_LABELS
    return "".join(
        '<option value="' + key + '"' + (" selected" if key == selected else "") + '>'
        + html.escape(label.title()) + '</option>'
        for key, label in ACTION_LABELS.items()
    )


def _render_due_followups() -> str:
    """Deferred items whose date has arrived. A deferral must come back."""
    from sales_support_agent.services.cashflow.bulk_resolve import list_due_followups

    try:
        data = list_due_followups()
    except Exception:
        return ""
    if not data["count"]:
        return ""

    rows = []
    for item in data["items"]:
        item_id = html.escape(str(item["id"]), quote=True)
        if item["nagging"]:
            nag = (f'<span style="color:#a12020"> deferred {item["defer_count"]}x '
                   "&mdash; time to decide</span>")
        elif item["defer_count"] > 1:
            nag = f'<span style="color:#6b7a8d"> deferred {item["defer_count"]}x</span>'
        else:
            nag = ""
        direction = "owed to you" if item["event_type"] == "inflow" else "you owe"
        rows.append(
            f'<tr><td><input type="checkbox" name="event_id" value="{item_id}"></td>'
            + "<td>" + html.escape(str(item["name"])) + nag + "</td>"
            + '<td style="text-align:right">' + _money(item["amount_cents"]) + "</td>"
            + "<td>" + html.escape(direction) + "</td>"
            + "<td>" + html.escape(str(item["came_back_on"])) + "</td></tr>"
        )

    nag_banner = ""
    if data["nagging_count"]:
        nag_banner = (
            '<div class="finance-plan-short">' + str(data["nagging_count"])
            + " item(s) have been pushed out three or more times. "
            + "That usually means the honest answer is write it off.</div>"
        )

    return f"""
    <div class="card" style="border-left:3px solid #d1a343">
      <h2>Back on your plate: {data['count']} item(s), {_money(data['amount_cents'])}</h2>
      <p style="font-size:13px;color:#6b7a8d;margin:0 0 10px">
        You deferred these and the date has arrived. Each one needs a decision now.
        Pushing it out again is allowed, but it is counted.
      </p>
      {nag_banner}
      <form method="post">
        <table class="finance-accounts-table">
          <thead><tr><th></th><th>Item</th><th style="text-align:right">Amount</th>
            <th>Direction</th><th>Came back</th></tr></thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
        <div class="finance-receivable-actions" style="margin-top:10px">
          <button type="submit" formaction="/admin/finances/review/preview" name="action"
                  value="uncollectible" class="btn btn-secondary">Write off (owed to you)</button>
          <button type="submit" formaction="/admin/finances/review/preview" name="action"
                  value="write_off" class="btn btn-secondary">Write off (you owe)</button>
          <label style="font-size:12px;color:#6b7a8d">Push out again to
            <input type="date" name="follow_up_on"></label>
          <button type="submit" formaction="/admin/finances/review/follow-up"
                  class="btn btn-secondary">Defer again</button>
        </div>
      </form>
    </div>"""


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
    """Render a short guided inbox; one case opens on its own page."""
    from sales_support_agent.services.cashflow.bulk_resolve import list_review_items

    try:
        data = list_review_items()
    except Exception:
        data = {"total": 0, "groups": []}
    cases: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for group in data.get("groups") or []:
        for item in group.get("items") or []:
            cases.append((group, item))
    visible = cases[:20]
    if visible:
        rows = "".join(
            f"""
            <li class="money-review-row">
              <a href="/admin/finances/review/{html.escape(str(item['id']), quote=True)}">
                <div><span>{html.escape(str(group['label']))}</span>
                <strong>{html.escape(str(item['name']))}</strong>
                <p>Due {html.escape(str(item['due_date']) or 'date unavailable')}</p></div>
                <div><strong>{_money(int(item['amount_cents']))}</strong>
                <span>{'Protected — review only' if item['protected'] else 'Open case'}</span></div>
              </a>
            </li>"""
            for group, item in visible
        )
        workspace = f"""
        <section class="money-review-workspace" aria-labelledby="review-list-title">
          <div class="money-section-heading"><div><p class="finance-eyebrow">Guided inbox</p>
          <h2 id="review-list-title">Start with the first item</h2></div>
          <span>Showing {len(visible)} of {int(data['total'])}</span></div>
          <ol class="money-review-list">{rows}</ol>
        </section>"""
    else:
        workspace = """
        <div class="money-empty"><h2>Nothing needs your decision</h2>
        <p>Finance has no unresolved case in the daily review inbox.</p>
        <a class="btn btn-secondary" href="/admin/finances">Back to money brief</a></div>"""
    body = f"""
    <div class="money-brief">
      {render_finance_nav("review", counts={})}
      <header class="money-page-header"><div><p class="finance-eyebrow">Review</p>
      <h1>Answer one money question at a time</h1>
      <p class="money-page-subtitle">Open a case, read the evidence, check what your answer changes, then confirm it. Nothing saves from this list.</p>
      </div><div class="money-page-status"><span class="money-status money-status--review">{int(data['total'])} open</span></div></header>
      {workspace}
      <div class="money-state-note"><strong>Historical cleanup is separate</strong>
      <p>Old bookkeeping and reconciliation work no longer competes with decisions that affect cash now.</p></div>
    </div>"""
    return _page_shell("Review", "review", body, flash=flash)


def render_review_case(event_id: str, *, flash: str = "") -> str:
    """Render one unresolved item as a complete, normal page."""
    from sales_support_agent.services.cashflow.bulk_resolve import list_review_items
    from sales_support_agent.services.cashflow.obligations import get_obligation

    item = get_obligation(event_id)
    if not item:
        return _page_shell(
            "Review item",
            "review",
            f"{render_finance_nav('review', counts={})}<div class='money-empty'><h1>This item is not available</h1>"
            "<p>It may already have been resolved.</p><a class='btn btn-secondary' href='/admin/finances/review'>Back to Review</a></div>",
            flash=flash,
        )
    reason = "This item needs a decision before Finance can rely on it."
    protected = False
    try:
        for group in list_review_items().get("groups") or []:
            for candidate in group.get("items") or []:
                if str(candidate.get("id")) == str(event_id):
                    reason = str(group.get("label") or reason)
                    protected = bool(candidate.get("protected"))
    except Exception:
        pass
    direction = str(item.get("event_type") or "outflow")
    write_action = "uncollectible" if direction == "inflow" else "no_action_needed"
    remove_label = "Write off this receivable" if direction == "inflow" else "Mark as not owed"
    protected_note = (
        "<p class='money-protected-note'>Payroll, tax, and debt cannot be removed here. Confirm settlement evidence or leave the item open.</p>"
        if protected else ""
    )
    remove_form = "" if protected else f"""
      <form method="post" action="/admin/finances/review/preview">
        <input type="hidden" name="event_id" value="{html.escape(str(event_id), quote=True)}">
        <input type="hidden" name="action" value="{write_action}">
        <button class="btn btn-secondary" type="submit">{remove_label}</button>
      </form>"""
    body = f"""
    <div class="money-brief">
      {render_finance_nav("review", counts={})}
      <a class="money-back-link" href="/admin/finances/review">&larr; Back to Review</a>
      <header class="money-page-header"><div><p class="finance-eyebrow">Money question</p>
      <h1>Does this still affect your cash?</h1><p class="money-page-subtitle">{html.escape(reason)}</p></div></header>
      <section class="money-review-case">
        <div class="money-review-question"><span>{html.escape(str(item.get('commitment_type') or direction).replace('_', ' ').title())}</span>
        <h2>{html.escape(str(item.get('name') or item.get('vendor_or_customer') or 'Financial item'))}</h2>
        <strong>{_money(int(item.get('amount_cents') or 0))}</strong>
        <p>Due {html.escape(str(item.get('due_date') or 'date unavailable')[:10])}</p></div>
        <dl class="money-review-evidence">
          <div><dt>Source</dt><dd>{html.escape(str(item.get('source') or 'Anata'))}</dd></div>
          <div><dt>Status</dt><dd>{html.escape(str(item.get('status') or 'Open').title())}</dd></div>
          <div><dt>Why it needs review</dt><dd>{html.escape(reason)}</dd></div>
        </dl>
        {protected_note}
        <div class="money-review-actions">
          <a class="btn btn-primary" href="/admin/finances/review">Back without changing</a>
          {remove_form}
        </div>
      </section>
    </div>"""
    return _page_shell("Review item", "review", body, flash=flash)


def render_review_receipt(batch: Mapping[str, Any]) -> str:
    batch_id = html.escape(str(batch.get("id") or ""), quote=True)
    body = f"""
    <div class="money-brief">
      {render_finance_nav("review", counts={})}
      <section class="money-receipt" role="status">
        <span class="money-receipt-mark" aria-hidden="true">&#10003;</span>
        <p class="finance-eyebrow">Saved confirmation</p>
        <h1>Your review answer was saved</h1>
        <p>{int(batch.get('item_count') or 0)} item(s) changed. The bank and QuickBooks were not edited.</p>
        <dl><div><dt>Amount affected</dt><dd>{_money(int(batch.get('amount_cents') or 0))}</dd></div>
        <div><dt>Reason</dt><dd>{html.escape(str(batch.get('reason') or 'Recorded review decision'))}</dd></div></dl>
        <div class="money-review-actions">
          <a class="btn btn-primary" href="/admin/finances/review">Review the next item</a>
          <form method="post" action="/admin/finances/review/undo/{batch_id}">
            <button class="btn btn-secondary" type="submit">Undo this change</button>
          </form>
        </div>
      </section>
    </div>"""
    return _page_shell("Review saved", "review", body)


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
        <div class="money-brief">
          {render_finance_nav("review", counts={})}
          <div class="money-empty"><h1>Nothing can change</h1>
          <p>That answer is not allowed for this item, so Finance left everything exactly as it was.</p>
          {skipped_html}
          <a class="btn btn-secondary" href="/admin/finances/review">Back to Review</a></div>
        </div>"""
        return _page_shell("Confirm review action", "review", body)

    body = f"""
    <div class="money-brief">
    {render_finance_nav("review", counts={})}
    <a class="money-back-link" href="/admin/finances/review">&larr; Back without changing</a>
    <header class="money-page-header"><div><p class="finance-eyebrow">Confirmation</p>
    <h1>Check this answer before saving</h1>
    <p class="money-page-subtitle">Nothing has changed yet. Review the exact result, add your reason, then confirm.</p>
    </div></header>
    <section class="money-review-case">
      <div class="money-review-question"><span>Proposed answer</span>
      <h2>{html.escape(str(preview['action_label']))}</h2>
      <strong>{_money(preview['amount_cents'])}</strong>
      <p>{preview['eligible_count']} item(s) affected</p></div>
      <div class="money-preview-copy">
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
      <div class="money-confirm-form">
        <div class="form-row">
          <div>
            <label for="review-reason">Why are you making this change?</label>
            <input id="review-reason" name="reason" required placeholder="For example: vendor confirmed this is no longer owed">
          </div>
        </div>
        <div class="money-review-actions">
          <a class="btn btn-secondary" href="/admin/finances/review">Cancel — change nothing</a>
          <button type="submit" class="btn btn-primary">
            Confirm {html.escape(str(preview['action_label']).lower())}
          </button>
        </div>
      </div>
    </form>"""
    body += "</section></div>"
    return _page_shell("Confirm review action", "review", body)
