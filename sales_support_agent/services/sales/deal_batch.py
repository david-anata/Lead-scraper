"""Batch cleanup: aggregate mid-confidence actions across all open deals.

Used by GET /admin/sales/deals/cleanup (preview) and
POST /admin/sales/deals/cleanup (bulk apply).
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import (
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
    MailboxSignal,
)
from sales_support_agent.services.sales.actions import (
    ContactInfo,
    SalesAction,
    compute_pending_actions,
)

_STAGE_LABELS: dict[str, str] = {
    "appointmentscheduled": "Appointment",
    "qualifiedtobuy": "Qualified",
    "presentationscheduled": "Presentation",
    "decisionmakerboughtin": "Decision Maker",
    "contractsent": "Contract Sent",
    "closedwon": "Closed Won",
    "closedlost": "Closed Lost",
}


def _esc(s: str) -> str:
    return html.escape(str(s))


@dataclass
class BatchCleanupRow:
    deal_id: str
    deal_name: str
    deal_stage: str
    action: SalesAction


def build_batch_cleanup(session: Session, *, portal_id: str = "") -> list[BatchCleanupRow]:
    """Aggregate all approvable mid-confidence actions across every open deal."""
    open_deals = list(session.scalars(
        select(HubSpotDeal)
        .where(HubSpotDeal.is_closed.is_(False))
        .order_by(HubSpotDeal.close_date.asc().nulls_last())
    ).all())

    rows: list[BatchCleanupRow] = []
    for deal in open_deals:
        signals = list(session.scalars(
            select(MailboxSignal).where(MailboxSignal.matched_deal_id == deal.hubspot_deal_id)
        ).all())
        li_total = session.execute(
            select(func.sum(HubSpotLineItem.amount_cents))
            .where(HubSpotLineItem.hubspot_deal_id == deal.hubspot_deal_id)
        ).scalar() or 0
        contact_link_ids = [r.hubspot_contact_id for r in session.scalars(
            select(HubSpotDealContact).where(
                HubSpotDealContact.hubspot_deal_id == deal.hubspot_deal_id
            )
        ).all()]
        contacts: list[ContactInfo] = []
        for cid in contact_link_ids:
            c = session.get(HubSpotContact, cid)
            if c:
                contacts.append(ContactInfo(contact_id=cid, email=c.email or ""))

        actions = compute_pending_actions(
            deal,
            signals,
            line_item_total_cents=int(li_total),
            contacts=contacts,
            portal_id=portal_id,
        )
        for a in actions:
            if a.confidence == "mid" and a.action_type != "note" and a.properties:
                rows.append(BatchCleanupRow(
                    deal_id=deal.hubspot_deal_id,
                    deal_name=deal.deal_name or deal.hubspot_deal_id,
                    deal_stage=deal.deal_stage or "",
                    action=a,
                ))

    return rows


def render_batch_cleanup_page(
    rows: list[BatchCleanupRow],
    *,
    user: dict | None = None,
    applied: int = 0,
    failed: int = 0,
    error: str = "",
) -> str:
    from sales_support_agent.services.admin_nav import (
        render_agent_favicon_links,
        render_agent_nav,
        render_agent_nav_styles,
    )

    nav_styles = render_agent_nav_styles()
    nav = render_agent_nav("sales", sales_section="sales_deals", user=user)
    favicons = render_agent_favicon_links()

    flash_html = ""
    if applied or failed:
        if failed:
            flash_html = (
                f'<div class="flash flash--warn">'
                f"{applied} action(s) applied, {failed} failed."
                f"</div>"
            )
        else:
            flash_html = (
                f'<div class="flash">'
                f"{applied} action(s) pushed to HubSpot. Board will refresh on next sync."
                f"</div>"
            )
    elif error:
        flash_html = f'<div class="flash flash--warn">{_esc(error)}</div>'

    if rows:
        rows_html = ""
        prev_deal_id = ""
        for row in rows:
            if row.deal_id != prev_deal_id:
                stage_label = _STAGE_LABELS.get(row.deal_stage, row.deal_stage)
                rows_html += (
                    f'<tr class="deal-hdr">'
                    f'<td colspan="3">'
                    f'<a href="/admin/sales/deals/{_esc(row.deal_id)}">{_esc(row.deal_name)}</a>'
                    f' <span class="stage-badge">{_esc(stage_label)}</span>'
                    f"</td></tr>"
                )
                prev_deal_id = row.deal_id

            action_id_esc = _esc(row.action.action_id)
            rows_html += (
                f'<tr class="action-row">'
                f'<td class="chk-cell">'
                f'<input type="checkbox" name="action_ids" value="{action_id_esc}"'
                f' id="cb-{action_id_esc}" class="action-cb">'
                f"</td>"
                f'<td><label for="cb-{action_id_esc}" class="action-label">'
                f'{_esc(row.action.label)}</label>'
                f'<div class="action-desc">{_esc(row.action.description)}</div></td>'
                f"<td>"
            )
            # Show what property values will be sent
            prop_pills = "".join(
                f'<span class="prop-pill">{_esc(k)} = {_esc(v)}</span>'
                for k, v in row.action.properties.items()
            )
            rows_html += f"{prop_pills}</td></tr>"

        n = len(rows)
        table_html = f"""
<form method="post" action="/admin/sales/deals/cleanup" id="cleanup-form">
  <div class="toolbar">
    <button type="submit" class="btn btn--apply" id="apply-btn" disabled>
      Apply selected (<span id="sel-count">0</span>)
    </button>
    <button type="button" class="btn btn--ghost" id="sel-all-btn">Select all</button>
    <button type="button" class="btn btn--ghost" id="sel-none-btn" style="display:none">Deselect all</button>
    <span class="muted">{n} action{"s" if n != 1 else ""} pending across your open deals</span>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th class="chk-th"></th>
        <th>Action</th>
        <th>HubSpot properties</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
  <div class="toolbar" style="margin-top:12px">
    <button type="submit" class="btn btn--apply" disabled
      data-mirror="apply-btn">Apply selected (<span class="sel-count-mirror">0</span>)</button>
  </div>
</form>"""
    else:
        table_html = (
            '<p class="empty">No mid-confidence actions pending. '
            'All deals look good, or run a HubSpot sync to refresh.</p>'
        )

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>agent | HubSpot Cleanup</title>
    {favicons}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      {nav_styles}
      :root {{
        --dark-blue:#2B3644;--light-brown:#F9F7F3;--border:rgba(43,54,68,.12);
        --shadow:rgba(43,54,68,.10);--white:#FFF;--accent:#3B7DD8;
        --green:#2f8f5b;--warn:#b84c00;
      }}
      *{{box-sizing:border-box;}}
      body{{margin:0;background:var(--light-brown);color:var(--dark-blue);
            font-family:"Inter","Segoe UI",sans-serif;}}
      a{{color:var(--accent);text-decoration:none;}}
      a:hover{{text-decoration:underline;}}
      .shell{{max-width:1100px;margin:0 auto;padding:40px 18px;}}
      .workspace{{background:var(--white);border:1px solid var(--border);
                  border-radius:20px;box-shadow:0 18px 40px var(--shadow);
                  padding:32px 28px;}}
      h1{{font-family:"Montserrat",sans-serif;font-weight:800;font-size:26px;
          margin:0 0 6px;}}
      .eyebrow{{font-size:11px;font-weight:600;letter-spacing:.08em;
                text-transform:uppercase;color:rgba(43,54,68,.5);margin:0 0 4px;}}
      .intro{{color:rgba(43,54,68,.7);font-size:14px;margin:0 0 24px;}}
      .flash{{background:#e8f5ee;border:1px solid #b0d8c0;border-radius:10px;
              padding:10px 14px;margin:0 0 18px;font-size:14px;color:#1a5c38;}}
      .flash--warn{{background:#fff3e0;border-color:#f5c47b;color:var(--warn);}}
      .toolbar{{display:flex;align-items:center;gap:12px;margin:0 0 14px;flex-wrap:wrap;}}
      .btn{{padding:7px 16px;border-radius:8px;border:1px solid transparent;
            font-size:13px;font-weight:600;cursor:pointer;font-family:inherit;}}
      .btn--apply{{background:var(--green);color:#fff;border-color:var(--green);}}
      .btn--apply:disabled{{background:rgba(43,54,68,.18);color:rgba(43,54,68,.4);
                            border-color:transparent;cursor:not-allowed;}}
      .btn--ghost{{background:transparent;border:1px solid var(--border);
                   color:var(--dark-blue);}}
      .btn--ghost:hover{{background:rgba(43,54,68,.05);}}
      .muted{{font-size:13px;color:rgba(43,54,68,.5);}}
      .table-wrap{{overflow-x:auto;}}
      table{{width:100%;border-collapse:collapse;font-size:13px;}}
      th{{text-align:left;padding:7px 12px;border-bottom:2px solid var(--border);
          font-size:11px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;
          color:rgba(43,54,68,.5);}}
      td{{padding:8px 12px;border-bottom:1px solid rgba(43,54,68,.07);
          vertical-align:middle;}}
      .deal-hdr td{{background:rgba(43,54,68,.03);font-weight:600;font-size:13px;
                    padding:10px 12px;}}
      .action-row:hover td{{background:rgba(59,125,216,.04);}}
      .chk-cell{{width:36px;}}
      .chk-th{{width:36px;}}
      input[type=checkbox]{{width:15px;height:15px;cursor:pointer;accent-color:var(--green);}}
      .action-label{{font-weight:600;cursor:pointer;}}
      .action-desc{{font-size:12px;color:rgba(43,54,68,.6);margin-top:2px;}}
      .stage-badge{{font-size:11px;font-weight:500;color:rgba(43,54,68,.5);
                    border:1px solid var(--border);border-radius:6px;
                    padding:2px 7px;margin-left:6px;}}
      .prop-pill{{display:inline-block;font-size:11px;background:rgba(43,54,68,.06);
                  border-radius:6px;padding:2px 8px;margin:2px;
                  font-family:monospace;white-space:nowrap;}}
      .empty{{color:rgba(43,54,68,.5);font-size:14px;padding:24px 0;}}
      .back-link{{display:inline-block;margin-bottom:20px;font-size:13px;
                  color:rgba(43,54,68,.6);}}
      .back-link:hover{{color:var(--dark-blue);}}
    </style>
  </head>
  <body>
    {nav}
    <main class="shell">
      <div class="workspace">
        <a href="/admin/sales/deals" class="back-link">← Deal Board</a>
        <p class="eyebrow">Sales — HubSpot</p>
        <h1>Cleanup <span style="color:#3B7DD8">Queue</span>.</h1>
        <p class="intro">All mid-confidence actions across every open deal, ready to push to HubSpot in one click.
          Low-confidence nudges (missing contacts, no company) are excluded — fix those in HubSpot directly.</p>
        {flash_html}
        {table_html}
      </div>
    </main>
    <script>
    (function() {{
      var cbs = document.querySelectorAll('.action-cb');
      var applyBtns = document.querySelectorAll('[data-mirror="apply-btn"], #apply-btn');
      var countSpans = document.querySelectorAll('#sel-count, .sel-count-mirror');
      var selAllBtn = document.getElementById('sel-all-btn');
      var selNoneBtn = document.getElementById('sel-none-btn');

      function updateCount() {{
        var n = document.querySelectorAll('.action-cb:checked').length;
        countSpans.forEach(function(s){{ s.textContent = n; }});
        applyBtns.forEach(function(b){{ b.disabled = n === 0; }});
        if (selAllBtn) selAllBtn.style.display = n === cbs.length ? 'none' : '';
        if (selNoneBtn) selNoneBtn.style.display = n === 0 ? 'none' : '';
      }}

      cbs.forEach(function(cb){{ cb.addEventListener('change', updateCount); }});

      if (selAllBtn) selAllBtn.addEventListener('click', function() {{
        cbs.forEach(function(cb){{ cb.checked = true; }});
        updateCount();
      }});
      if (selNoneBtn) selNoneBtn.addEventListener('click', function() {{
        cbs.forEach(function(cb){{ cb.checked = false; }});
        updateCount();
      }});

      updateCount();
    }})();
    </script>
  </body>
</html>"""
