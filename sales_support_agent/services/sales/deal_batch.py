"""Batch cleanup: aggregate mid-confidence actions across all open deals.

GET  /admin/sales/deals/cleanup  → preview checklist
POST /admin/sales/deals/cleanup  → bulk apply selected actions
"""
from __future__ import annotations

import html
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sales_support_agent.models.database import kv_get_json, kv_set_json
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

logger = logging.getLogger(__name__)

# Suppress duplicate Sales Director notes within this window.
NOTE_COOLDOWN_DAYS = 7


def note_applied_key(deal_id: str) -> str:
    return f"sales:note_applied:{deal_id}"


def record_note_applied(deal_id: str) -> None:
    """Record that a Sales Director note was just written for this deal."""
    kv_set_json(note_applied_key(deal_id), {
        "applied_at": datetime.now(timezone.utc).isoformat()
    })


def _last_note_days_ago(deal_id: str) -> Optional[int]:
    """Return how many days ago a note was applied, or None if no record."""
    record = kv_get_json(note_applied_key(deal_id))
    if not record or "applied_at" not in record:
        return None
    try:
        applied = datetime.fromisoformat(record["applied_at"])
        if not applied.tzinfo:
            applied = applied.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - applied).days
    except Exception:
        return None


def _esc(s: str) -> str:
    return html.escape(str(s) if s is not None else "")


def _fmt_money(cents: int) -> str:
    if cents <= 0:
        return "$0"
    if cents >= 100_000_00:  # $100k+
        return f"${cents / 100_000:.1f}k"
    return f"${cents // 100:,}"


def _days_ago_str(dt: Optional[datetime]) -> str:
    if dt is None:
        return "Never"
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    days = (datetime.now(timezone.utc) - dt).days
    if days == 0:
        return "Today"
    if days == 1:
        return "Yesterday"
    return f"{days}d ago"


@dataclass
class BatchCleanupRow:
    deal_id: str
    deal_name: str
    deal_stage_label: str
    amount_cents: int
    owner_email: str
    last_touch_at: Optional[datetime]
    contact_count: int
    actions: list[SalesAction] = field(default_factory=list)
    # Set to the number of days since a note was last applied when note actions
    # are suppressed by the cooldown window.
    last_note_days_ago: Optional[int] = None


def build_batch_cleanup(session: Session, *, portal_id: str = "") -> list[BatchCleanupRow]:
    """Aggregate all mid-confidence actions across every open deal."""
    open_deals = list(session.scalars(
        select(HubSpotDeal)
        .where(HubSpotDeal.is_closed.is_(False))
        .order_by(HubSpotDeal.close_date.asc().nulls_last())
    ).all())

    rows: list[BatchCleanupRow] = []
    for deal in open_deals:
        try:
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

            all_actions = compute_pending_actions(
                deal, signals,
                line_item_total_cents=int(li_total),
                contacts=contacts,
                portal_id=portal_id,
            )
        except Exception:
            logger.exception(
                "[cleanup] skipping deal %s due to action-computation error",
                deal.hubspot_deal_id,
            )
            continue

        # Suppress create_note actions within the cooldown window to prevent
        # duplicate Sales Director notes appearing in HubSpot.
        # active_cooldown_days is non-None only when suppression is currently active.
        _note_days = _last_note_days_ago(deal.hubspot_deal_id)
        active_cooldown_days: Optional[int] = None
        if _note_days is not None and _note_days < NOTE_COOLDOWN_DAYS:
            all_actions = [a for a in all_actions if a.action_type != "create_note"]
            active_cooldown_days = _note_days

        # Include mid-confidence actions (writeable or flag) and low-confidence
        # flags — everything gets shown so the user has a full picture.
        if not all_actions:
            continue

        # Resolve human-readable stage label
        stage_label = (
            deal.deal_stage_label
            or deal.deal_stage
            or "Unknown stage"
        )

        rows.append(BatchCleanupRow(
            deal_id=deal.hubspot_deal_id,
            deal_name=deal.deal_name or deal.hubspot_deal_id,
            deal_stage_label=stage_label,
            amount_cents=deal.amount_cents or 0,
            owner_email=deal.owner_email or "",
            last_touch_at=deal.last_meaningful_touch_at,
            contact_count=len(contacts),
            actions=all_actions,
            last_note_days_ago=active_cooldown_days,
        ))

    return rows


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

_SEVERITY_DOT = {
    "critical": '<span class="sev-dot sev-critical" title="Critical">●</span>',
    "warning":  '<span class="sev-dot sev-warning"  title="Warning">●</span>',
    "hygiene":  '<span class="sev-dot sev-hygiene"  title="Hygiene">●</span>',
}

_SEVERITY_ORDER = {"critical": 0, "warning": 1, "hygiene": 2}
_CATEGORY_LABEL = {
    "close_date": "Close Date",
    "amount":     "Amount",
    "staleness":  "Staleness",
    "stage":      "Stage",
    "hygiene":    "Hygiene",
    "review":     "Review Note",
}


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

    # Flash
    flash_html = ""
    if applied or failed:
        if failed:
            flash_html = (
                f'<div class="flash flash--warn">'
                f"{applied} action(s) applied, {failed} failed — check Render logs for details."
                f"</div>"
            )
        else:
            flash_html = (
                f'<div class="flash">'
                f"✓ {applied} action(s) pushed to HubSpot. The board will refresh on next sync."
                f"</div>"
            )
    elif error:
        flash_html = f'<div class="flash flash--warn">{_esc(error)}</div>'

    # Summary counts
    total_critical = sum(
        1 for row in rows
        for a in row.actions if a.severity == "critical" and a.confidence == "mid"
    )
    total_warning = sum(
        1 for row in rows
        for a in row.actions if a.severity == "warning" and a.confidence == "mid"
    )
    total_hygiene = sum(
        1 for row in rows
        for a in row.actions if a.severity == "hygiene" and a.confidence == "mid"
    )
    total_flags = sum(
        1 for row in rows
        for a in row.actions if a.confidence == "low"
    )
    total_mid = total_critical + total_warning + total_hygiene
    deal_count = len(rows)

    summary_html = f"""
<div class="summary-bar">
  <div class="summary-counts">
    <span class="count-pill count-crit">{total_critical} critical</span>
    <span class="count-pill count-warn">{total_warning} warnings</span>
    <span class="count-pill count-hyg">{total_hygiene} hygiene</span>
    <span class="count-pill count-flag">{total_flags} flags</span>
  </div>
  <span class="summary-sub">across {deal_count} open deal{"s" if deal_count != 1 else ""}</span>
</div>"""

    if rows:
        deals_html = ""
        for row in rows:
            mid_actions = [a for a in row.actions if a.confidence == "mid"]
            low_actions = [a for a in row.actions if a.confidence == "low"]

            # Sort mid by severity then category order
            mid_actions.sort(key=lambda a: (_SEVERITY_ORDER.get(a.severity, 9), a.category))

            # Deal header context line
            last_touch_str = _days_ago_str(row.last_touch_at)
            owner_short = row.owner_email.split("@")[0] if row.owner_email else "unassigned"
            amount_str = _fmt_money(row.amount_cents)
            contact_str = (
                f"{row.contact_count} contact{'s' if row.contact_count != 1 else ''}"
                if row.contact_count > 0 else
                '<span class="ctx-warn">no contacts</span>'
            )
            amount_cls = "ctx-warn" if row.amount_cents <= 0 else "ctx-ok"
            touch_cls = "ctx-warn" if row.last_touch_at is None else "ctx-ok"

            cooldown_html = ""
            if row.last_note_days_ago is not None and row.last_note_days_ago < NOTE_COOLDOWN_DAYS:
                remaining = NOTE_COOLDOWN_DAYS - row.last_note_days_ago
                cooldown_html = (
                    f'<div class="cooldown-notice">'
                    f'ℹ Sales Director note logged {row.last_note_days_ago}d ago — '
                    f'note actions suppressed for {remaining} more day{"s" if remaining != 1 else ""}.'
                    f'</div>'
                )

            deals_html += f"""
<div class="deal-card" data-deal="{_esc(row.deal_id)}">
  <div class="deal-hdr">
    <div class="deal-hdr-main">
      <a href="/admin/sales/deals/{_esc(row.deal_id)}" class="deal-name">{_esc(row.deal_name)}</a>
      <span class="stage-pill">{_esc(row.deal_stage_label)}</span>
    </div>
    <div class="deal-ctx">
      <span class="{amount_cls}">{_esc(amount_str)}</span>
      <span class="ctx-sep">·</span>
      <span>{_esc(owner_short)}</span>
      <span class="ctx-sep">·</span>
      <span class="{touch_cls}">last touch {_esc(last_touch_str)}</span>
      <span class="ctx-sep">·</span>
      <span>{contact_str}</span>
    </div>
    {cooldown_html}
  </div>"""

            # Mid-confidence action rows
            for action in mid_actions:
                action_id_esc = _esc(action.action_id)
                dot = _SEVERITY_DOT.get(action.severity, "")
                cat_label = _CATEGORY_LABEL.get(action.category, action.category)
                is_flag = action.action_type == "flag"

                prop_pills = ""
                if action.properties:
                    prop_pills = "".join(
                        f'<span class="prop-pill">{_esc(k)} = {_esc(v)}</span>'
                        for k, v in action.properties.items()
                    )
                elif action.note_body:
                    preview = action.note_body[:200].replace("\n", " ").strip()
                    prop_pills = f'<span class="note-preview">{_esc(preview)}…</span>'
                elif is_flag:
                    prop_pills = (
                        f'<a href="{_esc(action.link_url)}" target="_blank" class="hs-link">'
                        f'Fix in HubSpot →</a>'
                        if action.link_url else ""
                    )

                if is_flag:
                    # Flags show a "Fix in HubSpot" link, not a checkbox
                    cb_cell = '<td class="chk-cell"><span class="flag-icon" title="Needs manual fix in HubSpot">⚑</span></td>'
                else:
                    cb_cell = (
                        f'<td class="chk-cell">'
                        f'<input type="checkbox" name="action_ids" value="{action_id_esc}"'
                        f' id="cb-{action_id_esc}" class="action-cb"'
                        f' data-severity="{_esc(action.severity)}">'
                        f'</td>'
                    )

                row_cls = f"action-row sev-row-{_esc(action.severity)}" + (" flag-row" if is_flag else "")

                deals_html += f"""
  <div class="{row_cls}">
    <table class="action-tbl">
      <tr>
        {cb_cell}
        <td class="action-main">
          <label for="cb-{action_id_esc}" class="action-label">
            {dot} {_esc(action.label)}
            <span class="cat-tag">{_esc(cat_label)}</span>
          </label>
          <div class="action-desc">{_esc(action.description)}</div>
          <div class="prop-row">{prop_pills}</div>
        </td>
      </tr>
    </table>
  </div>"""

            # Low-confidence flag rows (collapsed by default if many)
            if low_actions:
                low_html = ""
                for action in low_actions:
                    dot = _SEVERITY_DOT.get(action.severity, "")
                    cat_label = _CATEGORY_LABEL.get(action.category, action.category)
                    hs_link = (
                        f'<a href="{_esc(action.link_url)}" target="_blank" class="hs-link">Fix in HubSpot →</a>'
                        if action.link_url else ""
                    )
                    low_html += f"""
    <div class="action-row sev-row-hygiene flag-row">
      <table class="action-tbl">
        <tr>
          <td class="chk-cell"><span class="flag-icon" title="Fix in HubSpot">⚑</span></td>
          <td class="action-main">
            <span class="action-label">{dot} {_esc(action.label)}
              <span class="cat-tag">{_esc(cat_label)}</span>
            </span>
            <div class="action-desc">{_esc(action.description)} {hs_link}</div>
          </td>
        </tr>
      </table>
    </div>"""

                deals_html += f"""
  <div class="low-flags">
    <button class="flags-toggle" type="button" onclick="toggleFlags(this)">
      ▸ {len(low_actions)} hygiene flag{"s" if len(low_actions) != 1 else ""} (fix in HubSpot)
    </button>
    <div class="flags-content" style="display:none">{low_html}</div>
  </div>"""

            deals_html += "</div>"  # .deal-card

        total_mid_count = sum(
            1 for row in rows
            for a in row.actions if a.confidence == "mid" and a.action_type != "flag"
        )
        toolbar_html = f"""
<div class="toolbar">
  <button type="submit" class="btn btn--apply" id="apply-btn" disabled>
    Apply selected (<span id="sel-count">0</span>)
  </button>
  <button type="button" class="btn btn--ghost" id="sel-all-btn">Select all ({total_mid_count})</button>
  <button type="button" class="btn btn--ghost btn--crit" id="sel-crit-btn">Select critical ({total_critical})</button>
  <button type="button" class="btn btn--ghost" id="sel-none-btn" style="display:none">Deselect all</button>
</div>"""

        body_html = f"""
<form method="post" action="/admin/sales/deals/cleanup" id="cleanup-form">
  {toolbar_html}
  {deals_html}
  <div class="toolbar" style="margin-top:16px">
    <button type="submit" class="btn btn--apply" disabled
      data-mirror="apply-btn">Apply selected (<span class="sel-count-mirror">0</span>)</button>
  </div>
</form>"""
    else:
        body_html = (
            '<p class="empty">✓ No actions pending. All open deals look clean — '
            'run a HubSpot sync to refresh if you just made changes.</p>'
        )

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>agent | Cleanup Queue</title>
    {favicons}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      {nav_styles}
      :root {{
        --dark-blue:#2B3644; --light-brown:#F9F7F3; --border:rgba(43,54,68,.12);
        --shadow:rgba(43,54,68,.10); --white:#FFF; --accent:#3B7DD8;
        --green:#2f8f5b; --warn-bg:#fff8e6; --warn-border:#f5c47b;
        --crit-bg:#fff0f0; --crit-border:#f5a0a0; --hyg-bg:#f0f5ff; --hyg-border:#b0c8f5;
        --red:#c0392b; --amber:#b45309; --blue:#1d4ed8;
      }}
      *{{box-sizing:border-box;}}
      body{{margin:0;background:var(--light-brown);color:var(--dark-blue);font-family:"Inter","Segoe UI",sans-serif;}}
      a{{color:var(--accent);text-decoration:none;}}
      a:hover{{text-decoration:underline;}}
      .shell{{max-width:1120px;margin:0 auto;padding:36px 18px;}}
      .workspace{{background:var(--white);border:1px solid var(--border);border-radius:20px;
                  box-shadow:0 18px 40px var(--shadow);padding:32px 28px;}}
      h1{{font-family:"Montserrat",sans-serif;font-weight:800;font-size:26px;margin:0 0 6px;}}
      .eyebrow{{font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;
                color:rgba(43,54,68,.5);margin:0 0 4px;}}
      .intro{{color:rgba(43,54,68,.65);font-size:13.5px;margin:0 0 20px;line-height:1.5;}}
      .flash{{background:#e8f5ee;border:1px solid #b0d8c0;border-radius:10px;
              padding:10px 14px;margin:0 0 18px;font-size:14px;color:#1a5c38;}}
      .flash--warn{{background:var(--warn-bg);border-color:var(--warn-border);color:var(--amber);}}

      /* Summary bar */
      .summary-bar{{display:flex;align-items:center;gap:14px;margin-bottom:22px;flex-wrap:wrap;}}
      .summary-counts{{display:flex;gap:8px;flex-wrap:wrap;}}
      .count-pill{{font-size:12px;font-weight:700;padding:3px 10px;border-radius:20px;}}
      .count-crit{{background:var(--crit-bg);color:var(--red);border:1px solid var(--crit-border);}}
      .count-warn{{background:var(--warn-bg);color:var(--amber);border:1px solid var(--warn-border);}}
      .count-hyg{{background:var(--hyg-bg);color:var(--blue);border:1px solid var(--hyg-border);}}
      .count-flag{{background:rgba(43,54,68,.06);color:rgba(43,54,68,.6);border:1px solid var(--border);}}
      .summary-sub{{font-size:13px;color:rgba(43,54,68,.5);}}

      /* Toolbar */
      .toolbar{{display:flex;align-items:center;gap:10px;margin-bottom:18px;flex-wrap:wrap;}}
      .btn{{padding:7px 16px;border-radius:8px;border:1px solid transparent;
            font-size:13px;font-weight:600;cursor:pointer;font-family:inherit;transition:.15s;}}
      .btn--apply{{background:var(--green);color:#fff;border-color:var(--green);}}
      .btn--apply:disabled{{background:rgba(43,54,68,.15);color:rgba(43,54,68,.35);
                            border-color:transparent;cursor:not-allowed;}}
      .btn--ghost{{background:transparent;border:1px solid var(--border);color:var(--dark-blue);}}
      .btn--ghost:hover{{background:rgba(43,54,68,.06);}}
      .btn--crit{{color:var(--red);border-color:var(--crit-border);}}
      .btn--crit:hover{{background:var(--crit-bg);}}

      /* Deal cards */
      .deal-card{{border:1px solid var(--border);border-radius:14px;margin-bottom:16px;overflow:hidden;}}
      .deal-hdr{{background:rgba(43,54,68,.03);padding:12px 16px;border-bottom:1px solid var(--border);}}
      .deal-hdr-main{{display:flex;align-items:center;gap:10px;margin-bottom:4px;}}
      .deal-name{{font-weight:700;font-size:15px;color:var(--dark-blue);}}
      .deal-name:hover{{color:var(--accent);}}
      .stage-pill{{font-size:11px;font-weight:600;background:rgba(43,54,68,.08);
                   border-radius:6px;padding:2px 8px;color:rgba(43,54,68,.65);white-space:nowrap;}}
      .deal-ctx{{font-size:12.5px;color:rgba(43,54,68,.6);display:flex;gap:6px;flex-wrap:wrap;align-items:center;}}
      .ctx-sep{{color:rgba(43,54,68,.25);}}
      .ctx-warn{{color:var(--red);font-weight:600;}}
      .ctx-ok{{color:rgba(43,54,68,.7);}}
      .cooldown-notice{{font-size:11.5px;color:var(--blue);background:var(--hyg-bg);
                        border:1px solid var(--hyg-border);border-radius:6px;
                        padding:4px 10px;margin-top:6px;display:inline-block;}}

      /* Action rows */
      .action-row{{padding:10px 16px;border-bottom:1px solid rgba(43,54,68,.06);}}
      .action-row:last-child{{border-bottom:none;}}
      .sev-row-critical{{border-left:3px solid var(--red);}}
      .sev-row-warning{{border-left:3px solid #f59e0b;}}
      .sev-row-hygiene{{border-left:3px solid #93c5fd;}}
      .flag-row{{opacity:.8;}}
      .action-tbl{{width:100%;border-collapse:collapse;}}
      .chk-cell{{width:36px;vertical-align:top;padding-top:2px;}}
      .action-main{{vertical-align:top;}}
      .action-label{{font-size:13.5px;font-weight:600;cursor:pointer;display:inline-flex;
                     align-items:center;gap:6px;flex-wrap:wrap;}}
      .action-desc{{font-size:12.5px;color:rgba(43,54,68,.6);margin-top:3px;line-height:1.45;}}
      .prop-row{{margin-top:6px;display:flex;flex-wrap:wrap;gap:4px;}}
      .prop-pill{{font-size:11px;background:rgba(43,54,68,.06);border-radius:6px;
                  padding:2px 8px;font-family:monospace;white-space:nowrap;}}
      .note-preview{{font-size:11px;background:#f0f5ff;border:1px solid #b0c8f5;border-radius:6px;
                     padding:4px 10px;color:#1d4ed8;font-style:italic;max-width:600px;
                     display:inline-block;line-height:1.4;}}
      .cat-tag{{font-size:10px;font-weight:500;background:rgba(43,54,68,.07);border-radius:4px;
                padding:1px 6px;color:rgba(43,54,68,.5);text-transform:uppercase;letter-spacing:.04em;}}
      .flag-icon{{font-size:14px;color:rgba(43,54,68,.3);cursor:default;}}
      .hs-link{{font-size:12px;font-weight:600;color:var(--accent);}}

      /* Severity dots */
      .sev-dot{{font-size:9px;}}
      .sev-critical{{color:var(--red);}}
      .sev-warning{{color:#f59e0b;}}
      .sev-hygiene{{color:#93c5fd;}}

      /* Low-confidence flags (collapsed) */
      .low-flags{{border-top:1px dashed rgba(43,54,68,.1);padding:8px 16px;}}
      .flags-toggle{{background:none;border:none;font-size:12.5px;font-weight:600;
                     color:rgba(43,54,68,.45);cursor:pointer;padding:0;font-family:inherit;}}
      .flags-toggle:hover{{color:var(--dark-blue);}}

      input[type=checkbox]{{width:15px;height:15px;cursor:pointer;accent-color:var(--green);}}
      .empty{{color:rgba(43,54,68,.5);font-size:14px;padding:24px 0;}}
      .back-link{{display:inline-block;margin-bottom:20px;font-size:13px;color:rgba(43,54,68,.55);}}
      .back-link:hover{{color:var(--dark-blue);}}
    </style>
  </head>
  <body>
    {nav}
    <main class="shell">
      <div class="workspace">
        <a href="/admin/sales/deals" class="back-link">← Deal Board</a>
        <p class="eyebrow">Sales — HubSpot</p>
        <h1>Cleanup <span style="color:#3B7DD8">Queue.</span></h1>
        <p class="intro">
          All mid-confidence actions across your open deals — close dates, amounts, staleness, and review notes —
          ready to push to HubSpot in one click. Flag items (⚑) need a manual fix directly in HubSpot.
        </p>
        {flash_html}
        {summary_html}
        {body_html}
      </div>
    </main>
    <script>
    (function() {{
      function updateCount() {{
        var checked = document.querySelectorAll('.action-cb:checked');
        var n = checked.length;
        document.querySelectorAll('#sel-count, .sel-count-mirror').forEach(function(s) {{ s.textContent = n; }});
        document.querySelectorAll('[data-mirror="apply-btn"], #apply-btn').forEach(function(b) {{ b.disabled = n === 0; }});
        var total = document.querySelectorAll('.action-cb').length;
        var selAll = document.getElementById('sel-all-btn');
        var selNone = document.getElementById('sel-none-btn');
        if (selAll) selAll.style.display = n === total ? 'none' : '';
        if (selNone) selNone.style.display = n === 0 ? 'none' : '';
      }}

      document.querySelectorAll('.action-cb').forEach(function(cb) {{
        cb.addEventListener('change', updateCount);
      }});

      var selAll = document.getElementById('sel-all-btn');
      if (selAll) selAll.addEventListener('click', function() {{
        document.querySelectorAll('.action-cb').forEach(function(cb) {{ cb.checked = true; }});
        updateCount();
      }});

      var selCrit = document.getElementById('sel-crit-btn');
      if (selCrit) selCrit.addEventListener('click', function() {{
        document.querySelectorAll('.action-cb').forEach(function(cb) {{
          cb.checked = cb.dataset.severity === 'critical';
        }});
        updateCount();
      }});

      var selNone = document.getElementById('sel-none-btn');
      if (selNone) selNone.addEventListener('click', function() {{
        document.querySelectorAll('.action-cb:checked').forEach(function(cb) {{ cb.checked = false; }});
        updateCount();
      }});

      updateCount();
    }})();

    function toggleFlags(btn) {{
      var content = btn.nextElementSibling;
      var hidden = content.style.display === 'none';
      content.style.display = hidden ? 'block' : 'none';
      btn.textContent = btn.textContent.replace(hidden ? '▸' : '▾', hidden ? '▾' : '▸');
    }}
    </script>
  </body>
</html>"""
