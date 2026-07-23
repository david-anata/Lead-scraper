"""Sales Priorities deal board — open HubSpot deals sorted top-down by close
date (soonest first), with a Sales-Operational-Director completeness read on
each deal (missing amount / line items / contacts / close date).

``build_deal_board`` is pure data (testable without a request);
``render_deal_board_page`` wraps it in the shared admin chrome.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import (
    HubSpotCompany,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
)
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.sales import hubspot_links


def _esc(value: object) -> str:
    return html.escape(str(value or ""))


@dataclass
class DealRow:
    deal_id: str
    name: str
    stage: str
    stage_label: str
    amount_cents: int
    close_date: Optional[datetime]
    owner_email: str
    company_name: str
    line_item_count: int
    contact_count: int
    last_inbound_at: Optional[datetime] = None
    missing: list[str] = field(default_factory=list)
    bucket: str = "later"
    is_stale: bool = False

    @property
    def is_complete(self) -> bool:
        return not self.missing


@dataclass
class DealBoard:
    rows: list[DealRow] = field(default_factory=list)
    total_open: int = 0
    incomplete_count: int = 0
    overdue_count: int = 0


_BUCKET_ORDER = ("overdue", "this_week", "this_month", "later", "no_date")
_BUCKET_LABELS = {
    "overdue": "Past close date",
    "this_week": "Closing this week",
    "this_month": "Closing this month",
    "later": "Later",
    "no_date": "No close date",
}


def _completeness(row: DealRow, *, as_of: datetime) -> list[str]:
    missing: list[str] = []
    if row.amount_cents <= 0:
        missing.append("amount")
    if row.line_item_count <= 0:
        missing.append("line items")
    if row.contact_count <= 0:
        missing.append("contacts")
    if row.close_date is None:
        missing.append("close date")
    return missing


def _bucket(row: DealRow, *, as_of: datetime) -> str:
    cd = row.close_date
    if cd is None:
        return "no_date"
    if cd.tzinfo is None:
        cd = cd.replace(tzinfo=timezone.utc)
    if cd < as_of:
        return "overdue"
    if cd <= as_of + timedelta(days=7):
        return "this_week"
    if cd <= as_of + timedelta(days=30):
        return "this_month"
    return "later"


def build_deal_board(
    session: Session,
    *,
    as_of: datetime | None = None,
    owner_filter: str | None = None,
    stale_days: int = 14,
) -> DealBoard:
    """Assemble the open-deal board, sorted by close date soonest-first."""
    as_of = as_of or datetime.now(timezone.utc)

    # Counts per deal in two grouped queries (avoids N+1).
    li_counts = dict(
        session.execute(
            select(HubSpotLineItem.hubspot_deal_id, func.count())
            .group_by(HubSpotLineItem.hubspot_deal_id)
        ).all()
    )
    contact_counts = dict(
        session.execute(
            select(HubSpotDealContact.hubspot_deal_id, func.count())
            .group_by(HubSpotDealContact.hubspot_deal_id)
        ).all()
    )
    company_names = dict(
        session.execute(
            select(HubSpotCompany.hubspot_company_id, HubSpotCompany.name)
        ).all()
    )

    q = select(HubSpotDeal).where(HubSpotDeal.is_closed.is_(False))
    if owner_filter:
        q = q.where(HubSpotDeal.owner_email == owner_filter)
    deals = session.scalars(q).all()

    stale_cutoff = as_of - timedelta(days=stale_days)
    rows: list[DealRow] = []
    for d in deals:
        row = DealRow(
            deal_id=d.hubspot_deal_id,
            name=d.deal_name,
            stage=d.deal_stage,
            stage_label=d.deal_stage_label,
            amount_cents=d.amount_cents or 0,
            close_date=d.close_date,
            owner_email=d.owner_email,
            company_name=company_names.get(d.hubspot_company_id, ""),
            line_item_count=int(li_counts.get(d.hubspot_deal_id, 0)),
            contact_count=int(contact_counts.get(d.hubspot_deal_id, 0)),
            last_inbound_at=d.last_inbound_at,
        )
        row.missing = _completeness(row, as_of=as_of)
        row.bucket = _bucket(row, as_of=as_of)
        # Stale = no inbound reply from prospect in > stale_days.
        li = d.last_inbound_at
        if li is not None and li.tzinfo is None:
            li = li.replace(tzinfo=timezone.utc)
        row.is_stale = (li is None or li < stale_cutoff)
        rows.append(row)

    # Soonest close date first; deals with no close date sink to the bottom.
    far_future = datetime.max.replace(tzinfo=timezone.utc)

    def _sort_key(r: DealRow):
        cd = r.close_date
        if cd is not None and cd.tzinfo is None:
            cd = cd.replace(tzinfo=timezone.utc)
        return (cd is None, cd or far_future, -r.amount_cents)

    rows.sort(key=_sort_key)

    overdue = sum(
        1
        for r in rows
        if r.close_date is not None
        and (r.close_date if r.close_date.tzinfo else r.close_date.replace(tzinfo=timezone.utc)) < as_of
    )
    board = DealBoard(
        rows=rows,
        total_open=len(rows),
        incomplete_count=sum(1 for r in rows if not r.is_complete),
        overdue_count=overdue,
    )
    return board


def _fmt_money(cents: int) -> str:
    if not cents:
        return "—"
    return f"${cents / 100:,.0f}"


def _fmt_date(dt: Optional[datetime]) -> str:
    if dt is None:
        return '<span class="muted">no close date</span>'
    return _esc(f"{dt:%b} {dt.day}, {dt:%Y}")


def _row_html(r: DealRow, *, as_of: datetime, portal_id: str = "") -> str:
    if r.missing:
        # Link flags directly to HubSpot (if portal configured) or deal detail.
        deal_hs = hubspot_links.deal_url(portal_id, r.deal_id) if portal_id else ""
        flag_href = deal_hs if deal_hs else f"/admin/sales/deals/{_esc(r.deal_id)}"
        flag_target = "_blank" if deal_hs else "_self"
        chips = "".join(
            f'<a class="flag" href="{_esc(flag_href)}" target="{flag_target}">{_esc(m)} →</a>'
            for m in r.missing
        )
        flags = f'<div class="flags">{chips}</div>'
    else:
        flags = '<span class="ok">✓ ready</span>'

    overdue = (
        r.close_date is not None
        and (r.close_date if r.close_date.tzinfo else r.close_date.replace(tzinfo=timezone.utc)) < as_of
    )
    date_cell = _fmt_date(r.close_date)
    if overdue:
        date_cell = f'<span class="overdue">{date_cell} · overdue</span>'

    name = _esc(r.name or "(untitled deal)")
    stale_badge = ' <span class="stale-badge" title="No inbound reply in 14+ days">⚠</span>' if r.is_stale else ""
    company = f'<div class="muted">{_esc(r.company_name)}</div>' if r.company_name else ""
    search_text = html.escape(
        " ".join((r.name, r.company_name, r.stage_label, r.owner_email)).lower(),
        quote=True,
    )
    readiness = "ready" if r.is_complete else "incomplete"
    return (
        f'<tr class="deal-row" data-bucket="{_esc(r.bucket)}" data-readiness="{readiness}" '
        f'data-overdue="{"true" if overdue else "false"}" data-search="{search_text}">'
        f'<td class="deal"><a href="/admin/sales/deals/{_esc(r.deal_id)}">{name}</a>{stale_badge}{company}</td>'
        f"<td>{_esc(r.stage_label or r.stage or '—')}</td>"
        f'<td class="num">{_fmt_money(r.amount_cents)}</td>'
        f"<td>{date_cell}</td>"
        f"<td>{_esc(r.owner_email or '—')}</td>"
        f"<td>{flags}</td>"
        "</tr>"
    )


_STYLES = """
  :root { --dark-blue:#2B3644; --light-blue:#85BBDA; --light-brown:#F9F7F3;
    --white:#FFF; --border:rgba(43,54,68,0.12); --shadow:rgba(43,54,68,0.10); }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--light-brown); color:var(--dark-blue);
    font-family:"Inter","Segoe UI",sans-serif; }
  a { color:var(--dark-blue); }
  __NAV__
  .shell { max-width:1320px; margin:0 auto; padding:28px 24px 64px; }
  .workspace { background:var(--white); border:1px solid var(--border);
    border-radius:20px; box-shadow:0 18px 40px var(--shadow); padding:26px 28px 30px; }
  h1 { font-family:"Montserrat",sans-serif; font-weight:800; font-size:26px; margin:0 0 4px; }
  .eyebrow { font-family:"Montserrat",sans-serif; font-weight:700; font-size:11px;
    letter-spacing:0.08em; text-transform:uppercase; color:rgba(43,54,68,0.55); margin:0 0 4px; }
  .intro { font-size:14px; color:rgba(43,54,68,0.75); margin:0 0 18px; max-width:760px; }
  .stats { display:flex; gap:14px; margin:0 0 18px; flex-wrap:wrap; }
  .stat { background:var(--light-brown); border:1px solid var(--border); border-radius:14px;
    padding:12px 18px; min-width:120px; }
  .stat .n { font-family:"Montserrat",sans-serif; font-weight:800; font-size:22px; }
  .stat .l { font-size:11px; text-transform:uppercase; letter-spacing:0.06em; color:rgba(43,54,68,0.55); }
  table { width:100%; border-collapse:collapse; font-size:13.5px; }
  th,td { text-align:left; padding:10px 12px; border-bottom:1px solid var(--border); vertical-align:top; }
  th { position:sticky; top:0; z-index:2; background:var(--white);
    font-family:"Montserrat",sans-serif; font-weight:700; font-size:11px; text-transform:uppercase;
    letter-spacing:0.05em; color:rgba(43,54,68,0.55); }
  .deal-row:hover td { background:rgba(133,187,218,0.07); }
  td.num { text-align:right; font-variant-numeric:tabular-nums; }
  td.deal a { font-weight:600; text-decoration:none; }
  .muted { color:rgba(43,54,68,0.5); font-size:12px; }
  .flags { display:flex; gap:5px; flex-wrap:wrap; }
  .flag { background:#fff4d9; border:1px solid #d2a94b; color:#7a5a12; border-radius:8px;
    padding:1px 8px; font-size:11px; text-decoration:none; }
  .flag:hover { background:#ffedb0; }
  .ok { color:#2f8f5b; font-size:12px; font-weight:600; }
  .overdue { color:#b23b3b; font-weight:600; }
  .empty { color:rgba(43,54,68,0.6); font-size:14px; padding:20px 0; }
  .syncbar { display:flex; align-items:center; gap:12px; margin:0 0 16px; font-size:12.5px;
    color:rgba(43,54,68,0.6); flex-wrap:wrap; }
  .syncbar button { font:inherit; font-weight:600; border:1px solid var(--border); background:var(--white);
    border-radius:10px; padding:7px 14px; cursor:pointer; color:var(--dark-blue); }
  .cleanup-link { font-size:13px; font-weight:600; color:#2f8f5b; text-decoration:none; }
  .cleanup-link:hover { text-decoration:underline; }
  .create-link { font-size:13px; font-weight:700; color:var(--dark-blue); text-decoration:none;
    border:1px solid var(--border); border-radius:10px; padding:7px 14px; background:var(--white); }
  .create-link:hover { border-color:rgba(43,54,68,0.3); }
  .stale-badge { font-size:13px; color:#b23b3b; margin-left:4px; cursor:default; }
  .stat--warn .n { color:#b23b3b; }
  .table-wrap { max-height:min(64vh,720px); overflow:auto; border:1px solid var(--border);
    border-radius:14px; -webkit-overflow-scrolling:touch; }
  .table-wrap table { margin:0; }
  .results-toolbar { display:grid; grid-template-columns:minmax(220px,1fr) minmax(170px,220px) auto;
    align-items:end; gap:10px; margin:0 0 12px; }
  .results-field { display:grid; gap:5px; }
  .results-field label { font:700 10px/1.2 "Montserrat",sans-serif; letter-spacing:.06em;
    text-transform:uppercase; color:rgba(43,54,68,.55); }
  .results-field input,.results-field select { width:100%; min-height:40px; padding:0 12px;
    border:1px solid var(--border); border-radius:10px; background:#fff; color:var(--dark-blue);
    font:500 13px/1.2 "Inter","Segoe UI",sans-serif; }
  .results-count { align-self:center; justify-self:end; color:rgba(43,54,68,.62);
    font-size:12px; white-space:nowrap; }
  .results-empty { display:none; margin:12px 0 0; padding:16px; border:1px dashed var(--border);
    border-radius:12px; color:rgba(43,54,68,.62); text-align:center; font-size:13px; }
  .filter-tabs { display:flex; gap:8px; margin:0 0 16px; }
  .tab { font-size:13px; font-weight:600; padding:7px 16px; border-radius:20px;
    border:1px solid var(--border); text-decoration:none; color:var(--dark-blue);
    background:var(--white); }
  .tab:hover { border-color:rgba(43,54,68,0.3); }
  .tab--active { background:var(--dark-blue); color:#fff; border-color:var(--dark-blue); }
  tr.bucket-hdr td { font-family:"Montserrat",sans-serif; font-weight:700; font-size:11px;
    text-transform:uppercase; letter-spacing:0.06em; color:rgba(43,54,68,0.55);
    background:var(--light-brown); padding:10px 12px 6px; border-bottom:none; border-top:1px solid var(--border); }
  tr.bucket-hdr--overdue td { color:#b23b3b; }
  tr.bucket-hdr--this_week td { color:#1a6e3a; }
  @media (max-width:900px) {
    .shell { padding-inline:16px; }
    .results-toolbar { grid-template-columns:1fr 1fr; }
    .results-count { grid-column:1 / -1; justify-self:start; }
  }
"""

_POLL_JS = """(function(){
  var note=document.getElementById('sync-note'),last=null;
  function check(){
    fetch('/admin/sales/deals/sync/status').then(function(r){return r.json();})
      .then(function(d){
        if(note&&d.message)note.textContent=d.message;
        if(d.status==='running'){setTimeout(check,3000);}
        else{
          if(last!==null&&d.completed_at&&d.completed_at!==last){window.location.reload();return;}
          if(d.completed_at)last=d.completed_at;
        }
      }).catch(function(){});
  }
  check();
})();
(function(){
  var search=document.getElementById('deal-search');
  var status=document.getElementById('deal-status');
  var count=document.getElementById('deal-count');
  var empty=document.getElementById('deal-empty');
  var rows=[].slice.call(document.querySelectorAll('tr.deal-row'));
  var buckets=[].slice.call(document.querySelectorAll('tr.bucket-hdr'));
  function apply(){
    var q=(search&&search.value||'').toLowerCase().trim();
    var state=(status&&status.value||'');
    var shown=0;
    rows.forEach(function(row){
      var matchesText=!q||(row.dataset.search||'').indexOf(q)!==-1;
      var matchesState=!state||
        (state==='overdue'&&row.dataset.overdue==='true')||
        (state!=='overdue'&&row.dataset.readiness===state);
      var visible=matchesText&&matchesState;
      row.hidden=!visible;
      if(visible)shown++;
    });
    buckets.forEach(function(bucket){
      var key=bucket.dataset.bucket;
      bucket.hidden=!rows.some(function(row){return !row.hidden&&row.dataset.bucket===key;});
    });
    if(count)count.textContent='Showing '+shown+' of '+rows.length+' open deals';
    if(empty)empty.style.display=shown?'none':'block';
  }
  if(search)search.addEventListener('input',apply);
  if(status)status.addEventListener('change',apply);
  apply();
})();"""


def render_deal_board_page(
    board: DealBoard,
    *,
    user: dict | None = None,
    sync_status: dict[str, Any] | None = None,
    as_of: datetime | None = None,
    show_my: bool = False,
    portal_id: str = "",
) -> str:
    as_of = as_of or datetime.now(timezone.utc)
    styles = _STYLES.replace("__NAV__", render_agent_nav_styles())

    if board.rows:
        all_rows_html = ""
        for bkey in _BUCKET_ORDER:
            brows = [r for r in board.rows if r.bucket == bkey]
            if not brows:
                continue
            warn_cls = " bucket-hdr--overdue" if bkey == "overdue" else (
                " bucket-hdr--this_week" if bkey == "this_week" else ""
            )
            label = _BUCKET_LABELS[bkey]
            n = len(brows)
            all_rows_html += (
                f'<tr class="bucket-hdr{warn_cls}" data-bucket="{_esc(bkey)}">'
                f'<td colspan="6">{_esc(label)} — {n} deal{"s" if n != 1 else ""}</td></tr>'
            )
            all_rows_html += "".join(_row_html(r, as_of=as_of, portal_id=portal_id) for r in brows)
        table = (
            '<div class="table-wrap">'
            "<table><thead><tr>"
            "<th>Deal</th><th>Stage</th><th>Amount</th><th>Close date</th>"
            "<th>Owner</th><th>Status</th>"
            "</tr></thead>"
            f"<tbody>{all_rows_html}</tbody></table>"
            "</div>"
        )
    else:
        empty_msg = (
            "No deals assigned to you yet."
            if show_my
            else "No open deals mirrored yet. Click <strong>Sync now</strong> to pull deals from HubSpot."
        )
        table = f'<p class="empty">{empty_msg}</p>'

    status = sync_status or {}
    status_msg = _esc(status.get("message") or "")
    sync_note = ""
    if status.get("status") == "unconfigured":
        sync_note = '<span class="overdue">HubSpot token not configured.</span>'
    elif status_msg:
        sync_note = status_msg

    completed_at = status.get("completed_at") or ""
    last_synced_html = ""
    if completed_at:
        try:
            dt = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            synced_label = f"{dt:%b} {dt.day}, {dt.strftime('%I:%M %p').lstrip('0')}"
            last_synced_html = f'<span class="muted">Last synced {_esc(synced_label)}</span>'
        except ValueError:
            pass

    overdue_cls = " stat--warn" if board.overdue_count > 0 else ""
    incomplete_cls = " stat--warn" if board.incomplete_count > 0 else ""

    all_active = "" if show_my else " tab--active"
    my_active = " tab--active" if show_my else ""
    results_controls_html = ""
    if board.rows:
        results_controls_html = f"""
        <div class="results-toolbar" aria-label="Deal result controls">
          <div class="results-field">
            <label for="deal-search">Search deals</label>
            <input id="deal-search" type="search" placeholder="Deal, company, stage, or owner">
          </div>
          <div class="results-field">
            <label for="deal-status">Readiness</label>
            <select id="deal-status">
              <option value="">All readiness</option>
              <option value="incomplete">Incomplete</option>
              <option value="ready">Ready</option>
              <option value="overdue">Past close date</option>
            </select>
          </div>
          <span class="results-count" id="deal-count" aria-live="polite">Showing {board.total_open} of {board.total_open} open deals</span>
        </div>
        """

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | Sales Deal Board</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>{styles}</style>
  </head>
  <body>
    {render_agent_nav("sales", sales_section="sales_deals", user=user)}
    <main class="shell">
      <div class="workspace">
        <p class="eyebrow">Sales — HubSpot</p>
        <h1>Deal <span style="color:var(--light-blue)">Board</span>.</h1>
        <p class="intro">Every open HubSpot deal, sorted top-down by close date — soonest first. Each deal is checked for the essentials needed to close: an amount, line items (what we’re offering), contacts, and a close date.</p>
        <div class="stats">
          <div class="stat"><div class="n">{board.total_open}</div><div class="l">Open deals</div></div>
          <div class="stat{overdue_cls}"><div class="n">{board.overdue_count}</div><div class="l">Past close date</div></div>
          <div class="stat{incomplete_cls}"><div class="n">{board.incomplete_count}</div><div class="l">Incomplete</div></div>
        </div>
        <div class="filter-tabs">
          <a href="/admin/sales/deals" class="tab{all_active}">All deals</a>
          <a href="/admin/sales/deals?my=1" class="tab{my_active}">My deals</a>
        </div>
        <div class="syncbar">
          <form method="post" action="/admin/sales/deals/sync" style="margin:0">
            <button type="submit" onclick="this.textContent='Syncing…';this.disabled=true">Sync now</button>
          </form>
          <a href="/admin/sales/deals/create" class="create-link">Create deal</a>
          <a href="/admin/sales/deals/cleanup" class="cleanup-link">Review HubSpot fixes -></a>
          {last_synced_html}
          <span id="sync-note">{sync_note}</span>
        </div>
        {results_controls_html}
        {table}
        {'<p class="results-empty" id="deal-empty">No deals match the current search and readiness filters.</p>' if board.rows else ''}
      </div>
    </main>
    <script>{_POLL_JS}</script>
  </body>
</html>"""
