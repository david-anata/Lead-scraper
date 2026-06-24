"""Deal Detail — a HubSpot *companion*, not a rebuild.

HubSpot remains the system of record: every record block shows the mirrored data
read-only and links out to HubSpot to edit. The agent's value-add is the
**accountability layer** — a completeness check, a computed next action, the
three closing-tool CTAs, and the comms timeline — everything a rep needs to know
what to do next and follow up to close, without leaving the tool to figure it out.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.models.entities import (
    CommunicationEvent,
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
    MailboxSignal,
    SalesDealAsset,
)
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.sales import hubspot_links
from sales_support_agent.services.sales.actions import ContactInfo, SalesAction, compute_pending_actions


def _esc(value: object) -> str:
    return html.escape(str(value or ""))


def _fmt_money(cents: int) -> str:
    if not cents:
        return "—"
    return f"${cents / 100:,.0f}"


def _fmt_date(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%b %-d, %Y")


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _owner_display(email: str) -> str:
    if not email:
        return "—"
    return email.split("@")[0].replace(".", " ").replace("_", " ").title()


# Asset-type → friendly label for the three closing-tool CTAs.
_ASSET_LABELS = {
    "deck": "Sales Deck",
    "rate_sheet": "Fulfillment Rate Sheet",
    "ads_audit": "Ads Audit",
}
_ASSET_ORDER = ("deck", "rate_sheet", "ads_audit")


@dataclass
class ContactView:
    contact_id: str
    name: str
    email: str
    phone: str
    title: str
    hubspot_url: str


@dataclass
class LineItemView:
    name: str
    quantity: int
    unit_price_cents: int
    amount_cents: int


@dataclass
class AssetView:
    asset_type: str
    label: str
    url: str
    cta_label: str


@dataclass
class TimelineEntry:
    when: Optional[datetime]
    kind: str
    title: str
    detail: str


@dataclass
class DealDetail:
    deal_id: str
    name: str
    stage: str
    amount_cents: int
    close_date: Optional[datetime]
    owner_email: str
    company_name: str
    company_id: str
    is_closed: bool
    is_won: bool
    deal_url: str
    company_url: str
    contacts: list[ContactView] = field(default_factory=list)
    line_items: list[LineItemView] = field(default_factory=list)
    assets: list[AssetView] = field(default_factory=list)
    timeline: list[TimelineEntry] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    next_action: str = ""
    overdue: bool = False
    pending_actions: list[SalesAction] = field(default_factory=list)


def _next_action(d: DealDetail, *, as_of: datetime) -> str:
    """The accountability nudge — the single most useful thing to do next."""
    if d.is_closed:
        return "This deal is closed. No action needed."
    if d.overdue:
        return "Close date has passed — update the date or move the stage in HubSpot."
    if "contacts" in d.missing:
        return "No contact yet — add the buyer in HubSpot so you can follow up."
    if "line items" in d.missing:
        return "No line items — add what you're offering (with costs) in HubSpot."
    if "amount" in d.missing:
        return "No deal value set — add the amount in HubSpot."
    if "close date" in d.missing:
        return "No close date — set one in HubSpot so this deal can be prioritized."
    if not d.assets:
        return "Link a sales deck, rate sheet, or ads audit so you have something to send."
    return "Looks ready — send a follow-up to keep it moving toward close."


def build_deal_detail(
    session: Session, deal_id: str, *, settings: Settings, as_of: datetime | None = None
) -> Optional[DealDetail]:
    as_of = as_of or datetime.now(timezone.utc)
    deal = session.get(HubSpotDeal, deal_id)
    if deal is None:
        return None

    portal = settings.hubspot_portal_id
    company = (
        session.get(HubSpotCompany, deal.hubspot_company_id)
        if deal.hubspot_company_id
        else None
    )

    detail = DealDetail(
        deal_id=deal.hubspot_deal_id,
        name=deal.deal_name,
        stage=deal.deal_stage_label or deal.deal_stage,
        amount_cents=deal.amount_cents or 0,
        close_date=deal.close_date,
        owner_email=deal.owner_email,
        company_name=company.name if company else "",
        company_id=deal.hubspot_company_id,
        is_closed=deal.is_closed,
        is_won=deal.is_won,
        deal_url=hubspot_links.deal_url(portal, deal.hubspot_deal_id),
        company_url=hubspot_links.company_url(portal, deal.hubspot_company_id) if company else "",
    )

    # Contacts (via the link mirror).
    contact_ids = [
        r.hubspot_contact_id
        for r in session.scalars(
            select(HubSpotDealContact).where(HubSpotDealContact.hubspot_deal_id == deal_id)
        ).all()
    ]
    for cid in contact_ids:
        c = session.get(HubSpotContact, cid)
        if c is None:
            continue
        name = " ".join(p for p in (c.first_name, c.last_name) if p).strip() or c.email or cid
        detail.contacts.append(
            ContactView(
                contact_id=cid,
                name=name,
                email=c.email,
                phone=c.phone,
                title=c.job_title,
                hubspot_url=hubspot_links.contact_url(portal, cid),
            )
        )

    # Line items.
    for li in session.scalars(
        select(HubSpotLineItem).where(HubSpotLineItem.hubspot_deal_id == deal_id)
    ).all():
        detail.line_items.append(
            LineItemView(
                name=li.name,
                quantity=li.quantity or 0,
                unit_price_cents=li.unit_price_cents or 0,
                amount_cents=li.amount_cents or 0,
            )
        )

    # Closing-tool CTAs (ordered deck → rate sheet → ads audit).
    assets = session.scalars(
        select(SalesDealAsset).where(SalesDealAsset.hubspot_deal_id == deal_id)
    ).all()
    by_type = {a.asset_type: a for a in assets}
    for t in _ASSET_ORDER:
        a = by_type.get(t)
        if a is None:
            continue
        detail.assets.append(
            AssetView(
                asset_type=t,
                label=a.label or _ASSET_LABELS.get(t, t),
                url=a.url,
                cta_label=f"Open {_ASSET_LABELS.get(t, t)}",
            )
        )

    # Comms timeline (logged events + matched inbound mail), newest first.
    for ev in session.scalars(
        select(CommunicationEvent).where(CommunicationEvent.hubspot_deal_id == deal_id)
    ).all():
        detail.timeline.append(
            TimelineEntry(
                when=ev.occurred_at, kind=ev.event_type or "event",
                title=ev.summary or ev.event_type or "Activity",
                detail=ev.recommended_next_action or ev.outcome or "",
            )
        )
    for sig in session.scalars(
        select(MailboxSignal).where(MailboxSignal.matched_deal_id == deal_id)
    ).all():
        detail.timeline.append(
            TimelineEntry(
                when=sig.received_at, kind="inbound_email",
                title=sig.subject or "Inbound email",
                detail=(sig.snippet or "")[:200],
            )
        )
    detail.timeline.sort(key=lambda e: _aware(e.when) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    # Completeness + next action (the accountability layer).
    if detail.amount_cents <= 0:
        detail.missing.append("amount")
    if not detail.line_items:
        detail.missing.append("line items")
    if not detail.contacts:
        detail.missing.append("contacts")
    if detail.close_date is None:
        detail.missing.append("close date")
    cd = _aware(detail.close_date)
    detail.overdue = bool(cd and not detail.is_closed and cd < as_of)
    detail.next_action = _next_action(detail, as_of=as_of)

    # Pending actions (mid/low confidence HubSpot write suggestions).
    signals = list(session.scalars(
        select(MailboxSignal).where(MailboxSignal.matched_deal_id == deal_id)
    ).all())
    li_total = sum(li.amount_cents or 0 for li in detail.line_items)
    contact_infos = [
        ContactInfo(contact_id=c.contact_id, email=c.email or "", hubspot_url=c.hubspot_url)
        for c in detail.contacts
    ]
    detail.pending_actions = compute_pending_actions(
        deal, signals,
        line_item_total_cents=li_total,
        contacts=contact_infos,
        portal_id=portal or "",
        as_of=as_of,
    )

    return detail


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _hs_link(url: str, text: str) -> str:
    if not url:
        return f'<span class="muted">{_esc(text)}</span>'
    return f'<a class="hs-link" href="{_esc(url)}" target="_blank" rel="noopener">{_esc(text)} ↗</a>'


def _contacts_html(d: DealDetail) -> str:
    if not d.contacts:
        return '<p class="empty">No contacts on this deal. ' + _hs_link(
            d.deal_url, "Add the buyer in HubSpot"
        ) + "</p>"
    rows = ""
    for c in d.contacts:
        parts = []
        if c.title:
            parts.append(_esc(c.title))
        if c.email:
            parts.append(f'<a href="mailto:{_esc(c.email)}">{_esc(c.email)}</a>')
        if c.phone:
            parts.append(f'<a href="tel:{_esc(c.phone)}">{_esc(c.phone)}</a>')
        meta = " · ".join(parts)
        rows += (
            f'<li><strong>{_esc(c.name)}</strong>'
            f'<div class="muted">{meta}</div>'
            f'<div>{_hs_link(c.hubspot_url, "View in HubSpot")}</div></li>'
        )
    return f"<ul class='records'>{rows}</ul>"


def _line_items_html(d: DealDetail) -> str:
    if not d.line_items:
        return '<p class="empty">No line items. ' + _hs_link(
            d.deal_url, "Add what you're offering in HubSpot"
        ) + "</p>"
    rows = ""
    for li in d.line_items:
        rows += (
            "<tr>"
            f"<td>{_esc(li.name or '—')}</td>"
            f"<td class='num'>{li.quantity}</td>"
            f"<td class='num'>{_fmt_money(li.unit_price_cents)}</td>"
            f"<td class='num'>{_fmt_money(li.amount_cents)}</td>"
            "</tr>"
        )
    total = sum(li.amount_cents for li in d.line_items)
    return (
        "<div class='li-wrap'><table class='li'>"
        "<thead><tr><th>Item</th><th>Qty</th><th>Unit</th><th>Amount</th></tr></thead>"
        f"<tbody>{rows}</tbody>"
        f"<tfoot><tr><td colspan='3'>Total</td><td class='num'>{_fmt_money(total)}</td></tr></tfoot>"
        "</table></div>"
    )


def _assets_html(d: DealDetail) -> str:
    cards = ""
    have = {a.asset_type for a in d.assets}
    for a in d.assets:
        cards += (
            f'<a class="cta" href="{_esc(a.url)}" target="_blank" rel="noopener">'
            f'<span class="cta-kind">{_esc(a.label)}</span>'
            f'<span class="cta-go">{_esc(a.cta_label)} →</span></a>'
        )
    for t in _ASSET_ORDER:
        if t in have:
            continue
        cards += (
            f'<div class="cta cta--empty"><span class="cta-kind">{_esc(_ASSET_LABELS[t])}</span>'
            f'<span class="muted">Not linked yet</span></div>'
        )
    return f'<div class="ctas">{cards}</div>'


def _timeline_html(d: DealDetail) -> str:
    if not d.timeline:
        return '<p class="empty">No logged activity yet. Emails and meeting notes will appear here.</p>'
    items = ""
    for e in d.timeline[:25]:
        when = _fmt_date(e.when)
        items += (
            f'<li><div class="t-when">{_esc(when)}</div>'
            f'<div class="t-body"><strong>{_esc(e.title)}</strong>'
            f'<div class="muted">{_esc(e.detail)}</div></div></li>'
        )
    footer = ""
    if len(d.timeline) > 25:
        footer = f'<p class="muted" style="margin-top:10px;font-size:12px">Showing most recent 25 of {len(d.timeline)} events.</p>'
    return f"<ul class='timeline'>{items}</ul>{footer}"


_STYLES = """
  :root { --dark-blue:#2B3644; --light-blue:#85BBDA; --light-brown:#F9F7F3;
    --white:#FFF; --border:rgba(43,54,68,0.12); --shadow:rgba(43,54,68,0.10); }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--light-brown); color:var(--dark-blue);
    font-family:"Inter","Segoe UI",sans-serif; }
  a { color:var(--dark-blue); }
  __NAV__
  .shell { max-width:1180px; margin:0 auto; padding:24px 18px 64px; }
  .crumbs { font-size:12.5px; margin:0 0 12px; }
  .crumbs a { color:rgba(43,54,68,0.6); text-decoration:none; }
  .workspace { background:var(--white); border:1px solid var(--border);
    border-radius:20px; box-shadow:0 18px 40px var(--shadow); padding:24px 26px 28px; margin-bottom:18px; }
  .dealhead { display:flex; justify-content:space-between; align-items:flex-start; gap:16px; flex-wrap:wrap; }
  .dealhead-actions { display:flex; flex-direction:column; gap:10px; align-items:flex-end; }
  h1 { font-family:"Montserrat",sans-serif; font-weight:800; font-size:24px; margin:0 0 6px; }
  .eyebrow { font-family:"Montserrat",sans-serif; font-weight:700; font-size:11px;
    letter-spacing:0.08em; text-transform:uppercase; color:rgba(43,54,68,0.55); margin:0 0 4px; }
  .facts { display:flex; gap:22px; flex-wrap:wrap; margin:12px 0 0; }
  .fact .l { font-size:10.5px; text-transform:uppercase; letter-spacing:0.05em; color:rgba(43,54,68,0.5); }
  .fact .v { font-family:"Montserrat",sans-serif; font-weight:700; font-size:15px; }
  .hs-link { font-weight:600; font-size:12.5px; text-decoration:none; color:#2B3644;
    border:1px solid var(--border); border-radius:9px; padding:7px 12px; display:inline-block; background:var(--white); }
  .hs-link:hover { border-color:rgba(43,54,68,0.28); }
  h2 { font-family:"Montserrat",sans-serif; font-weight:800; font-size:15px; margin:0 0 12px; }
  .grid { display:grid; grid-template-columns: 1.3fr 1fr; gap:18px; }
  @media (max-width: 880px){ .grid { grid-template-columns:1fr; } }
  .muted { color:rgba(43,54,68,0.55); font-size:12.5px; }
  .empty { color:rgba(43,54,68,0.6); font-size:13.5px; }
  .nudge { border-left:4px solid var(--light-blue); background:rgba(133,187,218,0.12);
    border-radius:0 12px 12px 0; padding:14px 16px; margin:0 0 14px; }
  .nudge .l { font-family:"Montserrat",sans-serif; font-weight:700; font-size:11px; text-transform:uppercase;
    letter-spacing:0.06em; color:rgba(43,54,68,0.6); }
  .nudge .a { font-size:14.5px; font-weight:600; margin-top:3px; }
  .nudge--overdue { border-left-color:#b23b3b; background:rgba(178,59,59,0.08); }
  .check { list-style:none; padding:0; margin:0; display:grid; gap:8px; }
  .check li { display:flex; align-items:center; gap:9px; font-size:13.5px; }
  .check .ok { color:#2f8f5b; } .check .no { color:#b23b3b; }
  .li-wrap { overflow-x:auto; -webkit-overflow-scrolling:touch; }
  table.li { width:100%; border-collapse:collapse; font-size:13px; }
  table.li th,table.li td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }
  table.li td.num,table.li th:nth-child(n+2){ text-align:right; }
  table.li tfoot td { font-weight:700; }
  .records { list-style:none; padding:0; margin:0; display:grid; gap:12px; }
  .records li { border:1px solid var(--border); border-radius:12px; padding:11px 13px; }
  .ctas { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }
  @media (max-width: 720px){ .ctas { grid-template-columns:1fr; } }
  .cta { display:flex; flex-direction:column; gap:6px; border:1px solid var(--border); border-radius:14px;
    padding:14px; text-decoration:none; background:var(--white); }
  .cta:hover { border-color:rgba(43,54,68,0.28); box-shadow:0 8px 18px var(--shadow); }
  .cta-kind { font-family:"Montserrat",sans-serif; font-weight:700; font-size:13px; }
  .cta-go { color:#4A8FAD; font-weight:700; font-size:12.5px; }
  .cta--empty { background:var(--light-brown); }
  .timeline { list-style:none; padding:0; margin:0; display:grid; gap:12px; }
  .timeline li { display:grid; grid-template-columns:88px 1fr; gap:10px; }
  .t-when { font-size:12px; color:rgba(43,54,68,0.55); }
  .badge { font-size:11px; border-radius:8px; padding:2px 9px; font-weight:700; vertical-align:middle; }
  .badge--won { background:rgba(47,143,91,0.15); color:#2f8f5b; }
  .badge--open { background:rgba(133,187,218,0.2); color:#2B3644; }
  .draft-btn { display:inline-block; font:inherit; font-weight:700; font-size:13px;
    background:var(--dark-blue); color:#fff; border:none; border-radius:12px;
    padding:9px 18px; text-decoration:none; cursor:pointer; }
  .draft-btn:hover { opacity:0.88; }
  .action-cards { display:grid; gap:10px; }
  .action-card { border:1px solid var(--border); border-radius:14px; padding:14px 16px;
    display:flex; justify-content:space-between; align-items:flex-start; gap:14px; flex-wrap:wrap; }
  .action-card--mid { border-color:rgba(43,54,68,0.22); }
  .action-card--low { background:var(--light-brown); }
  .action-info { flex:1; }
  .action-label { font-weight:700; font-size:13.5px; }
  .action-desc { font-size:12.5px; color:rgba(43,54,68,0.6); margin-top:3px; }
  .approve-btn { font:inherit; font-weight:700; font-size:12.5px; background:var(--dark-blue);
    color:#fff; border:none; border-radius:10px; padding:8px 14px; cursor:pointer; white-space:nowrap; }
  .approve-btn:hover { opacity:0.88; }
  .flash { padding:12px 16px; border-radius:12px; font-size:13.5px; margin-bottom:14px; font-weight:600; }
  .flash--ok { background:rgba(47,143,91,0.12); border:1px solid #2f8f5b; color:#2f8f5b; }
  .flash--err { background:rgba(178,59,59,0.08); border:1px solid #b23b3b; color:#b23b3b; }
  .low-summary { font-size:12.5px; color:rgba(43,54,68,0.55); cursor:pointer; list-style:none;
    padding:6px 0 0; user-select:none; }
  .low-summary::-webkit-details-marker { display:none; }
  .low-summary::before { content:"▸"; margin-right:5px; display:inline-block; transition:transform 120ms ease; }
  details[open] .low-summary::before { transform:rotate(90deg); }
  details[open] .low-summary { margin-bottom:10px; }
"""


def _pending_actions_html(d: DealDetail) -> str:
    mid = [a for a in d.pending_actions if a.confidence == "mid"]
    low = [a for a in d.pending_actions if a.confidence == "low"]
    if not mid and not low:
        return ""
    cards = ""
    for a in mid:
        cards += (
            f'<div class="action-card action-card--mid">'
            f'<div class="action-info">'
            f'<div class="action-label">{_esc(a.label)}</div>'
            f'<div class="action-desc">{_esc(a.description)}</div>'
            f'</div>'
            f'<form method="post" action="/admin/sales/deals/{_esc(d.deal_id)}/actions/approve" style="margin:0">'
            f'<input type="hidden" name="action_id" value="{_esc(a.action_id)}">'
            f'<button class="approve-btn" type="submit">Approve →</button>'
            f'</form></div>'
        )
    if low:
        low_cards = ""
        for a in low:
            fix_btn = ""
            if a.link_url:
                fix_btn = (
                    f'<a href="{_esc(a.link_url)}" target="_blank" rel="noopener" '
                    f'class="hs-link" style="font-size:12px;padding:5px 10px;white-space:nowrap">'
                    f'Fix in HubSpot ↗</a>'
                )
            low_cards += (
                f'<div class="action-card action-card--low">'
                f'<div class="action-info">'
                f'<div class="action-label">{_esc(a.label)}</div>'
                f'<div class="action-desc muted">{_esc(a.description)}</div>'
                f'</div>{fix_btn}</div>'
            )
        cards += (
            f'<details>'
            f'<summary class="low-summary">{len(low)} low-confidence suggestion{"s" if len(low) != 1 else ""}</summary>'
            f'{low_cards}'
            f'</details>'
        )
    subtitle = (
        "Mid-confidence writes are ready to push to HubSpot with one click."
        if mid else
        "Low-confidence suggestions — review and act manually in HubSpot."
    )
    return (
        f'<div class="workspace">'
        f'<h2>Pending actions</h2>'
        f'<p class="muted" style="margin-top:-6px;margin-bottom:12px">{subtitle}</p>'
        f'<div class="action-cards">{cards}</div>'
        f'</div>'
    )


def render_deal_detail_page(d: DealDetail, *, user: dict | None = None, flash: str = "", flash_ok: bool = True) -> str:
    styles = _STYLES.replace("__NAV__", render_agent_nav_styles())

    badge = (
        '<span class="badge badge--won">Won</span>'
        if d.is_won
        else ('<span class="badge badge--open">Open</span>' if not d.is_closed else '<span class="badge">Closed</span>')
    )
    open_in_hs = _hs_link(d.deal_url, "Open deal in HubSpot")

    facts = (
        f'<div class="fact"><div class="l">Stage</div><div class="v">{_esc(d.stage or "—")}</div></div>'
        f'<div class="fact"><div class="l">Amount</div><div class="v">{_fmt_money(d.amount_cents)}</div></div>'
        f'<div class="fact"><div class="l">Close date</div><div class="v">{_fmt_date(d.close_date)}'
        + (' · <span style="color:#b23b3b">overdue</span>' if d.overdue else "")
        + "</div></div>"
        f'<div class="fact"><div class="l">Owner</div><div class="v">{_esc(_owner_display(d.owner_email))}</div></div>'
        f'<div class="fact"><div class="l">Company</div><div class="v">'
        + (_hs_link(d.company_url, d.company_name) if d.company_name else "—")
        + "</div></div>"
    )

    checklist_items = [
        ("amount", "Deal amount set"),
        ("line items", "Line items (what we're offering + costs)"),
        ("contacts", "At least one contact"),
        ("close date", "Close date set"),
    ]
    checks = ""
    for key, label in checklist_items:
        ok = key not in d.missing
        mark = '<span class="ok">✓</span>' if ok else '<span class="no">✗</span>'
        fix = "" if ok else " " + _hs_link(d.deal_url, "Fix in HubSpot")
        checks += f"<li>{mark} {_esc(label)}{fix}</li>"

    nudge_cls = "nudge nudge--overdue" if d.overdue else "nudge"

    flash_html = ""
    if flash:
        cls = "flash flash--ok" if flash_ok else "flash flash--err"
        flash_html = f'<div class="{cls}">{_esc(flash)}</div>'

    draft_btn = ""
    if not d.is_closed:
        draft_btn = (
            f'<a href="/admin/sales/deals/{_esc(d.deal_id)}/draft-followup" class="draft-btn">'
            f'✉ Draft follow-up email →</a>'
        )

    pending_actions_html = _pending_actions_html(d)

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | {_esc(d.name or 'Deal')}</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>{styles}</style>
  </head>
  <body>
    {render_agent_nav("sales", sales_section="sales_deals", user=user)}
    <main class="shell">
      <div class="crumbs"><a href="/admin/sales/deals">← Deal Board</a></div>
      {flash_html}
      <div class="workspace">
        <div class="dealhead">
          <div>
            <p class="eyebrow">Sales Priorities — HubSpot companion</p>
            <h1>{_esc(d.name or '(untitled deal)')} {badge}</h1>
            <div class="facts">{facts}</div>
          </div>
          <div class="dealhead-actions">{open_in_hs}{draft_btn}</div>
        </div>
      </div>

      <div class="workspace">
        <div class="{nudge_cls}">
          <div class="l">Next action</div>
          <div class="a">{_esc(d.next_action)}</div>
        </div>
        <h2>Deal readiness</h2>
        <ul class="check">{checks}</ul>
      </div>

      {pending_actions_html}

      <div class="grid">
        <div class="workspace">
          <h2>Line items &amp; costs</h2>
          {_line_items_html(d)}
        </div>
        <div class="workspace">
          <h2>Contacts</h2>
          {_contacts_html(d)}
        </div>
      </div>

      <div class="workspace">
        <h2>Closing tools</h2>
        <p class="muted" style="margin-top:-6px">The three assets a rep sends to close — generate in <a href="/admin/fulfillment/sales">Fulfillment</a> or <a href="/admin/sales-decks">Sales Decks</a>, then link to this deal.</p>
        {_assets_html(d)}
      </div>

      <div class="workspace">
        <h2>Activity</h2>
        {_timeline_html(d)}
      </div>
    </main>
  </body>
</html>"""
