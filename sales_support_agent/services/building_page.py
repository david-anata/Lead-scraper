"""Server-rendered Building Control Room."""

from __future__ import annotations

import html
from collections import Counter
from typing import Any

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)


def _esc(value: Any) -> str:
    return html.escape(str(value or ""))


def _badge(value: str) -> str:
    normalized = str(value or "unknown").replace("_", " ")
    tone = "ok" if normalized in {"available", "active", "subscribed", "sent"} else (
        "warn" if normalized in {"soft hold", "previewed", "approved", "sending", "unknown"} else "muted"
    )
    return f'<span class="badge badge--{tone}">{_esc(normalized)}</span>'


def render_building_page(
    *,
    user: dict,
    spaces: list[dict[str, Any]],
    offerings: list[dict[str, Any]],
    contacts: list[dict[str, Any]],
    segments: list[dict[str, Any]],
    campaigns: list[dict[str, Any]],
    inquiries: list[dict[str, Any]],
    reservations: list[dict[str, Any]],
    invoices: list[dict[str, Any]],
) -> str:
    nav = render_agent_nav("building", user=user)
    nav_styles = render_agent_nav_styles()
    favicons = render_agent_favicon_links()
    availability = Counter(str(item.get("status") or "unknown") for item in spaces)
    active_tenants = sum(
        1
        for contact in contacts
        if any(
            rel.get("type") in {"tenant", "tenant_employee"} and rel.get("status") == "active"
            for rel in contact.get("relationships", [])
        )
    )
    subscribed = sum(1 for item in contacts if item.get("marketing_status") == "subscribed")
    needs_response = sum(1 for item in inquiries if item.get("status") in {"new", "crm_sync_needed"})
    active_reservations = sum(
        1
        for item in reservations
        if item.get("status") not in {"completed", "cancelled", "expired"}
    )
    open_invoice_cents = sum(
        max(0, int(item.get("amount_due_cents") or 0) - int(item.get("amount_paid_cents") or 0))
        for item in invoices
        if item.get("status") not in {"paid", "void", "uncollectible"}
    )

    space_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("name"))}</strong><span class="sub">{_esc(item.get("space_type"))}</span></td>
          <td>{_esc(item.get("floor") or "—")}</td>
          <td>{_esc(item.get("capacity") or "—")}</td>
          <td>{_badge(str(item.get("status") or "unknown"))}</td>
          <td>{'Published' if item.get('is_public') else 'Internal only'}</td>
        </tr>
        """
        for item in spaces
    ) or '<tr><td colspan="5"><div class="empty"><strong>No spaces entered yet.</strong><br>Add the reviewed room inventory before publishing availability.</div></td></tr>'

    contact_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("full_name") or item.get("email"))}</strong><span class="sub">{_esc(item.get("email"))}</span></td>
          <td>{_esc(", ".join(sorted({rel.get("type", "") for rel in item.get("relationships", []) if rel.get("type")})) or "No relationship")}</td>
          <td>{_badge(str(item.get("marketing_status") or "unknown"))}</td>
          <td>{_badge("suppressed") if item.get("suppressed") else "Allowed"}</td>
        </tr>
        """
        for item in contacts
    ) or '<tr><td colspan="4"><div class="empty"><strong>No building contacts yet.</strong><br>Connected website inquiries will create CRM contacts automatically.</div></td></tr>'

    segment_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("name"))}</strong><span class="sub">{_esc(item.get("description"))}</span></td>
          <td>{_esc(", ".join(item.get("relationship_types", [])) or "—")}</td>
          <td>{_esc(item.get("included_count", 0))}</td>
          <td>{_badge("active" if item.get("is_active") else "inactive")}</td>
        </tr>
        """
        for item in segments
    ) or '<tr><td colspan="4"><div class="empty"><strong>No audiences defined.</strong><br>Create reviewed segments before drafting a tenant campaign.</div></td></tr>'

    campaign_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("name"))}</strong><span class="sub">{_esc(item.get("subject"))}</span></td>
          <td>{_esc(item.get("segment_name") or "—")}</td>
          <td>{_esc(item.get("recipient_count", 0))}</td>
          <td>{_badge(str(item.get("status") or "draft"))}</td>
        </tr>
        """
        for item in campaigns
    ) or '<tr><td colspan="4"><div class="empty"><strong>No campaigns yet.</strong><br>Campaigns require a segment preview, test send, and approval before delivery.</div></td></tr>'

    inquiry_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("name"))}</strong><span class="sub">{_esc(item.get("email"))}</span></td>
          <td>{_esc(item.get("kind"))}</td>
          <td>{_esc(item.get("preferred_date") or "—")}</td>
          <td>{_badge(str(item.get("status") or "new"))}</td>
          <td>{_esc(item.get("source"))}</td>
        </tr>
        """
        for item in inquiries
    ) or '<tr><td colspan="5"><div class="empty"><strong>No building inquiries yet.</strong><br>The public forms remain usable through their safe delivery fallback.</div></td></tr>'

    reservation_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("space_name"))}</strong><span class="sub">{_esc(item.get("kind"))}</span></td>
          <td>{_esc(item.get("starts_at"))}</td>
          <td>{_badge(str(item.get("status") or "inquiry"))}</td>
          <td>{_badge(str(item.get("agreement_status") or "not started"))}</td>
          <td>{_badge(str(item.get("deposit_status") or "not started"))}</td>
        </tr>
        """
        for item in reservations
    ) or '<tr><td colspan="5"><div class="empty"><strong>No bookings or holds yet.</strong><br>An inquiry remains a lead until an operator starts the appropriate booking workflow.</div></td></tr>'

    invoice_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("description"))}</strong></td>
          <td>{_esc(str(item.get("currency") or "usd").upper())} {int(item.get("amount_due_cents") or 0) / 100:,.2f}</td>
          <td>{_esc(str(item.get("currency") or "usd").upper())} {int(item.get("amount_paid_cents") or 0) / 100:,.2f}</td>
          <td>{_badge(str(item.get("status") or "draft"))}</td>
          <td>{_badge(str(item.get("accounting_status") or "pending qbo"))}</td>
          <td>{f'<a href="{_esc(item.get("hosted_invoice_url"))}" target="_blank" rel="noreferrer">Open ↗</a>' if item.get("hosted_invoice_url") else "—"}</td>
        </tr>
        """
        for item in invoices
    ) or '<tr><td colspan="6"><div class="empty"><strong>No native invoices yet.</strong><br>Approved billing schedules can create Stripe invoices; QBO remains the accounting destination during transition.</div></td></tr>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Building Control · agent</title>
  {favicons}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
  <style>
    :root{{--ink:#2B3644;--paper:#F9F7F3;--white:#fff;--sky:#85BBDA;--teal:#18776f;--amber:#9b650e;--border:rgba(43,54,68,.14);}}
    {nav_styles}
    body{{margin:0;background:linear-gradient(180deg,#eef6fa 0,#f9f7f3 320px);color:var(--ink);font-family:"Inter","Segoe UI",sans-serif;}}
    .shell{{max-width:1320px;margin:0 auto;padding:42px 24px 80px;}}
    .page-head{{display:flex;align-items:end;justify-content:space-between;gap:28px;margin-bottom:26px;}}
    .eyebrow{{font-size:12px;font-weight:700;letter-spacing:.15em;text-transform:uppercase;color:#397a9d;}}
    h1,h2{{font-family:"Montserrat",sans-serif;margin:0;letter-spacing:-.045em;}}
    h1{{font-size:clamp(32px,4vw,52px);margin-top:8px;}} h2{{font-size:22px;}}
    .purpose{{max-width:700px;color:rgba(43,54,68,.7);line-height:1.6;margin:12px 0 0;}}
    .site-link{{display:inline-flex;min-height:42px;align-items:center;padding:0 16px;border:1px solid var(--border);border-radius:9px;background:#fff;color:var(--ink);font-weight:700;text-decoration:none;}}
    .metrics{{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--border);border-radius:14px;background:#fff;overflow:hidden;}}
    .metric{{padding:22px;border-right:1px solid var(--border);}} .metric:last-child{{border:0;}}
    .metric span{{display:block;font-size:12px;color:rgba(43,54,68,.6);text-transform:uppercase;letter-spacing:.08em;}}
    .metric strong{{display:block;font-family:"Montserrat";font-size:30px;margin-top:8px;}}
    .notice{{margin-top:18px;padding:16px 18px;border:1px solid rgba(155,101,14,.28);border-radius:12px;background:#fff8e8;line-height:1.55;}}
    .grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:20px;}}
    .panel{{background:#fff;border:1px solid var(--border);border-radius:14px;overflow:hidden;}}
    .panel--wide{{grid-column:1/-1;}} .panel-head{{display:flex;align-items:center;justify-content:space-between;padding:20px 22px;border-bottom:1px solid var(--border);}}
    .panel-head p{{margin:4px 0 0;color:rgba(43,54,68,.62);font-size:13px;}}
    .count{{font-variant-numeric:tabular-nums;font-weight:700;color:#397a9d;}}
    .table-wrap{{overflow:auto;}} table{{width:100%;border-collapse:collapse;font-size:14px;}}
    th{{padding:12px 18px;text-align:left;background:#f8f8f6;color:rgba(43,54,68,.62);font-size:11px;text-transform:uppercase;letter-spacing:.08em;}}
    td{{padding:15px 18px;border-top:1px solid rgba(43,54,68,.09);vertical-align:top;}}
    .sub{{display:block;margin-top:4px;color:rgba(43,54,68,.58);font-size:12px;max-width:380px;}}
    .badge{{display:inline-block;border-radius:99px;padding:5px 8px;font-size:11px;font-weight:700;text-transform:capitalize;background:#edf0f2;}}
    .badge--ok{{background:#e4f4f1;color:#11665f;}} .badge--warn{{background:#fff0d2;color:#845407;}} .badge--muted{{background:#edf0f2;color:#56616d;}}
    .empty{{padding:18px 0;color:rgba(43,54,68,.62);line-height:1.55;}}
    @media(max-width:900px){{.metrics{{grid-template-columns:1fr 1fr}}.metric:nth-child(2){{border-right:0}}.metric:nth-child(-n+2){{border-bottom:1px solid var(--border)}}.grid{{grid-template-columns:1fr}}.panel--wide{{grid-column:auto}}}}
    @media(max-width:600px){{.page-head{{align-items:start;flex-direction:column}}.metrics{{grid-template-columns:1fr}}.metric{{border-right:0;border-bottom:1px solid var(--border)!important}}.metric:last-child{{border-bottom:0!important}}.shell{{padding-inline:16px}}}}
  </style>
</head>
<body>
  {nav}
  <main class="shell">
    <header class="page-head">
      <div>
        <div class="eyebrow">Building operations</div>
        <h1>Building Control</h1>
        <p class="purpose">One operational view of sellable space, incoming demand, tenant relationships, communication permission, and campaign readiness.</p>
      </div>
      <a class="site-link" href="https://anata-building.vercel.app" target="_blank" rel="noreferrer">Open public site ↗</a>
    </header>
    <section class="metrics" aria-label="Building summary">
      <div class="metric"><span>Available spaces</span><strong>{availability.get("available", 0)}</strong></div>
      <div class="metric"><span>Needs response</span><strong>{needs_response}</strong></div>
      <div class="metric"><span>Tenant relationships</span><strong>{active_tenants}</strong></div>
      <div class="metric"><span>Open invoicing</span><strong>${open_invoice_cents / 100:,.0f}</strong></div>
    </section>
    <div class="notice"><strong>Data readiness:</strong> public availability stays conservative until reviewed spaces and offerings are entered. Campaign delivery stays locked behind permission, preview, approval, suppression, and provider configuration.</div>
    <div class="grid">
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Incoming inquiries</h2><p>New workspace, tour, and event demand.</p></div><span class="count">{len(inquiries)} records</span></div><div class="table-wrap"><table><thead><tr><th>Contact</th><th>Journey</th><th>Preferred date</th><th>Status</th><th>Source</th></tr></thead><tbody>{inquiry_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Bookings and holds</h2><p>Commercial state, agreement evidence, and deposit readiness stay distinct.</p></div><span class="count">{active_reservations} active</span></div><div class="table-wrap"><table><thead><tr><th>Space</th><th>Starts</th><th>Workflow</th><th>Agreement</th><th>Deposit</th></tr></thead><tbody>{reservation_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Billing and collections</h2><p>Provider-confirmed payment evidence stays separate from the QBO accounting handoff.</p></div><span class="count">{len(invoices)} invoices</span></div><div class="table-wrap"><table><thead><tr><th>Invoice</th><th>Due</th><th>Paid</th><th>Collection</th><th>Accounting</th><th>Link</th></tr></thead><tbody>{invoice_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Inventory</h2><p>Agent-owned space status and public readiness.</p></div><span class="count">{len(spaces)} spaces · {len(offerings)} offerings</span></div><div class="table-wrap"><table><thead><tr><th>Space</th><th>Floor</th><th>Capacity</th><th>Status</th><th>Visibility</th></tr></thead><tbody>{space_rows}</tbody></table></div></section>
      <section class="panel"><div class="panel-head"><div><h2>CRM and email list</h2><p>Relationships, permission, and suppression. {subscribed} subscribed.</p></div><span class="count">{len(contacts)} contacts</span></div><div class="table-wrap"><table><thead><tr><th>Contact</th><th>Relationships</th><th>Marketing</th><th>Delivery</th></tr></thead><tbody>{contact_rows}</tbody></table></div></section>
      <section class="panel"><div class="panel-head"><div><h2>Audiences</h2><p>Explainable tenant and community segments.</p></div><span class="count">{len(segments)} segments</span></div><div class="table-wrap"><table><thead><tr><th>Audience</th><th>Relationships</th><th>Eligible</th><th>Status</th></tr></thead><tbody>{segment_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Campaigns</h2><p>Draft, preview, approval, and delivery state.</p></div><span class="count">{len(campaigns)} campaigns</span></div><div class="table-wrap"><table><thead><tr><th>Campaign</th><th>Audience</th><th>Recipients</th><th>Status</th></tr></thead><tbody>{campaign_rows}</tbody></table></div></section>
    </div>
  </main>
</body>
</html>"""
