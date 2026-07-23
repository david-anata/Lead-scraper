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
    billing_accounts: list[dict[str, Any]],
    billing_schedules: list[dict[str, Any]],
    csrf_token: str = "",
    notice: str = "",
    error: str = "",
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

    def campaign_actions(item: dict[str, Any]) -> str:
        campaign_id = _esc(item.get("id"))
        status = str(item.get("status") or "draft")
        if status in {"draft", "previewed"}:
            approve = (
                f'<form method="post" action="/admin/building/campaigns/{campaign_id}/approve">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                '<button class="secondary secondary--small" type="submit">Approve snapshot</button></form>'
                if status == "previewed"
                else ""
            )
            return (
                '<div class="action-stack">'
                f'<form method="post" action="/admin/building/campaigns/{campaign_id}/preview">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                '<button class="secondary secondary--small" type="submit">Refresh preview</button></form>'
                f'<form class="inline-send" method="post" action="/admin/building/campaigns/{campaign_id}/test-send">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                '<input aria-label="Test recipient email" name="test_email" type="email" required placeholder="Test email">'
                '<button class="secondary secondary--small" type="submit">Send test</button></form>'
                f"{approve}</div>"
            )
        if status == "approved":
            return (
                f'<form class="inline-send" method="post" action="/admin/building/campaigns/{campaign_id}/send">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                f'<input aria-label="Send confirmation" name="confirmation" required placeholder="SEND {campaign_id}">'
                '<button class="primary secondary--small" type="submit">Send campaign</button></form>'
            )
        return '<span class="sub">No action required</span>'

    campaign_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("name"))}</strong><span class="sub">{_esc(item.get("subject"))}</span></td>
          <td>{_esc(item.get("segment_name") or "—")}</td>
          <td>{_esc(item.get("recipient_count", 0))}</td>
          <td>{_badge(str(item.get("status") or "draft"))}</td>
          <td>{campaign_actions(item)}</td>
        </tr>
        """
        for item in campaigns
    ) or '<tr><td colspan="5"><div class="empty"><strong>No campaigns yet.</strong><br>Campaigns require a segment preview, test send, and approval before delivery.</div></td></tr>'

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
          <td>
            <details class="row-actions"><summary>Manage</summary>
              <form method="post" action="/admin/building/reservations/{_esc(item.get("id"))}/transition">
                <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                <label>Next state<select name="target_status" required><option value="">Choose</option>{''.join(f'<option value="{_esc(state)}">{_esc(state.replace("_", " "))}</option>' for state in item.get("allowed_next", []))}</select></label>
                <label>Hold expires<input type="datetime-local" name="hold_expires_at"></label>
                <label>Reason<input name="reason" placeholder="Required context"></label>
                <button class="secondary secondary--small" type="submit">Move workflow</button>
              </form>
              <form method="post" action="/admin/building/reservations/{_esc(item.get("id"))}/agreements">
                <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                <label>Agreement<select name="status"><option value="draft">Draft</option><option value="sent">Sent</option><option value="signed">Signed</option><option value="voided">Voided</option></select></label>
                <label>Version<input type="number" name="version" min="1" value="1"></label>
                <label>Provider<input name="provider" placeholder="Dropbox Sign, manual"></label>
                <label>Evidence reference<input name="provider_reference" placeholder="Required when signed"></label>
                <label>Document URL<input type="url" name="document_url"></label>
                <button class="secondary secondary--small" type="submit">Record agreement</button>
              </form>
              <form method="post" action="/admin/building/reservations/{_esc(item.get("id"))}/deposits">
                <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                <label>Deposit<select name="status"><option value="due">Due</option><option value="pending">Pending</option><option value="paid">Paid</option><option value="refunded">Refunded</option><option value="waived">Waived</option></select></label>
                <label>Amount<input name="amount" inputmode="decimal" placeholder="500.00"></label>
                <label>Provider<input name="provider" placeholder="Stripe, check"></label>
                <label>Evidence reference<input name="provider_reference" placeholder="Required when paid/refunded"></label>
                <button class="secondary secondary--small" type="submit">Record deposit</button>
              </form>
            </details>
          </td>
        </tr>
        """
        for item in reservations
    ) or '<tr><td colspan="6"><div class="empty"><strong>No bookings or holds yet.</strong><br>An inquiry remains a lead until an operator starts the appropriate booking workflow.</div></td></tr>'

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

    linked_space_options = "".join(
        f'<option value="{_esc(item.get("id"))}">{_esc(item.get("name"))}</option>'
        for item in spaces
    )
    offering_options = "".join(
        f'<option value="{_esc(item.get("id"))}">{_esc(item.get("name") or item.get("id"))}</option>'
        for item in offerings
    )
    contact_options = "".join(
        f'<option value="{_esc(item.get("id"))}">{_esc(item.get("full_name") or item.get("email"))}</option>'
        for item in contacts
    )
    billing_account_options = "".join(
        f'<option value="{_esc(item.get("id"))}">{_esc(item.get("account_name"))}</option>'
        for item in billing_accounts
    )
    reservation_options = "".join(
        f'<option value="{_esc(item.get("id"))}">{_esc(item.get("space_name"))} · {_esc(item.get("starts_at"))}</option>'
        for item in reservations
    )
    billing_schedule_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("description"))}</strong><span class="sub">{_esc(item.get("schedule_type"))} · {_esc(item.get("billing_account_id"))}</span></td>
          <td>{_esc(str(item.get("currency") or "usd").upper())} {int(item.get("amount_cents") or 0) / 100:,.2f}</td>
          <td>{_esc(item.get("next_invoice_on") or "—")}</td>
          <td>{_badge(str(item.get("status") or "draft"))}</td>
          <td>{(
            f'<form class="inline-send" method="post" action="/admin/building/billing/schedules/{_esc(item.get("id"))}/approve"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><button class="secondary secondary--small" type="submit">Approve and lock</button></form>'
            if item.get("status") == "draft"
            else (
              f'<form class="inline-send" method="post" action="/admin/building/billing/schedules/{_esc(item.get("id"))}/invoice"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input aria-label="Invoice confirmation" name="confirmation" required placeholder="INVOICE {_esc(item.get("id"))}"><button class="primary secondary--small" type="submit">Create invoice</button></form>'
              if item.get("status") == "approved"
              else '<span class="sub">No action required</span>'
            )
          )}</td>
        </tr>
        """
        for item in billing_schedules
    ) or '<tr><td colspan="5"><div class="empty"><strong>No billing schedules yet.</strong><br>Create a reviewed draft, then approve it before any provider invoice can be created.</div></td></tr>'
    segment_options = "".join(
        f'<option value="{_esc(item.get("id"))}">{_esc(item.get("name"))} ({_esc(item.get("included_count", 0))} eligible)</option>'
        for item in segments
        if item.get("is_active")
    )
    flash = (
        f'<div class="flash flash--ok" role="status">{_esc(notice)}</div>'
        if notice
        else (
            f'<div class="flash flash--error" role="alert">{_esc(error)}</div>'
            if error
            else ""
        )
    )

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
    .flash{{margin:0 0 18px;padding:14px 16px;border-radius:10px;font-weight:700;}} .flash--ok{{background:#e4f4f1;color:#11665f;border:1px solid #acd8d2;}} .flash--error{{background:#fff0ed;color:#8b2f23;border:1px solid #e4b3aa;}}
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
    .form-grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;padding:20px 22px;}} .field{{display:grid;gap:6px;}} .field--wide{{grid-column:1/-1;}}
    label{{font-size:12px;font-weight:700;color:rgba(43,54,68,.72);}} input,select,textarea{{box-sizing:border-box;width:100%;min-height:42px;border:1px solid rgba(43,54,68,.22);border-radius:8px;background:#fff;padding:10px 11px;color:var(--ink);font:inherit;}} textarea{{min-height:92px;resize:vertical;}} input:focus,select:focus,textarea:focus{{outline:3px solid rgba(133,187,218,.34);border-color:#397a9d;}}
    .check{{display:flex;align-items:center;gap:9px;font-size:13px;}} .check input{{width:18px;min-height:18px;}} .check-stack{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 14px;padding:11px;border:1px solid rgba(43,54,68,.14);border-radius:8px;}} .form-actions{{grid-column:1/-1;display:flex;align-items:center;justify-content:space-between;gap:14px;border-top:1px solid var(--border);padding-top:16px;}} .form-note{{font-size:12px;color:rgba(43,54,68,.62);line-height:1.45;}} .primary,.secondary{{min-height:42px;border:0;border-radius:8px;background:var(--ink);color:#fff;padding:0 17px;font-weight:700;cursor:pointer;}} .primary:hover{{background:#17222d;}} .secondary{{border:1px solid var(--border);background:#fff;color:var(--ink);}} .secondary--small{{min-height:34px;padding:0 11px;font-size:12px;white-space:nowrap;}} .action-stack{{display:grid;gap:7px;min-width:210px;}} .inline-send{{display:flex;gap:6px;align-items:center;}} .inline-send input{{min-height:34px;padding:7px 8px;font-size:12px;}}
    .row-actions{{min-width:220px;}} .row-actions summary{{cursor:pointer;font-weight:700;color:#397a9d;}} .row-actions form{{display:grid;gap:7px;margin-top:10px;padding:10px;border:1px solid var(--border);border-radius:9px;background:#f8f8f6;}} .row-actions label{{display:grid;gap:4px;}} .row-actions input,.row-actions select{{min-height:34px;padding:7px 8px;font-size:12px;}}
    @media(max-width:900px){{.metrics{{grid-template-columns:1fr 1fr}}.metric:nth-child(2){{border-right:0}}.metric:nth-child(-n+2){{border-bottom:1px solid var(--border)}}.grid{{grid-template-columns:1fr}}.panel--wide{{grid-column:auto}}}}
    @media(max-width:600px){{.page-head{{align-items:start;flex-direction:column}}.metrics{{grid-template-columns:1fr}}.metric{{border-right:0;border-bottom:1px solid var(--border)!important}}.metric:last-child{{border-bottom:0!important}}.shell{{padding-inline:16px}}.form-grid{{grid-template-columns:1fr}}.field--wide{{grid-column:auto}}.form-actions{{grid-column:auto;align-items:stretch;flex-direction:column}}}}
  </style>
</head>
<body>
  {nav}
  <main class="shell">
    {flash}
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
      <section class="panel">
        <div class="panel-head"><div><h2>Add or update a space</h2><p>Save reviewed physical inventory. Publishing remains a separate choice.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/spaces">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="space-id">Stable ID</label><input id="space-id" name="space_id" required placeholder="office-201"></div>
          <div class="field"><label for="space-slug">Public URL slug</label><input id="space-slug" name="slug" required pattern="[a-z0-9]+(?:-[a-z0-9]+)*" placeholder="office-201"></div>
          <div class="field"><label for="space-name">Name</label><input id="space-name" name="name" required placeholder="Office 201"></div>
          <div class="field"><label for="space-type">Type</label><select id="space-type" name="space_type"><option value="private_office">Private office</option><option value="coworking">Coworking</option><option value="conference">Conference room</option><option value="event">Event space</option><option value="warehouse">Warehouse</option><option value="amenity">Amenity</option></select></div>
          <div class="field"><label for="space-floor">Floor or area</label><input id="space-floor" name="floor" placeholder="Second floor"></div>
          <div class="field"><label for="space-capacity">Capacity</label><input id="space-capacity" name="capacity" type="number" min="0" value="0"></div>
          <div class="field"><label for="space-status">Availability state</label><select id="space-status" name="status"><option value="unavailable">Unavailable</option><option value="available">Available</option><option value="soft_hold">Soft hold</option><option value="occupied">Occupied</option><option value="maintenance">Maintenance</option></select></div>
          <div class="field"><label for="space-features">Features</label><input id="space-features" name="features" placeholder="Natural light, furnished, whiteboard"></div>
          <div class="field field--wide"><label for="space-description">Public description</label><textarea id="space-description" name="public_description" placeholder="What a prospective tenant may safely see."></textarea></div>
          <div class="field field--wide"><label for="space-notes">Internal notes</label><textarea id="space-notes" name="internal_notes" placeholder="Occupancy, repairs, or operator context. Never shown publicly."></textarea></div>
          <div class="form-actions"><label class="check"><input type="checkbox" name="is_public" value="true"> Allow this space to appear publicly</label><button class="primary" type="submit">Save space</button></div>
        </form>
      </section>
      <section class="panel">
        <div class="panel-head"><div><h2>Add or update an offering</h2><p>Define what customers can inquire about and how pricing is described.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/offerings">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="offering-id">Stable ID</label><input id="offering-id" name="offering_id" required placeholder="private-office-201"></div>
          <div class="field"><label for="offering-slug">Public URL slug</label><input id="offering-slug" name="slug" required pattern="[a-z0-9]+(?:-[a-z0-9]+)*" placeholder="private-office-201"></div>
          <div class="field"><label for="offering-name">Name</label><input id="offering-name" name="name" required placeholder="Private Office 201"></div>
          <div class="field"><label for="offering-type">Offering type</label><select id="offering-type" name="offering_type"><option value="private_office">Private office</option><option value="coworking">Coworking</option><option value="meeting_room">Meeting room</option><option value="event">Event</option><option value="warehouse">Warehouse</option><option value="membership">Membership</option></select></div>
          <div class="field"><label for="offering-space">Linked space</label><select id="offering-space" name="space_id"><option value="">No specific space</option>{linked_space_options}</select></div>
          <div class="field"><label for="offering-price">Public price wording</label><input id="offering-price" name="price_display" placeholder="From $1,250/month"></div>
          <div class="field"><label for="offering-unit">Booking unit</label><select id="offering-unit" name="booking_unit"><option value="custom">Custom</option><option value="month">Monthly</option><option value="day">Daily</option><option value="hour">Hourly</option><option value="event">Per event</option></select></div>
          <div class="field"><label for="offering-cta">Call to action</label><select id="offering-cta" name="call_to_action"><option value="inquire">Inquire</option><option value="tour">Schedule a tour</option><option value="request_date">Request a date</option><option value="join_waitlist">Join waitlist</option></select></div>
          <div class="field field--wide"><label for="offering-features">Included features</label><input id="offering-features" name="features" placeholder="Conference access, mail service, Boom Standard"></div>
          <div class="field field--wide"><label for="offering-description">Public description</label><textarea id="offering-description" name="public_description" placeholder="Warm, specific copy for the public offering."></textarea></div>
          <div class="form-actions"><span class="form-note">Publish only after the linked space, price wording, and copy have been reviewed.</span><label class="check"><input type="checkbox" name="is_published" value="true"> Publish offering</label><button class="primary" type="submit">Save offering</button></div>
        </form>
      </section>
      <section class="panel">
        <div class="panel-head"><div><h2>Add a CRM relationship</h2><p>One person can be a tenant, prospect, event host, or community member without duplication.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/contacts">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="contact-name">Full name</label><input id="contact-name" name="full_name" placeholder="Taylor Morgan"></div>
          <div class="field"><label for="contact-email">Email</label><input id="contact-email" name="email" type="email" required placeholder="taylor@example.com"></div>
          <div class="field"><label for="contact-phone">Phone</label><input id="contact-phone" name="phone" type="tel"></div>
          <div class="field"><label for="contact-company">Company</label><input id="contact-company" name="company_name"></div>
          <div class="field"><label for="contact-relationship">Relationship</label><select id="contact-relationship" name="relationship_type"><option value="prospect">Prospect</option><option value="tenant">Tenant</option><option value="tenant_employee">Tenant employee</option><option value="event_host">Event host</option><option value="former_tenant">Former tenant</option><option value="waitlist">Waitlist</option><option value="vendor">Vendor</option><option value="partner">Partner</option><option value="community_member">Community member</option></select></div>
          <div class="field"><label for="contact-org">Relationship organization</label><input id="contact-org" name="organization" placeholder="Company or tenant account"></div>
          <div class="field"><label for="contact-reference">Source reference</label><input id="contact-reference" name="source_reference" placeholder="Lease, Eventective, Marketplace"></div>
          <div class="field"><label for="contact-marketing">Marketing permission</label><select id="contact-marketing" name="marketing_status"><option value="unknown">Unknown / no promotional email</option><option value="subscribed">Subscribed</option><option value="unsubscribed">Unsubscribed</option></select></div>
          <div class="form-actions"><label class="check"><input type="checkbox" name="consent_confirmed" value="true"> I have documented consent for “Subscribed”</label><button class="primary" type="submit">Save contact</button></div>
        </form>
      </section>
      <section class="panel">
        <div class="panel-head"><div><h2>Build an audience</h2><p>Audience rules remain explainable and respect permission and suppression.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/segments">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="segment-id">Stable ID</label><input id="segment-id" name="segment_id" required placeholder="active-tenants"></div>
          <div class="field"><label for="segment-name">Audience name</label><input id="segment-name" name="name" required placeholder="Active tenants"></div>
          <div class="field field--wide"><label for="segment-description">Description</label><input id="segment-description" name="description" placeholder="People currently working from the building."></div>
          <div class="field field--wide"><label>Relationships</label><div class="check-stack"><label class="check"><input type="checkbox" name="relationship_types" value="tenant"> Tenant</label><label class="check"><input type="checkbox" name="relationship_types" value="tenant_employee"> Tenant employee</label><label class="check"><input type="checkbox" name="relationship_types" value="event_host"> Event host</label><label class="check"><input type="checkbox" name="relationship_types" value="prospect"> Prospect</label><label class="check"><input type="checkbox" name="relationship_types" value="community_member"> Community member</label><label class="check"><input type="checkbox" name="relationship_types" value="former_tenant"> Former tenant</label></div></div>
          <div class="field"><label for="segment-relationship-status">Relationship state</label><select id="segment-relationship-status" name="relationship_status"><option value="active">Active only</option><option value="any">Any</option><option value="inactive">Inactive only</option></select></div>
          <div class="field"><label>Marketing status</label><div class="check-stack"><label class="check"><input type="checkbox" name="marketing_statuses" value="subscribed" checked> Subscribed</label><label class="check"><input type="checkbox" name="marketing_statuses" value="unknown"> Unknown</label><label class="check"><input type="checkbox" name="marketing_statuses" value="unsubscribed"> Unsubscribed</label></div></div>
          <div class="form-actions"><label class="check"><input type="checkbox" name="is_active" value="true" checked> Audience active</label><button class="primary" type="submit">Save and preview audience</button></div>
        </form>
      </section>
      <section class="panel panel--wide">
        <div class="panel-head"><div><h2>Draft a campaign</h2><p>Saving creates a draft only. Preview, test send, approval, and final send remain separate gates.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/campaigns">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="campaign-id">Stable ID</label><input id="campaign-id" name="campaign_id" required placeholder="tenant-august-update"></div>
          <div class="field"><label for="campaign-name">Internal campaign name</label><input id="campaign-name" name="name" required placeholder="August tenant update"></div>
          <div class="field"><label for="campaign-segment">Audience</label><select id="campaign-segment" name="segment_id" required><option value="">Choose a reviewed audience</option>{segment_options}</select></div>
          <div class="field"><label for="campaign-subject">Email subject</label><input id="campaign-subject" name="subject" required></div>
          <div class="field field--wide"><label for="campaign-body">Plain-text message</label><textarea id="campaign-body" name="body_text" required placeholder="Warm, useful, and specific."></textarea></div>
          <div class="form-actions"><span class="form-note">This button never sends email.</span><button class="primary" type="submit">Save campaign draft</button></div>
        </form>
      </section>
      <section class="panel panel--wide">
        <div class="panel-head"><div><h2>Start a booking workflow</h2><p>Create an event or workspace inquiry. This does not hold or confirm inventory.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/reservations">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="reservation-kind">Journey</label><select id="reservation-kind" name="kind"><option value="workspace">Workspace</option><option value="event">Event</option></select></div>
          <div class="field"><label for="reservation-space">Space</label><select id="reservation-space" name="space_id" required><option value="">Choose a reviewed space</option>{linked_space_options}</select></div>
          <div class="field"><label for="reservation-offering">Offering</label><select id="reservation-offering" name="offering_id"><option value="">No linked offering</option>{offering_options}</select></div>
          <div class="field"><label for="reservation-contact">Contact</label><select id="reservation-contact" name="contact_id"><option value="">No linked contact</option>{contact_options}</select></div>
          <div class="field"><label for="reservation-start">Starts (Mountain time)</label><input id="reservation-start" name="starts_at" type="datetime-local" required></div>
          <div class="field"><label for="reservation-end">Ends (Mountain time)</label><input id="reservation-end" name="ends_at" type="datetime-local" required></div>
          <div class="field"><label for="reservation-attendance">People</label><input id="reservation-attendance" name="attendance" type="number" min="0" value="0"></div>
          <div class="field"><label for="reservation-owner">Assigned owner</label><input id="reservation-owner" name="assigned_owner" value="{_esc(user.get("email"))}"></div>
          <div class="field"><label for="reservation-source">Lead source</label><select id="reservation-source" name="source"><option value="control_room">Direct/manual</option><option value="website">Building website</option><option value="facebook_marketplace">Facebook Marketplace</option><option value="eventective">Eventective</option><option value="referral">Referral</option></select></div>
          <div class="field"><label for="reservation-reference">Source reference</label><input id="reservation-reference" name="source_reference" placeholder="Listing, message, or inquiry ID"></div>
          <div class="field field--wide"><label for="reservation-requirements">Requirements and operator notes</label><textarea id="reservation-requirements" name="requirements"></textarea></div>
          <div class="form-actions"><label class="check"><input type="checkbox" name="deposit_required" value="true" checked> Deposit required before confirmation</label><button class="primary" type="submit">Create booking inquiry</button></div>
        </form>
      </section>
      <section class="panel">
        <div class="panel-head"><div><h2>Billing account</h2><p>Connect a person or company to Stripe and the current QBO record.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/billing/accounts">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="billing-account-id">Stable ID</label><input id="billing-account-id" name="account_id" required placeholder="acme-studio"></div>
          <div class="field"><label for="billing-contact">CRM contact</label><select id="billing-contact" name="contact_id"><option value="">No linked contact</option>{contact_options}</select></div>
          <div class="field"><label for="billing-account-name">Account name</label><input id="billing-account-name" name="account_name" required></div>
          <div class="field"><label for="billing-email">Billing email</label><input id="billing-email" name="billing_email" type="email" required></div>
          <div class="field field--wide"><label for="billing-qbo-customer">QBO customer ID</label><input id="billing-qbo-customer" name="qbo_customer_id" placeholder="Optional during setup"></div>
          <div class="form-actions"><span class="form-note">No provider customer or invoice is created yet.</span><button class="primary" type="submit">Save billing account</button></div>
        </form>
      </section>
      <section class="panel">
        <div class="panel-head"><div><h2>Billing schedule</h2><p>Define why, when, and how much to collect. Approval locks the schedule.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/billing/schedules">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="billing-schedule-id">Stable ID</label><input id="billing-schedule-id" name="schedule_id" required placeholder="acme-monthly"></div>
          <div class="field"><label for="billing-schedule-account">Billing account</label><select id="billing-schedule-account" name="billing_account_id" required><option value="">Choose an account</option>{billing_account_options}</select></div>
          <div class="field"><label for="billing-schedule-reservation">Booking</label><select id="billing-schedule-reservation" name="reservation_id"><option value="">No linked booking</option>{reservation_options}</select></div>
          <div class="field"><label for="billing-schedule-type">Schedule type</label><select id="billing-schedule-type" name="schedule_type"><option value="monthly">Monthly</option><option value="one_time">One time</option><option value="deposit">Deposit</option><option value="final_balance">Final balance</option></select></div>
          <div class="field field--wide"><label for="billing-description">Invoice description</label><input id="billing-description" name="description" required></div>
          <div class="field"><label for="billing-amount">Amount</label><input id="billing-amount" name="amount" inputmode="decimal" required placeholder="1250.00"></div>
          <div class="field"><label for="billing-method">Collection method</label><select id="billing-method" name="collection_method"><option value="send_invoice">Send invoice</option><option value="charge_automatically">Charge saved method</option></select></div>
          <div class="field"><label for="billing-due-days">Days until due</label><input id="billing-due-days" name="days_until_due" type="number" min="1" max="90" value="7"></div>
          <div class="field"><label for="billing-start">First invoice date</label><input id="billing-start" name="starts_on" type="date" required></div>
          <div class="field"><label for="billing-end">End date</label><input id="billing-end" name="ends_on" type="date"></div>
          <div class="form-actions"><span class="form-note">Saving remains a no-write draft.</span><button class="primary" type="submit">Save schedule draft</button></div>
        </form>
      </section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Incoming inquiries</h2><p>New workspace, tour, and event demand.</p></div><span class="count">{len(inquiries)} records</span></div><div class="table-wrap"><table><thead><tr><th>Contact</th><th>Journey</th><th>Preferred date</th><th>Status</th><th>Source</th></tr></thead><tbody>{inquiry_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Bookings and holds</h2><p>Commercial state, agreement evidence, and deposit readiness stay distinct.</p></div><span class="count">{active_reservations} active</span></div><div class="table-wrap"><table><thead><tr><th>Space</th><th>Starts</th><th>Workflow</th><th>Agreement</th><th>Deposit</th><th>Actions</th></tr></thead><tbody>{reservation_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Billing schedules</h2><p>Drafts are editable; approved schedules are locked and provider writes require typed confirmation.</p></div><span class="count">{len(billing_schedules)} schedules</span></div><div class="table-wrap"><table><thead><tr><th>Schedule</th><th>Amount</th><th>Next invoice</th><th>Status</th><th>Action</th></tr></thead><tbody>{billing_schedule_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Billing and collections</h2><p>Provider-confirmed payment evidence stays separate from the QBO accounting handoff.</p></div><span class="count">{len(invoices)} invoices</span></div><div class="table-wrap"><table><thead><tr><th>Invoice</th><th>Due</th><th>Paid</th><th>Collection</th><th>Accounting</th><th>Link</th></tr></thead><tbody>{invoice_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Inventory</h2><p>Agent-owned space status and public readiness.</p></div><span class="count">{len(spaces)} spaces · {len(offerings)} offerings</span></div><div class="table-wrap"><table><thead><tr><th>Space</th><th>Floor</th><th>Capacity</th><th>Status</th><th>Visibility</th></tr></thead><tbody>{space_rows}</tbody></table></div></section>
      <section class="panel"><div class="panel-head"><div><h2>CRM and email list</h2><p>Relationships, permission, and suppression. {subscribed} subscribed.</p></div><span class="count">{len(contacts)} contacts</span></div><div class="table-wrap"><table><thead><tr><th>Contact</th><th>Relationships</th><th>Marketing</th><th>Delivery</th></tr></thead><tbody>{contact_rows}</tbody></table></div></section>
      <section class="panel"><div class="panel-head"><div><h2>Audiences</h2><p>Explainable tenant and community segments.</p></div><span class="count">{len(segments)} segments</span></div><div class="table-wrap"><table><thead><tr><th>Audience</th><th>Relationships</th><th>Eligible</th><th>Status</th></tr></thead><tbody>{segment_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Campaigns</h2><p>Draft, preview, approval, and delivery state.</p></div><span class="count">{len(campaigns)} campaigns</span></div><div class="table-wrap"><table><thead><tr><th>Campaign</th><th>Audience</th><th>Recipients</th><th>Status</th><th>Action</th></tr></thead><tbody>{campaign_rows}</tbody></table></div></section>
    </div>
  </main>
</body>
</html>"""
