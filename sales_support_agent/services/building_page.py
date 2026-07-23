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
    tone = "bad" if normalized in {"error", "failed", "blocked", "overdue"} else (
        "ok" if normalized in {"available", "active", "subscribed", "sent", "synced"} else (
        "warn" if normalized in {"soft hold", "previewed", "approved", "sending", "unknown"} else "muted"
        )
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
    adjustments: list[dict[str, Any]],
    billing_accounts: list[dict[str, Any]],
    billing_schedules: list[dict[str, Any]],
    calendar_projections: list[dict[str, Any]],
    checklists: list[dict[str, Any]],
    service_requests: list[dict[str, Any]],
    rate_plans: list[dict[str, Any]] | None = None,
    collections: list[dict[str, Any]] | None = None,
    tours: list[dict[str, Any]] | None = None,
    contact_merges: list[dict[str, Any]] | None = None,
    privacy_requests: list[dict[str, Any]] | None = None,
    roster_imports: list[dict[str, Any]] | None = None,
    analytics: dict[str, Any] | None = None,
    can_finance: bool = False,
    csrf_token: str = "",
    notice: str = "",
    error: str = "",
) -> str:
    analytics = dict(analytics or {})
    privacy_requests = list(privacy_requests or [])
    roster_imports = list(roster_imports or [])
    tours = list(tours or [])
    contact_merges = list(contact_merges or [])
    rate_plans = list(rate_plans or [])
    collections = list(collections or [])
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
    needs_response = sum(
        1
        for item in inquiries
        if str((item.get("lifecycle") or {}).get("stage") or "new") == "new"
    )
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

    media_blocks = "".join(
        f"""
        <article class="checklist-card">
          <div class="checklist-head">
            <div><strong>{_esc(space.get("name"))}</strong><span class="sub">{len(space.get("media", []))} assigned asset(s)</span></div>
          </div>
          <form class="form-grid" method="post" action="/admin/building/spaces/{_esc(space.get("id"))}/media">
            <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
            <div class="field"><label>Stable media ID</label><input name="media_id" required pattern="[a-z0-9]+(?:-[a-z0-9]+)*" placeholder="office-201-hero"></div>
            <div class="field"><label>Asset URL or site path</label><input name="src" required placeholder="/media/office-201.webp"></div>
            <div class="field"><label>Type</label><select name="kind"><option value="image">Image</option><option value="video">Video</option></select></div>
            <div class="field"><label>Placement</label><select name="placement"><option value="card">Availability card</option><option value="hero">Page hero</option><option value="gallery">Gallery</option><option value="floor_plan">Floor plan</option></select></div>
            <div class="field"><label>Order</label><input name="sort_order" type="number" min="0" max="10000" value="0"></div>
            <div class="field field--wide"><label>Descriptive alt text</label><input name="alt" placeholder="Required before public approval"></div>
            <div class="field field--wide"><label>Caption</label><input name="caption" placeholder="Optional public context"></div>
            <div class="form-actions"><label class="check"><input type="checkbox" name="approved" value="true"> Approved for this exact space and public use</label><button class="primary" type="submit">Assign media</button></div>
          </form>
          <div class="table-wrap"><table><thead><tr><th>Asset</th><th>Placement</th><th>Approval</th><th>Remove</th></tr></thead><tbody>
            {''.join(
                f'''<tr>
                  <td><strong>{_esc(media.get("id"))}</strong><span class="sub">{_esc(media.get("src"))}</span><span class="sub">{_esc(media.get("alt") or "No alt text")}</span></td>
                  <td>{_esc(media.get("kind", "image"))} · {_esc(media.get("placement", "gallery"))} · #{_esc(media.get("sort_order", 0))}</td>
                  <td>{_badge("approved" if media.get("approved") else "draft")}</td>
                  <td><form class="inline-send" method="post" action="/admin/building/spaces/{_esc(space.get("id"))}/media/{_esc(media.get("id"))}/remove"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input name="reason" required minlength="5" placeholder="Removal reason"><button class="secondary secondary--small" type="submit">Remove</button></form></td>
                </tr>'''
                for media in sorted(space.get("media", []), key=lambda item: (int(item.get("sort_order") or 0), str(item.get("id") or "")))
            ) or '<tr><td colspan="4"><div class="empty"><strong>No media assigned.</strong><br>Save a draft first, then approve it only after confirming the room and alt text.</div></td></tr>'}
          </tbody></table></div>
        </article>
        """
        for space in spaces
    ) or '<div class="empty"><strong>No spaces available for media assignment.</strong><br>Add reviewed inventory first.</div>'

    def relationship_review_controls(item: dict[str, Any]) -> str:
        governed = [
            rel
            for rel in item.get("relationships", [])
            if rel.get("type") in {"tenant_employee", "community_member"}
        ]
        if not governed:
            return ""
        return "".join(
            f'''<details class="row-actions"><summary>{_esc(str(rel.get("type") or "").replace("_", " ").title())} review · {"current" if rel.get("review_current") else "needs attention"}</summary>
              <form method="post" action="/admin/building/contacts/{_esc(item.get("id"))}/relationships/{_esc(rel.get("id"))}/review">
                <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                <label>List owner<input name="list_owner" required value="{_esc(rel.get("list_owner"))}" placeholder="Accountable operator"></label>
                <label>Review through<input name="review_due_on" type="date" required value="{_esc(rel.get("review_due_on"))}"></label>
                <label>Relationship state<select name="status"><option value="active"{" selected" if rel.get("status") == "active" else ""}>Active</option><option value="inactive"{" selected" if rel.get("status") == "inactive" else ""}>Inactive / removed</option></select></label>
                <button class="secondary secondary--small" type="submit">Record review</button>
              </form>
            </details>'''
            for rel in governed
        )

    def relationship_labels(item: dict[str, Any]) -> str:
        labels = {
            (
                str(rel.get("type") or "").replace("_", " ")
                + f" ({str(rel.get('status') or 'active')})"
            )
            for rel in item.get("relationships", [])
            if rel.get("type")
        }
        return ", ".join(sorted(labels)) or "No relationship"

    contact_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("full_name") or item.get("email"))}</strong><span class="sub">{_esc(item.get("email"))} · {_esc(item.get("status") or "active")}</span></td>
          <td>{_esc(relationship_labels(item))}{relationship_review_controls(item)}</td>
          <td>{_badge(str(item.get("marketing_status") or "unknown"))}</td>
          <td>{_badge("suppressed") if item.get("suppressed") else "Allowed"}<span class="sub">{_esc(item.get("suppression_reason"))}</span></td>
          <td><a class="secondary secondary--small" href="/admin/building/privacy/contacts/{_esc(item.get("id"))}/export">Export</a>
            <details><summary>Correct or suppress</summary>
              <form class="inline-send" method="post" action="/admin/building/privacy/contacts/{_esc(item.get("id"))}/correct">
                <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                <input name="full_name" value="{_esc(item.get("full_name"))}" aria-label="Correct full name">
                <input name="phone" value="{_esc(item.get("phone"))}" aria-label="Correct phone">
                <input name="company_name" value="{_esc(item.get("company_name"))}" aria-label="Correct company">
                <input name="reason" minlength="5" required placeholder="Reason for correction">
                <button class="secondary secondary--small" type="submit">Save correction</button>
              </form>
              <form class="inline-send" method="post" action="/admin/building/privacy/contacts/{_esc(item.get("id"))}/suppress">
                <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                <select name="scope"><option value="marketing">Marketing only</option><option value="all">All email</option></select>
                <input name="reason" minlength="5" required placeholder="Suppression reason">
                <button class="secondary secondary--small" type="submit">Suppress</button>
              </form>
            </details>
          </td>
        </tr>
        """
        for item in contacts
    ) or '<tr><td colspan="5"><div class="empty"><strong>No building contacts yet.</strong><br>Connected website inquiries will create CRM contacts automatically.</div></td></tr>'

    privacy_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("request_type", "").replace("_", " "))}</strong><span class="sub">{_esc(item.get("requestor_email"))}</span></td>
          <td>{_badge(str(item.get("status") or "new"))}<span class="sub">Due {_esc(item.get("due_at"))}</span></td>
          <td>{_esc(item.get("assigned_owner") or "Unassigned")}</td>
          <td>
            <form class="inline-send" method="post" action="/admin/building/privacy/requests/{_esc(item.get("id"))}/transition">
              <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
              <select name="status"><option value="in_review">In review</option><option value="completed">Completed</option><option value="denied">Denied</option></select>
              <input name="resolution" placeholder="Resolution required to close">
              <input name="evidence_note" placeholder="Evidence required to close">
              <button class="secondary secondary--small" type="submit">Update</button>
            </form>
          </td>
        </tr>
        """
        for item in privacy_requests
    ) or '<tr><td colspan="4"><div class="empty"><strong>No privacy requests.</strong><br>Export, correction, suppression, deletion review, and retention review requests will appear here.</div></td></tr>'

    merge_rows = "".join(
        f"""<tr>
          <td><strong>{_esc(item.get("merged_contact_id"))}</strong><span class="sub">into {_esc(item.get("survivor_contact_id"))}</span></td>
          <td>{_esc(item.get("reason"))}</td>
          <td>{_esc((item.get("consent_result") or {}).get("marketing_status") or "unknown")}<span class="sub">Transactional: {'allowed' if (item.get("consent_result") or {}).get("transactional_allowed") else 'suppressed'}</span></td>
          <td>{_esc(item.get("actor"))}<span class="sub">{_esc(item.get("completed_at"))}</span></td>
        </tr>"""
        for item in contact_merges
    ) or '<tr><td colspan="4"><div class="empty"><strong>No contact merges.</strong><br>Merge history will remain here as permanent evidence.</div></td></tr>'

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
                '<div class="action-stack">'
                f'<form class="inline-send" method="post" action="/admin/building/campaigns/{campaign_id}/schedule">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                '<label class="sub" for="campaign-schedule-'
                f'{campaign_id}">Mountain Time · sends on the next hourly run</label>'
                f'<input id="campaign-schedule-{campaign_id}" aria-label="Scheduled delivery time in Mountain Time" '
                'name="scheduled_at" type="datetime-local" required>'
                '<button class="secondary secondary--small" type="submit">Schedule campaign</button></form>'
                f'<form class="inline-send" method="post" action="/admin/building/campaigns/{campaign_id}/send">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                f'<input aria-label="Send confirmation" name="confirmation" required placeholder="SEND {campaign_id}">'
                '<button class="primary secondary--small" type="submit">Send now</button></form>'
                '</div>'
            )
        if status == "scheduled":
            return (
                '<div class="action-stack">'
                f'<span class="sub">Scheduled {_esc(item.get("scheduled_at") or "for the next hourly run")} '
                f'by {_esc(item.get("scheduled_by") or "an operator")}</span>'
                f'<form method="post" action="/admin/building/campaigns/{campaign_id}/unschedule">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                '<button class="secondary secondary--small" type="submit">Cancel schedule</button></form>'
                '</div>'
            )
        if status == "sent_with_errors":
            return (
                f'<form class="inline-send" method="post" action="/admin/building/campaigns/{campaign_id}/retry">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                f'<input aria-label="Retry confirmation" name="confirmation" required placeholder="RETRY {campaign_id}">'
                '<button class="primary secondary--small" type="submit">Retry failed only</button></form>'
            )
        return '<span class="sub">No action required</span>'

    campaign_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("name"))}</strong><span class="sub">{_esc(item.get("subject"))}</span><span class="sub">{_esc(str(item.get("communication_class") or "marketing").replace("_", " ").title())} · from {_esc(item.get("sender_identity") or "configured sender")}</span></td>
          <td>{_esc(item.get("segment_name") or "—")}</td>
          <td>{_esc(item.get("recipient_count", 0))}{f'<span class="sub">{_esc(item.get("failed_recipient_count", 0))} failed</span>' if item.get("failed_recipient_count") else ''}</td>
          <td>{_badge(str(item.get("status") or "draft"))}{f'<span class="sub">{_esc(item.get("scheduled_at"))}</span>' if item.get("scheduled_at") else ''}</td>
          <td>{campaign_actions(item)}</td>
        </tr>
        """
        for item in campaigns
    ) or '<tr><td colspan="5"><div class="empty"><strong>No campaigns yet.</strong><br>Campaigns require a segment preview, test send, and approval before delivery.</div></td></tr>'

    def roster_contact_preview(item: dict[str, Any]) -> str:
        rows = list(item.get("rows") or [])
        if not rows:
            return ""
        contacts = "".join(
            f"<li><strong>{_esc(row.get('full_name') or row.get('email'))}</strong> "
            f"· {_esc(row.get('email'))} · "
            f"{_esc(str(row.get('marketing_status') or 'unknown').replace('_', ' '))}"
            f"{f' ({_esc(row.get('marketing_source'))})' if row.get('marketing_source') else ''}</li>"
            for row in rows
        )
        return (
            '<details class="row-actions"><summary>Review exact contacts</summary>'
            f'<ul class="roster-preview">{contacts}</ul></details>'
        )

    roster_import_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("filename"))}</strong><span class="sub">{_esc(item.get("relationship_type", "").replace("_", " ").title())}{f' · {_esc(item.get("organization"))}' if item.get("organization") else ''}</span>{roster_contact_preview(item)}</td>
          <td>{_esc(item.get("row_count", 0))}<span class="sub">{_esc(item.get("new_contact_count", 0))} new · {_esc(item.get("existing_contact_count", 0))} matched</span></td>
          <td>{_badge(str(item.get("status") or "previewed"))}<span class="sub">{_esc(item.get("created_by"))} · {_esc(item.get("created_at"))}</span></td>
          <td>{
            (
              '<div class="action-stack">'
              f'<form class="inline-send" method="post" action="/admin/building/roster-imports/{_esc(item.get("id"))}/apply">'
              f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
              f'<input aria-label="Import confirmation" name="confirmation" required placeholder="IMPORT {_esc(item.get("id"))}">'
              '<button class="primary secondary--small" type="submit">Apply roster</button></form>'
              f'<form method="post" action="/admin/building/roster-imports/{_esc(item.get("id"))}/cancel">'
              f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
              '<button class="secondary secondary--small" type="submit">Cancel preview</button></form>'
              '</div>'
            )
            if item.get("status") == "previewed"
            else f'<span class="sub">{_esc(item.get("applied_by") or "No action required")}</span>'
          }</td>
        </tr>
        """
        for item in roster_imports
    ) or '<tr><td colspan="4"><div class="empty"><strong>No roster previews yet.</strong><br>Paste a reviewed CSV above to stage tenant or community contacts without changing CRM data.</div></td></tr>'

    inquiry_stage_transitions = {
        "new": ("responded", "qualified", "closed_lost"),
        "responded": ("qualified", "closed_lost"),
        "qualified": ("closed_won", "closed_lost"),
        "closed_won": (),
        "closed_lost": (),
    }

    def inquiry_lifecycle_action(item: dict[str, Any]) -> str:
        lifecycle = dict(item.get("lifecycle") or {})
        current = str(lifecycle.get("stage") or "new")
        choices = inquiry_stage_transitions.get(current, ())
        if not choices:
            return '<span class="sub">Lifecycle complete</span>'
        return (
            f'<details class="row-actions"><summary>Record progress</summary>'
            f'<form method="post" action="/admin/building/inquiries/{_esc(item.get("id"))}/lifecycle">'
            f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
            f'<label>Next stage<select name="target_stage" required><option value="">Choose</option>'
            + "".join(
                f'<option value="{_esc(stage)}">{_esc(stage.replace("_", " "))}</option>'
                for stage in choices
            )
            + f'</select></label><label>Channel<select name="channel"><option value="email">Email</option><option value="phone">Phone</option><option value="text">Text</option><option value="in_person">In person</option><option value="other">Other</option></select></label>'
            f'<label>Owner<input name="assigned_owner" value="{_esc(item.get("assigned_owner"))}" placeholder="Responsible operator"></label>'
            '<label>Notes<input name="notes" placeholder="What happened?"></label>'
            '<button class="secondary secondary--small" type="submit">Save progress</button></form></details>'
        )

    inquiry_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("name"))}</strong><span class="sub">{_esc(item.get("email"))}</span></td>
          <td>{_esc(item.get("kind"))}</td>
          <td>{_esc(item.get("preferred_date") or "—")}</td>
          <td>{_badge(str((item.get("lifecycle") or {}).get("stage") or "new"))}{_badge("overdue") if item.get("response_overdue") else ""}<span class="sub">{_esc(item.get("assigned_owner") or "Unassigned")} · respond by {_esc(item.get("response_due_at") or "not set")}</span>{inquiry_lifecycle_action(item)}</td>
          <td>{_esc(item.get("source"))}<span class="sub">{_esc(item.get("source_reference"))}</span></td>
          <td>{(
            f'<form method="post" action="/admin/building/inquiries/{_esc(item.get("id"))}/retry-hubspot"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><button class="secondary secondary--small" type="submit">Retry HubSpot</button><span class="sub">{_esc(item.get("hubspot_error"))} · {int(item.get("hubspot_attempt_count") or 0)} attempt(s)</span></form>'
            if item.get("status") == "crm_sync_needed"
            else (
              f'<span class="sub">HubSpot {_esc(item.get("hubspot_contact_id"))}</span>'
              if item.get("hubspot_contact_id")
              else '<span class="sub">No CRM write configured</span>'
            )
          )}</td>
        </tr>
        """
        for item in inquiries
    ) or '<tr><td colspan="6"><div class="empty"><strong>No building inquiries yet.</strong><br>The public forms remain usable through their safe delivery fallback.</div></td></tr>'

    inquiry_metrics = dict(analytics.get("inquiries") or {})
    workspace_funnel = dict(analytics.get("workspace_funnel") or {})
    event_funnel = dict(analytics.get("event_funnel") or {})
    operation_metrics = dict(analytics.get("operations") or {})
    finance_metrics = dict(analytics.get("finance") or {})
    campaign_metrics = dict(analytics.get("campaigns") or {})

    def _metric_value(value: Any, *, suffix: str = "") -> str:
        if value is None:
            return "Not enough evidence"
        return f"{_esc(value)}{suffix}"

    def _pct(value: Any) -> str:
        if value is None:
            return "Not enough evidence"
        return f"{float(value) * 100:.1f}%"

    workspace_funnel_rows = "".join(
        f'<tr><td>{_esc(label.replace("_", " "))}</td><td><strong>{_esc(value)}</strong></td></tr>'
        for label, value in workspace_funnel.items()
    ) or '<tr><td colspan="2">No workspace funnel evidence yet.</td></tr>'
    event_funnel_rows = "".join(
        f'<tr><td>{_esc(label.replace("_", " "))}</td><td><strong>{_esc(value)}</strong></td></tr>'
        for label, value in event_funnel.items()
    ) or '<tr><td colspan="2">No event funnel evidence yet.</td></tr>'
    source_performance_rows = "".join(
        f"""<tr>
          <td><strong>{_esc(item.get("source"))}</strong></td>
          <td>{_esc(item.get("inquiries", 0))}</td>
          <td>${int(item.get("invoiced_cents") or 0) / 100:,.2f}</td>
          <td>${int(item.get("posted_collected_cents") or 0) / 100:,.2f}</td>
        </tr>"""
        for item in finance_metrics.get("by_source", [])
    ) or '<tr><td colspan="4"><div class="empty"><strong>No attributable revenue yet.</strong><br>Source reporting begins when inquiries connect to bookings and invoices.</div></td></tr>'

    reservation_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("space_name"))}</strong><span class="sub">{_esc(item.get("kind"))}</span></td>
          <td>{_esc(item.get("starts_at"))}</td>
          <td>{_badge(str(item.get("status") or "inquiry"))}</td>
          <td>{_badge(str((item.get("proposal") or {}).get("status") or "not started"))}<span class="sub">{(
            f'v{_esc((item.get("proposal") or {}).get("version"))} · {str((item.get("proposal") or {}).get("currency") or "USD")} {int((item.get("proposal") or {}).get("amount_cents") or 0) / 100:,.2f}'
            if item.get("proposal") else "Create a versioned quote or proposal"
          )}</span></td>
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
              {(
                f'''<form method="post" action="/admin/building/reservations/{_esc(item.get("id"))}/tours">
                  <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                  <label>Tour time<input type="datetime-local" name="scheduled_at" required></label>
                  <label>Duration<input type="number" name="duration_minutes" min="15" max="240" value="30"></label>
                  <label>Host<input name="host" placeholder="Tour host"></label>
                  <label>Meeting location<input name="meeting_location" value="Anata Building"></label>
                  <label>Notes<textarea name="notes"></textarea></label>
                  <button class="secondary secondary--small" type="submit">Schedule tour</button>
                  <span class="sub">Scheduling a tour does not hold or reserve the office.</span>
                </form>'''
                if item.get("kind") == "workspace"
                else ""
              )}
              <form method="post" action="/admin/building/reservations/{_esc(item.get("id"))}/proposals">
                <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                <label>{'Quote' if item.get("kind") == "event" else 'Proposal'} status<select name="status">{''.join(f'<option value="{state}"{" selected" if state == str((item.get("proposal") or {}).get("status") or "draft") else ""}>{state.title()}</option>' for state in ("draft", "approved", "sent", "accepted", "declined", "voided"))}</select></label>
                <input type="hidden" name="proposal_type" value="{'quote' if item.get("kind") == "event" else 'proposal'}">
                <label>Version<input type="number" name="version" min="1" value="{_esc((item.get("proposal") or {}).get("version") or 1)}"></label>
                <label>Amount<input name="amount" inputmode="decimal" required value="{int((item.get("proposal") or {}).get("amount_cents") or 0) / 100:.2f}"></label>
                <label>Approved rate plan<select name="rate_plan_id"><option value="">No rate plan snapshot</option>{''.join(
                  f'<option value="{_esc(plan.get("id"))}"{" selected" if plan.get("id") == (item.get("proposal") or {}).get("rate_plan_id") else ""}>{_esc(plan.get("name"))} · v{_esc(plan.get("version"))}</option>'
                  for plan in rate_plans
                  if plan.get("offering_id") == item.get("offering_id") and plan.get("status") == "approved"
                )}</select></label>
                <label>Line item<input name="line_item" value="{_esc((item.get("proposal") or {}).get("line_item"))}" placeholder="Office rent or event package"></label>
                <label>Valid until<input type="date" name="valid_until" value="{_esc((item.get("proposal") or {}).get("valid_until"))}"></label>
                <label>Document URL<input type="url" name="document_url" value="{_esc((item.get("proposal") or {}).get("document_url"))}" placeholder="Required before sent"></label>
                <label>Terms summary<textarea name="terms_summary">{_esc((item.get("proposal") or {}).get("terms_summary"))}</textarea></label>
                <button class="secondary secondary--small" type="submit">Record version</button>
                <span class="sub">Approve before sending. Sent content is locked; use a new version for revisions.</span>
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
    ) or '<tr><td colspan="7"><div class="empty"><strong>No bookings or holds yet.</strong><br>An inquiry remains a lead until an operator starts the appropriate booking workflow.</div></td></tr>'

    tour_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("space_name") or item.get("reservation_id"))}</strong><span class="sub">{_esc(item.get("meeting_location"))}</span></td>
          <td>{_esc(item.get("scheduled_label"))}<span class="sub">{_esc(item.get("duration_minutes"))} minutes</span></td>
          <td>{_badge(str(item.get("status") or "scheduled"))}</td>
          <td>{_esc(item.get("host") or "Unassigned")}</td>
          <td>{(
            f'''<form class="inline-send" method="post" action="/admin/building/tours/{_esc(item.get("id"))}">
              <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
              <input type="datetime-local" name="scheduled_at" value="{_esc(item.get("scheduled_at"))}" required aria-label="Tour time">
              <input type="number" name="duration_minutes" min="15" max="240" value="{_esc(item.get("duration_minutes") or 30)}" aria-label="Tour duration">
              <select name="status" aria-label="Tour status"><option value="scheduled">Scheduled / reschedule</option><option value="completed">Completed</option><option value="cancelled">Cancelled</option><option value="no_show">No show</option></select>
              <input name="host" value="{_esc(item.get("host"))}" placeholder="Host" aria-label="Tour host">
              <input name="meeting_location" value="{_esc(item.get("meeting_location"))}" placeholder="Location" aria-label="Meeting location">
              <input name="outcome" value="{_esc(item.get("outcome"))}" placeholder="Outcome required when complete">
              <input name="next_step" value="{_esc(item.get("next_step"))}" placeholder="Next step required when complete">
              <input name="reason" placeholder="Reason for reschedule/cancel/no-show">
              <textarea name="notes" aria-label="Tour notes">{_esc(item.get("notes"))}</textarea>
              <button class="secondary secondary--small" type="submit">Update tour</button>
            </form>'''
            if item.get("status") == "scheduled"
            else f'<span class="sub">{_esc(item.get("outcome") or "Closed")} · {_esc(item.get("next_step"))}</span>'
          )}</td>
        </tr>
        """
        for item in tours
    ) or '<tr><td colspan="5"><div class="empty"><strong>No tours scheduled.</strong><br>Schedule one from a workspace booking. Tours never block inventory.</div></td></tr>'

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

    collection_rows = "".join(
        f"""<tr>
          <td><strong>{_esc(item.get("account_name"))}</strong><span class="sub">{_esc(item.get("billing_email"))} · invoice {_esc(item.get("invoice_id"))}</span></td>
          <td>{_esc(str(item.get("currency") or "usd").upper())} {int(item.get("outstanding_cents") or 0) / 100:,.2f}<span class="sub">Due {_esc(item.get("due_at") or "unknown")}</span></td>
          <td>{_badge(str(item.get("status") or "open"))}<span class="sub">{_esc(item.get("assigned_owner") or "Unassigned")} · next {_esc(item.get("next_action_at") or "not set")}</span></td>
          <td>{int(item.get("reminder_count") or 0)}<span class="sub">{_esc(item.get("last_reminder_at") or "No reminder sent")}</span></td>
          <td><details class="row-actions"><summary>Collect</summary>
            <form method="post" action="/admin/building/billing/collections/{_esc(item.get("id"))}/transition">
              <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
              <label>State<select name="status"><option value="open">Open</option><option value="contacted">Contacted</option><option value="promised">Payment promised</option><option value="disputed">Disputed</option><option value="resolved">Resolved</option><option value="waived">Waived</option></select></label>
              <label>Owner<input name="assigned_owner" value="{_esc(item.get("assigned_owner"))}"></label>
              <label>Next action (Mountain time)<input name="next_action_at" type="datetime-local"></label>
              <label>Notes<input name="notes" value="{_esc(item.get("notes"))}"></label>
              <label>Resolution<input name="resolution" placeholder="Required to close"></label>
              <button class="secondary secondary--small" type="submit">Save collection work</button>
            </form>
            <form method="post" action="/admin/building/billing/collections/{_esc(item.get("id"))}/remind">
              <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
              <label>Next follow-up (Mountain time)<input name="next_action_at" type="datetime-local" required></label>
              <label>Confirmation<input name="confirmation" required placeholder="REMIND {_esc(item.get("id"))}"></label>
              <button class="primary secondary--small" type="submit" {'disabled' if not item.get("hosted_invoice_url") or item.get("status") in {"resolved", "waived"} else ''}>Send invoice reminder</button>
              <span class="sub">Sends only to the billing email with the secure Stripe invoice link.</span>
            </form>
          </details></td>
        </tr>"""
        for item in collections
    ) or '<tr><td colspan="5"><div class="empty"><strong>No collection cases.</strong><br>Refresh invoice aging to create reviewed follow-up work for overdue balances.</div></td></tr>'

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
        for item in contacts if item.get("status") != "merged"
    )
    offering_names = {
        str(item.get("id") or ""): str(item.get("name") or item.get("id") or "")
        for item in offerings
    }
    rate_plan_rows = "".join(
        f"""<tr>
          <td><strong>{_esc(item.get("name"))}</strong><span class="sub">{_esc(offering_names.get(str(item.get("offering_id") or ""), item.get("offering_id")))} · v{_esc(item.get("version"))}</span></td>
          <td>{_esc(item.get("currency"))} {int(item.get("unit_amount_cents") or 0) / 100:,.2f}<span class="sub">{_esc(item.get("public_price_display"))} · per {_esc(item.get("booking_unit"))}</span></td>
          <td>{_esc(str(item.get("deposit_type") or "none").title())}<span class="sub">{(
            f'{int(item.get("deposit_percent_bps") or 0) / 100:g}%'
            if item.get("deposit_type") == "percent"
            else (
              f'{_esc(item.get("currency"))} {int(item.get("deposit_amount_cents") or 0) / 100:,.2f}'
              if item.get("deposit_type") == "fixed"
              else "No deposit"
            )
          )}</span></td>
          <td>{_esc(item.get("effective_from"))} – {_esc(item.get("effective_until") or "ongoing")}</td>
          <td>{_badge(str(item.get("status") or "draft"))}<span class="sub">{_esc(item.get("approved_by"))}</span></td>
        </tr>"""
        for item in rate_plans
    ) or '<tr><td colspan="5"><div class="empty"><strong>No reviewed rate plans yet.</strong><br>Create commercial terms before quoting deposits or cancellation rules.</div></td></tr>'
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
    calendar_projection_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("space_name") or item.get("reservation_id"))}</strong><span class="sub">{_esc(item.get("reservation_id"))}</span></td>
          <td>{_esc(item.get("desired_action"))}</td>
          <td>{_badge(str(item.get("status") or "pending"))}<span class="sub">{_esc(item.get("last_error"))}</span></td>
          <td>{_esc(item.get("provider_event_id") or "Not created")}</td>
          <td>{_esc(item.get("updated_at") or "—")}</td>
        </tr>
        """
        for item in calendar_projections
    ) or '<tr><td colspan="5"><div class="empty"><strong>No calendar projections queued.</strong><br>Approved holds and confirmed bookings will appear here automatically.</div></td></tr>'
    checklist_blocks = "".join(
        f"""
        <div class="checklist-group">
          <div class="checklist-head">
            <div><strong>{_esc(checklist.get("title"))}</strong><span class="sub">{_esc(checklist.get("space_name") or checklist.get("reservation_id"))} · due {_esc(checklist.get("due_at") or "not set")} · owner {_esc(checklist.get("assigned_owner") or "unassigned")}</span></div>
            {_badge(str(checklist.get("status") or "open"))}
          </div>
          <div class="table-wrap"><table><thead><tr><th>Operational item</th><th>State</th><th>Required</th><th>Update</th></tr></thead><tbody>
            {''.join(
              f'''<tr>
                <td><strong>{_esc(item.get("label"))}</strong><span class="sub">{_esc(item.get("completion_reason"))}</span></td>
                <td>{_badge(str(item.get("status") or "pending"))}</td>
                <td>{"Yes" if item.get("is_required") else "No"}</td>
                <td><form class="inline-send" method="post" action="/admin/building/checklists/items/{_esc(item.get("id"))}/status"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><select aria-label="Operational item status" name="status"><option value="completed">Complete</option><option value="waived">Waive</option><option value="pending">Reopen</option></select><input aria-label="Reason for waiver or change" name="reason" placeholder="Reason required to waive"><button class="secondary secondary--small" type="submit">Save</button></form></td>
              </tr>'''
              for item in checklist.get("items", [])
            ) or '<tr><td colspan="4"><div class="empty">No checklist items.</div></td></tr>'}
          </tbody></table></div>
          <form class="checklist-add" method="post" action="/admin/building/checklists/{_esc(checklist.get("id"))}/items">
            <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
            <label>Additional operation<input name="label" required placeholder="Add a booking-specific task"></label>
            <label class="check"><input type="checkbox" name="is_required" value="true" checked> Required</label>
            <button class="secondary secondary--small" type="submit">Add item</button>
          </form>
        </div>
        """
        for checklist in checklists
    ) or '<div class="empty"><strong>No operational checklists yet.</strong><br>Confirming an event or workspace automatically creates the appropriate readiness checklist.</div>'
    invoice_options = "".join(
        f'<option value="{_esc(item.get("id"))}">{_esc(item.get("description"))} · {_esc(str(item.get("currency") or "usd").upper())} {int(item.get("amount_due_cents") or 0) / 100:,.2f}</option>'
        for item in invoices
    )
    open_service_requests = sum(
        1
        for item in service_requests
        if item.get("status") not in {"completed", "cancelled"}
    )
    adjustment_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(str(item.get("adjustment_type") or "").replace("_", " ").title())}</strong><span class="sub">{_esc(item.get("invoice_id"))} · requested by {_esc(item.get("requested_by"))}</span></td>
          <td>{_esc(str(item.get("currency") or "usd").upper())} {int(item.get("amount_cents") or 0) / 100:,.2f}</td>
          <td><span class="sub">{_esc(item.get("reason"))}</span></td>
          <td>{_badge(str(item.get("status") or "requested"))}</td>
          <td>{(
            f'<form class="inline-send" method="post" action="/admin/building/billing/adjustments/{_esc(item.get("id"))}/approve"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input aria-label="Adjustment approval confirmation" name="confirmation" required placeholder="APPROVE {_esc(item.get("id"))}"><button class="secondary secondary--small" type="submit">Approve</button></form>'
            if item.get("status") == "requested"
            else (
              f'<form class="adjustment-evidence" method="post" action="/admin/building/billing/adjustments/{_esc(item.get("id"))}/evidence"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><select aria-label="Final evidence type" name="status"><option value="provider_confirmed">Provider-confirmed refund</option><option value="accounting_confirmed">Accounting-confirmed credit/write-off</option><option value="voided">Void approved request</option></select><input name="provider_reference" placeholder="Provider reference"><input name="qbo_reference" placeholder="QBO reference"><input name="note" required placeholder="Evidence note"><input aria-label="Evidence confirmation" name="confirmation" required placeholder="CONFIRM {_esc(item.get("id"))}"><button class="secondary secondary--small" type="submit">Record evidence</button></form>'
              if item.get("status") == "approved"
              else '<span class="sub">Evidence recorded</span>'
            )
          )}</td>
        </tr>
        """
        for item in adjustments
    ) or '<tr><td colspan="5"><div class="empty"><strong>No financial adjustments.</strong><br>Refunds, credits, and write-offs begin as reviewed requests and never imply provider or accounting confirmation.</div></td></tr>'
    service_request_rows = "".join(
        f"""
        <tr>
          <td><strong>{_esc(item.get("title"))}</strong><span class="sub">{_esc(str(item.get("category") or "").replace("_", " ").title())} · {_esc(item.get("space_name") or "Building-wide")}</span></td>
          <td>{_badge(str(item.get("priority") or "normal"))}{'<span class="sub">Overdue</span>' if item.get("overdue") else ''}</td>
          <td>{_badge(str(item.get("status") or "new"))}</td>
          <td>{_esc(item.get("assigned_owner") or "Unassigned")}<span class="sub">{_esc(item.get("due_at") or "No due time")}</span></td>
          <td><details class="row-actions"><summary>Update work</summary>
            <form method="post" action="/admin/building/service-requests/{_esc(item.get("id"))}/transition">
              <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
              <label>Next state<select name="target_status" required>{''.join(f'<option value="{_esc(state)}">{_esc(state.replace("_", " ").title())}</option>' for state in item.get("allowed_next", []))}</select></label>
              <label>Assigned owner<input name="assigned_owner" value="{_esc(item.get("assigned_owner"))}"></label>
              <label>Response due (Mountain time)<input name="due_at" type="datetime-local"></label>
              <label>Resolution<input name="resolution" placeholder="Required when completing"></label>
              <label>Reason<input name="reason" required placeholder="Why is the state changing?"></label>
              <button class="secondary secondary--small" type="submit" {'disabled' if not item.get("allowed_next") else ''}>Save update</button>
            </form>
          </details></td>
        </tr>
        """
        for item in service_requests
    ) or '<tr><td colspan="5"><div class="empty"><strong>No service requests.</strong><br>Add maintenance, cleaning, access, internet, or tenant-service work when it is reported.</div></td></tr>'
    priority_items: list[dict[str, Any]] = []
    for item in service_requests:
        if item.get("status") in {"completed", "cancelled"}:
            continue
        priority = str(item.get("priority") or "normal")
        score = {"urgent": 100, "high": 80, "normal": 50, "low": 30}.get(priority, 40)
        if item.get("overdue"):
            score += 30
        priority_items.append({
            "score": score,
            "type": "Service request",
            "title": item.get("title"),
            "detail": (
                f"{priority.title()} · {item.get('status', 'new').replace('_', ' ')}"
                f" · {item.get('assigned_owner') or 'assign an owner'}"
            ),
            "next": "Triage or update the service request",
        })
    for item in inquiries:
        lifecycle_stage = str((item.get("lifecycle") or {}).get("stage") or "new")
        crm_failed = item.get("status") == "crm_sync_needed"
        if lifecycle_stage != "new" and not crm_failed:
            continue
        priority_items.append({
            "score": 95 if item.get("response_overdue") else (75 if crm_failed else 65),
            "type": "Inquiry",
            "title": item.get("name") or item.get("email"),
            "detail": (
                f"{item.get('kind', 'lead')} · {item.get('source', 'unknown source')}"
                f" · respond by {item.get('response_due_at') or 'not set'}"
            ),
            "next": "Retry HubSpot" if crm_failed else "Respond and qualify",
        })
    for item in calendar_projections:
        if item.get("status") != "error":
            continue
        priority_items.append({
            "score": 85,
            "type": "Calendar",
            "title": item.get("space_name") or item.get("reservation_id"),
            "detail": item.get("last_error") or "Calendar synchronization failed",
            "next": "Review and retry calendar sync",
        })
    for item in checklists:
        if item.get("status") != "open":
            continue
        remaining = sum(
            1
            for checklist_item in item.get("items", [])
            if checklist_item.get("is_required")
            and checklist_item.get("status") == "pending"
        )
        priority_items.append({
            "score": 55,
            "type": "Readiness",
            "title": item.get("title"),
            "detail": f"{item.get('space_name') or item.get('reservation_id')} · {remaining} required remaining",
            "next": "Complete or explicitly waive required items",
        })
    for item in collections:
        if item.get("status") in {"resolved", "waived"}:
            continue
        priority_items.append({
            "score": 90 if not item.get("assigned_owner") else 72,
            "type": "Collection",
            "title": item.get("account_name") or item.get("invoice_id"),
            "detail": (
                f"{str(item.get('currency') or 'usd').upper()} "
                f"{int(item.get('outstanding_cents') or 0) / 100:,.2f} outstanding"
                f" · {item.get('assigned_owner') or 'unassigned'}"
            ),
            "next": "Assign and schedule follow-up" if not item.get("assigned_owner") else "Complete the next collection action",
        })
    priority_items.sort(key=lambda item: (-int(item["score"]), str(item["title"])))
    priority_rows = "".join(
        f"<tr><td>{_badge(str(item.get('type')))}</td><td><strong>{_esc(item.get('title'))}</strong><span class=\"sub\">{_esc(item.get('detail'))}</span></td><td>{_esc(item.get('next'))}</td></tr>"
        for item in priority_items[:12]
    ) or '<tr><td colspan="3"><div class="empty"><strong>No urgent operating actions.</strong><br>New leads, failed integrations, readiness work, and service requests will appear here.</div></td></tr>'
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
    .badge--ok{{background:#e4f4f1;color:#11665f;}} .badge--warn{{background:#fff0d2;color:#845407;}} .badge--bad{{background:#fff0ed;color:#8b2f23;}} .badge--muted{{background:#edf0f2;color:#56616d;}}
    .empty{{padding:18px 0;color:rgba(43,54,68,.62);line-height:1.55;}}
    .form-grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;padding:20px 22px;}} .field{{display:grid;gap:6px;}} .field--wide{{grid-column:1/-1;}}
    label{{font-size:12px;font-weight:700;color:rgba(43,54,68,.72);}} input,select,textarea{{box-sizing:border-box;width:100%;min-height:42px;border:1px solid rgba(43,54,68,.22);border-radius:8px;background:#fff;padding:10px 11px;color:var(--ink);font:inherit;}} textarea{{min-height:92px;resize:vertical;}} input:focus,select:focus,textarea:focus{{outline:3px solid rgba(133,187,218,.34);border-color:#397a9d;}}
    .check{{display:flex;align-items:center;gap:9px;font-size:13px;}} .check input{{width:18px;min-height:18px;}} .check-stack{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 14px;padding:11px;border:1px solid rgba(43,54,68,.14);border-radius:8px;}} .form-actions{{grid-column:1/-1;display:flex;align-items:center;justify-content:space-between;gap:14px;border-top:1px solid var(--border);padding-top:16px;}} .form-note{{font-size:12px;color:rgba(43,54,68,.62);line-height:1.45;}} .primary,.secondary{{min-height:42px;border:0;border-radius:8px;background:var(--ink);color:#fff;padding:0 17px;font-weight:700;cursor:pointer;}} .primary:hover{{background:#17222d;}} .secondary{{border:1px solid var(--border);background:#fff;color:var(--ink);}} .secondary--small{{min-height:34px;padding:0 11px;font-size:12px;white-space:nowrap;}} .action-stack{{display:grid;gap:7px;min-width:210px;}} .inline-send{{display:flex;gap:6px;align-items:center;}} .inline-send input{{min-height:34px;padding:7px 8px;font-size:12px;}}
    .row-actions{{min-width:220px;}} .row-actions summary{{cursor:pointer;font-weight:700;color:#397a9d;}} .row-actions form{{display:grid;gap:7px;margin-top:10px;padding:10px;border:1px solid var(--border);border-radius:9px;background:#f8f8f6;}} .row-actions label{{display:grid;gap:4px;}} .row-actions input,.row-actions select{{min-height:34px;padding:7px 8px;font-size:12px;}}
    .roster-preview{{max-height:260px;overflow:auto;margin:10px 0 0;padding:10px 10px 10px 28px;border:1px solid var(--border);border-radius:8px;background:#f8f8f6;font-size:12px;line-height:1.6;min-width:320px;}}
    .checklist-list{{display:grid;gap:14px;padding:18px 22px;}} .checklist-group{{border:1px solid var(--border);border-radius:10px;overflow:hidden;}} .checklist-head{{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:15px 16px;background:#f8f8f6;}} .checklist-add{{display:grid;grid-template-columns:minmax(220px,1fr) auto auto;align-items:end;gap:10px;padding:12px 16px;border-top:1px solid var(--border);}} .checklist-add label:first-of-type{{display:grid;gap:5px;}}
    .adjustment-evidence{{display:grid;grid-template-columns:repeat(2,minmax(130px,1fr));gap:6px;min-width:360px;}} .adjustment-evidence button{{justify-self:start;}}
    @media(max-width:900px){{.metrics{{grid-template-columns:1fr 1fr}}.metric:nth-child(2){{border-right:0}}.metric:nth-child(-n+2){{border-bottom:1px solid var(--border)}}.grid{{grid-template-columns:1fr}}.panel--wide{{grid-column:auto}}}}
    @media(max-width:600px){{.page-head{{align-items:start;flex-direction:column}}.metrics{{grid-template-columns:1fr}}.metric{{border-right:0;border-bottom:1px solid var(--border)!important}}.metric:last-child{{border-bottom:0!important}}.shell{{padding-inline:16px}}.form-grid{{grid-template-columns:1fr}}.field--wide{{grid-column:auto}}.form-actions{{grid-column:auto;align-items:stretch;flex-direction:column}}.checklist-add{{grid-template-columns:1fr;align-items:stretch}}}}
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
      <section class="panel panel--wide" id="operator-queue"><div class="panel-head"><div><h2>Operator queue</h2><p>The highest-risk customer, revenue, readiness, and building-service actions in one place.</p></div><span class="count">{len(priority_items)} actions</span></div><div class="table-wrap"><table><thead><tr><th>Workstream</th><th>What needs attention</th><th>Next action</th></tr></thead><tbody>{priority_rows}</tbody></table></div></section>
      <section class="panel panel--wide" id="building-performance">
        <div class="panel-head"><div><h2>Building performance</h2><p>All-time funnel evidence with current operating and financial measures. Missing evidence stays visibly missing.</p></div><span class="count">{_esc(inquiry_metrics.get("total", 0))} inquiries</span></div>
        <div class="metrics" aria-label="Building performance summary">
          <div class="metric"><span>Median first response</span><strong>{_metric_value(inquiry_metrics.get("median_first_response_hours"), suffix=" hr")}</strong></div>
          <div class="metric"><span>30-day scheduled utilization</span><strong>{_pct(operation_metrics.get("scheduled_utilization_30d"))}</strong></div>
          <div class="metric"><span>Posted collected cash</span><strong>${int(finance_metrics.get("posted_collected_cents") or 0) / 100:,.0f}</strong></div>
          <div class="metric"><span>Overdue receivables</span><strong>${int(finance_metrics.get("overdue_cents") or 0) / 100:,.0f}</strong></div>
        </div>
        <div class="grid">
          <div><h3>Workspace funnel</h3><div class="table-wrap"><table><thead><tr><th>Stage reached</th><th>Count</th></tr></thead><tbody>{workspace_funnel_rows}</tbody></table></div></div>
          <div><h3>Event funnel</h3><div class="table-wrap"><table><thead><tr><th>Stage reached</th><th>Count</th></tr></thead><tbody>{event_funnel_rows}</tbody></table></div></div>
        </div>
        <div class="table-wrap"><table><thead><tr><th>Lead source</th><th>Inquiries</th><th>Invoiced</th><th>Posted collected</th></tr></thead><tbody>{source_performance_rows}</tbody></table></div>
        <p class="sub">Hold expiration: {_pct(operation_metrics.get("hold_expiration_rate"))} · Contract cycle: {_metric_value(operation_metrics.get("median_contract_cycle_hours"), suffix=" hr")} · Deposit cycle: {_metric_value(operation_metrics.get("median_deposit_cycle_hours"), suffix=" hr")} · Delivery feedback: {_esc(str(campaign_metrics.get("delivery_feedback") or "not configured").replace("_", " "))} · Campaign engagement telemetry: {_esc(str(campaign_metrics.get("engagement_tracking") or "not configured").replace("_", " "))}</p>
      </section>
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
      <section class="panel panel--wide">
        <div class="panel-head"><div><h2>Commercial rate plans</h2><p>Version pricing, deposits, included items, and cancellation terms. Approved versions are locked.</p></div><span class="count">{len(rate_plans)} versions</span></div>
        <div class="table-wrap"><table><thead><tr><th>Plan</th><th>Price</th><th>Deposit</th><th>Effective</th><th>State</th></tr></thead><tbody>{rate_plan_rows}</tbody></table></div>
        <form class="form-grid" method="post" action="/admin/building/rate-plans">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="rate-offering">Offering</label><select id="rate-offering" name="offering_id" required><option value="">Choose offering</option>{offering_options}</select></div>
          <div class="field"><label for="rate-id">Stable rate-plan ID</label><input id="rate-id" name="rate_plan_id" required placeholder="arena-events-v1"></div>
          <div class="field"><label for="rate-version">Version</label><input id="rate-version" name="version" type="number" min="1" value="1" required></div>
          <div class="field"><label for="rate-name">Internal name</label><input id="rate-name" name="name" required placeholder="Arena standard"></div>
          <div class="field"><label for="rate-status">State</label><select id="rate-status" name="status"><option value="draft">Draft</option><option value="approved">Approve and lock</option><option value="retired">Retire existing approved plan</option></select></div>
          <div class="field"><label for="rate-currency">Currency</label><input id="rate-currency" name="currency" value="USD" maxlength="3" required></div>
          <div class="field"><label for="rate-amount">Internal unit price (cents)</label><input id="rate-amount" name="unit_amount_cents" type="number" min="0" value="0" required></div>
          <div class="field"><label for="rate-public-price">Public price wording</label><input id="rate-public-price" name="public_price_display" placeholder="From $2,500/event"></div>
          <div class="field"><label for="rate-unit">Booking unit</label><select id="rate-unit" name="booking_unit"><option value="custom">Custom</option><option value="month">Month</option><option value="day">Day</option><option value="hour">Hour</option><option value="event">Event</option><option value="term">Term</option></select></div>
          <div class="field"><label for="rate-minimum">Minimum units</label><input id="rate-minimum" name="minimum_units" type="number" min="1" value="1"></div>
          <div class="field"><label for="rate-deposit-type">Deposit</label><select id="rate-deposit-type" name="deposit_type"><option value="none">None</option><option value="fixed">Fixed amount</option><option value="percent">Percentage</option></select></div>
          <div class="field"><label for="rate-deposit-amount">Fixed deposit (cents)</label><input id="rate-deposit-amount" name="deposit_amount_cents" type="number" min="0" value="0"></div>
          <div class="field"><label for="rate-deposit-percent">Deposit percent</label><input id="rate-deposit-percent" name="deposit_percent" type="number" min="0" max="100" step="0.01" value="0"></div>
          <div class="field"><label for="rate-from">Effective from</label><input id="rate-from" name="effective_from" type="date" required></div>
          <div class="field"><label for="rate-until">Effective until</label><input id="rate-until" name="effective_until" type="date"></div>
          <div class="field field--wide"><label for="rate-included">Included items</label><input id="rate-included" name="included" placeholder="Tables, chairs, standard cleaning"></div>
          <div class="field field--wide"><label for="rate-addons">Add-ons (JSON list)</label><textarea id="rate-addons" name="addons_json">[]</textarea></div>
          <div class="field field--wide"><label for="rate-cancellation">Cancellation policy</label><textarea id="rate-cancellation" name="cancellation_policy" placeholder="Required before approval."></textarea></div>
          <div class="form-actions"><span class="form-note">Approval locks the commercial terms. Create a new version for later changes.</span><button class="primary" type="submit">Save rate plan</button></div>
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
          <div class="field"><label for="contact-list-owner">Employee/community list owner</label><input id="contact-list-owner" name="list_owner" placeholder="Required for tenant employees and community members"></div>
          <div class="field"><label for="contact-review-due">Review through</label><input id="contact-review-due" name="review_due_on" type="date"><span class="form-note">Required for tenant employees and community members.</span></div>
          <div class="field"><label for="contact-marketing">Marketing permission</label><select id="contact-marketing" name="marketing_status"><option value="unknown">Unknown / no promotional email</option><option value="subscribed">Subscribed</option><option value="unsubscribed">Unsubscribed</option></select></div>
          <div class="form-actions"><label class="check"><input type="checkbox" name="consent_confirmed" value="true"> I have documented consent for “Subscribed”</label><button class="primary" type="submit">Save contact</button></div>
        </form>
      </section>
      <section class="panel">
        <div class="panel-head"><div><h2>Preview a roster import</h2><p>Stage up to 500 tenant or community contacts before anything changes.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/roster-imports/preview">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="roster-filename">List name</label><input id="roster-filename" name="filename" value="tenant-roster.csv" required></div>
          <div class="field"><label for="roster-relationship">Relationship</label><select id="roster-relationship" name="relationship_type"><option value="tenant">Tenant</option><option value="tenant_employee">Tenant employee</option><option value="event_host">Event host</option><option value="former_tenant">Former tenant</option><option value="community_member">Community member</option><option value="vendor">Vendor</option><option value="partner">Partner</option></select></div>
          <div class="field"><label for="roster-organization">Organization</label><input id="roster-organization" name="organization" placeholder="Required for tenant employees"></div>
          <div class="field"><label for="roster-owner">List owner</label><input id="roster-owner" name="list_owner" placeholder="Required for employee/community lists"></div>
          <div class="field"><label for="roster-review">Review through</label><input id="roster-review" name="review_due_on" type="date"><span class="form-note">Required for employee/community lists.</span></div>
          <div class="field field--wide"><label for="roster-csv">CSV data</label><textarea id="roster-csv" name="csv_text" required spellcheck="false" placeholder="email,full_name,phone,company_name,marketing_status,marketing_source,source_reference&#10;taylor@example.com,Taylor Morgan,,Acme,unknown,,tenant roster"></textarea><span class="form-note">Email is required. Optional columns: full_name, phone, company_name, marketing_status, marketing_source, source_reference. “Subscribed” requires a documented marketing_source. Existing unsubscribes are never overwritten.</span></div>
          <div class="form-actions"><span class="form-note">Previewing creates a reviewable snapshot only. Applying it requires a separate typed confirmation.</span><button class="primary" type="submit">Preview roster</button></div>
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
          <div class="field"><label for="campaign-class">Message type</label><select id="campaign-class" name="communication_class"><option value="marketing">Optional marketing</option><option value="operational">Required tenant / event operations</option></select><span class="form-note">Operational notices only work with active tenant, tenant employee, or event host audiences. They cannot be used for promotions.</span></div>
          <div class="field"><label for="campaign-subject">Email subject</label><input id="campaign-subject" name="subject" required></div>
          <div class="field field--wide"><label for="campaign-body">Plain-text message</label><textarea id="campaign-body" name="body_text" required placeholder="Warm, useful, and specific."></textarea></div>
          <div class="form-actions"><span class="form-note">This button never sends email.</span><button class="primary" type="submit">Save campaign draft</button></div>
        </form>
      </section>
      <section class="panel panel--wide">
        <div class="panel-head"><div><h2>Add an assisted lead</h2><p>Normalize Facebook Marketplace, Eventective, referral, phone, and walk-in leads into the same inquiry and CRM recovery queue.</p></div></div>
        <form class="form-grid" method="post" action="/admin/building/inquiries">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="lead-kind">Journey</label><select id="lead-kind" name="kind"><option value="workspace">Workspace</option><option value="tour">Tour</option><option value="event">Event</option></select></div>
          <div class="field"><label for="lead-source">Lead source</label><select id="lead-source" name="source"><option value="facebook_marketplace">Facebook Marketplace</option><option value="eventective">Eventective</option><option value="referral">Referral</option><option value="phone">Phone</option><option value="walk_in">Walk-in</option><option value="direct">Direct</option></select></div>
          <div class="field"><label for="lead-name">Name</label><input id="lead-name" name="name" required></div>
          <div class="field"><label for="lead-email">Email</label><input id="lead-email" name="email" type="email" required></div>
          <div class="field"><label for="lead-phone">Phone</label><input id="lead-phone" name="phone" type="tel"></div>
          <div class="field"><label for="lead-date">Preferred date</label><input id="lead-date" name="preferred_date" type="date"></div>
          <div class="field"><label for="lead-offering">Offering</label><select id="lead-offering" name="offering_id"><option value="">Not decided</option>{offering_options}</select></div>
          <div class="field"><label for="lead-reference">Original source reference</label><input id="lead-reference" name="source_reference" placeholder="Required for Marketplace or Eventective"></div>
          <div class="field field--wide"><label for="lead-details">Original request and operator notes</label><textarea id="lead-details" name="details" placeholder="Preserve the requested dates, capacity, budget, and original message."></textarea></div>
          <div class="form-actions">
            <div><label class="check"><input type="checkbox" name="consent_to_contact" value="true" required> The person asked to be contacted about this request</label><label class="check"><input type="checkbox" name="consent_to_marketing" value="true"> Separate promotional-email consent is documented</label></div>
            <button class="primary" type="submit">Add lead to response queue</button>
          </div>
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
      <section class="panel panel--wide" id="service-requests">
        <div class="panel-head"><div><h2>Building service</h2><p>Maintenance and tenant requests stay owned, due, auditable, and separate from commercial booking state.</p></div><span class="count">{open_service_requests} open</span></div>
        <form class="form-grid" method="post" action="/admin/building/service-requests">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="service-category">Category</label><select id="service-category" name="category"><option value="maintenance">Maintenance</option><option value="cleaning">Cleaning</option><option value="access">Access</option><option value="internet">Internet</option><option value="furniture">Furniture</option><option value="safety">Safety</option><option value="billing_question">Billing question</option><option value="event_support">Event support</option><option value="other">Other</option></select></div>
          <div class="field"><label for="service-priority">Priority</label><select id="service-priority" name="priority"><option value="normal">Normal</option><option value="low">Low</option><option value="high">High</option><option value="urgent">Urgent</option></select></div>
          <div class="field field--wide"><label for="service-title">Request</label><input id="service-title" name="title" required placeholder="What needs attention?"></div>
          <div class="field field--wide"><label for="service-description">Details</label><textarea id="service-description" name="description" placeholder="What happened, what is affected, and what has already been tried?"></textarea></div>
          <div class="field"><label for="service-space">Space</label><select id="service-space" name="space_id"><option value="">Building-wide</option>{linked_space_options}</select></div>
          <div class="field"><label for="service-contact">Related contact</label><select id="service-contact" name="contact_id"><option value="">No linked contact</option>{contact_options}</select></div>
          <div class="field"><label for="service-reservation">Related booking</label><select id="service-reservation" name="reservation_id"><option value="">No linked booking</option>{reservation_options}</select></div>
          <div class="field"><label for="service-source">Reported through</label><select id="service-source" name="source"><option value="operator">Operator</option><option value="tenant">Tenant</option><option value="event_host">Event host</option><option value="inspection">Inspection</option><option value="checklist">Checklist</option></select></div>
          <div class="field"><label for="service-reference">Source reference</label><input id="service-reference" name="source_reference" placeholder="Email, ticket, or inspection reference"></div>
          <div class="field"><label for="service-owner">Assigned owner</label><input id="service-owner" name="assigned_owner" value="{_esc(user.get("email"))}"></div>
          <div class="field"><label for="service-due">Response due (Mountain time)</label><input id="service-due" name="due_at" type="datetime-local"></div>
          <div class="form-actions"><span class="form-note">Urgent requests require an owner and response due time. This queue is not an emergency-response service.</span><button class="primary" type="submit">Add service request</button></div>
        </form>
        <div class="table-wrap"><table><thead><tr><th>Request</th><th>Priority</th><th>Status</th><th>Owner and due</th><th>Action</th></tr></thead><tbody>{service_request_rows}</tbody></table></div>
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
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Incoming inquiries</h2><p>New workspace, tour, and event demand. Partial CRM failures stay queued without losing the lead.</p></div><span class="count">{len(inquiries)} records</span></div><div class="table-wrap"><table><thead><tr><th>Contact</th><th>Journey</th><th>Preferred date</th><th>Status</th><th>Source</th><th>CRM recovery</th></tr></thead><tbody>{inquiry_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Bookings and holds</h2><p>Commercial state, proposal or quote, agreement evidence, and deposit readiness stay distinct.</p></div><span class="count">{active_reservations} active</span></div><div class="table-wrap"><table><thead><tr><th>Space</th><th>Starts</th><th>Workflow</th><th>Proposal / quote</th><th>Agreement</th><th>Deposit</th><th>Actions</th></tr></thead><tbody>{reservation_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Upcoming and recent tours</h2><p>Tour schedule, host, completion outcome, and next step. Tours are visits—not inventory holds.</p></div><span class="count">{len(tours)} tours</span></div><div class="table-wrap"><table><thead><tr><th>Workspace</th><th>Time</th><th>Status</th><th>Host</th><th>Tour action</th></tr></thead><tbody>{tour_rows}</tbody></table></div></section>
      <section class="panel panel--wide">
        <div class="panel-head">
          <div><h2>Calendar projection</h2><p>Agent remains authoritative. Approved holds and bookings are queued for Google Calendar; calendar edits never change a booking.</p></div>
          <form class="inline-send" method="post" action="/admin/building/calendar/sync">
            <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
            <input aria-label="Calendar sync confirmation" name="confirmation" required placeholder="SYNC CALENDAR">
            <button class="secondary secondary--small" type="submit">Sync pending</button>
          </form>
        </div>
        <div class="table-wrap"><table><thead><tr><th>Booking</th><th>Desired action</th><th>Sync state</th><th>Google event</th><th>Updated</th></tr></thead><tbody>{calendar_projection_rows}</tbody></table></div>
      </section>
      <section class="panel panel--wide">
        <div class="panel-head"><div><h2>Operational readiness</h2><p>Event, move-in, and move-out work stays attached to the booking. Required items must be completed or explicitly waived with a reason.</p></div><span class="count">{sum(1 for item in checklists if item.get("status") == "open")} open</span></div>
        <div class="checklist-list">{checklist_blocks}</div>
      </section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Billing schedules</h2><p>Drafts are editable; approved schedules are locked and provider writes require typed confirmation.</p></div><span class="count">{len(billing_schedules)} schedules</span></div><div class="table-wrap"><table><thead><tr><th>Schedule</th><th>Amount</th><th>Next invoice</th><th>Status</th><th>Action</th></tr></thead><tbody>{billing_schedule_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Billing and collections</h2><p>Provider-confirmed payment evidence stays separate from the QBO accounting handoff.</p></div><span class="count">{len(invoices)} invoices</span></div><div class="table-wrap"><table><thead><tr><th>Invoice</th><th>Due</th><th>Paid</th><th>Collection</th><th>Accounting</th><th>Link</th></tr></thead><tbody>{invoice_rows}</tbody></table></div></section>
      {(
        f'''<section class="panel panel--wide"><div class="panel-head"><div><h2>Collection work</h2><p>Overdue balances become owned follow-up cases. Reminders require typed confirmation and retain delivery evidence.</p></div><span class="count">{len(collections)} cases</span></div>
        <form class="inline-send" method="post" action="/admin/building/billing/collections/refresh"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input name="default_owner" placeholder="Default collection owner"><button class="secondary secondary--small" type="submit">Refresh invoice aging</button></form>
        <div class="table-wrap"><table><thead><tr><th>Account</th><th>Outstanding</th><th>State / owner</th><th>Reminders</th><th>Action</th></tr></thead><tbody>{collection_rows}</tbody></table></div></section>'''
        if can_finance else ""
      )}
      {(
        f'''<section class="panel panel--wide">
          <div class="panel-head"><div><h2>Refunds, credits, and write-offs</h2><p>Finance-only, two-person approval. Provider and accounting evidence remain distinct.</p></div><span class="count">{len(adjustments)} records</span></div>
          <form class="form-grid" method="post" action="/admin/building/billing/adjustments">
            <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
            <div class="field"><label for="adjustment-invoice">Invoice</label><select id="adjustment-invoice" name="invoice_id" required><option value="">Choose an invoice</option>{invoice_options}</select></div>
            <div class="field"><label for="adjustment-type">Exception type</label><select id="adjustment-type" name="adjustment_type"><option value="refund">Refund</option><option value="credit">Credit</option><option value="write_off">Write-off</option></select></div>
            <div class="field"><label for="adjustment-amount">Amount</label><input id="adjustment-amount" name="amount" inputmode="decimal" required placeholder="100.00"></div>
            <div class="field"><label for="adjustment-reason">Reviewed reason</label><input id="adjustment-reason" name="reason" minlength="10" required></div>
            <div class="form-actions"><span class="form-note">This creates a request only. A different finance operator must approve it.</span><button class="primary" type="submit">Request adjustment</button></div>
          </form>
          <div class="table-wrap"><table><thead><tr><th>Adjustment</th><th>Amount</th><th>Reason</th><th>Evidence state</th><th>Finance action</th></tr></thead><tbody>{adjustment_rows}</tbody></table></div>
        </section>'''
        if can_finance
        else '<section class="panel panel--wide"><div class="panel-head"><div><h2>Financial adjustments</h2><p>Refunds, credits, and write-offs require both Building and Finance access.</p></div></div></section>'
      )}
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Inventory</h2><p>Agent-owned space status and public readiness.</p></div><span class="count">{len(spaces)} spaces · {len(offerings)} offerings</span></div><div class="table-wrap"><table><thead><tr><th>Space</th><th>Floor</th><th>Capacity</th><th>Status</th><th>Visibility</th></tr></thead><tbody>{space_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Media assignments</h2><p>Attach images and videos to the exact physical space. Draft assets never reach the public site; approval requires descriptive alt text.</p></div></div><div class="checklist-list">{media_blocks}</div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Roster import reviews</h2><p>Previewed lists remain inert until an operator confirms the exact snapshot.</p></div><span class="count">{len(roster_imports)} imports</span></div><div class="table-wrap"><table><thead><tr><th>Roster</th><th>Contacts</th><th>Status</th><th>Action</th></tr></thead><tbody>{roster_import_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>CRM and email list</h2><p>Relationships, permission, suppression, and permissioned data controls. {subscribed} subscribed.</p></div><span class="count">{len(contacts)} contacts</span></div><div class="table-wrap"><table><thead><tr><th>Contact</th><th>Relationships</th><th>Marketing</th><th>Delivery</th><th>Data controls</th></tr></thead><tbody>{contact_rows}</tbody></table></div></section>
      <section class="panel panel--wide">
        <div class="panel-head"><div><h2>Duplicate contact review</h2><p>Preview every move before merging. The survivor keeps the most restrictive communication permission; campaign and inquiry history remains unchanged.</p></div><span class="count">{len(contact_merges)} completed</span></div>
        <form class="form-grid" method="post" action="/admin/building/contacts/merge/preview">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label for="merge-survivor">Surviving contact</label><select id="merge-survivor" name="survivor_contact_id" required><option value="">Choose survivor</option>{contact_options}</select></div>
          <div class="field"><label for="merge-duplicate">Duplicate contact</label><select id="merge-duplicate" name="merged_contact_id" required><option value="">Choose duplicate</option>{contact_options}</select></div>
          <div class="form-actions"><span class="form-note">Nothing changes until you review counts, conflicts, consent, and type the exact confirmation.</span><button class="secondary" type="submit">Preview merge</button></div>
        </form>
        <div class="table-wrap"><table><thead><tr><th>Merge</th><th>Reason</th><th>Permission result</th><th>Evidence</th></tr></thead><tbody>{merge_rows}</tbody></table></div>
      </section>
      <section class="panel panel--wide">
        <div class="panel-head"><div><h2>Data governance</h2><p>Track access, correction, suppression, deletion review, and retention review with a 30-day deadline. Deletion is never automatic.</p></div><span class="count">{len(privacy_requests)} requests</span></div>
        <form class="form-grid" method="post" action="/admin/building/privacy/requests">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <div class="field"><label>Request type</label><select name="request_type"><option value="access_export">Access export</option><option value="correction">Correction</option><option value="suppression">Suppression</option><option value="deletion_review">Deletion review</option><option value="retention_review">Retention review</option></select></div>
          <div class="field"><label>Requestor email</label><input name="requestor_email" type="email" required></div>
          <div class="field"><label>CRM contact</label><select name="contact_id"><option value="">Not yet matched</option>{contact_options}</select></div>
          <div class="field"><label>Owner</label><input name="assigned_owner"></div>
          <div class="field field--wide"><label>Details</label><textarea name="details"></textarea></div>
          <div class="form-actions"><span class="form-note">Closing requires a written resolution and evidence.</span><button class="primary" type="submit">Add request</button></div>
        </form>
        <div class="table-wrap"><table><thead><tr><th>Request</th><th>Status</th><th>Owner</th><th>Review action</th></tr></thead><tbody>{privacy_rows}</tbody></table></div>
      </section>
      <section class="panel"><div class="panel-head"><div><h2>Audiences</h2><p>Explainable tenant and community segments.</p></div><span class="count">{len(segments)} segments</span></div><div class="table-wrap"><table><thead><tr><th>Audience</th><th>Relationships</th><th>Eligible</th><th>Status</th></tr></thead><tbody>{segment_rows}</tbody></table></div></section>
      <section class="panel panel--wide"><div class="panel-head"><div><h2>Campaigns</h2><p>Draft, preview, approval, and delivery state.</p></div><span class="count">{len(campaigns)} campaigns</span></div><div class="table-wrap"><table><thead><tr><th>Campaign</th><th>Audience</th><th>Recipients</th><th>Status</th><th>Action</th></tr></thead><tbody>{campaign_rows}</tbody></table></div></section>
    </div>
  </main>
</body>
</html>"""
