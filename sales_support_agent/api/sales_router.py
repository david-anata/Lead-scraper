"""Sales Priorities — HubSpot-backed deal board controller.

Phase 0: a read-only deal board sorted top-down by close date, plus an
on-request background sync that refreshes the local HubSpot mirror. Tool-gated
by `sales.deals`. Lives under /admin/sales/* alongside the existing Sales
Priorities page (kept) and the off-limits "Generate sales deck" feature.
"""

from __future__ import annotations

import logging

from urllib.parse import quote

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
    MailboxSignal,
)
from sales_support_agent.services.auth_deps import get_current_user, require_tool
from sales_support_agent.services.hubspot_sync.trigger import (
    hubspot_sync_status,
    start_hubspot_sync,
)
from sales_support_agent.services.sales.actions import ContactInfo, compute_pending_actions
from sales_support_agent.services.sales.deal_board import (
    build_deal_board,
    render_deal_board_page,
)
from sales_support_agent.services.sales.deal_detail import (
    build_deal_detail,
    render_deal_detail_page,
)
from sales_support_agent.integrations.gmail import GmailClient
from sales_support_agent.services.sales.email_send import send_followup_email
from sales_support_agent.services.sales.followup_draft import (
    _HOOK_ORDER,
    build_followup_draft,
    render_draft_followup_page,
    render_send_preview_page,
)

from sqlalchemy import func, select

logger = logging.getLogger(__name__)


def _deal_not_found_page(request: Request) -> str:
    from sales_support_agent.services.admin_nav import (
        render_agent_favicon_links,
        render_agent_nav,
        render_agent_nav_styles,
    )
    nav_styles = render_agent_nav_styles()
    nav = render_agent_nav("sales", sales_section="sales_deals", user=get_current_user(request))
    favicons = render_agent_favicon_links()
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>agent | Deal Not Found</title>
    {favicons}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      :root {{--dark-blue:#2B3644;--light-brown:#F9F7F3;--border:rgba(43,54,68,0.12);--shadow:rgba(43,54,68,0.10);--white:#FFF;}}
      *{{box-sizing:border-box;}} body{{margin:0;background:var(--light-brown);color:var(--dark-blue);font-family:"Inter","Segoe UI",sans-serif;}}
      a{{color:var(--dark-blue);}}
      {nav_styles}
      .shell{{max-width:1180px;margin:0 auto;padding:48px 18px;}}
      .workspace{{background:var(--white);border:1px solid var(--border);border-radius:20px;box-shadow:0 18px 40px var(--shadow);padding:32px 28px;}}
      h1{{font-family:"Montserrat",sans-serif;font-weight:800;font-size:24px;margin:0 0 10px;}}
    </style>
  </head>
  <body>
    {nav}
    <main class="shell">
      <div class="workspace">
        <h1>Deal not found</h1>
        <p style="color:rgba(43,54,68,0.7)">This deal doesn't exist in the local mirror. It may not have synced yet, or the link is stale.</p>
        <p><a href="/admin/sales/deals">← Back to Deal Board</a></p>
      </div>
    </main>
  </body>
</html>"""


router = APIRouter(
    prefix="/admin/sales",
    tags=["sales-deals"],
    dependencies=[Depends(require_tool("sales.deals"))],
)


@router.get("/deals", response_class=HTMLResponse)
def deal_board(request: Request, my: bool = False) -> HTMLResponse:
    # Kick a background refresh on load (non-blocking); render from the mirror.
    try:
        start_hubspot_sync(request.app, force=False)
    except Exception:  # noqa: BLE001 — a sync hiccup must not break the page
        logger.exception("[sales] failed to start hubspot sync")
    status = hubspot_sync_status(request.app)
    settings = request.app.state.settings
    user = get_current_user(request)
    owner_filter = user.get("email") if (my and user) else None
    with session_scope(request.app.state.session_factory) as session:
        board = build_deal_board(session, owner_filter=owner_filter)
    return HTMLResponse(
        render_deal_board_page(
            board,
            user=user,
            sync_status=status,
            show_my=my,
            portal_id=settings.hubspot_portal_id or "",
        )
    )


@router.post("/deals/sync")
def trigger_sync(request: Request) -> RedirectResponse:
    start_hubspot_sync(request.app, force=True)
    return RedirectResponse(url="/admin/sales/deals", status_code=303)


@router.get("/deals/sync/status")
def sync_status(request: Request) -> JSONResponse:
    return JSONResponse(hubspot_sync_status(request.app))


# Defined after the static /deals/sync* paths so {deal_id} can't shadow them.
@router.get("/deals/{deal_id}", response_class=HTMLResponse)
def deal_detail(
    request: Request,
    deal_id: str,
    actioned: str = "",
    sent: str = "",
    error: str = "",
) -> Response:
    settings = request.app.state.settings
    flash = ""
    flash_ok = True
    if actioned:
        flash = "Done — action pushed to HubSpot. The board will refresh on next load."
    elif sent:
        flash = "Email sent and logged to HubSpot. Timeline will update on next sync."
    elif error:
        flash = f"Could not apply: {error}"
        flash_ok = False
    with session_scope(request.app.state.session_factory) as session:
        detail = build_deal_detail(session, deal_id, settings=settings)
        if detail is None:
            return HTMLResponse(_deal_not_found_page(request), status_code=404)
        html = render_deal_detail_page(
            detail, user=get_current_user(request), flash=flash, flash_ok=flash_ok
        )
    return HTMLResponse(html)


@router.post("/deals/{deal_id}/actions/approve")
def approve_action(
    request: Request,
    deal_id: str,
    action_id: str = Form(...),
) -> RedirectResponse:
    """Re-derive the action from current deal state and execute it against HubSpot."""
    settings = request.app.state.agent_settings
    client = HubSpotClient(settings)
    if not client.is_configured:
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}?error=HubSpot+token+not+configured",
            status_code=303,
        )

    with session_scope(request.app.state.session_factory) as session:
        deal = session.get(HubSpotDeal, deal_id)
        if deal is None:
            return RedirectResponse(url="/admin/sales/deals", status_code=303)
        signals = list(session.scalars(
            select(MailboxSignal).where(MailboxSignal.matched_deal_id == deal_id)
        ).all())
        li_total = session.execute(
            select(func.sum(HubSpotLineItem.amount_cents))
            .where(HubSpotLineItem.hubspot_deal_id == deal_id)
        ).scalar() or 0
        contact_link_ids = [
            r.hubspot_contact_id for r in session.scalars(
                select(HubSpotDealContact).where(HubSpotDealContact.hubspot_deal_id == deal_id)
            ).all()
        ]
        contacts = []
        for cid in contact_link_ids:
            c = session.get(HubSpotContact, cid)
            if c:
                contacts.append(ContactInfo(contact_id=cid, email=c.email or ""))
        actions = compute_pending_actions(
            deal, signals,
            line_item_total_cents=int(li_total),
            contacts=contacts,
            portal_id=settings.hubspot_portal_id or "",
        )

    action = next((a for a in actions if a.action_id == action_id), None)
    if action is None or action.action_type == "note" or not action.properties:
        return RedirectResponse(url=f"/admin/sales/deals/{deal_id}", status_code=303)

    try:
        if action.hubspot_object_type == "deals":
            client.update_deal(action.hubspot_object_id, action.properties)
        elif action.hubspot_object_type == "contacts":
            client.update_contact(action.hubspot_object_id, action.properties)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[sales] action approve failed for %s", action_id)
        msg = quote(str(exc)[:120], safe="")
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}?error={msg}", status_code=303
        )

    try:
        start_hubspot_sync(request.app, force=True)
    except Exception:  # noqa: BLE001
        logger.warning("[sales] post-approve sync failed to start")

    return RedirectResponse(url=f"/admin/sales/deals/{deal_id}?actioned=1", status_code=303)


@router.get("/deals/{deal_id}/draft-followup", response_class=HTMLResponse)
def draft_followup(request: Request, deal_id: str) -> Response:
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        detail = build_deal_detail(session, deal_id, settings=settings)
        if detail is None:
            return HTMLResponse("Deal not found.", status_code=404)

    hooks_sent = [a.asset_type for a in detail.assets]
    hooks_pending = [h for h in _HOOK_ORDER if h not in hooks_sent]
    contact_first = detail.contacts[0].name.split()[0] if detail.contacts else ""
    contact_emails = [c.email for c in detail.contacts if c.email]
    recent_subject = detail.timeline[0].title if detail.timeline else ""

    draft = build_followup_draft(
        company_name=detail.company_name,
        contact_first_name=contact_first,
        owner_email=detail.owner_email,
        deal_name=detail.name,
        deal_amount_cents=detail.amount_cents,
        hooks_sent=hooks_sent,
        hooks_pending=hooks_pending,
        recent_subject=recent_subject,
        contact_emails=contact_emails,
    )
    draft.gmail_configured = GmailClient(settings).is_configured()

    html = render_draft_followup_page(
        draft,
        deal_id=deal_id,
        deal_name=detail.name or deal_id,
        user=get_current_user(request),
    )
    return HTMLResponse(html)


@router.post("/deals/{deal_id}/send-followup")
def send_followup(
    request: Request,
    deal_id: str,
    subject: str = Form(...),
    body: str = Form(...),
    to_emails: str = Form(""),
    confirmed: str = Form(""),
) -> Response:
    """Two-step send: first POST shows preview; second POST (confirmed=1) sends."""
    settings = request.app.state.agent_settings
    gmail_client = GmailClient(settings)

    if not gmail_client.is_configured():
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}/draft-followup?error=Gmail+not+configured",
            status_code=303,
        )

    if not confirmed:
        # Step 1: show the confirmation preview page.
        with session_scope(request.app.state.session_factory) as session:
            deal = session.get(HubSpotDeal, deal_id)
            deal_name = deal.deal_name if deal else deal_id

        from_email = ""
        try:
            profile = gmail_client.get_profile()
            from_email = str(profile.get("emailAddress") or "").strip()
        except Exception:
            logger.warning("[sales] could not fetch Gmail profile for send preview")

        return HTMLResponse(
            render_send_preview_page(
                deal_id=deal_id,
                deal_name=deal_name,
                subject=subject,
                body=body,
                to_emails=to_emails,
                from_email=from_email,
                user=get_current_user(request),
            )
        )

    # Step 2: confirmed — execute the send.
    to_list = [e.strip() for e in to_emails.split(",") if e.strip()]
    if not to_list:
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}/draft-followup?error=No+recipients",
            status_code=303,
        )

    hubspot_client = HubSpotClient(settings)
    with session_scope(request.app.state.session_factory) as session:
        deal = session.get(HubSpotDeal, deal_id)
        if deal is None:
            return RedirectResponse(url="/admin/sales/deals", status_code=303)
        contact_ids = [
            r.hubspot_contact_id for r in session.scalars(
                select(HubSpotDealContact).where(HubSpotDealContact.hubspot_deal_id == deal_id)
            ).all()
        ]

    result = send_followup_email(
        gmail_client=gmail_client,
        hubspot_client=hubspot_client,
        deal_id=deal_id,
        contact_ids=contact_ids,
        to_emails=to_list,
        subject=subject,
        body_text=body,
    )

    if not result.ok:
        msg = quote(result.error[:120], safe="")
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}?error={msg}", status_code=303
        )

    try:
        start_hubspot_sync(request.app, force=True)
    except Exception:
        logger.warning("[sales] post-send sync failed to start")

    return RedirectResponse(
        url=f"/admin/sales/deals/{deal_id}?sent=1", status_code=303
    )
