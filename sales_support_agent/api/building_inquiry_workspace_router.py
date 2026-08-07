"""Authenticated staff workspace for one Building inquiry."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingContact,
    BuildingInquiry,
    BuildingRatePlan,
    BuildingRelationship,
    BuildingReservation,
)
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_inquiry_workspace import (
    is_test_inquiry,
    render_inquiry_workspace,
)
from sales_support_agent.services.building_lead_intake import prefill_event_interview
from sales_support_agent.services.building_lead_pricing import (
    LeadPricingError,
    compute_totals,
    default_pricing,
    parse_pricing_form,
)
from sales_support_agent.services.building_public_availability import candidate_date_availability
from sales_support_agent.integrations.building_google_calendar import BuildingGoogleCalendarClient
from sales_support_agent.services.building_security import (
    csrf_token,
    require_building_form_security,
)


FORM_DEPS = [Depends(require_building_form_security)]
router = APIRouter(
    prefix="/admin/building/inquiries",
    tags=["building-inquiry-workspace"],
)


@router.get("/{inquiry_id}/availability")
def inquiry_date_availability(
    inquiry_id: str,
    request: Request,
    dates: str = Query(min_length=10, max_length=32),
    setup_start_time: str = Query(default="", max_length=8),
    guest_start_time: str = Query(default="", max_length=8),
    guest_end_time: str = Query(default="", max_length=8),
    teardown_end_time: str = Query(default="", max_length=8),
    user: dict = Depends(require_tool("building.events.manage")),
) -> dict:
    """Compare candidate dates without exposing calendar event details."""

    del user
    raw_dates = list(dict.fromkeys(item.strip() for item in dates.split(",") if item.strip()))
    if not raw_dates or len(raw_dates) > 3:
        raise HTTPException(status_code=422, detail="Provide one to three candidate dates.")
    try:
        candidates = [date.fromisoformat(item) for item in raw_dates]
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Candidate dates must use YYYY-MM-DD.") from exc
    today = date.today()
    if any(item < today or item > today + timedelta(days=730) for item in candidates):
        raise HTTPException(status_code=422, detail="Candidate dates must be within the next two years.")
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None or inquiry.kind != "event":
            raise HTTPException(status_code=404, detail="Event inquiry not found.")
        try:
            return candidate_date_availability(
                session,
                calendar=BuildingGoogleCalendarClient(),
                candidates=candidates,
                setup_start_time=setup_start_time,
                guest_start_time=guest_start_time,
                guest_end_time=guest_end_time,
                teardown_end_time=teardown_end_time,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/{inquiry_id}", response_class=HTMLResponse)
def inquiry_workspace(
    inquiry_id: str,
    request: Request,
    notice: str = "",
    error: str = "",
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    """Show one inquiry without exposing unrelated customer records."""

    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        relationship = session.execute(
            select(BuildingRelationship).where(
                BuildingRelationship.source_reference == f"inquiry:{inquiry.id}"
            )
        ).scalars().first()
        contact = (
            session.get(BuildingContact, relationship.contact_id)
            if relationship is not None
            else None
        )
        reservation = session.execute(
            select(BuildingReservation)
            .where(BuildingReservation.inquiry_id == inquiry.id)
            .order_by(BuildingReservation.created_at.desc())
        ).scalars().first()
        activity = session.execute(
            select(BuildingAuditEvent)
            .where(
                BuildingAuditEvent.entity_type == "inquiry",
                BuildingAuditEvent.entity_id == inquiry.id,
            )
            .order_by(BuildingAuditEvent.created_at.desc())
            .limit(100)
        ).scalars().all()
        contact_options = [] if contact is not None else [
            {"id": row.id, "label": f"{row.full_name or row.email} · {row.email}"}
            for row in session.execute(
                select(BuildingContact)
                .where(BuildingContact.status == "active")
                .order_by(BuildingContact.full_name, BuildingContact.email)
                .limit(500)
            ).scalars().all()
        ]
        rate_plan_rows = session.execute(
            select(BuildingRatePlan)
            .where(BuildingRatePlan.status == "approved")
            .order_by(BuildingRatePlan.name, BuildingRatePlan.version.desc())
        ).scalars().all()
        payload = dict(inquiry.payload_json or {})
        stored_pricing = dict(payload.get("_pricing") or {})
        if not stored_pricing:
            plan = rate_plan_rows[0] if rate_plan_rows else None
            stored_pricing = default_pricing({
                "id": plan.id, "name": plan.name, "currency": plan.currency,
                "unit_amount_cents": plan.unit_amount_cents,
                "minimum_units": plan.minimum_units,
                "deposit_percent_bps": plan.deposit_percent_bps,
            } if plan is not None else None)
        public_details = {
            key: value for key, value in payload.items() if not str(key).startswith("_")
        }
        prefilled_interview, _ = prefill_event_interview(
            preferred_date=inquiry.preferred_date,
            details=public_details,
        )
        event_interview = {
            **prefilled_interview,
            **dict(payload.get("_event_interview") or {}),
        }
        data = {
            "id": inquiry.id,
            "name": inquiry.name,
            "email": inquiry.email,
            "phone": inquiry.phone or (contact.phone if contact else ""),
            "kind": inquiry.kind,
            "preferred_date": (
                inquiry.preferred_date.isoformat() if inquiry.preferred_date else ""
            ),
            "source": inquiry.source,
            "source_reference": inquiry.source_reference,
            "assigned_owner": inquiry.assigned_owner,
            "response_due_at": inquiry.response_due_at,
            "created_at": inquiry.created_at,
            "details": public_details,
            "lifecycle": dict(payload.get("_lifecycle") or {}),
            "attribution": dict(payload.get("_attribution") or {}),
            "event_interview": event_interview,
            "lead_notification": dict(payload.get("_lead_notification") or {}),
            "lead_escalation": dict(payload.get("_lead_escalation") or {}),
            "customer_receipt": dict(payload.get("_customer_receipt") or {}),
            "follow_up_sequence": list(payload.get("_follow_up_sequence") or []),
            "reservation_id": reservation.id if reservation else "",
            "contact": {
                "id": contact.id,
                "full_name": contact.full_name,
                "email": contact.email,
                "phone": contact.phone or "",
            } if contact is not None else {},
            "contact_options": contact_options,
            "pricing": stored_pricing,
            "pricing_totals": compute_totals(stored_pricing),
            "rate_plans": [
                {
                    "name": row.name,
                    "version": row.version,
                    "currency": row.currency,
                    "unit_amount_cents": row.unit_amount_cents,
                    "public_price_display": row.public_price_display,
                    "booking_unit": row.booking_unit,
                    "deposit_type": row.deposit_type,
                    "deposit_amount_cents": row.deposit_amount_cents,
                    "deposit_percent_bps": row.deposit_percent_bps,
                }
                for row in rate_plan_rows
            ],
            "is_test": is_test_inquiry(
                name=inquiry.name,
                email=inquiry.email,
                source=inquiry.source,
            ),
            "activity": [
                {
                    "action": row.action,
                    "actor": row.actor,
                    "created_at": row.created_at,
                }
                for row in activity
            ],
        }
    return HTMLResponse(
        render_inquiry_workspace(
            navigation=render_agent_nav("building", user=user),
            data=data,
            csrf_token=csrf_token(user),
            notice=notice,
            error=error,
        ),
        headers={"Cache-Control": "private, no-store"},
    )


@router.post("/{inquiry_id}/link-contact", dependencies=FORM_DEPS)
def link_existing_contact(
    inquiry_id: str,
    request: Request,
    contact_id: str = Form(...),
    user: dict = Depends(require_tool("building.crm.manage")),
) -> RedirectResponse:
    """Attach an existing customer to this lead.

    Only the relationship is written. The saved contact's name, email, phone,
    and company are left exactly as they are — the control-room contact form
    overwrites those fields, so linking deliberately does not go through it.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        contact = session.get(BuildingContact, contact_id.strip())
        if contact is None:
            return RedirectResponse(
                f"{target}?error=That+customer+no+longer+exists.", status_code=303
            )
        reference = f"inquiry:{inquiry.id}"
        existing = session.execute(
            select(BuildingRelationship).where(
                BuildingRelationship.source_reference == reference,
                BuildingRelationship.status == "active",
            )
        ).scalars().first()
        if existing is not None:
            if existing.contact_id == contact.id:
                return RedirectResponse(
                    f"{target}?notice=That+customer+is+already+linked.", status_code=303
                )
            return RedirectResponse(
                f"{target}?error=This+lead+is+already+linked+to+another+customer.",
                status_code=303,
            )
        session.add(BuildingRelationship(
            id=str(uuid4()),
            contact_id=contact.id,
            relationship_type="prospect",
            status="active",
            source_reference=reference,
        ))
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="inquiry_contact_linked",
            actor=str(user.get("email") or "building-operator"),
            after_json={
                "contact_id": contact.id,
                "created_contact": False,
                "contact_details_modified": False,
            },
        ))
    return RedirectResponse(
        f"{target}?notice=Customer+linked+to+this+lead.", status_code=303
    )


@router.post("/{inquiry_id}/pricing", dependencies=FORM_DEPS)
async def save_lead_pricing(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Save pricing for this lead only.

    Never writes to the rate plan, so the standard rate and every other lead are
    untouched. Deliberately editable at any stage; each generated contract keeps
    its own copy of the numbers it was built from.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    form = await request.form()
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        payload = dict(inquiry.payload_json or {})
        try:
            pricing = parse_pricing_form(
                form,
                existing=dict(payload.get("_pricing") or {}) or default_pricing(None),
                actor=str(user.get("email") or "building-operator"),
            )
        except LeadPricingError as exc:
            return RedirectResponse(
                f"{target}?error={quote_plus(str(exc))}", status_code=303
            )
        before = dict(payload.get("_pricing") or {})
        payload["_pricing"] = pricing
        inquiry.payload_json = payload
        inquiry.updated_at = datetime.now(timezone.utc)
        session.add(inquiry)
        totals = compute_totals(pricing)
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="lead_pricing_updated",
            actor=str(user.get("email") or "building-operator"),
            before_json={"total_cents": compute_totals(before)["total_cents"]} if before else {},
            after_json={
                "total_cents": totals["total_cents"],
                "deposit_cents": totals["deposit_cents"],
                "rate_plan_changed": False,
                "customer_contacted": False,
            },
        ))
    return RedirectResponse(
        f"{target}?notice=Pricing+saved+for+this+lead+only.", status_code=303
    )
