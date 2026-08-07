"""Authenticated staff workspace for one Building inquiry."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAgreementTemplate,
    BuildingAuditEvent,
    BuildingContact,
    BuildingInquiry,
    BuildingOffering,
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
from pydantic import ValidationError

from sales_support_agent.api.building_booking_router import (
    EventReviewInput,
    create_event_review,
)
from sales_support_agent.api.building_agreement_readiness_router import (
    AgreementPackageInput,
    prepare_agreement_package,
)
from sales_support_agent.services.building_lead_quote_sync import (
    sync_quote_from_lead_pricing,
)
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


MOUNTAIN = ZoneInfo("America/Denver")
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


@router.post("/{inquiry_id}/contract", dependencies=FORM_DEPS)
def create_contract_from_lead(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Turn this lead into a contract without leaving it.

    Writes the lead's pricing into the booking's quote, then prepares the
    agreement package from it. The quote keeps its existing shape, so billing,
    invoicing, and the QuickBooks handoff are untouched.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    actor = str(user.get("email") or "building-operator")
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        reservation = session.execute(
            select(BuildingReservation)
            .where(BuildingReservation.inquiry_id == inquiry.id)
            .order_by(BuildingReservation.created_at.desc())
        ).scalars().first()
        if reservation is None:
            return RedirectResponse(
                f"{target}?error={quote_plus('Hold the date first — use Take this date under Date review on this page. A contract attaches to a held date.')}"
                "#date-review",
                status_code=303,
            )
        pricing = dict((inquiry.payload_json or {}).get("_pricing") or {})
        if not pricing:
            return RedirectResponse(
                f"{target}?error={quote_plus('Save the pricing first — use Pricing for this event on this page.')}"
                "#lead-pricing",
                status_code=303,
            )
        quote = sync_quote_from_lead_pricing(
            session, reservation=reservation, pricing=pricing, actor=actor
        )
        session.flush()
        quote_id = quote.id
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="lead_quote_synced",
            actor=actor,
            after_json={
                "quote_id": quote_id,
                "quote_version": quote.version,
                "amount_cents": quote.amount_cents,
                "customer_contacted": False,
            },
        ))

    template = _approved_template_id(request)
    if not template:
        return RedirectResponse(
            f"{target}?error={quote_plus('No approved contract template exists yet.')}",
            status_code=303,
        )
    try:
        result = prepare_agreement_package(
            AgreementPackageInput(
                reservation_id=reservation.id,
                quote_id=quote_id,
                template_id=template,
                agreement_version=_next_agreement_version(request, reservation.id),
                payment_version=_next_agreement_version(request, reservation.id),
                actor=actor,
            ),
            request,
            f"lead-contract-{uuid4().hex[:16]}",
            _internal_api_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return RedirectResponse(
            f"{target}?error={quote_plus(str(getattr(exc, 'detail', exc)))}",
            status_code=303,
        )
    agreement_id = str((result.get("agreement") or {}).get("id") or "")
    return RedirectResponse(
        f"/admin/building/contracts/{agreement_id}"
        "?notice=Contract+prepared+from+this+lead.+Nothing+was+sent.",
        status_code=303,
    )


def _approved_template_id(request: Request) -> str:
    with session_scope(request.app.state.session_factory) as session:
        row = session.execute(
            select(BuildingAgreementTemplate.id)
            .where(BuildingAgreementTemplate.status == "approved")
            .order_by(BuildingAgreementTemplate.version.desc())
        ).scalars().first()
    return str(row or "")


def _next_agreement_version(request: Request, reservation_id: str) -> int:
    with session_scope(request.app.state.session_factory) as session:
        used = session.execute(
            select(BuildingAgreement.version).where(
                BuildingAgreement.reservation_id == reservation_id
            )
        ).scalars().all()
    return (max(used) + 1) if used else 1


def _internal_api_key(request: Request) -> str:
    key = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not key:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    return key


@router.post("/{inquiry_id}/hold-date", dependencies=FORM_DEPS)
async def hold_date_from_lead(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.events.manage")),
) -> RedirectResponse:
    """Take the date for this lead without leaving it.

    Creates the authoritative access window and its frozen quote through the
    same conflict-checked path the booking workspace uses, so nothing about how
    a hold is made changes — only where it can be started.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    actor = str(user.get("email") or "building-operator")
    form = await request.form()

    def fail(message: str) -> RedirectResponse:
        return RedirectResponse(f"{target}?error={quote_plus(message)}", status_code=303)

    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        if inquiry.kind != "event":
            return fail("Only event inquiries take a date this way.")
        existing = session.execute(
            select(BuildingReservation).where(
                BuildingReservation.inquiry_id == inquiry.id
            )
        ).scalars().first()
        if existing is not None:
            return fail("This lead already has a booking.")
        relationship = session.execute(
            select(BuildingRelationship).where(
                BuildingRelationship.source_reference == f"inquiry:{inquiry.id}"
            )
        ).scalars().first()
        if relationship is None:
            return fail("Link or create the customer on this lead first.")
        contact_id = relationship.contact_id
        offering = session.execute(
            select(BuildingOffering)
            .where(BuildingOffering.offering_type == "event")
            .order_by(BuildingOffering.name)
        ).scalars().first()
        if offering is None:
            return fail("No event offering exists to book against.")
        offering_id, space_id = offering.id, offering.space_id
        pricing = dict((inquiry.payload_json or {}).get("_pricing") or {})
        interview = dict((inquiry.payload_json or {}).get("_event_interview") or {})
        assigned_owner = str(inquiry.assigned_owner or "").strip()

    def _at(field: str) -> Optional[datetime]:
        raw = str(form.get(field) or "").strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=MOUNTAIN)

    setup = _at("setup_starts_at")
    guest_start = _at("guest_starts_at")
    guest_end = _at("guest_ends_at")
    teardown = _at("teardown_ends_at")
    if not all((setup, guest_start, guest_end, teardown)):
        return fail("Give the full window: setup, guests in, guests out, teardown.")
    if not setup <= guest_start < guest_end <= teardown:
        return fail("The window must run setup, guests in, guests out, teardown in order.")

    try:
        attendance = int(str(form.get("attendance") or "0").strip() or 0)
    except ValueError:
        return fail("Attendance must be a whole number.")
    if attendance < 1:
        attendance = int(str(interview.get("attendance") or "0").split()[0] or 0) or 1

    hours = max(1, int(pricing.get("hours") or 1))
    try:
        result = create_event_review(
            EventReviewInput(
                inquiry_id=inquiry_id,
                reservation_id=f"event-{uuid4().hex[:12]}",
                space_id=space_id,
                offering_id=offering_id,
                contact_id=contact_id,
                setup_starts_at=setup.astimezone(timezone.utc),
                guest_starts_at=guest_start.astimezone(timezone.utc),
                guest_ends_at=guest_end.astimezone(timezone.utc),
                teardown_ends_at=teardown.astimezone(timezone.utc),
                hold_expires_at=datetime.now(timezone.utc) + timedelta(days=7),
                attendance=attendance,
                units=hours,
                addons=[],
                terms_summary=(
                    f"{hours} hour booking held from this lead by {actor}."
                ),
                assigned_owner=assigned_owner or actor,
                actor=actor,
            ),
            request,
            f"lead-hold-{uuid4().hex[:16]}",
            _internal_api_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return fail(str(getattr(exc, "detail", exc)))
    del result
    return RedirectResponse(
        f"{target}?notice={quote_plus('Date held for seven days and a quote frozen. Nothing was sent.')}",
        status_code=303,
    )
