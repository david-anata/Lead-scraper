"""Authenticated staff workspace for one Building booking."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.api.building_billing_router import (
    EventBillingPreparationInput,
    prepare_event_billing,
)
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingBillingSchedule,
    BuildingCalendarProjection,
    BuildingContact,
    BuildingInquiry,
    BuildingOperationalChecklist,
    BuildingOperationalChecklistItem,
    BuildingPaymentRequestReadiness,
    BuildingInvoice,
    BuildingProposal,
    BuildingRatePlan,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_booking_workspace import (
    render_booking_workspace,
)
from sales_support_agent.services.building_security import csrf_token
from sales_support_agent.services.building_security import require_building_form_security


router = APIRouter(
    prefix="/admin/building/bookings",
    tags=["building-booking-workspace"],
)
FORM_DEPS = [Depends(require_building_form_security)]


def _actor(user: dict) -> str:
    return str(user.get("email") or "building-operator")


@router.post("/{reservation_id}/billing/prepare", dependencies=FORM_DEPS)
def prepare_booking_billing(
    reservation_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Prepare exact booking billing drafts; create no provider objects."""

    internal_key = str(
        getattr(request.app.state.settings, "internal_api_key", "") or ""
    ).strip()
    if not internal_key:
        return RedirectResponse(
            f"/admin/building/bookings/{reservation_id}?error=Internal+billing+API+is+not+configured.",
            status_code=303,
        )
    try:
        result = prepare_event_billing(
            reservation_id,
            EventBillingPreparationInput(actor=_actor(user)),
            request,
            internal_key,
        )
    except HTTPException as exc:
        from urllib.parse import urlencode

        return RedirectResponse(
            f"/admin/building/bookings/{reservation_id}?{urlencode({'error': str(exc.detail)})}",
            status_code=303,
        )
    from urllib.parse import urlencode

    notice = (
        "Billing drafts already match this signed booking; nothing was sent."
        if result.get("duplicate")
        else f"Prepared {result['component_count']} billing drafts; nothing was sent to QuickBooks or the customer."
    )
    return RedirectResponse(
        f"/admin/building/bookings/{reservation_id}?{urlencode({'notice': notice})}",
        status_code=303,
    )


@router.get("/{reservation_id}", response_class=HTMLResponse)
def booking_workspace(
    reservation_id: str,
    request: Request,
    notice: str = "",
    error: str = "",
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    """Show one reservation without exposing unrelated customer records."""

    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(BuildingReservation, reservation_id)
        if reservation is None:
            raise HTTPException(status_code=404, detail="Booking not found.")
        contact = (
            session.get(BuildingContact, reservation.contact_id)
            if reservation.contact_id
            else None
        )
        inquiry = (
            session.get(BuildingInquiry, reservation.inquiry_id)
            if reservation.inquiry_id
            else None
        )
        space = session.get(BuildingSpace, reservation.space_id)
        proposals = session.execute(
            select(BuildingProposal)
            .where(BuildingProposal.reservation_id == reservation.id)
            .order_by(BuildingProposal.version.desc())
        ).scalars().all()
        proposal = proposals[0] if proposals else None
        transaction_date = reservation.starts_at.date()
        rate_plans = session.execute(
            select(BuildingRatePlan).where(
                BuildingRatePlan.offering_id == reservation.offering_id,
                BuildingRatePlan.status == "approved",
                BuildingRatePlan.effective_from <= transaction_date,
                (
                    BuildingRatePlan.effective_until.is_(None)
                    | (BuildingRatePlan.effective_until >= transaction_date)
                ),
            )
        ).scalars().all() if reservation.offering_id else []
        agreement = session.execute(
            select(BuildingAgreement)
            .where(BuildingAgreement.reservation_id == reservation.id)
            .order_by(BuildingAgreement.version.desc())
        ).scalars().first()
        payment = session.execute(
            select(BuildingPaymentRequestReadiness)
            .where(BuildingPaymentRequestReadiness.reservation_id == reservation.id)
            .order_by(BuildingPaymentRequestReadiness.version.desc())
        ).scalars().first()
        billing_schedules = session.execute(
            select(BuildingBillingSchedule)
            .where(BuildingBillingSchedule.reservation_id == reservation.id)
            .order_by(BuildingBillingSchedule.starts_on, BuildingBillingSchedule.id)
        ).scalars().all()
        invoices = session.execute(
            select(BuildingInvoice)
            .where(BuildingInvoice.reservation_id == reservation.id)
            .order_by(BuildingInvoice.created_at.desc())
        ).scalars().all()
        calendar = session.execute(
            select(BuildingCalendarProjection).where(
                BuildingCalendarProjection.reservation_id == reservation.id
            )
        ).scalar_one_or_none()
        checklists = session.execute(
            select(BuildingOperationalChecklist).where(
                BuildingOperationalChecklist.reservation_id == reservation.id
            )
        ).scalars().all()
        checklist_ids = [row.id for row in checklists]
        checklist_items = (
            session.execute(
                select(BuildingOperationalChecklistItem).where(
                    BuildingOperationalChecklistItem.checklist_id.in_(checklist_ids)
                )
            ).scalars().all()
            if checklist_ids
            else []
        )
        required = [row for row in checklist_items if row.is_required]
        completed = [
            row for row in required if row.status in {"completed", "waived"}
        ]
        checklist_status = (
            "completed"
            if required and len(completed) == len(required)
            else "in_progress"
            if checklist_items
            else "not_started"
        )
        data = {
            "reservation": {
                "id": reservation.id,
                "kind": reservation.kind,
                "status": reservation.status,
                "inquiry_id": reservation.inquiry_id,
                "contact_id": reservation.contact_id,
                "offering_id": reservation.offering_id,
                "starts_at": reservation.starts_at,
                "ends_at": reservation.ends_at,
                "guest_starts_at": reservation.guest_starts_at,
                "guest_ends_at": reservation.guest_ends_at,
                "hold_expires_at": reservation.hold_expires_at,
                "attendance": reservation.attendance,
                "agreement_status": reservation.agreement_status,
                "deposit_status": reservation.deposit_status,
                "deposit_required": reservation.deposit_required,
                "assigned_owner": reservation.assigned_owner,
                "source": reservation.source,
                "source_reference": reservation.source_reference,
            },
            "contact": (
                {
                    "full_name": contact.full_name,
                    "email": contact.email,
                }
                if contact
                else None
            ),
            "inquiry": (
                {"name": inquiry.name, "email": inquiry.email}
                if inquiry
                else None
            ),
            "space_name": space.name if space else "Unknown space",
            "proposal": (
                {
                    "id": proposal.id,
                    "version": proposal.version,
                    "status": proposal.status,
                    "currency": proposal.currency,
                    "amount_cents": proposal.amount_cents,
                    "line_items": list(proposal.line_items_json or []),
                    "rate_plan_id": proposal.rate_plan_id,
                    "rate_plan_snapshot": dict(proposal.rate_plan_snapshot_json or {}),
                    "terms_summary": proposal.terms_summary,
                    "valid_until": proposal.valid_until.isoformat() if proposal.valid_until else "",
                    "document_url": proposal.document_url,
                }
                if proposal
                else None
            ),
            "quote_versions": [
                {
                    "id": row.id,
                    "version": row.version,
                    "status": row.status,
                    "currency": row.currency,
                    "amount_cents": row.amount_cents,
                    "line_items": list(row.line_items_json or []),
                    "rate_plan_snapshot": dict(row.rate_plan_snapshot_json or {}),
                    "terms_summary": row.terms_summary,
                    "valid_until": row.valid_until.isoformat() if row.valid_until else "",
                    "document_url": row.document_url,
                }
                for row in proposals
            ],
            "approved_rate_plans": [
                {
                    "id": row.id,
                    "name": row.name,
                    "version": row.version,
                    "tax_status": row.tax_status,
                    "tax_rate_bps": row.tax_rate_bps,
                }
                for row in rate_plans
            ],
            "agreement": (
                {
                    "id": agreement.id,
                    "status": agreement.status,
                    "preparation_status": agreement.preparation_status,
                }
                if agreement
                else None
            ),
            "payment": (
                {"id": payment.id, "status": payment.status}
                if payment
                else None
            ),
            "billing": {
                "schedules": [
                    {
                        "id": row.id,
                        "component": row.billing_component or row.schedule_type,
                        "status": row.status,
                        "amount_cents": row.amount_cents,
                        "currency": row.currency,
                        "starts_on": row.starts_on.isoformat(),
                    }
                    for row in billing_schedules
                ],
                "invoices": [
                    {
                        "id": row.id,
                        "status": row.status,
                        "amount_due_cents": row.amount_due_cents,
                        "amount_paid_cents": row.amount_paid_cents,
                        "currency": row.currency,
                        "qbo_invoice_id": row.qbo_invoice_id,
                        "url": row.hosted_invoice_url,
                    }
                    for row in invoices
                ],
            },
            "calendar": (
                {"status": calendar.status}
                if calendar
                else None
            ),
            "checklist": {
                "status": checklist_status,
                "summary": (
                    f"{len(completed)} of {len(required)} required items complete"
                    if required
                    else "No event-day checklist yet"
                ),
            },
        }
    return HTMLResponse(
        render_booking_workspace(
            navigation=render_agent_nav("building", user=user),
            data=data,
            csrf_token=csrf_token(user),
            notice=notice,
            error=error,
        ),
        headers={"Cache-Control": "private, no-store"},
    )
