"""Authenticated staff workspace for one Building booking."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingCalendarProjection,
    BuildingContact,
    BuildingInquiry,
    BuildingOperationalChecklist,
    BuildingOperationalChecklistItem,
    BuildingPaymentRequestReadiness,
    BuildingProposal,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_booking_workspace import (
    render_booking_workspace,
)
from sales_support_agent.services.building_security import csrf_token


router = APIRouter(
    prefix="/admin/building/bookings",
    tags=["building-booking-workspace"],
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
        proposal = session.execute(
            select(BuildingProposal)
            .where(BuildingProposal.reservation_id == reservation.id)
            .order_by(BuildingProposal.version.desc())
        ).scalars().first()
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
                }
                if proposal
                else None
            ),
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
