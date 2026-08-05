"""Authenticated staff workspace for one Building inquiry."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingContact,
    BuildingInquiry,
    BuildingRelationship,
    BuildingReservation,
)
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_inquiry_workspace import (
    is_test_inquiry,
    render_inquiry_workspace,
)
from sales_support_agent.services.building_security import csrf_token


router = APIRouter(
    prefix="/admin/building/inquiries",
    tags=["building-inquiry-workspace"],
)


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
        payload = dict(inquiry.payload_json or {})
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
            "details": {
                key: value
                for key, value in payload.items()
                if not str(key).startswith("_")
            },
            "lifecycle": dict(payload.get("_lifecycle") or {}),
            "attribution": dict(payload.get("_attribution") or {}),
            "event_interview": dict(payload.get("_event_interview") or {}),
            "lead_notification": dict(payload.get("_lead_notification") or {}),
            "lead_escalation": dict(payload.get("_lead_escalation") or {}),
            "customer_receipt": dict(payload.get("_customer_receipt") or {}),
            "follow_up_sequence": list(payload.get("_follow_up_sequence") or []),
            "reservation_id": reservation.id if reservation else "",
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
