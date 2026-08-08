"""Deterministic workspace and event booking workflows."""

from __future__ import annotations

import hashlib
import hmac
import json
import base64
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional
from uuid import NAMESPACE_URL, uuid4, uuid5

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import delete, select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAuditEvent,
    BuildingAvailabilityBlock,
    BuildingContact,
    BuildingDepositEvidence,
    BuildingEventLifecycleCommand,
    BuildingInvoice,
    BuildingInquiry,
    BuildingOffering,
    BuildingProposal,
    BuildingPaymentRequestReadiness,
    BuildingRatePlan,
    BuildingRelationship,
    BuildingReservation,
    BuildingCalendarProjection,
    BuildingSignatureRequestReadiness,
    BuildingServiceRequest,
    BuildingSpace,
    BuildingTour,
)
from sales_support_agent.services.building_calendar import queue_calendar_projection
from sales_support_agent.integrations.building_google_calendar import (
    BuildingGoogleCalendarClient,
)
from sales_support_agent.services.building_checklists import (
    ensure_operational_checklist,
)
from sales_support_agent.services.building_agreement_readiness import (
    propagate_event_readiness_terminal_state,
)
from sales_support_agent.services.building_lead_intake import (
    event_qualification_missing,
)
from sales_support_agent.services.building_transactional_messages import (
    attempt_booking_message,
)


router = APIRouter(prefix="/api/internal/building/bookings", tags=["building-bookings"])
public_router = APIRouter(
    prefix="/api/public/building/bookings", tags=["building-bookings-public"]
)

EVENT_TRANSITIONS = {
    "inquiry": {"requirements_review", "cancelled"},
    "requirements_review": {"soft_hold", "quote_sent", "cancelled"},
    "soft_hold": {"quote_sent", "expired", "cancelled"},
    "quote_sent": {"contract_pending", "cancelled"},
    "contract_pending": {"deposit_due", "confirmed", "cancelled"},
    "deposit_due": {"confirmed", "cancelled"},
    "confirmed": {"pre_event", "cancelled"},
    "pre_event": {"completed", "cancelled"},
    "completed": set(),
    "expired": set(),
    "cancelled": set(),
}
WORKSPACE_TRANSITIONS = {
    "inquiry": {"qualified", "cancelled"},
    "qualified": {"tour_scheduled", "proposal_sent", "cancelled"},
    "tour_scheduled": {"tour_completed", "cancelled"},
    "tour_completed": {"proposal_sent", "cancelled"},
    "proposal_sent": {"contract_pending", "cancelled"},
    "contract_pending": {"deposit_due", "confirmed", "cancelled"},
    "deposit_due": {"confirmed", "cancelled"},
    "confirmed": {"occupied", "cancelled"},
    "occupied": {"renewal", "move_out"},
    "renewal": {"occupied", "move_out"},
    "move_out": {"completed"},
    "completed": set(),
    "cancelled": set(),
}
AGREEMENT_STATUSES = {"draft", "sent", "signed", "voided"}
DEPOSIT_STATUSES = {"not_started", "due", "pending", "paid", "refunded", "waived"}
PROPOSAL_TRANSITIONS = {
    "draft": {"approved", "voided"},
    "approved": {"sent", "voided"},
    "sent": {"accepted", "declined", "voided"},
    "accepted": set(),
    "declined": set(),
    "voided": set(),
}
TOUR_TERMINAL_STATUSES = {"completed", "cancelled", "no_show"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


class ReservationInput(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    kind: Literal["event", "workspace"]
    space_id: str = Field(min_length=1, max_length=64)
    offering_id: str | None = Field(default=None, max_length=64)
    inquiry_id: str | None = Field(default=None, max_length=64)
    contact_id: str | None = Field(default=None, max_length=64)
    starts_at: datetime
    ends_at: datetime
    attendance: int = Field(default=0, ge=0)
    deposit_required: bool = True
    assigned_owner: str = Field(default="", max_length=255)
    requirements: dict[str, Any] = Field(default_factory=dict)
    source: str = Field(default="agent", max_length=64)
    source_reference: str = Field(default="", max_length=255)
    actor: str = Field(min_length=1, max_length=255)

    @model_validator(mode="after")
    def valid_window(self) -> "ReservationInput":
        if self.ends_at <= self.starts_at:
            raise ValueError("Reservation end must be after start.")
        return self


class CustomerStatusAccessInput(BaseModel):
    expires_in_days: int = Field(default=30, ge=1, le=90)
    actor: str = Field(min_length=1, max_length=255)


class CustomerBookingRequestInput(BaseModel):
    request_type: Literal["reschedule", "cancellation", "question"]
    details: str = Field(min_length=10, max_length=4000)
    requested_starts_at: datetime | None = None
    requested_ends_at: datetime | None = None

    @model_validator(mode="after")
    def valid_requested_window(self) -> "CustomerBookingRequestInput":
        if bool(self.requested_starts_at) != bool(self.requested_ends_at):
            raise ValueError("Provide both requested start and end times.")
        if (
            self.requested_starts_at
            and self.requested_ends_at
            and self.requested_ends_at <= self.requested_starts_at
        ):
            raise ValueError("Requested end must be after requested start.")
        return self


class CommunicationRunInput(BaseModel):
    execute: bool = False
    actor: str = Field(min_length=1, max_length=255)


class TransitionInput(BaseModel):
    target_status: str = Field(min_length=1, max_length=32)
    hold_expires_at: datetime | None = None
    actor: str = Field(min_length=1, max_length=255)
    reason: str = Field(default="", max_length=1000)


class EventReviewInput(BaseModel):
    """One atomic, idempotent date-review and quote-readiness command."""

    inquiry_id: str = Field(min_length=1, max_length=64)
    reservation_id: str = Field(min_length=1, max_length=64)
    space_id: str = Field(min_length=1, max_length=64)
    offering_id: str = Field(min_length=1, max_length=64)
    contact_id: str | None = Field(default=None, max_length=64)
    setup_starts_at: datetime
    guest_starts_at: datetime
    guest_ends_at: datetime
    teardown_ends_at: datetime
    hold_expires_at: datetime
    attendance: int = Field(ge=1)
    units: int = Field(ge=1)
    addons: list[dict[str, Any]] = Field(default_factory=list, max_length=50)
    terms_summary: str = Field(min_length=1, max_length=4000)
    operator_notes: str = Field(default="", max_length=4000)
    assigned_owner: str = Field(min_length=1, max_length=255)
    actor: str = Field(min_length=1, max_length=255)
    #: Take the date even though something else already occupies it. A double
    #: booking is the owner's call to make, never the system's to make quietly,
    #: so the clash it overrode is written into the audit trail and the booking.
    override_conflicts: bool = False

    @model_validator(mode="after")
    def valid_windows(self) -> "EventReviewInput":
        if not (
            self.setup_starts_at
            <= self.guest_starts_at
            < self.guest_ends_at
            <= self.teardown_ends_at
        ):
            raise ValueError(
                "Times must run from setup start through guest start, guest end, and teardown end."
            )
        if self.hold_expires_at <= _now():
            raise ValueError("A future hold expiration is required.")
        addon_ids = [str(item.get("addon_id") or "") for item in self.addons]
        if any(not value for value in addon_ids) or len(addon_ids) != len(set(addon_ids)):
            raise ValueError("Each selected add-on requires one unique addon_id.")
        return self


class AgreementInput(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    version: int = Field(default=1, ge=1)
    status: str
    provider: str = Field(default="", max_length=64)
    provider_reference: str = Field(default="", max_length=255)
    template_name: str = Field(default="", max_length=255)
    document_url: str = Field(default="", max_length=1024)
    evidence: dict[str, Any] = Field(default_factory=dict)
    actor: str = Field(min_length=1, max_length=255)


class ProposalInput(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    version: int = Field(default=1, ge=1)
    status: Literal["draft", "approved", "sent", "accepted", "declined", "voided"]
    proposal_type: Literal["proposal", "quote"] = "proposal"
    currency: str = Field(default="USD", min_length=3, max_length=3)
    amount_cents: int = Field(default=0, ge=0)
    pricing_subtotal_cents: int | None = Field(default=None, ge=0)
    discount_cents: int = Field(default=0, ge=0)
    discount_reason: str = Field(default="", max_length=1000)
    line_items: list[dict[str, Any]] = Field(default_factory=list)
    rate_plan_id: str | None = Field(default=None, max_length=64)
    terms_summary: str = Field(default="", max_length=4000)
    valid_until: date | None = None
    document_url: str = Field(default="", max_length=1024)
    approved_by: str = Field(default="", max_length=255)
    actor: str = Field(min_length=1, max_length=255)


class TourInput(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    scheduled_at: datetime
    duration_minutes: int = Field(default=30, ge=15, le=240)
    status: Literal["scheduled", "completed", "cancelled", "no_show"] = "scheduled"
    host: str = Field(default="", max_length=255)
    meeting_location: str = Field(default="Anata Building", max_length=255)
    notes: str = Field(default="", max_length=4000)
    outcome: str = Field(default="", max_length=64)
    next_step: str = Field(default="", max_length=2000)
    reason: str = Field(default="", max_length=1000)
    actor: str = Field(min_length=1, max_length=255)


class TourInquiryHandoffInput(BaseModel):
    inquiry_id: str = Field(min_length=1, max_length=64)
    offering_id: str = Field(min_length=1, max_length=64)
    space_id: str = Field(min_length=1, max_length=64)
    scheduled_at: datetime
    duration_minutes: int = Field(ge=15, le=240)
    host: str = Field(min_length=1, max_length=255)
    meeting_location: str = Field(min_length=1, max_length=255)
    notes: str = Field(default="", max_length=4000)
    actor: str = Field(min_length=1, max_length=255)


class DepositInput(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    status: str
    amount_cents: int = Field(default=0, ge=0)
    provider: str = Field(default="", max_length=64)
    provider_reference: str = Field(default="", max_length=255)
    evidence: dict[str, Any] = Field(default_factory=dict)
    actor: str = Field(min_length=1, max_length=255)


def _reservation_payload(row: BuildingReservation) -> dict[str, Any]:
    return {
        "id": row.id,
        "kind": row.kind,
        "status": row.status,
        "space_id": row.space_id,
        "offering_id": row.offering_id,
        "inquiry_id": row.inquiry_id,
        "contact_id": row.contact_id,
        "starts_at": row.starts_at.isoformat(),
        "ends_at": row.ends_at.isoformat(),
        "guest_starts_at": (
            row.guest_starts_at.isoformat() if row.guest_starts_at else None
        ),
        "guest_ends_at": row.guest_ends_at.isoformat() if row.guest_ends_at else None,
        "hold_expires_at": row.hold_expires_at.isoformat() if row.hold_expires_at else None,
        "attendance": row.attendance,
        "agreement_status": row.agreement_status,
        "deposit_status": row.deposit_status,
        "deposit_required": row.deposit_required,
        "assigned_owner": row.assigned_owner,
        "requirements": dict(row.requirements_json or {}),
        "calendar_event_id": row.calendar_event_id,
        "updated_at": (row.updated_at or _now()).isoformat(),
    }


def _customer_event_status(row: BuildingReservation) -> dict[str, Any]:
    """Deliberately redacted projection; never implies booking, signature, or payment."""

    stages = {
        "inquiry": ("Request received", "Our team is reviewing your request."),
        "requirements_review": ("Date review", "We are reviewing the full event access window."),
        "soft_hold": ("Temporary hold", "A temporary hold is active while quote details are reviewed."),
        "quote_sent": ("Quote sent", "Review the current quote; the event is not booked yet."),
        "contract_pending": ("Agreement review", "Agreement review is in progress; the event is not booked yet."),
        "deposit_due": ("Payment required", "Required agreement and payment steps remain."),
        "confirmed": ("Confirmed", "Agent has verified the agreement and required payment evidence."),
        "expired": ("Hold expired", "The temporary hold expired; contact the events team to review dates."),
        "cancelled": ("Cancelled", "This event request is closed."),
    }
    label, message = stages.get(
        row.status, ("In progress", "Contact the events team for the latest status.")
    )
    return {
        "status": row.status,
        "label": label,
        "message": message,
        "is_booked": row.status in {"confirmed", "pre_event", "completed"},
        "hold_expires_at": (
            row.hold_expires_at.isoformat() if row.status == "soft_hold" and row.hold_expires_at else None
        ),
        "updated_at": (row.updated_at or _now()).isoformat(),
    }


def _customer_status_secret(request: Request) -> str:
    """Reuse the configured Building HMAC secret with a domain-separated token."""

    secret = str(
        getattr(
            request.app.state.settings, "building_campaign_token_secret", ""
        )
        or ""
    ).strip()
    if not secret:
        raise HTTPException(
            status_code=503,
            detail="Customer status access is not configured.",
        )
    return secret


def _encode_customer_status_token(
    request: Request,
    *,
    reservation_id: str,
    contact_id: str,
    expires_at: datetime,
) -> str:
    payload = {
        "aud": "building-customer-status-v1",
        "reservation_id": reservation_id,
        "contact_id": contact_id,
        "exp": int(expires_at.timestamp()),
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).decode().rstrip("=")
    signature = hmac.new(
        _customer_status_secret(request).encode(),
        f"building-customer-status:{encoded}".encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{encoded}.{signature}"


def _decode_customer_status_token(request: Request, token: str) -> dict[str, Any]:
    try:
        encoded, signature = token.split(".", 1)
        expected = hmac.new(
            _customer_status_secret(request).encode(),
            f"building-customer-status:{encoded}".encode(),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise ValueError("signature")
        padded = encoded + "=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        if payload.get("aud") != "building-customer-status-v1":
            raise ValueError("audience")
        if int(payload.get("exp") or 0) <= int(_now().timestamp()):
            raise HTTPException(status_code=410, detail="Status link has expired.")
        if not payload.get("reservation_id") or not payload.get("contact_id"):
            raise ValueError("subject")
        return payload
    except HTTPException:
        raise
    except (ValueError, TypeError, json.JSONDecodeError):
        raise HTTPException(status_code=404, detail="Status link is invalid.") from None


def _customer_status_projection(
    session,
    reservation: BuildingReservation,
) -> dict[str, Any]:
    """Return customer-safe lifecycle evidence without operator or provider data."""

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
    invoice = session.execute(
        select(BuildingInvoice)
        .where(BuildingInvoice.reservation_id == reservation.id)
        .order_by(BuildingInvoice.created_at.desc())
    ).scalars().first()
    calendar = session.execute(
        select(BuildingCalendarProjection).where(
            BuildingCalendarProjection.reservation_id == reservation.id
        )
    ).scalar_one_or_none()
    status = _customer_event_status(reservation)
    terminal = reservation.status in {"cancelled", "expired"}
    return {
        "reservation_id": reservation.id,
        "event_window": {
            "starts_at": (
                reservation.guest_starts_at or reservation.starts_at
            ).isoformat(),
            "ends_at": (
                reservation.guest_ends_at or reservation.ends_at
            ).isoformat(),
        },
        "access_window": {
            "starts_at": reservation.starts_at.isoformat(),
            "ends_at": reservation.ends_at.isoformat(),
        },
        "hold_expires_at": (
            reservation.hold_expires_at.isoformat()
            if reservation.status == "soft_hold" and reservation.hold_expires_at
            else None
        ),
        "status": status,
        "quote": {
            "status": "closed" if terminal else proposal.status if proposal else "not_started",
            "version": proposal.version if proposal else None,
        },
        "agreement": {
            "status": "closed" if terminal else reservation.agreement_status,
            "preparation_status": (
                agreement.preparation_status if agreement else "not_started"
            ),
            "signature_verified": reservation.agreement_status == "signed",
        },
        "payment": {
            "status": "closed" if terminal else reservation.deposit_status,
            "request_status": payment.status if payment else "not_started",
            "payment_verified": reservation.deposit_status == "paid",
            "invoice_status": invoice.status if invoice else "not_created",
        },
        "operations": {
            "calendar_projection": (
                "closed"
                if terminal
                else "ready"
                if calendar and calendar.status == "synced"
                else "pending"
                if calendar
                else "not_started"
            ),
        },
        "documents": {
            "quote_url": (
                proposal.document_url
                if proposal and proposal.status in {"sent", "accepted"}
                else ""
            ),
            "agreement_url": (
                agreement.document_url
                if agreement and agreement.status == "signed"
                else ""
            ),
            "invoice_url": (
                invoice.hosted_invoice_url
                if invoice and invoice.status in {"open", "paid"}
                else ""
            ),
        },
        "requests": {
            "accepted_types": ["reschedule", "cancellation", "question"],
            "changes_booking_directly": False,
        },
        "communications": {
            "delivery_claimed": False,
            "message": (
                "This page shows Agent status only. It does not claim that an "
                "email or text message was delivered."
            ),
        },
        "updated_at": (reservation.updated_at or _now()).isoformat(),
    }


def _event_review_response(
    row: BuildingReservation,
    proposal: BuildingProposal,
    *,
    replayed: bool,
) -> dict[str, Any]:
    return {
        "ok": True,
        "replayed": replayed,
        "reservation": _reservation_payload(row),
        "quote": {
            "id": proposal.id,
            "version": proposal.version,
            "status": proposal.status,
            "amount_cents": proposal.amount_cents,
            "currency": proposal.currency,
            "rate_plan_id": proposal.rate_plan_id,
            "rate_plan_snapshot": dict(proposal.rate_plan_snapshot_json or {}),
            "terms_summary": proposal.terms_summary,
        },
        "readiness": {
            "quote_ready": proposal.status == "draft",
            "agreement_ready": proposal.status == "draft" and row.status == "soft_hold",
            "contract_generated": False,
            "signature_verified": False,
            "payment_verified": False,
            "booking_confirmed": False,
        },
        "customer_status": _customer_event_status(row),
    }


def _activate_tenant_relationship(
    session,
    reservation: BuildingReservation,
    *,
    actor: str,
    renewed: bool,
) -> BuildingRelationship:
    if not reservation.contact_id:
        raise HTTPException(
            status_code=409,
            detail="A linked contact is required before workspace occupancy.",
        )
    contact = session.get(BuildingContact, reservation.contact_id)
    if contact is None:
        raise HTTPException(status_code=409, detail="Linked workspace contact is missing.")
    if contact.status != "active":
        raise HTTPException(
            status_code=409,
            detail="Linked workspace contact must be active before occupancy.",
        )
    source_reference = f"reservation:{reservation.id}"
    relationship = session.execute(
        select(BuildingRelationship).where(
            BuildingRelationship.contact_id == contact.id,
            BuildingRelationship.relationship_type == "tenant",
            BuildingRelationship.source_reference == source_reference,
        )
    ).scalar_one_or_none()
    before = (
        {
            "status": relationship.status,
            "starts_on": (
                relationship.starts_on.isoformat() if relationship.starts_on else None
            ),
            "ends_on": (
                relationship.ends_on.isoformat() if relationship.ends_on else None
            ),
        }
        if relationship
        else {}
    )
    if relationship is None:
        relationship = BuildingRelationship(
            id=str(uuid4()),
            contact_id=contact.id,
            relationship_type="tenant",
            source_reference=source_reference,
        )
    metadata = dict(relationship.metadata_json or {})
    metadata.update({
        "reservation_id": reservation.id,
        "space_id": reservation.space_id,
        "offering_id": reservation.offering_id or "",
        "activated_at": metadata.get("activated_at") or _now().isoformat(),
        "last_renewed_at": _now().isoformat() if renewed else metadata.get("last_renewed_at"),
        "activated_by": actor,
    })
    relationship.status = "active"
    relationship.organization = relationship.organization or contact.company_name
    relationship.starts_on = reservation.starts_at.date()
    relationship.ends_on = reservation.ends_at.date()
    relationship.metadata_json = metadata
    relationship.updated_at = _now()
    session.add(relationship)
    session.add(BuildingAuditEvent(
        entity_type="relationship",
        entity_id=relationship.id,
        action="tenant_renewed" if renewed else "tenant_activated",
        actor=actor,
        before_json=before,
        after_json={
            "contact_id": contact.id,
            "reservation_id": reservation.id,
            "space_id": reservation.space_id,
            "status": relationship.status,
            "starts_on": relationship.starts_on.isoformat(),
            "ends_on": relationship.ends_on.isoformat(),
        },
    ))
    return relationship


def _complete_tenant_relationship(
    session,
    reservation: BuildingReservation,
    *,
    actor: str,
) -> None:
    if not reservation.contact_id:
        return
    source_reference = f"reservation:{reservation.id}"
    tenant = session.execute(
        select(BuildingRelationship).where(
            BuildingRelationship.contact_id == reservation.contact_id,
            BuildingRelationship.relationship_type == "tenant",
            BuildingRelationship.source_reference == source_reference,
        )
    ).scalar_one_or_none()
    if tenant is None:
        return
    tenant.status = "inactive"
    tenant.ends_on = _now().date()
    tenant.updated_at = _now()
    former_reference = f"former:{source_reference}"
    former = session.execute(
        select(BuildingRelationship).where(
            BuildingRelationship.contact_id == reservation.contact_id,
            BuildingRelationship.relationship_type == "former_tenant",
            BuildingRelationship.source_reference == former_reference,
        )
    ).scalar_one_or_none()
    if former is None:
        former = BuildingRelationship(
            id=str(uuid4()),
            contact_id=reservation.contact_id,
            relationship_type="former_tenant",
            status="active",
            organization=tenant.organization,
            starts_on=tenant.starts_on,
            ends_on=tenant.ends_on,
            source_reference=former_reference,
            metadata_json={
                "reservation_id": reservation.id,
                "space_id": reservation.space_id,
                "offering_id": reservation.offering_id or "",
                "completed_at": _now().isoformat(),
            },
        )
        session.add(former)
    session.add(BuildingAuditEvent(
        entity_type="relationship",
        entity_id=tenant.id,
        action="tenant_moved_out",
        actor=actor,
        before_json={"status": "active"},
        after_json={
            "status": "inactive",
            "former_tenant_relationship_id": former.id,
            "reservation_id": reservation.id,
            "ends_on": tenant.ends_on.isoformat(),
        },
    ))


def _activate_event_host_relationship(
    session,
    reservation: BuildingReservation,
    *,
    actor: str,
) -> BuildingRelationship:
    if not reservation.contact_id:
        raise HTTPException(
            status_code=409,
            detail="A responsible contact is required before event confirmation.",
        )
    contact = session.get(BuildingContact, reservation.contact_id)
    if contact is None or contact.status != "active":
        raise HTTPException(
            status_code=409,
            detail="The linked event contact must be active before confirmation.",
        )
    source_reference = f"reservation:{reservation.id}"
    relationship = session.execute(
        select(BuildingRelationship).where(
            BuildingRelationship.contact_id == contact.id,
            BuildingRelationship.relationship_type == "event_host",
            BuildingRelationship.source_reference == source_reference,
        )
    ).scalar_one_or_none()
    if relationship is None:
        relationship = BuildingRelationship(
            id=str(uuid4()),
            contact_id=contact.id,
            relationship_type="event_host",
            source_reference=source_reference,
        )
    relationship.status = "active"
    relationship.organization = relationship.organization or contact.company_name
    relationship.starts_on = reservation.starts_at.date()
    relationship.ends_on = reservation.ends_at.date()
    relationship.metadata_json = {
        **dict(relationship.metadata_json or {}),
        "reservation_id": reservation.id,
        "space_id": reservation.space_id,
        "offering_id": reservation.offering_id or "",
        "confirmed_at": _now().isoformat(),
        "confirmed_by": actor,
    }
    relationship.updated_at = _now()
    session.add(relationship)
    session.add(BuildingAuditEvent(
        entity_type="relationship",
        entity_id=relationship.id,
        action="event_host_confirmed",
        actor=actor,
        after_json={
            "contact_id": contact.id,
            "reservation_id": reservation.id,
            "space_id": reservation.space_id,
            "status": relationship.status,
            "starts_on": relationship.starts_on.isoformat(),
            "ends_on": relationship.ends_on.isoformat(),
        },
    ))
    return relationship


def _complete_event_host_relationship(
    session,
    reservation: BuildingReservation,
    *,
    actor: str,
    outcome: str,
) -> None:
    if not reservation.contact_id:
        return
    relationship = session.execute(
        select(BuildingRelationship).where(
            BuildingRelationship.contact_id == reservation.contact_id,
            BuildingRelationship.relationship_type == "event_host",
            BuildingRelationship.source_reference == f"reservation:{reservation.id}",
        )
    ).scalar_one_or_none()
    if relationship is None or relationship.status == "inactive":
        return
    relationship.status = "inactive"
    relationship.ends_on = _now().date()
    relationship.updated_at = _now()
    metadata = dict(relationship.metadata_json or {})
    metadata.update({
        "closed_at": _now().isoformat(),
        "closed_by": actor,
        "outcome": outcome,
    })
    relationship.metadata_json = metadata
    session.add(BuildingAuditEvent(
        entity_type="relationship",
        entity_id=relationship.id,
        action="event_host_completed" if outcome == "completed" else "event_host_cancelled",
        actor=actor,
        before_json={"status": "active"},
        after_json={
            "status": "inactive",
            "reservation_id": reservation.id,
            "outcome": outcome,
            "ends_on": relationship.ends_on.isoformat(),
        },
    ))


def _active_conflicts(
    session,
    *,
    space_id: str,
    starts_at: datetime,
    ends_at: datetime,
    reservation_id: str = "",
) -> list[BuildingAvailabilityBlock]:
    rows = session.execute(
        select(BuildingAvailabilityBlock).where(
            BuildingAvailabilityBlock.space_id == space_id,
            BuildingAvailabilityBlock.starts_at < ends_at,
            (
                BuildingAvailabilityBlock.ends_at.is_(None)
                | (BuildingAvailabilityBlock.ends_at > starts_at)
            ),
        )
    ).scalars().all()
    now = _now()
    conflicts: list[BuildingAvailabilityBlock] = []
    for row in rows:
        if reservation_id and row.source_reference == f"reservation:{reservation_id}":
            continue
        expires = row.expires_at
        if expires is not None and expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        if row.state == "soft_hold" and expires and expires <= now:
            continue
        conflicts.append(row)
    return conflicts


def _calendar_is_authoritative() -> bool:
    return os.getenv(
        "BUILDING_GOOGLE_CALENDAR_AVAILABILITY_AUTHORITY", ""
    ).strip().lower() in {"1", "true", "yes", "on"}


def _require_calendar_availability(
    *,
    starts_at: datetime,
    ends_at: datetime,
    reservation_id: str = "",
    override: bool = False,
) -> tuple[BuildingGoogleCalendarClient | None, list[dict[str, Any]]]:
    """Fail closed against the Anata Events calendar when authority is enabled.

    ``override`` lets a named operator take a date the calendar says is busy,
    and returns what they overrode so it can be recorded. It never applies to a
    calendar that could not be read: overriding a clash you were shown is a
    decision, and booking blind is not the same thing.
    """

    if not _calendar_is_authoritative():
        return None, []
    client = BuildingGoogleCalendarClient()
    if not client.configured:
        raise HTTPException(status_code=503, detail=client.readiness_error)
    try:
        conflicts = client.find_conflicts(
            starts_at=starts_at,
            ends_at=ends_at,
            exclude_reservation_id=reservation_id,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Anata Events calendar availability could not be verified; no hold was created.",
        ) from exc
    if conflicts and not override:
        raise HTTPException(
            status_code=409,
            detail="The full setup-through-teardown window is occupied on the Anata Events calendar.",
        )
    return client, [
        {"source": "anata_events_calendar", "summary": str(item.get("summary") or "Busy")}
        for item in conflicts
    ]


def _availability_block(
    session,
    reservation: BuildingReservation,
) -> BuildingAvailabilityBlock | None:
    return session.execute(
        select(BuildingAvailabilityBlock).where(
            BuildingAvailabilityBlock.source_reference == f"reservation:{reservation.id}"
        )
    ).scalar_one_or_none()


@router.post("/event-reviews", status_code=201)
def create_event_review(
    payload: EventReviewInput,
    request: Request,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=8, max_length=128),
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Create the authoritative access-window hold and frozen quote draft."""

    _require_internal_key(request, x_internal_api_key)
    canonical = payload.model_dump(mode="json")
    request_hash = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    with session_scope(request.app.state.session_factory) as session:
        prior = session.execute(
            select(BuildingEventLifecycleCommand).where(
                BuildingEventLifecycleCommand.idempotency_key == idempotency_key
            )
        ).scalar_one_or_none()
        if prior is not None:
            if prior.request_hash != request_hash:
                raise HTTPException(
                    status_code=409,
                    detail="This idempotency key was already used for a different request.",
                )
            row = session.get(BuildingReservation, prior.reservation_id)
            proposal = session.get(
                BuildingProposal, str(prior.response_json.get("quote_id") or "")
            )
            if row is None or proposal is None:
                raise HTTPException(
                    status_code=409,
                    detail="The original operation evidence is incomplete; operator review is required.",
                )
            return _event_review_response(row, proposal, replayed=True)

        inquiry = session.get(BuildingInquiry, payload.inquiry_id)
        if inquiry is None or inquiry.kind != "event":
            raise HTTPException(status_code=404, detail="Accepted event inquiry not found.")
        inquiry_payload = dict(inquiry.payload_json or {})
        lifecycle = dict(inquiry_payload.get("_lifecycle") or {})
        entry_stage = str(lifecycle.get("stage") or "new")
        if entry_stage == "closed_lost":
            raise HTTPException(
                status_code=409,
                detail="This inquiry is closed lost; reopen it before holding a date.",
            )
        # An already-qualified lead keeps the decision it recorded. Anything
        # earlier is qualified by this hold instead of by a separate step on
        # another screen, so it must meet the same evidence bar here.
        if entry_stage not in {"qualified", "closed_won"}:
            unanswered = event_qualification_missing(
                dict(inquiry_payload.get("_event_interview") or {}), inquiry_payload
            )
            if unanswered:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Answer these before holding a date: "
                        + ", ".join(unanswered)
                        + "."
                    ),
                )
        if session.get(BuildingReservation, payload.reservation_id) is not None:
            raise HTTPException(
                status_code=409,
                detail="Reservation ID already exists; retry with the original idempotency key.",
            )
        space = session.get(BuildingSpace, payload.space_id)
        offering = session.get(BuildingOffering, payload.offering_id)
        if (
            space is None
            or offering is None
            or offering.offering_type != "event"
            or offering.space_id != space.id
        ):
            raise HTTPException(status_code=422, detail="Event space and offering do not match.")
        if space.capacity and payload.attendance > space.capacity:
            raise HTTPException(status_code=422, detail="Attendance exceeds reviewed capacity.")
        if payload.contact_id and session.get(BuildingContact, payload.contact_id) is None:
            raise HTTPException(status_code=422, detail="Unknown contact.")
        conflicts = _active_conflicts(
            session,
            space_id=space.id,
            starts_at=payload.setup_starts_at,
            ends_at=payload.teardown_ends_at,
            reservation_id=payload.reservation_id,
        )
        if conflicts and not payload.override_conflicts:
            raise HTTPException(
                status_code=409,
                detail="The full setup-through-teardown window conflicts with Agent availability.",
            )
        calendar_client, calendar_conflicts = _require_calendar_availability(
            starts_at=payload.setup_starts_at,
            ends_at=payload.teardown_ends_at,
            reservation_id=payload.reservation_id,
            override=payload.override_conflicts,
        )
        # What was overridden is part of the record. A double booking nobody can
        # trace back to a decision is indistinguishable from a bug.
        overridden = [
            {"source": "agent_hold", "reservation_id": str(row.source_reference or "")}
            for row in conflicts
        ] + calendar_conflicts

        event_date = payload.guest_starts_at.date()
        plans = session.execute(
            select(BuildingRatePlan).where(
                BuildingRatePlan.offering_id == offering.id,
                BuildingRatePlan.status == "approved",
                BuildingRatePlan.effective_from <= event_date,
                (
                    BuildingRatePlan.effective_until.is_(None)
                    | (BuildingRatePlan.effective_until >= event_date)
                ),
            )
        ).scalars().all()
        if len(plans) != 1 or not plans[0].approval_evidence.strip():
            raise HTTPException(
                status_code=409,
                detail="Quote readiness requires exactly one approved effective rate plan with approval evidence.",
            )
        plan = plans[0]
        billable_units = max(payload.units, plan.minimum_units)
        line_items: list[dict[str, Any]] = [{
            "type": "base",
            "name": plan.name,
            "quantity": billable_units,
            "amount_cents": billable_units * plan.unit_amount_cents,
        }]
        allowed_addons = {
            str(item.get("id") or ""): item
            for item in list(plan.addons_json or [])
            if isinstance(item, dict) and item.get("id")
        }
        for selected in sorted(payload.addons, key=lambda item: str(item["addon_id"])):
            addon_id = str(selected["addon_id"])
            quantity = int(selected.get("quantity") or 1)
            addon = allowed_addons.get(addon_id)
            if addon is None or quantity < 1:
                raise HTTPException(
                    status_code=422,
                    detail=f"Add-on '{addon_id}' is not available with that quantity.",
                )
            mode = str(addon.get("pricing_mode") or "flat")
            calculated_quantity = {
                "flat": quantity,
                "per_guest": payload.attendance * quantity,
                "per_unit": billable_units * quantity,
            }.get(mode)
            if calculated_quantity is None:
                raise HTTPException(
                    status_code=409,
                    detail=f"Approved add-on '{addon_id}' has an unsupported pricing mode.",
                )
            line_items.append({
                "type": "addon",
                "addon_id": addon_id,
                "name": str(addon.get("name") or addon_id),
                "quantity": calculated_quantity,
                "amount_cents": int(addon.get("amount_cents") or 0) * calculated_quantity,
            })
        subtotal_cents = sum(item["amount_cents"] for item in line_items)
        tax_cents = (
            (subtotal_cents * plan.tax_rate_bps + 5000) // 10000
            if plan.tax_status == "taxable"
            else 0
        )
        amount_cents = subtotal_cents + tax_cents
        if amount_cents <= 0:
            raise HTTPException(
                status_code=409,
                detail="The approved rate plan does not produce a reviewable quote amount.",
            )

        row = BuildingReservation(
            id=payload.reservation_id,
            kind="event",
            status="soft_hold",
            inquiry_id=inquiry.id,
            contact_id=payload.contact_id,
            offering_id=offering.id,
            space_id=space.id,
            starts_at=payload.setup_starts_at,
            ends_at=payload.teardown_ends_at,
            guest_starts_at=payload.guest_starts_at,
            guest_ends_at=payload.guest_ends_at,
            hold_expires_at=payload.hold_expires_at,
            attendance=payload.attendance,
            deposit_required=plan.deposit_type != "none",
            assigned_owner=payload.assigned_owner,
            requirements_json={
                "operator_notes": payload.operator_notes,
                "access_window_reviewed": True,
                "units": payload.units,
                "addons": payload.addons,
                **({
                    "double_booked": True,
                    "double_booked_by": payload.actor,
                    "double_booked_over": overridden,
                } if overridden else {}),
            },
            source="agent_event_review",
            source_reference=f"inquiry:{inquiry.id}",
            created_by=payload.actor,
            updated_at=_now(),
        )
        session.add(row)
        session.flush()
        block = BuildingAvailabilityBlock(
            id=str(uuid4()),
            space_id=space.id,
            state="soft_hold",
            starts_at=row.starts_at,
            ends_at=row.ends_at,
            expires_at=row.hold_expires_at,
            source="agent",
            source_reference=f"reservation:{row.id}",
            public_label="Contact us for availability",
            notes="Authoritative Agent event-review hold.",
            created_by=payload.actor,
        )
        session.add(block)
        snapshot = {
            "id": plan.id,
            "version": plan.version,
            "name": plan.name,
            "offering_id": plan.offering_id,
            "currency": plan.currency,
            "booking_unit": plan.booking_unit,
            "minimum_units": plan.minimum_units,
            "unit_amount_cents": plan.unit_amount_cents,
            "deposit_type": plan.deposit_type,
            "deposit_amount_cents": plan.deposit_amount_cents,
            "deposit_percent_bps": plan.deposit_percent_bps,
            "cancellation_policy": plan.cancellation_policy,
            "included": list(plan.included_json or []),
            "addons": list(plan.addons_json or []),
            "commercial_terms": dict(plan.commercial_terms_json or {}),
            "source_evidence": list(plan.source_evidence_json or []),
            "conflicts": list(plan.conflicts_json or []),
            "tax_status": plan.tax_status,
            "tax_rate_bps": plan.tax_rate_bps,
            "tax_note": plan.tax_note,
            "transaction_date": event_date.isoformat(),
            "approval_evidence": plan.approval_evidence,
            "approved_by": plan.approved_by,
            "approved_at": plan.approved_at.isoformat() if plan.approved_at else None,
            "effective_from": plan.effective_from.isoformat(),
            "effective_until": plan.effective_until.isoformat() if plan.effective_until else None,
            "access_window": {
                "setup_starts_at": row.starts_at.isoformat(),
                "guest_starts_at": row.guest_starts_at.isoformat(),
                "guest_ends_at": row.guest_ends_at.isoformat(),
                "teardown_ends_at": row.ends_at.isoformat(),
            },
            "line_items": line_items,
            "subtotal_cents": subtotal_cents,
            "tax_cents": tax_cents if plan.tax_status != "review_required" else None,
            "terms_summary": payload.terms_summary,
            "snapshotted_at": _now().isoformat(),
        }
        proposal = BuildingProposal(
            id=str(uuid4()),
            reservation_id=row.id,
            version=1,
            proposal_type="quote",
            status="draft",
            currency=plan.currency,
            amount_cents=amount_cents,
            line_items_json=line_items,
            rate_plan_id=plan.id,
            rate_plan_snapshot_json=snapshot,
            terms_summary=payload.terms_summary,
            created_by=payload.actor,
            updated_at=_now(),
        )
        session.add(proposal)
        queue_calendar_projection(session, row)
        session.add(BuildingAuditEvent(
            entity_type="reservation",
            entity_id=row.id,
            action="event_review_hold_created",
            actor=payload.actor,
            after_json={
                "inquiry_id": inquiry.id,
                "status": row.status,
                "access_window": snapshot["access_window"],
                "hold_expires_at": row.hold_expires_at.isoformat(),
                "quote_id": proposal.id,
                "rate_plan_id": plan.id,
                "operator_notes": payload.operator_notes,
                **({"double_booked_over": overridden} if overridden else {}),
            },
        ))
        if overridden:
            session.add(BuildingAuditEvent(
                entity_type="reservation",
                entity_id=row.id,
                action="double_booking_authorised",
                actor=payload.actor,
                after_json={
                    "inquiry_id": inquiry.id,
                    "access_window": snapshot["access_window"],
                    "over": overridden,
                },
            ))
        command = BuildingEventLifecycleCommand(
            id=str(uuid4()),
            idempotency_key=idempotency_key,
            command_type="accepted_inquiry_to_quote_ready_hold",
            request_hash=request_hash,
            inquiry_id=inquiry.id,
            reservation_id=row.id,
            response_json={"quote_id": proposal.id},
            actor=payload.actor,
        )
        session.add(command)
        if entry_stage not in {"qualified", "closed_won"}:
            # Taking a date is the qualifying act, and the evidence for it was
            # already checked above. Recording it here keeps the lead honest
            # without sending the operator to another screen to say so.
            held_at = _now()
            before_lifecycle = dict(lifecycle)
            lifecycle["stage"] = "qualified"
            lifecycle.setdefault("qualified_at", held_at.isoformat())
            lifecycle["last_changed_at"] = held_at.isoformat()
            lifecycle["last_changed_by"] = payload.actor
            lifecycle["qualified_by"] = "event_review_hold"
            inquiry_payload["_lifecycle"] = lifecycle
            inquiry.payload_json = inquiry_payload
            inquiry.updated_at = held_at
            session.add(inquiry)
            session.add(BuildingAuditEvent(
                entity_type="inquiry",
                entity_id=inquiry.id,
                action="lifecycle_changed",
                actor=payload.actor,
                before_json=before_lifecycle,
                after_json={**lifecycle, "reservation_id": row.id},
            ))
        session.flush()
        if calendar_client is not None:
            if os.getenv(
                "BUILDING_GOOGLE_CALENDAR_WRITES_ENABLED", ""
            ).strip().lower() not in {"1", "true", "yes", "on"}:
                raise HTTPException(
                    status_code=503,
                    detail="Anata Events calendar writes are disabled; no hold was created.",
                )
            projection = session.execute(
                select(BuildingCalendarProjection).where(
                    BuildingCalendarProjection.reservation_id == row.id
                )
            ).scalar_one()
            try:
                event_id = calendar_client.upsert_event(
                    reservation_id=row.id,
                    payload=dict(projection.payload_json or {}),
                    provider_event_id=row.calendar_event_id or "",
                )
            except Exception as exc:
                raise HTTPException(
                    status_code=503,
                    detail="Anata Events calendar hold could not be written; no Agent hold was created.",
                ) from exc
            row.calendar_event_id = event_id
            projection.provider = calendar_client.provider
            projection.provider_event_id = event_id
            projection.target_calendar_id = calendar_client.target_calendar_id
            projection.status = "synced"
            projection.synced_at = _now()
            projection.last_error = ""
        return _event_review_response(row, proposal, replayed=False)


@router.post("/{reservation_id}/customer-status-access")
def prepare_customer_status_access(
    reservation_id: str,
    payload: CustomerStatusAccessInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Prepare a time-limited read-only status URL without sending it."""

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(BuildingReservation, reservation_id)
        if reservation is None or reservation.kind != "event":
            raise HTTPException(status_code=404, detail="Event reservation not found.")
        if not reservation.contact_id:
            raise HTTPException(
                status_code=409, detail="A linked customer contact is required."
            )
        contact = session.get(BuildingContact, reservation.contact_id)
        if contact is None or contact.status != "active":
            raise HTTPException(
                status_code=409,
                detail="The linked customer contact is unavailable.",
            )
        expires_at = _now() + timedelta(days=payload.expires_in_days)
        token = _encode_customer_status_token(
            request,
            reservation_id=reservation.id,
            contact_id=contact.id,
            expires_at=expires_at,
        )
        public_base_url = str(
            request.app.state.settings.building_public_base_url
        ).rstrip("/")
        status_url = (
            f"{public_base_url}/event-status?token={token}"
        )
        session.add(BuildingAuditEvent(
            entity_type="reservation",
            entity_id=reservation.id,
            action="customer_status_access_prepared",
            actor=payload.actor,
            after_json={
                "contact_id": contact.id,
                "expires_at": expires_at.isoformat(),
                "sent": False,
            },
        ))
        return {
            "ok": True,
            "reservation_id": reservation.id,
            "expires_at": expires_at.isoformat(),
            "status_url": status_url,
            "sent": False,
        }


@public_router.get("/status")
def public_customer_status(token: str, request: Request) -> dict[str, Any]:
    """Read a redacted event status using a signed, expiring bearer URL."""

    claims = _decode_customer_status_token(request, token)
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(
            BuildingReservation, str(claims["reservation_id"])
        )
        if (
            reservation is None
            or reservation.kind != "event"
            or reservation.contact_id != str(claims["contact_id"])
        ):
            raise HTTPException(status_code=404, detail="Status link is invalid.")
        contact = session.get(BuildingContact, reservation.contact_id)
        if contact is None or contact.status != "active":
            raise HTTPException(status_code=404, detail="Status link is invalid.")
        return {
            "ok": True,
            "expires_at": datetime.fromtimestamp(
                int(claims["exp"]), tz=timezone.utc
            ).isoformat(),
            "booking": _customer_status_projection(session, reservation),
        }


@public_router.post("/status/requests", status_code=202)
def submit_customer_booking_request(
    token: str,
    payload: CustomerBookingRequestInput,
    request: Request,
    idempotency_key: str = Header(
        alias="Idempotency-Key", min_length=8, max_length=128
    ),
) -> dict[str, Any]:
    """Create staff work from a customer request; never mutate the booking."""

    claims = _decode_customer_status_token(request, token)
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(BuildingReservation, str(claims["reservation_id"]))
        if (
            reservation is None
            or reservation.kind != "event"
            or reservation.contact_id != str(claims["contact_id"])
        ):
            raise HTTPException(status_code=404, detail="Status link is invalid.")
        request_id = str(uuid5(
            NAMESPACE_URL,
            f"building-customer-request:{reservation.id}:{idempotency_key}",
        ))
        existing = session.get(BuildingServiceRequest, request_id)
        if existing is not None:
            return {
                "ok": True,
                "duplicate": True,
                "request_id": existing.id,
                "booking_changed": False,
            }
        requested_window = ""
        if payload.requested_starts_at and payload.requested_ends_at:
            requested_window = (
                f"\nRequested window: {payload.requested_starts_at.isoformat()} "
                f"to {payload.requested_ends_at.isoformat()}"
            )
        row = BuildingServiceRequest(
            id=request_id,
            category="event_support",
            priority="high" if payload.request_type == "cancellation" else "normal",
            status="new",
            title=f"Customer {payload.request_type} request",
            description=f"{payload.details.strip()}{requested_window}",
            space_id=reservation.space_id,
            contact_id=reservation.contact_id,
            reservation_id=reservation.id,
            source="customer_status",
            source_reference=idempotency_key,
            assigned_owner=reservation.assigned_owner,
            reported_by="customer-status-link",
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="reservation",
            entity_id=reservation.id,
            action="customer_booking_change_requested",
            actor="customer-status-link",
            after_json={
                "request_id": row.id,
                "request_type": payload.request_type,
                "booking_changed": False,
                "inventory_changed": False,
            },
        ))
        return {
            "ok": True,
            "duplicate": False,
            "request_id": row.id,
            "booking_changed": False,
        }


@router.post("/communications/run")
def run_booking_communications(
    payload: CommunicationRunInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Preview or deliver due event reminders through the idempotent outbox."""

    _require_internal_key(request, x_internal_api_key)
    now = _now()
    window_start = now + timedelta(days=6)
    window_end = now + timedelta(days=8)
    with session_scope(request.app.state.session_factory) as session:
        rows = session.execute(
            select(BuildingReservation).where(
                BuildingReservation.kind == "event",
                BuildingReservation.status.in_(("confirmed", "pre_event")),
                BuildingReservation.starts_at >= window_start,
                BuildingReservation.starts_at <= window_end,
            )
        ).scalars().all()
        if not payload.execute:
            return {
                "ok": True,
                "execute": False,
                "due_count": len(rows),
                "reservation_ids": [row.id for row in rows],
            }
        results = [
            {
                "reservation_id": row.id,
                **attempt_booking_message(
                    session,
                    request=request,
                    reservation=row,
                    milestone="event_reminder",
                    actor=payload.actor,
                ),
            }
            for row in rows
        ]
        return {
            "ok": all(item["status"] in {"sent", "delivered"} for item in results),
            "execute": True,
            "due_count": len(rows),
            "results": results,
        }


@router.get("/{reservation_id}/lifecycle")
def get_event_lifecycle(
    reservation_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return operator timeline plus a separately redacted customer projection."""

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingReservation, reservation_id)
        if row is None or row.kind != "event":
            raise HTTPException(status_code=404, detail="Event reservation not found.")
        audits = session.execute(
            select(BuildingAuditEvent)
            .where(
                BuildingAuditEvent.entity_type == "reservation",
                BuildingAuditEvent.entity_id == row.id,
            )
            .order_by(BuildingAuditEvent.created_at)
        ).scalars().all()
        proposals = session.execute(
            select(BuildingProposal)
            .where(BuildingProposal.reservation_id == row.id)
            .order_by(BuildingProposal.version.desc())
        ).scalars().all()
        return {
            "reservation": _reservation_payload(row),
            "timeline": [{
                "action": item.action,
                "actor": item.actor,
                "at": item.created_at.isoformat(),
                "evidence": dict(item.after_json or {}),
            } for item in audits],
            "quote_versions": [{
                "id": item.id,
                "version": item.version,
                "status": item.status,
                "rate_plan_id": item.rate_plan_id,
                "amount_cents": item.amount_cents,
            } for item in proposals],
            "customer_status": _customer_event_status(row),
            "readiness": {
                "agreement_can_be_prepared": bool(
                    row.status == "soft_hold" and proposals and proposals[0].status == "draft"
                ),
                "contract_generated": bool(
                    session.execute(
                        select(BuildingAgreement).where(
                            BuildingAgreement.reservation_id == row.id,
                            BuildingAgreement.document_url != "",
                        )
                    ).scalars().first()
                ),
                "signature_verified": row.agreement_status == "signed",
                "payment_verified": row.deposit_status == "paid",
                "booking_confirmed": row.status in {"confirmed", "pre_event", "completed"},
            },
        }


@router.get("")
def list_reservations(
    request: Request,
    kind: str = "",
    status: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        query = select(BuildingReservation).order_by(BuildingReservation.starts_at)
        if kind:
            query = query.where(BuildingReservation.kind == kind)
        if status:
            query = query.where(BuildingReservation.status == status)
        rows = session.execute(query).scalars().all()
        return {"reservations": [_reservation_payload(row) for row in rows]}


@router.post("", status_code=201)
def create_reservation(
    payload: ReservationInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        space = session.get(BuildingSpace, payload.space_id)
        if space is None:
            raise HTTPException(status_code=422, detail="Unknown space.")
        if payload.offering_id and session.get(BuildingOffering, payload.offering_id) is None:
            raise HTTPException(status_code=422, detail="Unknown offering.")
        if payload.inquiry_id and session.get(BuildingInquiry, payload.inquiry_id) is None:
            raise HTTPException(status_code=422, detail="Unknown inquiry.")
        if payload.contact_id and session.get(BuildingContact, payload.contact_id) is None:
            raise HTTPException(status_code=422, detail="Unknown contact.")
        if space.capacity and payload.attendance > space.capacity:
            raise HTTPException(status_code=422, detail="Attendance exceeds the reviewed space capacity.")
        row = BuildingReservation(
            id=payload.id or str(uuid4()),
            kind=payload.kind,
            status="inquiry",
            inquiry_id=payload.inquiry_id,
            contact_id=payload.contact_id,
            offering_id=payload.offering_id,
            space_id=payload.space_id,
            starts_at=payload.starts_at,
            ends_at=payload.ends_at,
            attendance=payload.attendance,
            deposit_required=payload.deposit_required,
            assigned_owner=payload.assigned_owner,
            requirements_json=payload.requirements,
            source=payload.source,
            source_reference=payload.source_reference,
            created_by=payload.actor,
            updated_at=_now(),
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="reservation",
            entity_id=row.id,
            action="created",
            actor=payload.actor,
            after_json={
                "kind": row.kind,
                "status": row.status,
                "space_id": row.space_id,
                "starts_at": row.starts_at.isoformat(),
                "ends_at": row.ends_at.isoformat(),
            },
        ))
        return {"ok": True, "reservation": _reservation_payload(row)}


@router.post("/{reservation_id}/transition")
def transition_reservation(
    reservation_id: str,
    payload: TransitionInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingReservation, reservation_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Reservation not found.")
        transitions = EVENT_TRANSITIONS if row.kind == "event" else WORKSPACE_TRANSITIONS
        allowed = transitions.get(row.status, set())
        if payload.target_status not in allowed:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot move {row.kind} reservation from {row.status} to {payload.target_status}.",
            )
        if payload.target_status == "soft_hold":
            if payload.hold_expires_at is None or payload.hold_expires_at <= _now():
                raise HTTPException(status_code=422, detail="A future hold expiration is required.")
            conflicts = _active_conflicts(
                session,
                space_id=row.space_id,
                starts_at=row.starts_at,
                ends_at=row.ends_at,
                reservation_id=row.id,
            )
            if conflicts:
                raise HTTPException(status_code=409, detail="Space is not available for this time.")
            block = _availability_block(session, row)
            if block is None:
                block = BuildingAvailabilityBlock(
                    id=str(uuid4()),
                    space_id=row.space_id,
                    state="soft_hold",
                    starts_at=row.starts_at,
                    ends_at=row.ends_at,
                    expires_at=payload.hold_expires_at,
                    source="agent",
                    source_reference=f"reservation:{row.id}",
                    public_label="Contact us for availability",
                    created_by=payload.actor,
                )
            else:
                block.state = "soft_hold"
                block.expires_at = payload.hold_expires_at
                block.updated_at = _now()
            session.add(block)
            row.hold_expires_at = payload.hold_expires_at
        if payload.target_status == "tour_scheduled":
            latest_tour = session.execute(
                select(BuildingTour)
                .where(BuildingTour.reservation_id == row.id)
                .order_by(BuildingTour.scheduled_at.desc())
            ).scalars().first()
            if latest_tour is None or latest_tour.status != "scheduled":
                raise HTTPException(
                    status_code=409,
                    detail="A scheduled tour record is required.",
                )
        if payload.target_status == "tour_completed":
            completed_tour = session.execute(
                select(BuildingTour).where(
                    BuildingTour.reservation_id == row.id,
                    BuildingTour.status == "completed",
                )
            ).scalars().first()
            if completed_tour is None:
                raise HTTPException(
                    status_code=409,
                    detail="A completed tour with an outcome is required.",
                )
        if payload.target_status in {"proposal_sent", "quote_sent"}:
            latest_proposal = session.execute(
                select(BuildingProposal)
                .where(BuildingProposal.reservation_id == row.id)
                .order_by(BuildingProposal.version.desc())
            ).scalars().first()
            if latest_proposal is None or latest_proposal.status not in {
                "sent", "accepted"
            }:
                noun = "quote" if row.kind == "event" else "proposal"
                raise HTTPException(
                    status_code=409,
                    detail=f"A versioned, approved, sent {noun} is required.",
                )
        if payload.target_status == "contract_pending":
            latest_proposal = session.execute(
                select(BuildingProposal)
                .where(BuildingProposal.reservation_id == row.id)
                .order_by(BuildingProposal.version.desc())
            ).scalars().first()
            if latest_proposal is None or latest_proposal.status != "accepted":
                raise HTTPException(
                    status_code=409,
                    detail="An accepted proposal or quote is required before contract preparation.",
                )
        if payload.target_status == "confirmed":
            if row.agreement_status != "signed":
                raise HTTPException(status_code=409, detail="A signed agreement is required.")
            if row.deposit_required and row.deposit_status != "paid":
                raise HTTPException(status_code=409, detail="A verified deposit is required.")
            _require_calendar_availability(
                starts_at=row.starts_at,
                ends_at=row.ends_at,
                reservation_id=row.id,
            )
            if row.kind == "event":
                _activate_event_host_relationship(
                    session,
                    row,
                    actor=payload.actor,
                )
            conflicts = _active_conflicts(
                session,
                space_id=row.space_id,
                starts_at=row.starts_at,
                ends_at=row.ends_at,
                reservation_id=row.id,
            )
            if conflicts:
                raise HTTPException(status_code=409, detail="Space is not available for this time.")
            block = _availability_block(session, row)
            if block is None:
                block = BuildingAvailabilityBlock(
                    id=str(uuid4()),
                    space_id=row.space_id,
                    state="booked",
                    starts_at=row.starts_at,
                    ends_at=row.ends_at,
                    source="agent",
                    source_reference=f"reservation:{row.id}",
                    public_label="Booked",
                    created_by=payload.actor,
                )
            block.state = "booked"
            block.expires_at = None
            block.updated_at = _now()
            session.add(block)
            row.hold_expires_at = None
        if payload.target_status == "occupied":
            block = _availability_block(session, row)
            if block:
                block.state = "occupied"
                block.updated_at = _now()
            if row.kind == "workspace":
                _activate_tenant_relationship(
                    session,
                    row,
                    actor=payload.actor,
                    renewed=row.status == "renewal",
                )
        if payload.target_status in {"cancelled", "expired", "completed"}:
            session.execute(
                delete(BuildingAvailabilityBlock).where(
                    BuildingAvailabilityBlock.source_reference == f"reservation:{row.id}"
                )
            )
            row.hold_expires_at = None
        if payload.target_status in {"cancelled", "expired"}:
            propagate_event_readiness_terminal_state(
                session,
                row,
                terminal_status=payload.target_status,
                actor=payload.actor,
            )
        before = row.status
        if (
            row.kind == "event"
            and payload.target_status in {"completed", "cancelled"}
        ):
            _complete_event_host_relationship(
                session,
                row,
                actor=payload.actor,
                outcome=payload.target_status,
            )
        if (
            row.kind == "workspace"
            and payload.target_status == "completed"
            and row.status == "move_out"
        ):
            _complete_tenant_relationship(session, row, actor=payload.actor)
        row.status = payload.target_status
        row.updated_at = _now()
        queue_calendar_projection(session, row)
        ensure_operational_checklist(session, row, actor=payload.actor)
        session.add(BuildingAuditEvent(
            entity_type="reservation",
            entity_id=row.id,
            action="status_changed",
            actor=payload.actor,
            before_json={"status": before},
            after_json={"status": row.status, "reason": payload.reason},
        ))
        if row.kind == "event":
            milestone = {
                "confirmed": "booking_confirmed",
                "cancelled": "booking_cancelled",
                "completed": "post_event",
            }.get(payload.target_status)
            if milestone:
                attempt_booking_message(
                    session,
                    request=request,
                    reservation=row,
                    milestone=milestone,
                    actor=payload.actor,
                )
        return {"ok": True, "reservation": _reservation_payload(row)}


def _tour_payload(row: BuildingTour) -> dict[str, Any]:
    return {
        "id": row.id,
        "reservation_id": row.reservation_id,
        "scheduled_at": row.scheduled_at.isoformat(),
        "duration_minutes": row.duration_minutes,
        "status": row.status,
        "host": row.host,
        "meeting_location": row.meeting_location,
        "notes": row.notes,
        "outcome": row.outcome,
        "next_step": row.next_step,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "cancelled_at": row.cancelled_at.isoformat() if row.cancelled_at else None,
    }


def _tour_conflicts(
    session,
    *,
    space_id: str,
    host: str,
    starts_at: datetime,
    ends_at: datetime,
    tour_id: str = "",
) -> list[str]:
    """Return inventory and host conflicts without creating an inventory hold."""

    conflicts = [
        f"inventory:{row.id}"
        for row in _active_conflicts(
            session,
            space_id=space_id,
            starts_at=starts_at,
            ends_at=ends_at,
        )
    ]
    rows = session.execute(
        select(BuildingTour).where(
            BuildingTour.status == "scheduled",
            BuildingTour.host == host,
            BuildingTour.scheduled_at < ends_at,
        )
    ).scalars().all()
    for row in rows:
        if tour_id and row.id == tour_id:
            continue
        row_start = row.scheduled_at
        if row_start.tzinfo is None:
            row_start = row_start.replace(tzinfo=timezone.utc)
        if row_start + timedelta(minutes=row.duration_minutes) > starts_at:
            conflicts.append(f"host:{row.id}")
    return conflicts


@router.post("/tour-inquiry-handoffs", status_code=201)
def create_tour_inquiry_handoff(
    payload: TourInquiryHandoffInput,
    request: Request,
    idempotency_key: str = Header(
        alias="Idempotency-Key", min_length=8, max_length=128
    ),
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Atomically convert one eligible inquiry into a scheduled workspace tour."""

    _require_internal_key(request, x_internal_api_key)
    scheduled_at = payload.scheduled_at
    if scheduled_at.tzinfo is None:
        scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
    scheduled_at = scheduled_at.astimezone(timezone.utc)
    if scheduled_at <= _now():
        raise HTTPException(status_code=422, detail="Tour time must be in the future.")
    ends_at = scheduled_at + timedelta(minutes=payload.duration_minutes)
    canonical = {
        **payload.model_dump(mode="json"),
        "scheduled_at": scheduled_at.isoformat(),
        "host": payload.host.strip(),
        "meeting_location": payload.meeting_location.strip(),
        "notes": payload.notes.strip(),
    }
    request_hash = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    command_type = "tour_inquiry_to_scheduled_tour"

    with session_scope(request.app.state.session_factory) as session:
        prior = session.execute(
            select(BuildingEventLifecycleCommand).where(
                BuildingEventLifecycleCommand.idempotency_key == idempotency_key
            )
        ).scalar_one_or_none()
        if prior is not None:
            if (
                prior.command_type != command_type
                or prior.request_hash != request_hash
                or prior.inquiry_id != payload.inquiry_id
            ):
                raise HTTPException(
                    status_code=409,
                    detail="This idempotency key was already used for a different request.",
                )
            reservation = session.get(BuildingReservation, prior.reservation_id)
            tour = session.get(
                BuildingTour, str((prior.response_json or {}).get("tour_id") or "")
            )
            if reservation is None or tour is None:
                raise HTTPException(
                    status_code=409,
                    detail="The original handoff evidence is incomplete; review it manually.",
                )
            return {
                "ok": True,
                "replayed": True,
                "reservation": _reservation_payload(reservation),
                "tour": _tour_payload(tour),
                "inventory_hold_created": False,
            }

        inquiry = session.execute(
            select(BuildingInquiry)
            .where(BuildingInquiry.id == payload.inquiry_id)
            .with_for_update()
        ).scalar_one_or_none()
        if inquiry is None or inquiry.kind != "tour":
            raise HTTPException(
                status_code=422,
                detail="Only an eligible tour inquiry can be scheduled.",
            )
        lifecycle = dict((inquiry.payload_json or {}).get("_lifecycle") or {})
        if str(lifecycle.get("stage") or "new") in {"closed_won", "closed_lost"}:
            raise HTTPException(status_code=409, detail="This inquiry is already closed.")
        contact = session.execute(
            select(BuildingContact).where(BuildingContact.email == inquiry.email)
        ).scalar_one_or_none()
        if contact is None or contact.status != "active":
            raise HTTPException(
                status_code=409,
                detail="The linked inquiry contact is unavailable.",
            )
        offering = session.get(BuildingOffering, payload.offering_id)
        space = session.get(BuildingSpace, payload.space_id)
        if (
            offering is None
            or offering.offering_type
            not in {"private_office", "coworking", "meeting_room", "membership"}
            or offering.space_id != payload.space_id
            or space is None
            or space.space_type
            not in {"private_office", "coworking", "conference", "amenity"}
            or space.status not in {"available", "turnover", "occupied"}
        ):
            raise HTTPException(
                status_code=422,
                detail="Choose a valid linked workspace offering and space.",
            )

        reservation = session.execute(
            select(BuildingReservation).where(
                BuildingReservation.inquiry_id == inquiry.id,
                BuildingReservation.kind == "workspace",
            )
        ).scalars().first()
        if reservation is not None and (
            reservation.contact_id != contact.id
            or reservation.offering_id != offering.id
            or reservation.space_id != space.id
        ):
            raise HTTPException(
                status_code=409,
                detail="This inquiry is already linked to a different workspace journey.",
            )
        tour = None
        if reservation is not None:
            tour = session.execute(
                select(BuildingTour)
                .where(BuildingTour.reservation_id == reservation.id)
                .order_by(BuildingTour.created_at.desc())
            ).scalars().first()
            if tour is not None:
                tour_start = tour.scheduled_at
                if tour_start.tzinfo is None:
                    tour_start = tour_start.replace(tzinfo=timezone.utc)
                if not (
                    tour.status == "scheduled"
                    and tour_start == scheduled_at
                    and tour.duration_minutes == payload.duration_minutes
                    and tour.host == payload.host.strip()
                    and tour.meeting_location == payload.meeting_location.strip()
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="This inquiry already has different tour evidence.",
                    )

        conflicts = _tour_conflicts(
            session,
            space_id=space.id,
            host=payload.host.strip(),
            starts_at=scheduled_at,
            ends_at=ends_at,
            tour_id=tour.id if tour else "",
        )
        if conflicts:
            raise HTTPException(
                status_code=409,
                detail="The space or host is not available for this tour time.",
            )

        if reservation is None:
            reservation = BuildingReservation(
                id=str(uuid4()),
                kind="workspace",
                status="qualified",
                inquiry_id=inquiry.id,
                contact_id=contact.id,
                offering_id=offering.id,
                space_id=space.id,
                starts_at=scheduled_at,
                ends_at=ends_at,
                deposit_required=False,
                assigned_owner=payload.host.strip(),
                requirements_json={
                    "journey_purpose": "tour",
                    "tour_inquiry_id": inquiry.id,
                },
                source="tour_inquiry_handoff",
                source_reference=f"inquiry:{inquiry.id}",
                created_by=payload.actor,
                updated_at=_now(),
            )
            session.add(reservation)
            session.flush()
            session.add(BuildingAuditEvent(
                entity_type="reservation",
                entity_id=reservation.id,
                action="created_from_tour_inquiry",
                actor=payload.actor,
                after_json={
                    "inquiry_id": inquiry.id,
                    "contact_id": contact.id,
                    "offering_id": offering.id,
                    "space_id": space.id,
                    "status": "qualified",
                    "inventory_hold_created": False,
                },
            ))
        if tour is None:
            tour = BuildingTour(
                id=str(uuid4()),
                reservation_id=reservation.id,
                scheduled_at=scheduled_at,
                duration_minutes=payload.duration_minutes,
                status="scheduled",
                host=payload.host.strip(),
                meeting_location=payload.meeting_location.strip(),
                notes=payload.notes.strip(),
                created_by=payload.actor,
                updated_at=_now(),
            )
            session.add(tour)
            session.flush()
            session.add(BuildingAuditEvent(
                entity_type="tour",
                entity_id=tour.id,
                action="tour_scheduled_from_inquiry",
                actor=payload.actor,
                after_json={
                    "inquiry_id": inquiry.id,
                    "contact_id": contact.id,
                    "reservation_id": reservation.id,
                    "scheduled_at": scheduled_at.isoformat(),
                    "ends_at": ends_at.isoformat(),
                    "duration_minutes": tour.duration_minutes,
                    "host": tour.host,
                    "meeting_location": tour.meeting_location,
                    "inventory_hold_created": False,
                },
            ))

        before_status = reservation.status
        if before_status not in {"qualified", "tour_scheduled"}:
            raise HTTPException(
                status_code=409,
                detail="The linked workspace journey cannot be moved to a tour.",
            )
        reservation.status = "tour_scheduled"
        reservation.starts_at = scheduled_at
        reservation.ends_at = ends_at
        reservation.assigned_owner = payload.host.strip()
        reservation.updated_at = _now()
        inquiry_payload = dict(inquiry.payload_json or {})
        inquiry_payload["_tour_handoff"] = {
            "reservation_id": reservation.id,
            "tour_id": tour.id,
            "scheduled_at": scheduled_at.isoformat(),
            "actor": payload.actor,
        }
        inquiry.payload_json = inquiry_payload
        inquiry.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="reservation",
            entity_id=reservation.id,
            action="status_changed",
            actor=payload.actor,
            before_json={"status": before_status},
            after_json={
                "status": "tour_scheduled",
                "reason": "Governed tour inquiry handoff completed.",
                "tour_id": tour.id,
            },
        ))
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="tour_handoff_completed",
            actor=payload.actor,
            after_json={
                "contact_id": contact.id,
                "reservation_id": reservation.id,
                "tour_id": tour.id,
                "idempotency_key": idempotency_key,
            },
        ))
        session.add(BuildingEventLifecycleCommand(
            id=str(uuid4()),
            idempotency_key=idempotency_key,
            command_type=command_type,
            request_hash=request_hash,
            inquiry_id=inquiry.id,
            reservation_id=reservation.id,
            response_json={"tour_id": tour.id},
            actor=payload.actor,
        ))
        session.flush()
        return {
            "ok": True,
            "replayed": False,
            "reservation": _reservation_payload(reservation),
            "tour": _tour_payload(tour),
            "inventory_hold_created": False,
        }


@router.get("/{reservation_id}/tours")
def list_tours(
    reservation_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        if session.get(BuildingReservation, reservation_id) is None:
            raise HTTPException(status_code=404, detail="Reservation not found.")
        rows = session.execute(
            select(BuildingTour)
            .where(BuildingTour.reservation_id == reservation_id)
            .order_by(BuildingTour.scheduled_at.desc())
        ).scalars().all()
        return {"tours": [_tour_payload(row) for row in rows]}


@router.post("/{reservation_id}/tours", status_code=201)
def create_tour(
    reservation_id: str,
    payload: TourInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.status != "scheduled":
        raise HTTPException(status_code=422, detail="New tours begin as scheduled.")
    scheduled_at = payload.scheduled_at
    if scheduled_at.tzinfo is None:
        scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
    if scheduled_at <= _now():
        raise HTTPException(status_code=422, detail="Tour time must be in the future.")
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(BuildingReservation, reservation_id)
        if reservation is None:
            raise HTTPException(status_code=404, detail="Reservation not found.")
        if reservation.kind != "workspace":
            raise HTTPException(status_code=422, detail="Tours belong to workspace journeys.")
        row = BuildingTour(
            id=payload.id or str(uuid4()),
            reservation_id=reservation_id,
            scheduled_at=scheduled_at,
            duration_minutes=payload.duration_minutes,
            status="scheduled",
            host=payload.host.strip(),
            meeting_location=payload.meeting_location.strip() or "Anata Building",
            notes=payload.notes.strip(),
            created_by=payload.actor,
            updated_at=_now(),
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="tour",
            entity_id=row.id,
            action="tour_scheduled",
            actor=payload.actor,
            after_json={
                "reservation_id": reservation_id,
                "scheduled_at": row.scheduled_at.isoformat(),
                "duration_minutes": row.duration_minutes,
                "host": row.host,
            },
        ))
        return {"ok": True, "tour": _tour_payload(row)}


@router.put("/tours/{tour_id}")
def update_tour(
    tour_id: str,
    payload: TourInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingTour, tour_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Tour not found.")
        if row.status in TOUR_TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail="Completed or closed tour evidence is immutable.")
        scheduled_at = payload.scheduled_at
        if scheduled_at.tzinfo is None:
            scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
        previous_scheduled_at = row.scheduled_at
        if previous_scheduled_at.tzinfo is None:
            previous_scheduled_at = previous_scheduled_at.replace(tzinfo=timezone.utc)
        rescheduled = scheduled_at != previous_scheduled_at
        if rescheduled and scheduled_at <= _now():
            raise HTTPException(status_code=422, detail="Rescheduled tour time must be in the future.")
        if rescheduled and len(payload.reason.strip()) < 5:
            raise HTTPException(status_code=422, detail="Rescheduling requires a reason.")
        if payload.status == "completed" and (
            len(payload.outcome.strip()) < 3 or len(payload.next_step.strip()) < 3
        ):
            raise HTTPException(
                status_code=422,
                detail="Completed tours require an outcome and next step.",
            )
        if payload.status in {"cancelled", "no_show"} and len(payload.reason.strip()) < 5:
            raise HTTPException(
                status_code=422,
                detail="Cancelled and no-show tours require a reason.",
            )
        before = {
            "scheduled_at": row.scheduled_at.isoformat(),
            "status": row.status,
            "host": row.host,
        }
        row.scheduled_at = scheduled_at
        row.duration_minutes = payload.duration_minutes
        row.status = payload.status
        row.host = payload.host.strip()
        row.meeting_location = payload.meeting_location.strip() or "Anata Building"
        row.notes = payload.notes.strip()
        row.outcome = payload.outcome.strip()
        row.next_step = payload.next_step.strip()
        row.updated_at = _now()
        if payload.status == "completed":
            row.completed_at = _now()
        if payload.status in {"cancelled", "no_show"}:
            row.cancelled_at = _now()
        session.add(row)
        action = "tour_rescheduled" if rescheduled else f"tour_{payload.status}"
        session.add(BuildingAuditEvent(
            entity_type="tour",
            entity_id=row.id,
            action=action,
            actor=payload.actor,
            before_json=before,
            after_json={
                "scheduled_at": row.scheduled_at.isoformat(),
                "status": row.status,
                "host": row.host,
                "outcome": row.outcome,
                "next_step": row.next_step,
                "reason": payload.reason.strip(),
            },
        ))
        return {"ok": True, "tour": _tour_payload(row)}


@router.get("/{reservation_id}/proposals")
def list_proposals(
    reservation_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        if session.get(BuildingReservation, reservation_id) is None:
            raise HTTPException(status_code=404, detail="Reservation not found.")
        rows = session.execute(
            select(BuildingProposal)
            .where(BuildingProposal.reservation_id == reservation_id)
            .order_by(BuildingProposal.version.desc())
        ).scalars().all()
        return {"proposals": [
            {
                "id": row.id,
                "version": row.version,
                "proposal_type": row.proposal_type,
                "status": row.status,
                "currency": row.currency,
                "amount_cents": row.amount_cents,
                "line_items": list(row.line_items_json or []),
                "rate_plan_id": row.rate_plan_id,
                "rate_plan_snapshot": dict(row.rate_plan_snapshot_json or {}),
                "terms_summary": row.terms_summary,
                "valid_until": row.valid_until.isoformat() if row.valid_until else None,
                "document_url": row.document_url,
                "approved_by": row.approved_by,
                "approved_at": row.approved_at.isoformat() if row.approved_at else None,
                "sent_at": row.sent_at.isoformat() if row.sent_at else None,
                "accepted_at": row.accepted_at.isoformat() if row.accepted_at else None,
            }
            for row in rows
        ]}


@router.post("/{reservation_id}/proposals", status_code=201)
def record_proposal(
    reservation_id: str,
    payload: ProposalInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.status in {"sent", "accepted"} and not payload.document_url.strip():
        raise HTTPException(status_code=422, detail="Sent proposals require a document link.")
    if (
        payload.status in {"approved", "sent", "accepted"}
        and payload.valid_until is not None
        and payload.valid_until < _now().date()
    ):
        raise HTTPException(
            status_code=409,
            detail="This proposal or quote has expired; create a new version.",
        )
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(BuildingReservation, reservation_id)
        if reservation is None:
            raise HTTPException(status_code=404, detail="Reservation not found.")
        if reservation.status in {"cancelled", "expired", "completed"}:
            raise HTTPException(
                status_code=409,
                detail=f"This booking is {reservation.status}; quote history is read-only.",
            )
        expected_type = "quote" if reservation.kind == "event" else "proposal"
        if payload.proposal_type != expected_type:
            raise HTTPException(
                status_code=422,
                detail=f"{reservation.kind.title()} reservations use {expected_type} records.",
            )
        if payload.proposal_type != "quote" and (
            payload.pricing_subtotal_cents is not None or payload.discount_cents
        ):
            raise HTTPException(
                status_code=422,
                detail="Audited event pricing adjustments apply only to event quotes.",
            )
        selected_rate_plan: BuildingRatePlan | None = None
        if payload.rate_plan_id:
            selected_rate_plan = session.get(BuildingRatePlan, payload.rate_plan_id)
            if selected_rate_plan is None:
                raise HTTPException(status_code=422, detail="Rate plan not found.")
            if selected_rate_plan.offering_id != reservation.offering_id:
                raise HTTPException(
                    status_code=422,
                    detail="Rate plan does not belong to the reservation offering.",
                )
            reservation_date = reservation.starts_at.date()
            if (
                selected_rate_plan.status != "approved"
                or selected_rate_plan.effective_from > reservation_date
                or (
                    selected_rate_plan.effective_until is not None
                    and selected_rate_plan.effective_until < reservation_date
                )
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Rate plan is not approved and effective for this reservation.",
                )
        row = session.execute(
            select(BuildingProposal).where(
                BuildingProposal.reservation_id == reservation_id,
                BuildingProposal.version == payload.version,
            )
        ).scalar_one_or_none()
        if row is None:
            if payload.status != "draft":
                raise HTTPException(
                    status_code=409, detail="Create a draft before approving or sending it."
                )
            row = BuildingProposal(
                id=payload.id or str(uuid4()),
                reservation_id=reservation_id,
                version=payload.version,
                proposal_type=payload.proposal_type,
                created_by=payload.actor,
            )
            before: dict[str, Any] = {}
        else:
            before = {
                "status": row.status,
                "amount_cents": row.amount_cents,
                "document_url": row.document_url,
            }
            if payload.status != row.status and payload.status not in PROPOSAL_TRANSITIONS[row.status]:
                raise HTTPException(
                    status_code=409,
                    detail=f"Cannot move proposal from {row.status} to {payload.status}.",
                )
            if row.status in {"sent", "accepted", "declined", "voided"}:
                content_changed = any((
                    payload.amount_cents != row.amount_cents,
                    payload.currency.upper() != row.currency,
                    payload.line_items != list(row.line_items_json or []),
                    (
                        payload.rate_plan_id is not None
                        and payload.rate_plan_id != row.rate_plan_id
                    ),
                    payload.terms_summary.strip() != row.terms_summary,
                    payload.valid_until != row.valid_until,
                    payload.document_url.strip() != row.document_url,
                ))
                if content_changed:
                    raise HTTPException(
                        status_code=409,
                        detail="Sent proposal content is immutable; create a new version.",
                    )
        calculated_amount_cents = payload.amount_cents
        calculated_line_items = payload.line_items
        pricing_adjustment: dict[str, Any] = {}
        if payload.proposal_type == "quote" and payload.pricing_subtotal_cents is not None:
            if selected_rate_plan is None:
                raise HTTPException(
                    status_code=409,
                    detail="Adjusted event quotes require an approved effective rate plan.",
                )
            if selected_rate_plan.tax_status == "review_required":
                raise HTTPException(
                    status_code=409,
                    detail="Tax treatment must be approved before calculating an adjusted quote.",
                )
            if payload.discount_cents > payload.pricing_subtotal_cents:
                raise HTTPException(
                    status_code=422,
                    detail="The discount cannot exceed the pre-tax subtotal.",
                )
            discount_reason = payload.discount_reason.strip()
            if payload.discount_cents and not discount_reason:
                raise HTTPException(
                    status_code=422,
                    detail="A business reason is required for every discount.",
                )
            taxable_subtotal_cents = (
                payload.pricing_subtotal_cents - payload.discount_cents
            )
            tax_cents = (
                (taxable_subtotal_cents * selected_rate_plan.tax_rate_bps + 5000)
                // 10000
                if selected_rate_plan.tax_status == "taxable"
                else 0
            )
            calculated_amount_cents = taxable_subtotal_cents + tax_cents
            calculated_line_items = [
                {
                    "type": "pricing_subtotal",
                    "description": "Event package before discount and tax",
                    "amount_cents": payload.pricing_subtotal_cents,
                }
            ]
            if payload.discount_cents:
                calculated_line_items.append(
                    {
                        "type": "discount",
                        "description": discount_reason,
                        "amount_cents": -payload.discount_cents,
                    }
                )
            if selected_rate_plan.tax_status == "taxable":
                calculated_line_items.append(
                    {
                        "type": "tax",
                        "description": (
                            f"Lehi, Utah sales tax "
                            f"({selected_rate_plan.tax_rate_bps / 100:.2f}%)"
                        ),
                        "amount_cents": tax_cents,
                    }
                )
            pricing_adjustment = {
                "pricing_subtotal_cents": payload.pricing_subtotal_cents,
                "discount_cents": payload.discount_cents,
                "discount_reason": discount_reason,
                "tax_status": selected_rate_plan.tax_status,
                "tax_rate_bps": selected_rate_plan.tax_rate_bps,
                "tax_cents": tax_cents,
                "final_amount_cents": calculated_amount_cents,
                "transaction_date": reservation.starts_at.date().isoformat(),
            }
        if (
            payload.status in {"approved", "sent", "accepted"}
            and calculated_amount_cents <= 0
        ):
            raise HTTPException(
                status_code=422, detail="Approved proposals require an amount."
            )
        if row.status not in {"sent", "accepted", "declined", "voided"}:
            row.currency = payload.currency.upper()
            row.amount_cents = calculated_amount_cents
            row.line_items_json = calculated_line_items
            if selected_rate_plan is not None:
                row.rate_plan_id = selected_rate_plan.id
                row.rate_plan_snapshot_json = {
                    "id": selected_rate_plan.id,
                    "version": selected_rate_plan.version,
                    "name": selected_rate_plan.name,
                    "offering_id": selected_rate_plan.offering_id,
                    "currency": selected_rate_plan.currency,
                    "unit_amount_cents": selected_rate_plan.unit_amount_cents,
                    "public_price_display": selected_rate_plan.public_price_display,
                    "booking_unit": selected_rate_plan.booking_unit,
                    "minimum_units": selected_rate_plan.minimum_units,
                    "deposit_type": selected_rate_plan.deposit_type,
                    "deposit_amount_cents": selected_rate_plan.deposit_amount_cents,
                    "deposit_percent_bps": selected_rate_plan.deposit_percent_bps,
                    "cancellation_policy": selected_rate_plan.cancellation_policy,
                    "included": list(selected_rate_plan.included_json or []),
                    "addons": list(selected_rate_plan.addons_json or []),
                    "commercial_terms": dict(
                        selected_rate_plan.commercial_terms_json or {}
                    ),
                    "source_evidence": list(
                        selected_rate_plan.source_evidence_json or []
                    ),
                    "conflicts": list(selected_rate_plan.conflicts_json or []),
                    "effective_from": selected_rate_plan.effective_from.isoformat(),
                    "effective_until": (
                        selected_rate_plan.effective_until.isoformat()
                        if selected_rate_plan.effective_until
                        else None
                    ),
                    "pricing_adjustment": pricing_adjustment,
                    "transaction_date": reservation.starts_at.date().isoformat(),
                    "snapshotted_at": _now().isoformat(),
                }
            row.terms_summary = payload.terms_summary.strip()
            row.valid_until = payload.valid_until
            row.document_url = payload.document_url.strip()
        row.status = payload.status
        row.updated_at = _now()
        if payload.status == "approved":
            approver = payload.approved_by.strip()
            if not approver:
                raise HTTPException(status_code=422, detail="Proposal approval requires an approver.")
            row.approved_by = approver
            row.approved_at = row.approved_at or _now()
        if payload.status == "sent":
            if not row.approved_by:
                raise HTTPException(status_code=409, detail="Approve the proposal before sending.")
            row.sent_at = row.sent_at or _now()
        if payload.status == "accepted":
            row.accepted_at = row.accepted_at or _now()
        if payload.status == "voided":
            row.voided_at = row.voided_at or _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="proposal",
            entity_id=row.id,
            action=f"proposal_{payload.status}",
            actor=payload.actor,
            before_json=before,
            after_json={
                "reservation_id": reservation_id,
                "version": row.version,
                "type": row.proposal_type,
                "status": row.status,
                "amount_cents": row.amount_cents,
                "currency": row.currency,
                "rate_plan_id": row.rate_plan_id,
                "rate_plan_snapshot": dict(row.rate_plan_snapshot_json or {}),
                "document_url": row.document_url,
                "approved_by": row.approved_by,
                "pricing_adjustment": pricing_adjustment,
            },
        ))
        if reservation.kind == "event" and payload.status == "sent":
            attempt_booking_message(
                session,
                request=request,
                reservation=reservation,
                milestone="quote_sent",
                actor=payload.actor,
            )
        return {
            "ok": True,
            "proposal_id": row.id,
            "version": row.version,
            "status": row.status,
        }


@router.post("/{reservation_id}/agreements", status_code=201)
def record_agreement(
    reservation_id: str,
    payload: AgreementInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.status not in AGREEMENT_STATUSES:
        raise HTTPException(status_code=422, detail="Unsupported agreement status.")
    if payload.status == "signed" and not payload.provider_reference:
        raise HTTPException(status_code=422, detail="Signed agreements require provider evidence.")
    if payload.provider == "quickbooks_contract_builder":
        if payload.status in {"sent", "signed"} and not payload.document_url.strip():
            raise HTTPException(
                status_code=422,
                detail="QuickBooks contract evidence requires the Contract Builder document URL.",
            )
        if payload.status == "signed":
            certificate_reference = str(
                payload.evidence.get("esign_certificate_reference") or ""
            ).strip()
            signed_document_checksum = str(
                payload.evidence.get("signed_document_checksum") or ""
            ).strip().lower()
            if not certificate_reference:
                raise HTTPException(
                    status_code=422,
                    detail="Signed QuickBooks contracts require the e-sign certificate reference.",
                )
            if (
                len(signed_document_checksum) != 64
                or any(character not in "0123456789abcdef" for character in signed_document_checksum)
            ):
                raise HTTPException(
                    status_code=422,
                    detail="Signed QuickBooks contracts require a SHA-256 signed-document checksum.",
                )
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(BuildingReservation, reservation_id)
        if reservation is None:
            raise HTTPException(status_code=404, detail="Reservation not found.")
        existing = session.execute(
            select(BuildingAgreement).where(
                BuildingAgreement.reservation_id == reservation_id,
                BuildingAgreement.version == payload.version,
            )
        ).scalar_one_or_none()
        if existing and existing.status == "signed" and payload.status != "signed":
            raise HTTPException(status_code=409, detail="Signed agreement evidence is immutable.")
        row = existing or BuildingAgreement(
            id=payload.id or str(uuid4()),
            reservation_id=reservation_id,
            version=payload.version,
            created_by=payload.actor,
        )
        row.status = payload.status
        row.provider = payload.provider
        row.provider_reference = payload.provider_reference
        row.template_name = payload.template_name
        row.document_url = payload.document_url
        row.evidence_json = payload.evidence
        row.updated_at = _now()
        if payload.status == "sent" and row.sent_at is None:
            row.sent_at = _now()
        if payload.status == "signed":
            row.signed_at = row.signed_at or _now()
            reservation.agreement_status = "signed"
        elif payload.status == "voided":
            row.voided_at = _now()
            reservation.agreement_status = "voided"
        else:
            reservation.agreement_status = payload.status
        reservation.updated_at = _now()
        session.add(row)
        if payload.provider == "quickbooks_contract_builder":
            signature_readiness = session.execute(
                select(BuildingSignatureRequestReadiness).where(
                    BuildingSignatureRequestReadiness.agreement_id == row.id
                )
            ).scalar_one_or_none()
            if signature_readiness is None or signature_readiness.status != "approved":
                raise HTTPException(
                    status_code=409,
                    detail="Approve the frozen QuickBooks signature handoff first.",
                )
            if signature_readiness.agreement_checksum != row.package_checksum:
                raise HTTPException(
                    status_code=409,
                    detail="QuickBooks evidence does not match the approved agreement checksum.",
                )
            signature_readiness.provider = "quickbooks_contract_builder"
            signature_readiness.provider_reference = payload.provider_reference
            signature_readiness.delivery_status = (
                "completed" if payload.status == "signed" else "sent"
            )
            signature_readiness.updated_at = _now()
            session.add(signature_readiness)
        session.add(BuildingAuditEvent(
            entity_type="agreement",
            entity_id=row.id,
            action=f"agreement_{payload.status}",
            actor=payload.actor,
            after_json={
                "reservation_id": reservation_id,
                "version": row.version,
                "provider": row.provider,
                "provider_reference": row.provider_reference,
                "document_url": row.document_url,
                "evidence": dict(row.evidence_json or {}),
            },
        ))
        if reservation.kind == "event" and payload.status == "signed":
            attempt_booking_message(
                session,
                request=request,
                reservation=reservation,
                milestone="agreement_signed",
                actor=payload.actor,
            )
        return {"ok": True, "agreement_id": row.id, "status": row.status}


@router.post("/{reservation_id}/deposit-evidence", status_code=201)
def record_deposit(
    reservation_id: str,
    payload: DepositInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.status not in DEPOSIT_STATUSES:
        raise HTTPException(status_code=422, detail="Unsupported deposit status.")
    if payload.status in {"paid", "refunded"} and not payload.provider_reference:
        raise HTTPException(status_code=422, detail="Posted deposit states require provider evidence.")
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(BuildingReservation, reservation_id)
        if reservation is None:
            raise HTTPException(status_code=404, detail="Reservation not found.")
        row = BuildingDepositEvidence(
            id=payload.id or str(uuid4()),
            reservation_id=reservation_id,
            status=payload.status,
            amount_cents=payload.amount_cents,
            provider=payload.provider,
            provider_reference=payload.provider_reference,
            evidence_json=payload.evidence,
            recorded_by=payload.actor,
        )
        session.add(row)
        reservation.deposit_status = payload.status
        reservation.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="deposit",
            entity_id=row.id,
            action=f"deposit_{payload.status}",
            actor=payload.actor,
            after_json={
                "reservation_id": reservation_id,
                "amount_cents": row.amount_cents,
                "provider": row.provider,
                "provider_reference": row.provider_reference,
            },
        ))
        if reservation.kind == "event" and payload.status == "paid":
            attempt_booking_message(
                session,
                request=request,
                reservation=reservation,
                milestone="payment_received",
                actor=payload.actor,
            )
        return {"ok": True, "deposit_id": row.id, "status": row.status}
