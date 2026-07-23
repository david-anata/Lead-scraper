"""Deterministic workspace and event booking workflows."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4

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
    BuildingInquiry,
    BuildingOffering,
    BuildingProposal,
    BuildingRatePlan,
    BuildingRelationship,
    BuildingReservation,
    BuildingSpace,
    BuildingTour,
)
from sales_support_agent.services.building_calendar import queue_calendar_projection
from sales_support_agent.services.building_checklists import (
    ensure_operational_checklist,
)


router = APIRouter(prefix="/api/internal/building/bookings", tags=["building-bookings"])

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


class TransitionInput(BaseModel):
    target_status: str = Field(min_length=1, max_length=32)
    hold_expires_at: datetime | None = None
    actor: str = Field(min_length=1, max_length=255)
    reason: str = Field(default="", max_length=1000)


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


def _availability_block(
    session,
    reservation: BuildingReservation,
) -> BuildingAvailabilityBlock | None:
    return session.execute(
        select(BuildingAvailabilityBlock).where(
            BuildingAvailabilityBlock.source_reference == f"reservation:{reservation.id}"
        )
    ).scalar_one_or_none()


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
        before = row.status
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
    if payload.status in {"approved", "sent", "accepted"} and payload.amount_cents <= 0:
        raise HTTPException(status_code=422, detail="Approved proposals require an amount.")
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
        expected_type = "quote" if reservation.kind == "event" else "proposal"
        if payload.proposal_type != expected_type:
            raise HTTPException(
                status_code=422,
                detail=f"{reservation.kind.title()} reservations use {expected_type} records.",
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
        if row.status not in {"sent", "accepted", "declined", "voided"}:
            row.currency = payload.currency.upper()
            row.amount_cents = payload.amount_cents
            row.line_items_json = payload.line_items
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
                    "effective_from": selected_rate_plan.effective_from.isoformat(),
                    "effective_until": (
                        selected_rate_plan.effective_until.isoformat()
                        if selected_rate_plan.effective_until
                        else None
                    ),
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
            },
        ))
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
            },
        ))
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
        return {"ok": True, "deposit_id": row.id, "status": row.status}
