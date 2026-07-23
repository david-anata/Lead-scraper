"""Deterministic workspace and event booking workflows."""

from __future__ import annotations

from datetime import datetime, timezone
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
    BuildingReservation,
    BuildingSpace,
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
        if payload.target_status in {"cancelled", "expired", "completed"}:
            session.execute(
                delete(BuildingAvailabilityBlock).where(
                    BuildingAvailabilityBlock.source_reference == f"reservation:{row.id}"
                )
            )
            row.hold_expires_at = None
        before = row.status
        row.status = payload.target_status
        row.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="reservation",
            entity_id=row.id,
            action="status_changed",
            actor=payload.actor,
            before_json={"status": before},
            after_json={"status": row.status, "reason": payload.reason},
        ))
        return {"ok": True, "reservation": _reservation_payload(row)}


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
