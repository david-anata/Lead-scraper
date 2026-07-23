"""Anata Building public catalog, inquiry, and internal inventory routes."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import delete, select

from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.services.building_hubspot_sync import (
    sync_building_inquiry_to_hubspot,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingAvailabilityBlock,
    BuildingCommunicationPreference,
    BuildingContact,
    BuildingInquiry,
    BuildingOffering,
    BuildingRelationship,
    BuildingSuppression,
    BuildingSpace,
)


public_router = APIRouter(prefix="/api/public/building", tags=["building-public"])
internal_router = APIRouter(prefix="/api/internal/building", tags=["building-internal"])

SPACE_STATUSES = {"available", "soft_hold", "contract_pending", "occupied", "turnover", "maintenance", "unavailable"}
BLOCK_STATES = {"soft_hold", "contract_pending", "booked", "occupied", "turnover", "maintenance", "unavailable"}
INQUIRY_KINDS = {"tour", "event", "workspace"}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_building_key(request: Request, provided: Optional[str]) -> None:
    configured = str(getattr(request.app.state.settings, "building_site_intake_key", "") or "").strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Building integration is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid building integration key.")


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _space_public_payload(space: BuildingSpace) -> dict[str, Any]:
    safe_status = space.status if space.status in {"available", "turnover"} else "contact"
    return {
        "id": space.id,
        "slug": space.slug,
        "name": space.name,
        "space_type": space.space_type,
        "floor": space.floor,
        "capacity": space.capacity or None,
        "availability": safe_status,
        "description": space.public_description,
        "features": list(space.features_json or []),
        "media": list(space.media_json or []),
        "updated_at": space.updated_at.isoformat(),
    }


def _offering_public_payload(offering: BuildingOffering, space: BuildingSpace | None) -> dict[str, Any]:
    return {
        "id": offering.id,
        "slug": offering.slug,
        "name": offering.name,
        "offering_type": offering.offering_type,
        "description": offering.public_description,
        "price_display": offering.price_display,
        "booking_unit": offering.booking_unit,
        "call_to_action": offering.call_to_action,
        "features": list(offering.features_json or []),
        "space": _space_public_payload(space) if space and space.is_public else None,
        "updated_at": offering.updated_at.isoformat(),
    }


class InquiryInput(BaseModel):
    kind: Literal["tour", "event", "workspace"]
    name: str = Field(min_length=1, max_length=120)
    email: str = Field(min_length=3, max_length=255)
    phone: str = Field(default="", max_length=128)
    preferred_date: date | None = None
    offering_id: str | None = Field(default=None, max_length=64)
    source: str = Field(default="anata-building", max_length=64)
    source_reference: str = Field(default="", max_length=255)
    consent_to_contact: bool
    consent_to_marketing: bool = False
    details: dict[str, Any] = Field(default_factory=dict)

    @field_validator("email")
    @classmethod
    def valid_email(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not EMAIL_RE.fullmatch(normalized):
            raise ValueError("Enter a valid email address.")
        return normalized

    @field_validator("name", "phone", "source", "source_reference", mode="before")
    @classmethod
    def clean_text(cls, value: Any) -> str:
        return str(value or "").strip()


class InquiryRetryInput(BaseModel):
    actor: str = Field(min_length=1, max_length=255)


class SpaceInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    slug: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=128)
    name: str = Field(min_length=1, max_length=255)
    space_type: str = Field(min_length=1, max_length=64)
    floor: str = Field(default="", max_length=64)
    capacity: int = Field(default=0, ge=0)
    status: str = Field(default="unavailable")
    public_description: str = Field(default="", max_length=4000)
    internal_notes: str = Field(default="", max_length=4000)
    features: list[str] = Field(default_factory=list)
    media: list[dict[str, Any]] = Field(default_factory=list)
    is_public: bool = False

    @field_validator("status")
    @classmethod
    def valid_status(cls, value: str) -> str:
        if value not in SPACE_STATUSES:
            raise ValueError("Unsupported space status.")
        return value


class OfferingInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    slug: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=128)
    name: str = Field(min_length=1, max_length=255)
    offering_type: str = Field(min_length=1, max_length=64)
    space_id: str | None = Field(default=None, max_length=64)
    public_description: str = Field(default="", max_length=4000)
    price_display: str = Field(default="", max_length=128)
    booking_unit: str = Field(default="custom", max_length=32)
    call_to_action: str = Field(default="inquire", max_length=64)
    features: list[str] = Field(default_factory=list)
    is_published: bool = False


class AvailabilityInput(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    space_id: str = Field(min_length=1, max_length=64)
    state: str
    starts_at: datetime
    ends_at: datetime | None = None
    expires_at: datetime | None = None
    source: str = Field(default="agent", max_length=64)
    source_reference: str = Field(default="", max_length=255)
    public_label: str = Field(default="", max_length=128)
    notes: str = Field(default="", max_length=4000)
    actor: str = Field(default="", max_length=255)

    @field_validator("state")
    @classmethod
    def valid_state(cls, value: str) -> str:
        if value not in BLOCK_STATES:
            raise ValueError("Unsupported availability state.")
        return value


@public_router.get("/offerings")
def list_public_offerings(request: Request) -> dict[str, Any]:
    with session_scope(request.app.state.session_factory) as session:
        offerings = session.execute(
            select(BuildingOffering)
            .where(BuildingOffering.is_published.is_(True))
            .order_by(BuildingOffering.name)
        ).scalars().all()
        space_ids = {item.space_id for item in offerings if item.space_id}
        spaces = {
            item.id: item
            for item in session.execute(
                select(BuildingSpace).where(BuildingSpace.id.in_(space_ids))
            ).scalars().all()
        } if space_ids else {}
        return {
            "offerings": [
                _offering_public_payload(item, spaces.get(item.space_id or ""))
                for item in offerings
            ],
            "updated_at": max((item.updated_at for item in offerings), default=_now()).isoformat(),
        }


@public_router.get("/offerings/{slug}")
def get_public_offering(slug: str, request: Request) -> dict[str, Any]:
    with session_scope(request.app.state.session_factory) as session:
        offering = session.execute(
            select(BuildingOffering).where(
                BuildingOffering.slug == slug,
                BuildingOffering.is_published.is_(True),
            )
        ).scalar_one_or_none()
        if offering is None:
            raise HTTPException(status_code=404, detail="Offering not found.")
        space = session.get(BuildingSpace, offering.space_id) if offering.space_id else None
        return _offering_public_payload(offering, space)


@public_router.get("/availability")
def list_public_availability(request: Request) -> dict[str, Any]:
    with session_scope(request.app.state.session_factory) as session:
        spaces = session.execute(
            select(BuildingSpace)
            .where(BuildingSpace.is_public.is_(True))
            .order_by(BuildingSpace.name)
        ).scalars().all()
        return {
            "spaces": [_space_public_payload(space) for space in spaces],
            "updated_at": max((space.updated_at for space in spaces), default=_now()).isoformat(),
        }


@public_router.post("/inquiries", status_code=201)
def create_inquiry(
    payload: InquiryInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    _require_building_key(request, x_internal_api_key)
    if not payload.consent_to_contact:
        raise HTTPException(status_code=422, detail="Contact consent is required.")
    dedupe_key = str(idempotency_key or "").strip()
    if not dedupe_key or len(dedupe_key) > 128:
        raise HTTPException(status_code=400, detail="A valid Idempotency-Key is required.")

    with session_scope(request.app.state.session_factory) as session:
        existing = session.execute(
            select(BuildingInquiry).where(BuildingInquiry.idempotency_key == dedupe_key)
        ).scalar_one_or_none()
        if existing is not None:
            return {"ok": True, "inquiry_id": existing.id, "status": existing.status, "duplicate": True}

        if payload.offering_id and session.get(BuildingOffering, payload.offering_id) is None:
            raise HTTPException(status_code=422, detail="Unknown offering.")

        inquiry = BuildingInquiry(
            id=str(uuid4()),
            idempotency_key=dedupe_key,
            kind=payload.kind,
            source=payload.source or "anata-building",
            source_reference=payload.source_reference,
            offering_id=payload.offering_id,
            name=payload.name.strip(),
            email=payload.email,
            phone=payload.phone,
            preferred_date=payload.preferred_date,
            consent_to_contact=True,
            consent_to_marketing=payload.consent_to_marketing,
            payload_json=payload.details,
        )
        session.add(inquiry)
        session.flush()
        contact = session.execute(
            select(BuildingContact).where(BuildingContact.email == inquiry.email)
        ).scalar_one_or_none()
        if contact is None:
            contact = BuildingContact(
                id=str(uuid4()),
                email=inquiry.email,
                full_name=inquiry.name,
                phone=inquiry.phone,
                source=inquiry.source,
            )
        else:
            contact.full_name = contact.full_name or inquiry.name
            contact.phone = contact.phone or inquiry.phone
            contact.updated_at = _now()
        session.add(contact)
        session.flush()
        relationship_type = "event_host" if inquiry.kind == "event" else "prospect"
        relationship_reference = f"inquiry:{inquiry.id}"
        session.add(BuildingRelationship(
            id=str(uuid4()),
            contact_id=contact.id,
            relationship_type=relationship_type,
            status="active",
            source_reference=relationship_reference,
            metadata_json={"inquiry_kind": inquiry.kind, "offering_id": inquiry.offering_id},
        ))
        preference = session.get(BuildingCommunicationPreference, contact.id)
        if preference is None:
            preference = BuildingCommunicationPreference(contact_id=contact.id)
        if payload.consent_to_marketing:
            preference.marketing_status = "subscribed"
            preference.marketing_source = "building_inquiry"
            preference.marketing_changed_at = _now()
            session.execute(
                delete(BuildingSuppression).where(
                    BuildingSuppression.email == contact.email,
                    BuildingSuppression.scope == "marketing",
                    BuildingSuppression.reason == "unsubscribe",
                )
            )
        preference.updated_at = _now()
        session.add(preference)
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="created",
            actor="building-site",
            after_json={"kind": inquiry.kind, "source": inquiry.source, "offering_id": inquiry.offering_id},
        ))

        client = HubSpotClient(request.app.state.settings)
        if client.is_configured:
            sync_building_inquiry_to_hubspot(
                session=session,
                inquiry=inquiry,
                contact=contact,
                client=client,
                actor="building-site",
            )
        return {"ok": True, "inquiry_id": inquiry.id, "status": inquiry.status, "duplicate": False}


@internal_router.post("/inquiries/{inquiry_id}/retry-hubspot")
def retry_inquiry_hubspot(
    inquiry_id: str,
    payload: InquiryRetryInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    client = HubSpotClient(request.app.state.settings)
    if not client.is_configured:
        raise HTTPException(
            status_code=503,
            detail="HubSpot is not configured; the inquiry remains queued.",
        )
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        contact = session.execute(
            select(BuildingContact).where(BuildingContact.email == inquiry.email)
        ).scalar_one_or_none()
        if contact is None:
            raise HTTPException(
                status_code=409,
                detail="The linked building contact is missing; review the inquiry manually.",
            )
        ok = sync_building_inquiry_to_hubspot(
            session=session,
            inquiry=inquiry,
            contact=contact,
            client=client,
            actor=payload.actor,
        )
        state = dict((inquiry.payload_json or {}).get("_hubspot_sync") or {})
        return {
            "ok": ok,
            "inquiry_id": inquiry.id,
            "status": inquiry.status,
            "hubspot_contact_id": inquiry.hubspot_contact_id,
            "attempt_count": int(state.get("attempt_count") or 0),
            "error": str(state.get("last_error") or ""),
        }


@internal_router.put("/spaces/{space_id}")
def upsert_space(
    space_id: str,
    payload: SpaceInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != space_id:
        raise HTTPException(status_code=422, detail="Space ID does not match route.")
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingSpace, space_id)
        before = _space_public_payload(row) if row else {}
        if row is None:
            row = BuildingSpace(id=payload.id, slug=payload.slug, name=payload.name, space_type=payload.space_type)
        for key, value in {
            "slug": payload.slug,
            "name": payload.name,
            "space_type": payload.space_type,
            "floor": payload.floor,
            "capacity": payload.capacity,
            "status": payload.status,
            "public_description": payload.public_description,
            "internal_notes": payload.internal_notes,
            "features_json": payload.features,
            "media_json": payload.media,
            "is_public": payload.is_public,
            "updated_at": _now(),
        }.items():
            setattr(row, key, value)
        session.add(row)
        session.flush()
        session.add(BuildingAuditEvent(
            entity_type="space", entity_id=row.id, action="upserted",
            actor="internal-api", before_json=before, after_json=_space_public_payload(row),
        ))
        return {"ok": True, "space": _space_public_payload(row)}


@internal_router.put("/offerings/{offering_id}")
def upsert_offering(
    offering_id: str,
    payload: OfferingInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != offering_id:
        raise HTTPException(status_code=422, detail="Offering ID does not match route.")
    with session_scope(request.app.state.session_factory) as session:
        if payload.space_id and session.get(BuildingSpace, payload.space_id) is None:
            raise HTTPException(status_code=422, detail="Unknown space.")
        row = session.get(BuildingOffering, offering_id)
        before = {"published": row.is_published, "name": row.name} if row else {}
        if row is None:
            row = BuildingOffering(
                id=payload.id, slug=payload.slug, name=payload.name,
                offering_type=payload.offering_type,
            )
        for key, value in {
            "slug": payload.slug,
            "name": payload.name,
            "offering_type": payload.offering_type,
            "space_id": payload.space_id,
            "public_description": payload.public_description,
            "price_display": payload.price_display,
            "booking_unit": payload.booking_unit,
            "call_to_action": payload.call_to_action,
            "features_json": payload.features,
            "is_published": payload.is_published,
            "updated_at": _now(),
        }.items():
            setattr(row, key, value)
        session.add(row)
        session.flush()
        session.add(BuildingAuditEvent(
            entity_type="offering", entity_id=row.id, action="upserted",
            actor="internal-api", before_json=before,
            after_json={"published": row.is_published, "name": row.name, "space_id": row.space_id},
        ))
        space = session.get(BuildingSpace, row.space_id) if row.space_id else None
        return {"ok": True, "offering": _offering_public_payload(row, space)}


@internal_router.post("/availability", status_code=201)
def create_availability_block(
    payload: AvailabilityInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.ends_at and payload.ends_at <= payload.starts_at:
        raise HTTPException(status_code=422, detail="End must be after start.")
    with session_scope(request.app.state.session_factory) as session:
        space = session.get(BuildingSpace, payload.space_id)
        if space is None:
            raise HTTPException(status_code=422, detail="Unknown space.")
        block_end = payload.ends_at or datetime.max.replace(tzinfo=timezone.utc)
        conflicts = session.execute(
            select(BuildingAvailabilityBlock).where(
                BuildingAvailabilityBlock.space_id == payload.space_id,
                BuildingAvailabilityBlock.starts_at < block_end,
                (
                    (BuildingAvailabilityBlock.ends_at.is_(None))
                    | (BuildingAvailabilityBlock.ends_at > payload.starts_at)
                ),
            )
        ).scalars().all()
        active_conflicts = [
            item for item in conflicts
            if not (item.state == "soft_hold" and item.expires_at and item.expires_at <= _now())
        ]
        if active_conflicts:
            raise HTTPException(status_code=409, detail="Space already has an overlapping availability block.")
        row = BuildingAvailabilityBlock(
            id=payload.id or str(uuid4()),
            space_id=payload.space_id,
            state=payload.state,
            starts_at=payload.starts_at,
            ends_at=payload.ends_at,
            expires_at=payload.expires_at,
            source=payload.source,
            source_reference=payload.source_reference,
            public_label=payload.public_label,
            notes=payload.notes,
            created_by=payload.actor,
        )
        session.add(row)
        now = _now()
        is_current = payload.starts_at <= now and (
            payload.ends_at is None or payload.ends_at > now
        )
        if is_current:
            space.status = payload.state if payload.state in SPACE_STATUSES else (
                "occupied" if payload.state == "booked" else "unavailable"
            )
            space.updated_at = now
        session.add(BuildingAuditEvent(
            entity_type="availability", entity_id=row.id, action="created",
            actor=payload.actor or "internal-api",
            after_json={"space_id": row.space_id, "state": row.state},
        ))
        return {"ok": True, "block_id": row.id, "space_status": space.status}
