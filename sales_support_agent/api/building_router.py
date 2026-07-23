"""Anata Building public catalog, inquiry, and internal inventory routes."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import delete, select

from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.services.building_hubspot_sync import (
    sync_building_inquiry_to_hubspot,
)
from sales_support_agent.services.building_analytics import (
    apply_attribution,
    build_attribution,
    build_building_analytics,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingAvailabilityBlock,
    BuildingCommunicationPreference,
    BuildingContact,
    BuildingInquiry,
    BuildingOffering,
    BuildingRatePlan,
    BuildingRelationship,
    BuildingSuppression,
    BuildingSpace,
)


public_router = APIRouter(prefix="/api/public/building", tags=["building-public"])
internal_router = APIRouter(prefix="/api/internal/building", tags=["building-internal"])

SPACE_STATUSES = {"available", "soft_hold", "contract_pending", "occupied", "turnover", "maintenance", "unavailable"}
BLOCK_STATES = {"soft_hold", "contract_pending", "booked", "occupied", "turnover", "maintenance", "unavailable"}
RATE_PLAN_STATUSES = {"draft", "approved", "retired"}
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
    def publishable(item: Any) -> bool:
        if not isinstance(item, dict) or item.get("approved") is not True:
            return False
        media_id = str(item.get("id") or "")
        src = str(item.get("src") or "").strip()
        alt = str(item.get("alt") or "").strip()
        kind = str(item.get("kind") or "image")
        placement = str(item.get("placement") or "gallery")
        return bool(
            re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", media_id)
            and alt
            and kind in {"image", "video"}
            and placement in {"hero", "card", "gallery", "floor_plan"}
            and (
                (src.startswith("/") and not src.startswith("//"))
                or src.startswith("https://")
            )
        )

    approved_media = sorted(
        (
            {
                "id": str(item.get("id") or ""),
                "src": str(item.get("src") or ""),
                "kind": str(item.get("kind") or "image"),
                "alt": str(item.get("alt") or ""),
                "placement": str(item.get("placement") or "gallery"),
                "caption": str(item.get("caption") or ""),
                "sort_order": int(item.get("sort_order") or 0),
            }
            for item in list(space.media_json or [])
            if publishable(item)
        ),
        key=lambda item: (item["sort_order"], item["id"]),
    )
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
        "media": approved_media,
        "updated_at": space.updated_at.isoformat(),
    }


def _rate_plan_public_payload(rate_plan: BuildingRatePlan | None) -> dict[str, Any] | None:
    if rate_plan is None:
        return None
    return {
        "id": rate_plan.id,
        "version": rate_plan.version,
        "name": rate_plan.name,
        "currency": rate_plan.currency,
        "public_price_display": rate_plan.public_price_display,
        "booking_unit": rate_plan.booking_unit,
        "minimum_units": rate_plan.minimum_units,
        "deposit": {
            "type": rate_plan.deposit_type,
            "amount_cents": (
                rate_plan.deposit_amount_cents
                if rate_plan.deposit_type == "fixed"
                else None
            ),
            "percent": (
                rate_plan.deposit_percent_bps / 100
                if rate_plan.deposit_type == "percent"
                else None
            ),
        },
        "cancellation_policy": rate_plan.cancellation_policy,
        "included": list(rate_plan.included_json or []),
        "addons": list(rate_plan.addons_json or []),
        "effective_from": rate_plan.effective_from.isoformat(),
        "effective_until": (
            rate_plan.effective_until.isoformat()
            if rate_plan.effective_until
            else None
        ),
    }


def _offering_public_payload(
    offering: BuildingOffering,
    space: BuildingSpace | None,
    rate_plan: BuildingRatePlan | None = None,
) -> dict[str, Any]:
    return {
        "id": offering.id,
        "slug": offering.slug,
        "name": offering.name,
        "offering_type": offering.offering_type,
        "description": offering.public_description,
        "price_display": (
            rate_plan.public_price_display if rate_plan else offering.price_display
        ),
        "booking_unit": rate_plan.booking_unit if rate_plan else offering.booking_unit,
        "call_to_action": offering.call_to_action,
        "features": list(offering.features_json or []),
        "rate_plan": _rate_plan_public_payload(rate_plan),
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

    @field_validator("source")
    @classmethod
    def normalize_source(cls, value: str) -> str:
        normalized = re.sub(r"[^a-z0-9_-]+", "_", value.lower()).strip("_")
        return normalized or "unknown"


class InquiryRetryInput(BaseModel):
    actor: str = Field(min_length=1, max_length=255)


class InquiryLifecycleInput(BaseModel):
    target_stage: Literal["responded", "qualified", "closed_won", "closed_lost"]
    actor: str = Field(min_length=1, max_length=255)
    assigned_owner: str = Field(default="", max_length=255)
    channel: Literal["email", "phone", "text", "in_person", "other"] = "email"
    notes: str = Field(default="", max_length=2000)


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


class SpaceMediaInput(BaseModel):
    id: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=64)
    src: str = Field(min_length=1, max_length=1000)
    kind: Literal["image", "video"] = "image"
    alt: str = Field(default="", max_length=500)
    placement: Literal["hero", "card", "gallery", "floor_plan"] = "gallery"
    caption: str = Field(default="", max_length=500)
    sort_order: int = Field(default=0, ge=0, le=10000)
    approved: bool = False
    actor: str = Field(default="internal-api", min_length=1, max_length=255)

    @field_validator("src")
    @classmethod
    def valid_src(cls, value: str) -> str:
        cleaned = value.strip()
        if not (
            (cleaned.startswith("/") and not cleaned.startswith("//"))
            or cleaned.startswith("https://")
        ):
            raise ValueError("Media source must be a root-relative path or HTTPS URL.")
        return cleaned

    @field_validator("alt", "caption", "actor", mode="before")
    @classmethod
    def clean_media_text(cls, value: Any) -> str:
        return str(value or "").strip()

    def as_storage_dict(self) -> dict[str, Any]:
        if self.approved and not self.alt:
            raise ValueError("Approved media requires descriptive alt text.")
        return {
            "id": self.id,
            "src": self.src,
            "kind": self.kind,
            "alt": self.alt,
            "placement": self.placement,
            "caption": self.caption,
            "sort_order": self.sort_order,
            "approved": self.approved,
        }


class SpaceMediaDeleteInput(BaseModel):
    actor: str = Field(min_length=1, max_length=255)
    reason: str = Field(min_length=5, max_length=500)


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


class RatePlanInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    version: int = Field(default=1, ge=1)
    name: str = Field(min_length=1, max_length=255)
    status: Literal["draft", "approved", "retired"] = "draft"
    currency: str = Field(default="USD", min_length=3, max_length=3)
    unit_amount_cents: int = Field(default=0, ge=0)
    public_price_display: str = Field(default="", max_length=128)
    booking_unit: str = Field(default="custom", max_length=32)
    minimum_units: int = Field(default=1, ge=1)
    deposit_type: Literal["none", "fixed", "percent"] = "none"
    deposit_amount_cents: int = Field(default=0, ge=0)
    deposit_percent_bps: int = Field(default=0, ge=0, le=10000)
    cancellation_policy: str = Field(default="", max_length=4000)
    included: list[str] = Field(default_factory=list)
    addons: list[dict[str, Any]] = Field(default_factory=list)
    effective_from: date
    effective_until: date | None = None
    approved_by: str = Field(default="", max_length=255)
    actor: str = Field(min_length=1, max_length=255)

    @model_validator(mode="after")
    def valid_commercial_terms(self) -> "RatePlanInput":
        if self.effective_until and self.effective_until < self.effective_from:
            raise ValueError("Rate-plan end date precedes its start date.")
        if self.deposit_type == "fixed" and self.deposit_amount_cents <= 0:
            raise ValueError("A fixed deposit requires a positive amount.")
        if self.deposit_type == "percent" and self.deposit_percent_bps <= 0:
            raise ValueError("A percentage deposit requires a positive percentage.")
        if self.status == "approved":
            if self.unit_amount_cents <= 0:
                raise ValueError("An approved rate plan requires a positive unit price.")
            if not self.public_price_display.strip():
                raise ValueError("An approved rate plan requires public price wording.")
            if not self.cancellation_policy.strip():
                raise ValueError("An approved rate plan requires a cancellation policy.")
            if not self.approved_by.strip():
                raise ValueError("An approved rate plan requires an approver.")
        self.currency = self.currency.upper()
        return self


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
        today = _now().date()
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
        offering_ids = [item.id for item in offerings]
        current_rate_plans: dict[str, BuildingRatePlan] = {}
        if offering_ids:
            for rate_plan in session.execute(
                select(BuildingRatePlan)
                .where(
                    BuildingRatePlan.offering_id.in_(offering_ids),
                    BuildingRatePlan.status == "approved",
                    BuildingRatePlan.effective_from <= today,
                    (
                        BuildingRatePlan.effective_until.is_(None)
                        | (BuildingRatePlan.effective_until >= today)
                    ),
                )
                .order_by(
                    BuildingRatePlan.offering_id,
                    BuildingRatePlan.version.desc(),
                )
            ).scalars().all():
                current_rate_plans.setdefault(rate_plan.offering_id, rate_plan)
        return {
            "offerings": [
                _offering_public_payload(
                    item,
                    spaces.get(item.space_id or ""),
                    current_rate_plans.get(item.id),
                )
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
        today = _now().date()
        rate_plan = session.execute(
            select(BuildingRatePlan)
            .where(
                BuildingRatePlan.offering_id == offering.id,
                BuildingRatePlan.status == "approved",
                BuildingRatePlan.effective_from <= today,
                (
                    BuildingRatePlan.effective_until.is_(None)
                    | (BuildingRatePlan.effective_until >= today)
                ),
            )
            .order_by(BuildingRatePlan.version.desc())
        ).scalars().first()
        return _offering_public_payload(offering, space, rate_plan)


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
        actor = str(
            getattr(request.state, "building_inquiry_actor", "building-site")
            or "building-site"
        )
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
        attribution = build_attribution(
            source=inquiry.source,
            source_reference=inquiry.source_reference,
            details={
                **dict(payload.details or {}),
                "offering_id": inquiry.offering_id or "",
            },
            captured_at=inquiry.created_at,
        )
        first_captured_at = inquiry.created_at
        first_captured_raw = str(payload.details.get("firstCapturedAt") or "").strip()
        if first_captured_raw:
            try:
                first_captured_at = datetime.fromisoformat(
                    first_captured_raw.replace("Z", "+00:00")
                )
            except ValueError:
                first_captured_at = inquiry.created_at
        first_attribution = build_attribution(
            source=str(payload.details.get("firstUtmSource") or inquiry.source),
            source_reference=str(
                payload.details.get("firstReferrer") or inquiry.source_reference
            ),
            details={
                "medium": payload.details.get("firstUtmMedium"),
                "campaign": payload.details.get("firstUtmCampaign"),
                "content": payload.details.get("firstUtmContent"),
                "term": payload.details.get("firstUtmTerm"),
                "landing_page": payload.details.get("firstLandingPage"),
                "offering_id": inquiry.offering_id or "",
            },
            captured_at=first_captured_at,
        )
        contact.metadata_json = apply_attribution(
            inquiry=inquiry,
            contact_metadata=dict(contact.metadata_json or {}),
            attribution=attribution,
            first_attribution=first_attribution,
        )
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
            actor=actor,
            after_json={
                "kind": inquiry.kind,
                "source": inquiry.source,
                "offering_id": inquiry.offering_id,
                "attribution": attribution,
            },
        ))

        client = HubSpotClient(request.app.state.settings)
        if client.is_configured:
            sync_building_inquiry_to_hubspot(
                session=session,
                inquiry=inquiry,
                contact=contact,
                client=client,
                actor=actor,
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


@internal_router.post("/inquiries/{inquiry_id}/lifecycle")
def update_inquiry_lifecycle(
    inquiry_id: str,
    payload: InquiryLifecycleInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Record customer-response and qualification evidence without changing CRM sync state."""

    _require_internal_key(request, x_internal_api_key)
    transitions = {
        "new": {"responded", "qualified", "closed_lost"},
        "responded": {"qualified", "closed_lost"},
        "qualified": {"closed_won", "closed_lost"},
        "closed_won": set(),
        "closed_lost": set(),
    }
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        inquiry_payload = dict(inquiry.payload_json or {})
        lifecycle = dict(inquiry_payload.get("_lifecycle") or {})
        current = str(lifecycle.get("stage") or "new")
        if payload.target_stage not in transitions.get(current, set()):
            raise HTTPException(
                status_code=409,
                detail=f"Cannot move inquiry from {current} to {payload.target_stage}.",
            )
        changed_at = _now()
        before = dict(lifecycle)
        lifecycle["stage"] = payload.target_stage
        lifecycle["last_changed_at"] = changed_at.isoformat()
        lifecycle["last_changed_by"] = payload.actor
        lifecycle["last_channel"] = payload.channel
        if payload.notes.strip():
            lifecycle["last_notes"] = payload.notes.strip()
        if payload.target_stage in {"responded", "qualified", "closed_won"}:
            lifecycle.setdefault("first_responded_at", changed_at.isoformat())
            lifecycle["last_responded_at"] = changed_at.isoformat()
            lifecycle["response_count"] = int(lifecycle.get("response_count") or 0) + 1
        if payload.target_stage in {"qualified", "closed_won"}:
            lifecycle.setdefault("qualified_at", changed_at.isoformat())
        if payload.target_stage in {"closed_won", "closed_lost"}:
            lifecycle["closed_at"] = changed_at.isoformat()
        inquiry_payload["_lifecycle"] = lifecycle
        inquiry.payload_json = inquiry_payload
        if payload.assigned_owner.strip():
            inquiry.assigned_owner = payload.assigned_owner.strip()
        inquiry.updated_at = changed_at
        session.add(inquiry)
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="lifecycle_changed",
            actor=payload.actor,
            before_json=before,
            after_json={
                **lifecycle,
                "assigned_owner": inquiry.assigned_owner,
            },
        ))
        return {
            "ok": True,
            "inquiry_id": inquiry.id,
            "crm_sync_status": inquiry.status,
            "lifecycle": lifecycle,
            "assigned_owner": inquiry.assigned_owner,
        }


@internal_router.get("/analytics")
def get_building_analytics(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        return build_building_analytics(session)


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


@internal_router.put("/spaces/{space_id}/media/{media_id}")
def upsert_space_media(
    space_id: str,
    media_id: str,
    payload: SpaceMediaInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != media_id:
        raise HTTPException(status_code=422, detail="Media ID does not match route.")
    try:
        stored = payload.as_storage_dict()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    with session_scope(request.app.state.session_factory) as session:
        space = session.get(BuildingSpace, space_id)
        if space is None:
            raise HTTPException(status_code=404, detail="Space not found.")
        current = [item for item in list(space.media_json or []) if isinstance(item, dict)]
        before = next((dict(item) for item in current if item.get("id") == media_id), {})
        remaining = [item for item in current if item.get("id") != media_id]
        space.media_json = [*remaining, stored]
        space.updated_at = _now()
        session.add(space)
        session.add(BuildingAuditEvent(
            entity_type="space_media",
            entity_id=f"{space_id}:{media_id}",
            action="upserted",
            actor=payload.actor,
            before_json=before,
            after_json=stored,
        ))
        session.flush()
        return {"ok": True, "media": stored, "space": _space_public_payload(space)}


@internal_router.delete("/spaces/{space_id}/media/{media_id}")
def delete_space_media(
    space_id: str,
    media_id: str,
    payload: SpaceMediaDeleteInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        space = session.get(BuildingSpace, space_id)
        if space is None:
            raise HTTPException(status_code=404, detail="Space not found.")
        current = [item for item in list(space.media_json or []) if isinstance(item, dict)]
        before = next((dict(item) for item in current if item.get("id") == media_id), None)
        if before is None:
            raise HTTPException(status_code=404, detail="Media assignment not found.")
        space.media_json = [item for item in current if item.get("id") != media_id]
        space.updated_at = _now()
        session.add(space)
        session.add(BuildingAuditEvent(
            entity_type="space_media",
            entity_id=f"{space_id}:{media_id}",
            action="removed",
            actor=payload.actor,
            before_json=before,
            after_json={"reason": payload.reason},
        ))
        return {"ok": True, "removed": media_id}


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


def _rate_plan_internal_payload(row: BuildingRatePlan) -> dict[str, Any]:
    return {
        **(_rate_plan_public_payload(row) or {}),
        "offering_id": row.offering_id,
        "status": row.status,
        "unit_amount_cents": row.unit_amount_cents,
        "deposit_amount_cents": row.deposit_amount_cents,
        "deposit_percent_bps": row.deposit_percent_bps,
        "approved_by": row.approved_by,
        "approved_at": row.approved_at.isoformat() if row.approved_at else None,
        "created_by": row.created_by,
        "updated_at": row.updated_at.isoformat(),
    }


@internal_router.get("/offerings/{offering_id}/rate-plans")
def list_rate_plans(
    offering_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        if session.get(BuildingOffering, offering_id) is None:
            raise HTTPException(status_code=404, detail="Offering not found.")
        rows = session.execute(
            select(BuildingRatePlan)
            .where(BuildingRatePlan.offering_id == offering_id)
            .order_by(BuildingRatePlan.version.desc())
        ).scalars().all()
        return {"rate_plans": [_rate_plan_internal_payload(row) for row in rows]}


@internal_router.put("/offerings/{offering_id}/rate-plans/{rate_plan_id}")
def upsert_rate_plan(
    offering_id: str,
    rate_plan_id: str,
    payload: RatePlanInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != rate_plan_id:
        raise HTTPException(status_code=422, detail="Rate-plan ID does not match route.")
    with session_scope(request.app.state.session_factory) as session:
        offering = session.get(BuildingOffering, offering_id)
        if offering is None:
            raise HTTPException(status_code=404, detail="Offering not found.")
        row = session.get(BuildingRatePlan, rate_plan_id)
        before = _rate_plan_internal_payload(row) if row else {}
        if row is not None and row.offering_id != offering_id:
            raise HTTPException(status_code=409, detail="Rate plan belongs to another offering.")
        if row is not None and row.status in {"approved", "retired"}:
            if row.status == "approved" and payload.status == "retired":
                row.status = "retired"
                row.updated_at = _now()
                session.add(BuildingAuditEvent(
                    entity_type="rate_plan",
                    entity_id=row.id,
                    action="retired",
                    actor=payload.actor,
                    before_json=before,
                    after_json={"status": "retired"},
                ))
                return {"ok": True, "rate_plan": _rate_plan_internal_payload(row)}
            raise HTTPException(
                status_code=409,
                detail="Approved or retired rate-plan terms are immutable; create a new version.",
            )
        version_conflict = session.execute(
            select(BuildingRatePlan).where(
                BuildingRatePlan.offering_id == offering_id,
                BuildingRatePlan.version == payload.version,
                BuildingRatePlan.id != rate_plan_id,
            )
        ).scalar_one_or_none()
        if version_conflict is not None:
            raise HTTPException(
                status_code=409,
                detail="That rate-plan version already exists for this offering.",
            )
        if payload.status == "approved":
            another_approved = session.execute(
                select(BuildingRatePlan).where(
                    BuildingRatePlan.offering_id == offering_id,
                    BuildingRatePlan.status == "approved",
                    BuildingRatePlan.id != rate_plan_id,
                )
            ).scalar_one_or_none()
            if another_approved is not None:
                raise HTTPException(
                    status_code=409,
                    detail="Retire the current approved rate plan before approving a replacement.",
                )
        if row is None:
            row = BuildingRatePlan(
                id=rate_plan_id,
                offering_id=offering_id,
                version=payload.version,
                name=payload.name,
                effective_from=payload.effective_from,
                created_by=payload.actor,
            )
        for key, value in {
            "version": payload.version,
            "name": payload.name.strip(),
            "status": payload.status,
            "currency": payload.currency,
            "unit_amount_cents": payload.unit_amount_cents,
            "public_price_display": payload.public_price_display.strip(),
            "booking_unit": payload.booking_unit.strip(),
            "minimum_units": payload.minimum_units,
            "deposit_type": payload.deposit_type,
            "deposit_amount_cents": payload.deposit_amount_cents,
            "deposit_percent_bps": payload.deposit_percent_bps,
            "cancellation_policy": payload.cancellation_policy.strip(),
            "included_json": payload.included,
            "addons_json": payload.addons,
            "effective_from": payload.effective_from,
            "effective_until": payload.effective_until,
            "updated_at": _now(),
        }.items():
            setattr(row, key, value)
        if payload.status == "approved":
            row.approved_by = payload.approved_by.strip()
            row.approved_at = _now()
        session.add(row)
        session.flush()
        after = _rate_plan_internal_payload(row)
        session.add(BuildingAuditEvent(
            entity_type="rate_plan",
            entity_id=row.id,
            action="approved" if row.status == "approved" else "draft_saved",
            actor=payload.actor,
            before_json=before,
            after_json=after,
        ))
        return {"ok": True, "rate_plan": after}


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
