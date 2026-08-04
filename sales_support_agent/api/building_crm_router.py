"""Tenant/community CRM, segmentation, preferences, and campaign delivery."""

from __future__ import annotations

import csv
import hashlib
import hmac
import html
import io
import json
import os
from datetime import date, datetime, timezone
from typing import Any, Literal, Optional
from urllib.parse import urlencode
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field, ValidationError, field_validator
from sqlalchemy import delete, select

from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.integrations.building_google_calendar import (
    BuildingGoogleCalendarClient,
)
from sales_support_agent.api.building_router import (
    OfferingInput,
    RatePlanInput,
    SpaceInput,
    SpaceMediaInput,
    _date_ranges_overlap,
)
from sales_support_agent.api.building_booking_router import (
    EVENT_TRANSITIONS,
    WORKSPACE_TRANSITIONS,
)
from sales_support_agent.api.building_service_request_router import (
    TRANSITIONS as SERVICE_REQUEST_TRANSITIONS,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingAgreementTemplate,
    BuildingBillingAccount,
    BuildingBillingSchedule,
    BuildingBillingAdjustment,
    BuildingCampaign,
    BuildingCampaignRecipient,
    BuildingCalendarProjection,
    BuildingCollectionCase,
    BuildingCommunicationPreference,
    BuildingContact,
    BuildingContactMerge,
    BuildingRosterImport,
    BuildingRelationship,
    BuildingSegment,
    BuildingServiceRequest,
    BuildingSuppression,
    BuildingTour,
    BuildingInquiry,
    BuildingInvoice,
    BuildingLaunchDecision,
    BuildingOffering,
    BuildingRatePlan,
    BuildingOperationalChecklist,
    BuildingOperationalChecklistItem,
    BuildingPrivacyRequest,
    BuildingProposal,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_security import (
    csrf_token as building_csrf_token,
    require_building_form_security,
)
from sales_support_agent.services.building_analytics import build_building_analytics
from sales_support_agent.integrations.building_quickbooks import (
    BuildingQuickBooksClient,
)
from sales_support_agent.services.building_sender import building_from_address
from sales_support_agent.services.building_money import (
    cents_to_dollars,
    dollars_to_cents,
    parse_lines,
    suggested_rate_plan_id,
)
from sales_support_agent.services.building_arena_rate_plan_seed import (
    build_arena_commercial_draft,
)
from sales_support_agent.services.building_page import render_building_page
from sales_support_agent.services.building_launch_readiness import (
    ARENA_LAUNCH_DECISIONS,
    arena_rate_plan_decision_blockers,
    launch_decision_id,
    sync_arena_effective_date_decision,
)
from sales_support_agent.services.building_launch_status import (
    build_arena_launch_status,
)


public_router = APIRouter(prefix="/api/public/building", tags=["building-public"])
internal_router = APIRouter(prefix="/api/internal/building/crm", tags=["building-crm"])
admin_router = APIRouter(prefix="/admin/building", tags=["building-admin"])

RELATIONSHIP_TYPES = {
    "prospect",
    "tenant",
    "tenant_employee",
    "event_host",
    "former_tenant",
    "waitlist",
    "vendor",
    "partner",
    "community_member",
}
MARKETING_STATUSES = {"unknown", "subscribed", "unsubscribed"}
CONTACT_STATUSES = {"active", "inactive", "merged"}
CAMPAIGN_COMMUNICATION_CLASSES = {"marketing", "operational"}
OPERATIONAL_RELATIONSHIP_TYPES = {"tenant", "tenant_employee", "event_host"}
REVIEWED_RELATIONSHIP_TYPES = {"tenant_employee", "community_member"}
SEGMENT_TYPES = {
    "custom",
    "current_tenants",
    "former_tenants",
    "workspace_prospects",
    "event_prospects_hosts",
    "manual_approved_list",
}
SEGMENT_PURPOSE_SCOPES = {"marketing", "operational", "both"}
INQUIRY_KINDS = {"workspace", "event", "tour"}
CAMPAIGN_CONTENT_CLASSIFICATIONS = {"standard", "tenant_private"}
MOUNTAIN = ZoneInfo("America/Denver")
ARENA_CATALOG_CONFIRMATION = "PREPARE ARENA CATALOG"
ARENA_SPACE_ID = "arena"
ARENA_OFFERING_ID = "arena-events"

def _building_redirect(*, notice: str = "", error: str = "") -> RedirectResponse:
    query = urlencode({"notice": notice} if notice else {"error": error})
    return RedirectResponse(f"/admin/building?{query}", status_code=303)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _prepare_verified_arena_catalog(session, *, actor: str) -> dict[str, bool]:
    """Create the factual Arena records without publishing commercial claims.

    Existing records are reused only when their identity and relationship are
    compatible. This deliberately fails closed instead of overwriting an
    operator-managed record.
    """

    space = session.get(BuildingSpace, ARENA_SPACE_ID)
    created_space = space is None
    updated_space = False
    if space is not None and (
        space.slug != ARENA_SPACE_ID
        or space.name.strip().casefold() != "the arena"
        or space.space_type != "event"
        or space.capacity not in {0, 200}
    ):
        raise ValueError(
            "The existing arena space conflicts with the verified catalog identity; review it manually."
        )
    if space is None:
        space = BuildingSpace(
            id=ARENA_SPACE_ID,
            slug=ARENA_SPACE_ID,
            name="The Arena",
            space_type="event",
            floor="2nd floor",
            capacity=200,
            status="unavailable",
            public_description=(
                "A 6,000-square-foot event venue for company gatherings, "
                "workshops, celebrations, and community events."
            ),
            internal_notes=(
                "Prepared from the approved Listing Copy Pack baseline. "
                "Maximum public capacity 200. Unavailable and private until "
                "an operator completes launch readiness."
            ),
            features_json=[],
            media_json=[],
            is_public=False,
            updated_at=_now(),
        )
        session.add(space)
        session.flush()
        session.add(
            BuildingAuditEvent(
                entity_type="space",
                entity_id=space.id,
                action="verified_arena_catalog_prepared",
                actor=actor,
                before_json={},
                after_json={
                    "name": space.name,
                    "capacity": space.capacity,
                    "status": space.status,
                    "is_public": space.is_public,
                    "source": "approved_listing_copy_pack",
                },
            )
        )
    elif space.capacity == 0:
        before = {
            "capacity": space.capacity,
            "status": space.status,
            "is_public": space.is_public,
        }
        space.capacity = 200
        space.floor = space.floor or "2nd floor"
        space.internal_notes = (
            "Prepared from the approved Listing Copy Pack baseline. "
            "Maximum public capacity 200. Existing publication and "
            "availability state preserved."
        )
        space.updated_at = _now()
        session.add(space)
        session.add(
            BuildingAuditEvent(
                entity_type="space",
                entity_id=space.id,
                action="verified_arena_catalog_reconciled",
                actor=actor,
                before_json=before,
                after_json={
                    "capacity": space.capacity,
                    "status": space.status,
                    "is_public": space.is_public,
                    "source": "approved_listing_copy_pack",
                },
            )
        )
        updated_space = True

    offering = session.get(BuildingOffering, ARENA_OFFERING_ID)
    created_offering = offering is None
    if offering is not None and (
        offering.slug != ARENA_OFFERING_ID
        or offering.offering_type != "event"
        or offering.space_id != ARENA_SPACE_ID
    ):
        raise ValueError(
            "The existing arena-events offering conflicts with the verified catalog relationship; review it manually."
        )
    if offering is None:
        offering = BuildingOffering(
            id=ARENA_OFFERING_ID,
            slug=ARENA_OFFERING_ID,
            name="The Arena",
            offering_type="event",
            space_id=ARENA_SPACE_ID,
            public_description=(
                "Request a reviewed event date for The Arena at The Anata Building."
            ),
            price_display="",
            booking_unit="hour",
            call_to_action="request_date",
            features_json=[],
            is_published=False,
            updated_at=_now(),
        )
        session.add(offering)
        session.flush()
        session.add(
            BuildingAuditEvent(
                entity_type="offering",
                entity_id=offering.id,
                action="verified_arena_catalog_prepared",
                actor=actor,
                before_json={},
                after_json={
                    "name": offering.name,
                    "space_id": offering.space_id,
                    "is_published": offering.is_published,
                    "price_display": offering.price_display,
                    "source": "approved_listing_copy_pack",
                },
            )
        )

    session.add(
        BuildingAuditEvent(
            entity_type="catalog_preparation",
            entity_id=ARENA_OFFERING_ID,
            action="verified_arena_catalog_preparation_completed",
            actor=actor,
            after_json={
                "created_space": created_space,
                "updated_space": updated_space,
                "created_offering": created_offering,
                "published": False,
                "availability_claimed": False,
                "rate_plan_approved": False,
                "external_write": False,
            },
        )
    )
    return {
        "created_space": created_space,
        "updated_space": updated_space,
        "created_offering": created_offering,
    }


def _mountain(value: datetime) -> datetime:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(MOUNTAIN)


def _follow_up_step_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """Add a staff-readable Mountain Time deadline to a stored sequence step."""

    step = dict(raw)
    due_raw = str(step.get("due_at") or "").strip()
    try:
        due = datetime.fromisoformat(due_raw.replace("Z", "+00:00"))
        step["due_at_display"] = _mountain(due).strftime("%b %d, %Y · %I:%M %p MT")
    except ValueError:
        step["due_at_display"] = "not set"
    return step


def _utc(value: datetime) -> datetime:
    """Normalize a database or API timestamp for safe UTC comparison."""

    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc)


def _local_mountain_datetime(value: str) -> datetime:
    """Interpret an admin datetime-local control in the building's timezone."""

    parsed = datetime.fromisoformat(value)
    aware = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=MOUNTAIN)
    return aware.astimezone(timezone.utc)


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _campaign_secret(request: Request) -> str:
    secret = str(
        getattr(request.app.state.settings, "building_campaign_token_secret", "") or ""
    ).strip()
    if not secret:
        raise HTTPException(
            status_code=503,
            detail="Campaign unsubscribe signing is not configured.",
        )
    return secret


def _unsubscribe_token(secret: str, contact_id: str, email: str) -> str:
    message = f"{contact_id}:{email.strip().lower()}".encode()
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def _normalize_email(value: str) -> str:
    email = str(value or "").strip().lower()
    if "@" not in email or "." not in email.rsplit("@", 1)[-1]:
        raise ValueError("Enter a valid email address.")
    return email


ROSTER_RELATIONSHIP_TYPES = {
    "tenant",
    "tenant_employee",
    "event_host",
    "former_tenant",
    "community_member",
    "vendor",
    "partner",
}
ROSTER_COLUMNS = {
    "email",
    "full_name",
    "phone",
    "company_name",
    "marketing_status",
    "marketing_source",
    "source_reference",
}


def _parse_roster_csv(csv_text: str) -> list[dict[str, str]]:
    """Normalize a small roster CSV and fail closed on ambiguous consent data."""

    if len(csv_text.encode("utf-8")) > 200_000:
        raise ValueError("Roster CSV is larger than 200 KB.")
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff")))
    if not reader.fieldnames:
        raise ValueError("Roster CSV needs a header row.")
    normalized_headers = [str(item or "").strip().lower() for item in reader.fieldnames]
    if "email" not in normalized_headers:
        raise ValueError("Roster CSV must include an email column.")
    unknown = set(normalized_headers) - ROSTER_COLUMNS
    if unknown:
        raise ValueError(
            "Unsupported roster columns: " + ", ".join(sorted(unknown)) + "."
        )
    reader.fieldnames = normalized_headers
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for line_number, raw in enumerate(reader, start=2):
        if not any(str(value or "").strip() for value in raw.values()):
            continue
        try:
            email = _normalize_email(str(raw.get("email") or ""))
        except ValueError as exc:
            raise ValueError(f"Row {line_number}: {exc}") from exc
        if email in seen:
            raise ValueError(f"Row {line_number}: duplicate email {email}.")
        seen.add(email)
        marketing_status = str(raw.get("marketing_status") or "unknown").strip().lower()
        if marketing_status not in MARKETING_STATUSES:
            raise ValueError(
                f"Row {line_number}: marketing_status must be unknown, subscribed, "
                "or unsubscribed."
            )
        marketing_source = str(raw.get("marketing_source") or "").strip()[:64]
        if marketing_status == "subscribed" and not marketing_source:
            raise ValueError(
                f"Row {line_number}: subscribed requires a documented marketing_source."
            )
        row = {
            "email": email,
            "full_name": str(raw.get("full_name") or "").strip()[:255],
            "phone": str(raw.get("phone") or "").strip()[:128],
            "company_name": str(raw.get("company_name") or "").strip()[:255],
            "marketing_status": marketing_status,
            "marketing_source": marketing_source,
            "source_reference": str(raw.get("source_reference") or "").strip()[:255],
        }
        rows.append(row)
        if len(rows) > 500:
            raise ValueError("Roster CSV may contain at most 500 contacts.")
    if not rows:
        raise ValueError("Roster CSV has no contact rows.")
    return rows


def _roster_preview_hash(
    *,
    rows: list[dict[str, str]],
    relationship_type: str,
    organization: str,
    list_owner: str,
    review_due_on: date | None,
) -> str:
    payload = {
        "rows": rows,
        "relationship_type": relationship_type,
        "organization": organization,
        "list_owner": list_owner,
        "review_due_on": review_due_on.isoformat() if review_due_on else None,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class ContactInput(BaseModel):
    email: str = Field(max_length=255)
    full_name: str = Field(default="", max_length=255)
    phone: str = Field(default="", max_length=128)
    company_name: str = Field(default="", max_length=255)
    hubspot_contact_id: str = Field(default="", max_length=64)
    source: str = Field(default="manual", max_length=64)
    status: str = "active"
    metadata: dict[str, Any] = Field(default_factory=dict)
    actor: str = Field(default="", max_length=255)

    @field_validator("email")
    @classmethod
    def valid_email(cls, value: str) -> str:
        return _normalize_email(value)

    @field_validator("status")
    @classmethod
    def valid_status(cls, value: str) -> str:
        if value not in CONTACT_STATUSES:
            raise ValueError("Unsupported contact status.")
        return value


class RelationshipInput(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    billing_account_id: str | None = Field(default=None, max_length=64)
    relationship_type: str
    status: Literal["active", "inactive"] = "active"
    organization: str = Field(default="", max_length=255)
    starts_on: date | None = None
    ends_on: date | None = None
    source_reference: str = Field(default="", max_length=255)
    list_owner: str = Field(default="", max_length=255)
    review_due_on: date | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    actor: str = Field(default="", max_length=255)

    @field_validator("relationship_type")
    @classmethod
    def valid_relationship(cls, value: str) -> str:
        if value not in RELATIONSHIP_TYPES:
            raise ValueError("Unsupported relationship type.")
        return value


class PreferenceInput(BaseModel):
    marketing_status: str
    source: str = Field(default="operator", max_length=64)
    actor: str = Field(default="", max_length=255)

    @field_validator("marketing_status")
    @classmethod
    def valid_marketing_status(cls, value: str) -> str:
        if value not in MARKETING_STATUSES:
            raise ValueError("Unsupported marketing status.")
        return value


class OperationalPreferenceInput(BaseModel):
    transactional_allowed: bool
    source: str = Field(min_length=1, max_length=64)
    evidence_reference: str = Field(min_length=1, max_length=1024)
    actor: str = Field(min_length=1, max_length=255)


class RelationshipReviewInput(BaseModel):
    list_owner: str = Field(min_length=1, max_length=255)
    review_due_on: date
    status: Literal["active", "inactive"] = "active"
    actor: str = Field(min_length=1, max_length=255)


class SegmentInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    description: str = Field(default="", max_length=2000)
    relationship_types: list[str] = Field(default_factory=list)
    relationship_status: Literal["active", "inactive", "any"] = "active"
    marketing_statuses: list[str] = Field(default_factory=lambda: ["subscribed"])
    segment_type: str = "custom"
    purpose_scope: str = "both"
    inquiry_kinds: list[str] = Field(default_factory=list)
    manual_contact_ids: list[str] = Field(default_factory=list, max_length=500)
    approval_evidence: str = Field(default="", max_length=2000)
    is_active: bool = True
    actor: str = Field(default="", max_length=255)

    @field_validator("relationship_types")
    @classmethod
    def valid_relationships(cls, values: list[str]) -> list[str]:
        unknown = set(values) - RELATIONSHIP_TYPES
        if unknown:
            raise ValueError(f"Unsupported relationship types: {', '.join(sorted(unknown))}")
        return sorted(set(values))

    @field_validator("marketing_statuses")
    @classmethod
    def valid_preferences(cls, values: list[str]) -> list[str]:
        unknown = set(values) - MARKETING_STATUSES
        if unknown:
            raise ValueError(f"Unsupported marketing statuses: {', '.join(sorted(unknown))}")
        return sorted(set(values))

    @field_validator("segment_type")
    @classmethod
    def valid_segment_type(cls, value: str) -> str:
        if value not in SEGMENT_TYPES:
            raise ValueError("Unsupported segment type.")
        return value

    @field_validator("purpose_scope")
    @classmethod
    def valid_purpose_scope(cls, value: str) -> str:
        if value not in SEGMENT_PURPOSE_SCOPES:
            raise ValueError("Unsupported segment purpose scope.")
        return value

    @field_validator("inquiry_kinds")
    @classmethod
    def valid_inquiry_kinds(cls, values: list[str]) -> list[str]:
        unknown = set(values) - INQUIRY_KINDS
        if unknown:
            raise ValueError(f"Unsupported inquiry kinds: {', '.join(sorted(unknown))}")
        return sorted(set(values))


class CampaignInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    segment_id: str = Field(min_length=1, max_length=64)
    communication_class: Literal["marketing", "operational"] = "marketing"
    subject: str = Field(min_length=1, max_length=255)
    body_text: str = Field(min_length=1, max_length=20000)
    template_reference: str = Field(default="", max_length=1024)
    content_classification: Literal["standard", "tenant_private"] = "standard"
    private_content_approval_evidence: str = Field(default="", max_length=2000)
    actor: str = Field(default="", max_length=255)


class ApprovalInput(BaseModel):
    preview_hash: str = Field(min_length=64, max_length=128)
    confirmation: str = Field(default="", max_length=255)
    actor: str = Field(min_length=1, max_length=255)


class CampaignReviewInput(BaseModel):
    preview_hash: str = Field(min_length=64, max_length=128)
    confirmation: str = Field(min_length=1, max_length=255)
    actor: str = Field(min_length=1, max_length=255)


class SendInput(BaseModel):
    actor: str = Field(min_length=1, max_length=255)


class ScheduleInput(BaseModel):
    scheduled_at: datetime
    actor: str = Field(min_length=1, max_length=255)


class ScheduledRunInput(BaseModel):
    dry_run: bool = False
    max_campaigns: int = Field(default=10, ge=1, le=25)
    actor: str = Field(
        default="job:building-campaign-scheduler",
        min_length=1,
        max_length=255,
    )


class TestSendInput(BaseModel):
    email: str = Field(max_length=255)
    actor: str = Field(min_length=1, max_length=255)

    @field_validator("email")
    @classmethod
    def valid_email(cls, value: str) -> str:
        return _normalize_email(value)


class ContactMergeInput(BaseModel):
    survivor_contact_id: str = Field(min_length=1, max_length=64)
    merged_contact_id: str = Field(min_length=1, max_length=64)
    preview_hash: str = Field(min_length=64, max_length=64)
    confirmation: str = Field(min_length=1, max_length=255)
    reason: str = Field(min_length=10, max_length=2000)
    actor: str = Field(min_length=1, max_length=255)


def _contact_payload(
    contact: BuildingContact,
    relationships: list[BuildingRelationship],
    preference: BuildingCommunicationPreference | None,
    suppressed: bool,
) -> dict[str, Any]:
    return {
        "id": contact.id,
        "email": contact.email,
        "full_name": contact.full_name,
        "phone": contact.phone,
        "company_name": contact.company_name,
        "hubspot_contact_id": contact.hubspot_contact_id,
        "source": contact.source,
        "status": contact.status,
        "relationships": [
            {
                "id": item.id,
                "billing_account_id": item.billing_account_id,
                "type": item.relationship_type,
                "status": item.status,
                "organization": item.organization,
                "starts_on": item.starts_on.isoformat() if item.starts_on else None,
                "ends_on": item.ends_on.isoformat() if item.ends_on else None,
                "source_reference": item.source_reference,
                "list_owner": str((item.metadata_json or {}).get("list_owner") or ""),
                "review_due_on": (item.metadata_json or {}).get("review_due_on"),
                "reviewed_at": (item.metadata_json or {}).get("reviewed_at"),
                "reviewed_by": (item.metadata_json or {}).get("reviewed_by"),
            }
            for item in relationships
        ],
        "marketing_status": preference.marketing_status if preference else "unknown",
        "marketing_source": preference.marketing_source if preference else "",
        "marketing_changed_at": (
            preference.marketing_changed_at.isoformat() if preference else None
        ),
        "operational_allowed": (
            bool(
                preference
                and preference.transactional_allowed
                and str(preference.operational_source or "").strip()
                and str(preference.operational_evidence_reference or "").strip()
            )
        ),
        "operational_source": preference.operational_source if preference else "",
        "operational_evidence_reference": (
            preference.operational_evidence_reference if preference else ""
        ),
        "operational_changed_at": (
            preference.operational_changed_at.isoformat() if preference else None
        ),
        "suppressed": suppressed,
        "updated_at": contact.updated_at.isoformat(),
    }


def _validate_campaign_segment(
    segment: BuildingSegment,
    communication_class: str,
) -> None:
    if communication_class not in CAMPAIGN_COMMUNICATION_CLASSES:
        raise HTTPException(status_code=422, detail="Unsupported communication class.")
    purpose_scope = str(segment.purpose_scope or "marketing")
    if purpose_scope not in {communication_class, "both"}:
        raise HTTPException(
            status_code=422,
            detail=(
                f"This audience is approved for {purpose_scope} messages, "
                f"not {communication_class} messages."
            ),
        )
    if communication_class != "operational":
        return
    rules = segment.rules_json or {}
    relationship_types = set(rules.get("relationship_types") or [])
    if (
        not relationship_types
        or not relationship_types.issubset(OPERATIONAL_RELATIONSHIP_TYPES)
        or str(rules.get("relationship_status") or "active") != "active"
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                "Operational notices require an active audience limited to tenants, "
                "tenant employees, or event hosts."
            ),
        )


def _validate_private_campaign(
    campaign: BuildingCampaign,
    segment: BuildingSegment,
) -> None:
    """Keep private tenant benefits out of marketing and public defaults."""

    if campaign.content_classification != "tenant_private":
        return
    rules = dict(segment.rules_json or {})
    relationship_types = set(rules.get("relationship_types") or [])
    if (
        campaign.communication_class != "operational"
        or not relationship_types
        or not relationship_types.issubset({"tenant", "tenant_employee"})
        or str(rules.get("relationship_status") or "active") != "active"
        or not campaign.private_content_approval_evidence.strip()
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                "Private tenant content requires explicit approval evidence and "
                "an active tenant-only operational audience."
            ),
        )


def _campaign_content_checksum(campaign: BuildingCampaign) -> str:
    canonical = {
        "campaign_id": campaign.id,
        "version": campaign.content_version,
        "communication_class": campaign.communication_class,
        "subject": campaign.subject,
        "body_text": campaign.body_text,
        "template_reference": campaign.template_reference,
        "content_classification": campaign.content_classification,
        "private_content_approval_evidence": (
            campaign.private_content_approval_evidence
            if campaign.content_classification == "tenant_private"
            else ""
        ),
    }
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _relationship_review_is_current(relationship: BuildingRelationship) -> bool:
    if relationship.relationship_type not in REVIEWED_RELATIONSHIP_TYPES:
        return True
    metadata = relationship.metadata_json or {}
    owner = str(metadata.get("list_owner") or "").strip()
    due_value = str(metadata.get("review_due_on") or "").strip()
    try:
        due_on = date.fromisoformat(due_value)
    except ValueError:
        return False
    return bool(owner) and due_on >= datetime.now(MOUNTAIN).date()


def _resolve_segment(
    session,
    segment: BuildingSegment,
    *,
    communication_class: str = "marketing",
) -> list[dict[str, Any]]:
    _validate_campaign_segment(segment, communication_class)
    rules = segment.rules_json or {}
    wanted_types = set(rules.get("relationship_types") or [])
    wanted_relationship_status = str(rules.get("relationship_status") or "active")
    wanted_marketing = set(rules.get("marketing_statuses") or ["subscribed"])
    wanted_inquiry_kinds = set(rules.get("inquiry_kinds") or [])
    manual_contact_ids = set(rules.get("manual_contact_ids") or [])

    contacts = session.execute(
        select(BuildingContact)
        .where(BuildingContact.status == "active")
        .order_by(BuildingContact.email)
    ).scalars().all()
    contact_ids = [item.id for item in contacts]
    relationships: dict[str, list[BuildingRelationship]] = {}
    preferences: dict[str, BuildingCommunicationPreference] = {}
    if contact_ids:
        for item in session.execute(
            select(BuildingRelationship).where(BuildingRelationship.contact_id.in_(contact_ids))
        ).scalars().all():
            relationships.setdefault(item.contact_id, []).append(item)
        preferences = {
            item.contact_id: item
            for item in session.execute(
                select(BuildingCommunicationPreference).where(
                    BuildingCommunicationPreference.contact_id.in_(contact_ids)
                )
            ).scalars().all()
        }
    suppression_scopes = {
        item.email: item.scope
        for item in session.execute(select(BuildingSuppression)).scalars().all()
    }

    resolved: list[dict[str, Any]] = []
    for contact in contacts:
        contact_relationships = relationships.get(contact.id, [])
        matching_relationships = [
            item
            for item in contact_relationships
            if (not wanted_types or item.relationship_type in wanted_types)
            and (
                wanted_relationship_status == "any"
                or item.status == wanted_relationship_status
            )
        ]
        if wanted_inquiry_kinds:
            matching_relationships = [
                item
                for item in matching_relationships
                if (
                    item.relationship_type == "event_host"
                    and "event" in wanted_inquiry_kinds
                )
                or str((item.metadata_json or {}).get("inquiry_kind") or "")
                in wanted_inquiry_kinds
            ]
        if segment.segment_type == "manual_approved_list":
            matching_relationships = (
                contact_relationships if contact.id in manual_contact_ids else []
            )
        eligible_relationships = [
            item
            for item in matching_relationships
            if _relationship_review_is_current(item)
        ]
        preference = preferences.get(contact.id)
        marketing_status = preference.marketing_status if preference else "unknown"
        reasons: list[str] = []
        exclusions: list[str] = []
        if matching_relationships and not eligible_relationships:
            exclusions.append("relationship review is overdue or missing an owner")
        elif wanted_types and not eligible_relationships:
            exclusions.append("relationship does not match")
        elif segment.segment_type == "manual_approved_list" and contact.id not in manual_contact_ids:
            exclusions.append("contact is not on the approved manual list")
        elif eligible_relationships:
            reasons.append(
                ", ".join(sorted({item.relationship_type for item in eligible_relationships}))
            )
        if segment.segment_type == "manual_approved_list" and contact.id in manual_contact_ids:
            reasons.append("explicitly approved manual list")
        if communication_class == "marketing":
            if marketing_status not in wanted_marketing:
                exclusions.append(f"marketing status is {marketing_status}")
            else:
                reasons.append(f"marketing status is {marketing_status}")
            if suppression_scopes.get(contact.email) in {"marketing", "all"}:
                exclusions.append("email is suppressed for marketing")
        else:
            if preference is None:
                exclusions.append("operational contact authority is not documented")
            elif not preference.transactional_allowed:
                exclusions.append("required operational email is disabled")
            elif not (
                str(preference.operational_source or "").strip()
                and str(preference.operational_evidence_reference or "").strip()
            ):
                exclusions.append("operational contact authority evidence is incomplete")
            else:
                reasons.append("required operational email authority is documented")
            if suppression_scopes.get(contact.email) == "all":
                exclusions.append("all email is suppressed")
        resolved.append(
            {
                "contact": contact,
                "included": not exclusions,
                "inclusion_reason": "; ".join(reasons),
                "exclusion_reason": "; ".join(exclusions),
            }
        )
    return resolved


def _merge_preview(session, survivor_id: str, merged_id: str) -> dict[str, Any]:
    if survivor_id == merged_id:
        raise HTTPException(status_code=422, detail="Choose two different contacts.")
    survivor = session.get(BuildingContact, survivor_id)
    merged = session.get(BuildingContact, merged_id)
    if survivor is None or merged is None:
        raise HTTPException(status_code=404, detail="Contact not found.")
    if survivor.status == "merged" or merged.status == "merged":
        raise HTTPException(status_code=409, detail="Merged contacts cannot be merged again.")
    relationship_rows = session.execute(
        select(BuildingRelationship).where(BuildingRelationship.contact_id == merged.id)
    ).scalars().all()
    survivor_relationship_keys = {
        (row.relationship_type, row.source_reference)
        for row in session.execute(
            select(BuildingRelationship).where(
                BuildingRelationship.contact_id == survivor.id
            )
        ).scalars().all()
    }
    reservations = session.execute(
        select(BuildingReservation).where(BuildingReservation.contact_id == merged.id)
    ).scalars().all()
    billing_accounts = session.execute(
        select(BuildingBillingAccount).where(BuildingBillingAccount.contact_id == merged.id)
    ).scalars().all()
    service_requests = session.execute(
        select(BuildingServiceRequest).where(BuildingServiceRequest.contact_id == merged.id)
    ).scalars().all()
    privacy_requests = session.execute(
        select(BuildingPrivacyRequest).where(BuildingPrivacyRequest.contact_id == merged.id)
    ).scalars().all()
    campaign_snapshots = session.execute(
        select(BuildingCampaignRecipient).where(
            BuildingCampaignRecipient.contact_id == merged.id
        )
    ).scalars().all()
    inquiries = session.execute(
        select(BuildingInquiry).where(BuildingInquiry.email == merged.email)
    ).scalars().all()
    counts = {
        "relationships_to_move": sum(
            1 for row in relationship_rows
            if (row.relationship_type, row.source_reference) not in survivor_relationship_keys
        ),
        "duplicate_relationships_preserved": sum(
            1 for row in relationship_rows
            if (row.relationship_type, row.source_reference) in survivor_relationship_keys
        ),
        "reservations_to_move": len(reservations),
        "billing_accounts_to_move": len(billing_accounts),
        "service_requests_to_move": len(service_requests),
        "privacy_requests_to_move": len(privacy_requests),
        "campaign_snapshots_preserved": len(campaign_snapshots),
        "inquiries_preserved_by_original_email": len(inquiries),
    }
    conflicts = []
    if survivor.hubspot_contact_id and merged.hubspot_contact_id and (
        survivor.hubspot_contact_id != merged.hubspot_contact_id
    ):
        conflicts.append("Both contacts have different HubSpot contact IDs; the survivor ID wins and the other is preserved in merge evidence.")
    survivor_pref = session.get(BuildingCommunicationPreference, survivor.id)
    merged_pref = session.get(BuildingCommunicationPreference, merged.id)
    statuses = {
        pref.marketing_status
        for pref in (survivor_pref, merged_pref)
        if pref is not None
    }
    marketing_status = (
        "unsubscribed" if "unsubscribed" in statuses
        else "subscribed" if statuses == {"subscribed"}
        else "unknown"
    )
    transactional_allowed = all(
        pref.transactional_allowed
        for pref in (survivor_pref, merged_pref)
        if pref is not None
    )
    seed = {
        "survivor_id": survivor.id,
        "survivor_updated_at": str(survivor.updated_at),
        "merged_id": merged.id,
        "merged_updated_at": str(merged.updated_at),
        "counts": counts,
        "reference_ids": {
            "relationships": sorted(row.id for row in relationship_rows),
            "reservations": sorted(row.id for row in reservations),
            "billing_accounts": sorted(row.id for row in billing_accounts),
            "service_requests": sorted(row.id for row in service_requests),
            "privacy_requests": sorted(row.id for row in privacy_requests),
            "campaign_snapshots": sorted(row.id for row in campaign_snapshots),
            "inquiries": sorted(row.id for row in inquiries),
        },
        "marketing_status": marketing_status,
        "transactional_allowed": transactional_allowed,
    }
    preview_hash = hashlib.sha256(
        json.dumps(seed, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "preview_hash": preview_hash,
        "survivor": {
            "id": survivor.id, "email": survivor.email,
            "full_name": survivor.full_name, "hubspot_contact_id": survivor.hubspot_contact_id,
        },
        "merged": {
            "id": merged.id, "email": merged.email,
            "full_name": merged.full_name, "hubspot_contact_id": merged.hubspot_contact_id,
        },
        "counts": counts,
        "conflicts": conflicts,
        "consent_result": {
            "marketing_status": marketing_status,
            "transactional_allowed": transactional_allowed,
            "rule": "most restrictive permission wins",
        },
        "_rows": {
            "relationships": relationship_rows,
            "survivor_relationship_keys": survivor_relationship_keys,
        },
    }


def _execute_merge(session, payload: ContactMergeInput) -> dict[str, Any]:
    preview = _merge_preview(
        session, payload.survivor_contact_id, payload.merged_contact_id
    )
    if not hmac.compare_digest(preview["preview_hash"], payload.preview_hash):
        raise HTTPException(
            status_code=409,
            detail="Contact data changed after preview. Review the merge again.",
        )
    expected = f"MERGE {payload.merged_contact_id} INTO {payload.survivor_contact_id}"
    if payload.confirmation.strip() != expected:
        raise HTTPException(status_code=422, detail=f"Type {expected} to confirm.")
    survivor = session.get(BuildingContact, payload.survivor_contact_id)
    merged = session.get(BuildingContact, payload.merged_contact_id)
    moved_counts = dict(preview["counts"])
    relationship_keys = preview["_rows"]["survivor_relationship_keys"]
    for row in preview["_rows"]["relationships"]:
        if (row.relationship_type, row.source_reference) not in relationship_keys:
            row.contact_id = survivor.id
            session.add(row)
    for model in (
        BuildingReservation,
        BuildingBillingAccount,
        BuildingServiceRequest,
        BuildingPrivacyRequest,
    ):
        for row in session.execute(
            select(model).where(model.contact_id == merged.id)
        ).scalars().all():
            row.contact_id = survivor.id
            session.add(row)
    survivor_pref = session.get(BuildingCommunicationPreference, survivor.id)
    merged_pref = session.get(BuildingCommunicationPreference, merged.id)
    if survivor_pref is None:
        survivor_pref = BuildingCommunicationPreference(contact_id=survivor.id)
    survivor_pref.marketing_status = preview["consent_result"]["marketing_status"]
    survivor_pref.marketing_source = "contact_merge"
    survivor_pref.marketing_changed_at = _now()
    survivor_pref.transactional_allowed = preview["consent_result"]["transactional_allowed"]
    survivor_pref.updated_by = payload.actor
    survivor_pref.updated_at = _now()
    session.add(survivor_pref)
    source_suppression = session.get(BuildingSuppression, merged.email)
    survivor_suppression = session.get(BuildingSuppression, survivor.email)
    if source_suppression:
        if survivor_suppression is None:
            survivor_suppression = BuildingSuppression(email=survivor.email)
        survivor_suppression.scope = (
            "all"
            if "all" in {source_suppression.scope, survivor_suppression.scope}
            else "marketing"
        )
        survivor_suppression.reason = "merged_contact_suppression"
        survivor_suppression.source = "contact_merge"
        session.add(survivor_suppression)
    before_survivor = {
        "full_name": survivor.full_name,
        "phone": survivor.phone,
        "company_name": survivor.company_name,
        "hubspot_contact_id": survivor.hubspot_contact_id,
    }
    survivor.full_name = survivor.full_name or merged.full_name
    survivor.phone = survivor.phone or merged.phone
    survivor.company_name = survivor.company_name or merged.company_name
    survivor.hubspot_contact_id = survivor.hubspot_contact_id or merged.hubspot_contact_id
    survivor_metadata = dict(survivor.metadata_json or {})
    survivor_metadata.setdefault("_merged_contact_ids", []).append(merged.id)
    if merged.hubspot_contact_id and merged.hubspot_contact_id != survivor.hubspot_contact_id:
        survivor_metadata.setdefault("_merged_hubspot_contact_ids", []).append(
            merged.hubspot_contact_id
        )
    survivor.metadata_json = survivor_metadata
    survivor.updated_at = _now()
    merged_metadata = dict(merged.metadata_json or {})
    merged_metadata["_merged_into_contact_id"] = survivor.id
    merged.metadata_json = merged_metadata
    merged.status = "merged"
    merged.updated_at = _now()
    session.add(survivor)
    session.add(merged)
    merge = BuildingContactMerge(
        id=f"merge-{uuid4().hex}",
        survivor_contact_id=survivor.id,
        merged_contact_id=merged.id,
        preview_hash=payload.preview_hash,
        reason=payload.reason.strip(),
        moved_counts_json=moved_counts,
        preserved_history_json={
            "campaign_snapshots": moved_counts["campaign_snapshots_preserved"],
            "inquiries_by_original_email": moved_counts["inquiries_preserved_by_original_email"],
            "source_contact_retained_as": "merged",
        },
        consent_result_json=preview["consent_result"],
        actor=payload.actor,
    )
    session.add(merge)
    session.add(BuildingAuditEvent(
        entity_type="contact_merge",
        entity_id=merge.id,
        action="contacts_merged",
        actor=payload.actor,
        before_json={
            "survivor": before_survivor,
            "merged_contact_id": merged.id,
            "merged_email": merged.email,
        },
        after_json={
            "survivor_contact_id": survivor.id,
            "moved_counts": moved_counts,
            "consent_result": preview["consent_result"],
            "reason": payload.reason.strip(),
        },
    ))
    return {
        "merge_id": merge.id,
        "survivor_contact_id": survivor.id,
        "merged_contact_id": merged.id,
        "moved_counts": moved_counts,
        "consent_result": preview["consent_result"],
    }


@internal_router.post("/contacts/merge/preview")
def preview_contact_merge(
    survivor_contact_id: str,
    merged_contact_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        preview = _merge_preview(session, survivor_contact_id, merged_contact_id)
        preview.pop("_rows", None)
        return preview


@internal_router.post("/contacts/merge")
def merge_contacts(
    payload: ContactMergeInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        return {"ok": True, **_execute_merge(session, payload)}


@admin_router.post(
    "/contacts/merge/preview",
    dependencies=[Depends(require_building_form_security)],
    response_class=HTMLResponse,
)
def preview_contact_merge_from_control_room(
    request: Request,
    survivor_contact_id: str = Form(...),
    merged_contact_id: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
):
    try:
        with session_scope(request.app.state.session_factory) as session:
            preview = _merge_preview(
                session, survivor_contact_id.strip(), merged_contact_id.strip()
            )
    except HTTPException as exc:
        return _building_redirect(error=str(exc.detail))
    survivor = preview["survivor"]
    merged = preview["merged"]
    counts = preview["counts"]
    conflicts = preview["conflicts"]
    expected = f"MERGE {merged['id']} INTO {survivor['id']}"
    conflict_html = "".join(
        f"<li>{html.escape(str(item))}</li>" for item in conflicts
    ) or "<li>No provider-ID conflicts detected.</li>"
    count_html = "".join(
        f"<li>{html.escape(key.replace('_', ' ').title())}: <strong>{int(value)}</strong></li>"
        for key, value in counts.items()
    )
    body = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Review contact merge · Anata Agent</title>
<style>
body{{font-family:Inter,Arial,sans-serif;background:#f5f7f8;color:#17222b;margin:0;padding:32px}}
main{{max-width:820px;margin:auto;background:white;border:1px solid #d9e0e4;border-radius:18px;padding:28px}}
h1,h2{{font-family:Montserrat,Arial,sans-serif}} .pair{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
.card{{border:1px solid #d9e0e4;border-radius:12px;padding:16px}} label{{display:block;font-weight:700;margin:16px 0 6px}}
input,textarea{{box-sizing:border-box;width:100%;padding:12px;border:1px solid #87949d;border-radius:8px}}
button,a{{display:inline-block;margin-top:18px;padding:12px 16px;border-radius:8px}}button{{background:#17222b;color:white;border:0;font-weight:700}}
.warning{{background:#fff5d9;border:1px solid #e0bd5b;border-radius:10px;padding:14px}}:focus-visible{{outline:3px solid #168dcc;outline-offset:2px}}
</style></head><body><main>
<a href="/admin/building">← Back to Building Control</a>
<h1>Review contact merge</h1>
<p class="warning"><strong>This changes operational references.</strong> Historical campaign recipients and inquiries keep their original contact/email evidence. The duplicate contact remains as a merged record.</p>
<div class="pair"><section class="card"><h2>Survivor</h2><p><strong>{html.escape(survivor['full_name'] or survivor['email'])}</strong><br>{html.escape(survivor['email'])}<br>ID: {html.escape(survivor['id'])}</p></section>
<section class="card"><h2>Duplicate</h2><p><strong>{html.escape(merged['full_name'] or merged['email'])}</strong><br>{html.escape(merged['email'])}<br>ID: {html.escape(merged['id'])}</p></section></div>
<h2>What will happen</h2><ul>{count_html}</ul>
<h2>Conflicts and permission result</h2><ul>{conflict_html}</ul>
<p>Marketing: <strong>{html.escape(preview['consent_result']['marketing_status'])}</strong><br>
Transactional allowed: <strong>{'yes' if preview['consent_result']['transactional_allowed'] else 'no'}</strong><br>
Rule: most restrictive permission wins.</p>
<form method="post" action="/admin/building/contacts/merge">
<input type="hidden" name="_csrf_token" value="{html.escape(building_csrf_token(user))}">
<input type="hidden" name="survivor_contact_id" value="{html.escape(survivor['id'])}">
<input type="hidden" name="merged_contact_id" value="{html.escape(merged['id'])}">
<input type="hidden" name="preview_hash" value="{html.escape(preview['preview_hash'])}">
<label for="merge-reason">Why are these the same person?</label><textarea id="merge-reason" name="reason" minlength="10" required></textarea>
<label for="merge-confirmation">Type <code>{html.escape(expected)}</code></label><input id="merge-confirmation" name="confirmation" required autocomplete="off">
<button type="submit">Merge duplicate into survivor</button>
</form></main></body></html>"""
    return HTMLResponse(body)


@admin_router.post(
    "/contacts/merge",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def merge_contacts_from_control_room(
    request: Request,
    survivor_contact_id: str = Form(...),
    merged_contact_id: str = Form(...),
    preview_hash: str = Form(...),
    confirmation: str = Form(...),
    reason: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        payload = ContactMergeInput(
            survivor_contact_id=survivor_contact_id,
            merged_contact_id=merged_contact_id,
            preview_hash=preview_hash,
            confirmation=confirmation,
            reason=reason,
            actor=str(user.get("email") or "building-operator"),
        )
        with session_scope(request.app.state.session_factory) as session:
            result = _execute_merge(session, payload)
        return _building_redirect(
            notice=(
                f"Duplicate contact merged into {result['survivor_contact_id']}. "
                "Historical campaign and inquiry evidence was preserved."
            )
        )
    except (ValidationError, HTTPException) as exc:
        detail = (
            exc.errors()[0].get("msg", "Invalid merge.")
            if isinstance(exc, ValidationError)
            else exc.detail
        )
        return _building_redirect(error=str(detail))


def _preview_payload(
    session,
    campaign: BuildingCampaign,
    *,
    sender_identity: str,
) -> dict[str, Any]:
    segment = session.get(BuildingSegment, campaign.segment_id)
    if segment is None or not segment.is_active:
        raise HTTPException(status_code=422, detail="Campaign segment is unavailable.")
    _validate_private_campaign(campaign, segment)
    expected_content_checksum = _campaign_content_checksum(campaign)
    if campaign.content_checksum and campaign.content_checksum != expected_content_checksum:
        raise HTTPException(
            status_code=409,
            detail="Campaign content checksum changed; save a new draft version.",
        )
    resolved = _resolve_segment(
        session,
        segment,
        communication_class=campaign.communication_class,
    )
    included = [
        {
            "contact_id": item["contact"].id,
            "email": item["contact"].email,
            "full_name": item["contact"].full_name,
            "reason": item["inclusion_reason"],
        }
        for item in resolved
        if item["included"]
    ]
    excluded = [
        {
            "contact_id": item["contact"].id,
            "email": item["contact"].email,
            "reason": item["exclusion_reason"],
        }
        for item in resolved
        if not item["included"]
    ]
    canonical = json.dumps(
        {
            "campaign_id": campaign.id,
            "segment_id": campaign.segment_id,
            "communication_class": campaign.communication_class,
            "content_version": campaign.content_version,
            "content_checksum": expected_content_checksum,
            "template_reference": campaign.template_reference,
            "content_classification": campaign.content_classification,
            "sender_identity": sender_identity,
            "subject": campaign.subject,
            "body_text": campaign.body_text,
            "recipients": included,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "campaign_id": campaign.id,
        "sender_identity": sender_identity,
        "communication_class": campaign.communication_class,
        "content_version": campaign.content_version,
        "content_checksum": expected_content_checksum,
        "template_reference": campaign.template_reference,
        "content_classification": campaign.content_classification,
        "private_content_approved": bool(
            campaign.content_classification == "tenant_private"
            and campaign.private_content_approval_evidence
        ),
        "permission_rule": (
            "Subscribed marketing contacts only; marketing and all-email suppressions apply."
            if campaign.communication_class == "marketing"
            else (
                "Active tenants, tenant employees, or event hosts with operational email "
                "allowed; marketing opt-out does not apply, but all-email suppression does."
            )
        ),
        "unsubscribe_behavior": (
            "Includes the marketing unsubscribe link."
            if campaign.communication_class == "marketing"
            else "Does not include a marketing unsubscribe link."
        ),
        "included": included,
        "excluded": excluded,
        "included_count": len(included),
        "excluded_count": len(excluded),
        "preview_hash": hashlib.sha256(canonical.encode()).hexdigest(),
    }


def _current_campaign_eligibility(
    session,
    campaign: BuildingCampaign,
) -> dict[str, dict[str, Any]]:
    segment = session.get(BuildingSegment, campaign.segment_id)
    if segment is None or not segment.is_active:
        return {}
    return {
        item["contact"].id: item
        for item in _resolve_segment(
            session,
            segment,
            communication_class=campaign.communication_class,
        )
        if item["included"]
    }


def _campaign_delivery_text(
    request: Request,
    campaign: BuildingCampaign,
    recipient: BuildingCampaignRecipient,
) -> str:
    if campaign.communication_class == "operational":
        return (
            f"{campaign.body_text.rstrip()}\n\n"
            "This required operational notice is being sent because of your active "
            "relationship with The Anata Building."
        )
    secret = _campaign_secret(request)
    token = _unsubscribe_token(secret, recipient.contact_id, recipient.email)
    unsubscribe_url = (
        f"{str(request.base_url).rstrip('/')}/api/public/building/unsubscribe?"
        + urlencode({"contact_id": recipient.contact_id, "token": token})
    )
    return (
        f"{campaign.body_text.rstrip()}\n\n"
        f"Stop receiving optional Anata Building news: {unsubscribe_url}"
    )


def _approved_campaign_sender(campaign: BuildingCampaign) -> str:
    """Return the sender frozen with approval, failing closed for legacy rows."""

    sender = str(campaign.sender_identity or "").strip()
    if not sender:
        raise HTTPException(
            status_code=409,
            detail=(
                "Campaign has no approved sender snapshot. Create and approve a "
                "replacement campaign before delivery."
            ),
        )
    return sender


def _deliver_campaign_recipients(
    session,
    request: Request,
    campaign: BuildingCampaign,
    client: ResendClient,
    *,
    eligible_statuses: set[str],
) -> dict[str, int]:
    recipients = session.execute(
        select(BuildingCampaignRecipient)
        .where(BuildingCampaignRecipient.campaign_id == campaign.id)
        .order_by(BuildingCampaignRecipient.id)
    ).scalars().all()
    current_eligibility = _current_campaign_eligibility(session, campaign)
    counts = {"sent": 0, "suppressed": 0, "failed": 0}
    for recipient in recipients:
        if recipient.status not in eligible_statuses:
            continue
        if current_eligibility.get(recipient.contact_id) is None:
            recipient.status = "suppressed"
            recipient.exclusion_reason = (
                "Contact no longer meets the current audience and permission rules."
            )
            counts["suppressed"] += 1
            continue
        try:
            provider_message_id = client.send_message(
                to=recipient.email,
                subject=campaign.subject,
                text=_campaign_delivery_text(request, campaign, recipient),
                idempotency_key=f"building-campaign/{campaign.id}/{recipient.id}",
                from_address=_approved_campaign_sender(campaign),
            )
            recipient.status = "sent"
            recipient.provider_message_id = (
                provider_message_id
                if isinstance(provider_message_id, str) and provider_message_id
                else "resend"
            )
            recipient.sent_at = _now()
            recipient.exclusion_reason = ""
            counts["sent"] += 1
        except Exception as exc:  # noqa: BLE001 - preserve retry evidence
            recipient.status = "failed"
            recipient.exclusion_reason = str(exc)[:500]
            counts["failed"] += 1
    return counts


@internal_router.put("/contacts/{contact_id}")
def upsert_contact(
    contact_id: str,
    payload: ContactInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        duplicate = session.execute(
            select(BuildingContact).where(
                BuildingContact.email == payload.email,
                BuildingContact.id != contact_id,
            )
        ).scalar_one_or_none()
        if duplicate:
            raise HTTPException(status_code=409, detail="A contact with this email already exists.")
        row = session.get(BuildingContact, contact_id)
        before = {"email": row.email, "status": row.status} if row else {}
        if row is None:
            row = BuildingContact(id=contact_id, email=payload.email)
        for key, value in {
            "email": payload.email,
            "full_name": payload.full_name.strip(),
            "phone": payload.phone.strip(),
            "company_name": payload.company_name.strip(),
            "hubspot_contact_id": payload.hubspot_contact_id.strip(),
            "source": payload.source.strip() or "manual",
            "status": payload.status,
            "metadata_json": payload.metadata,
            "updated_at": _now(),
        }.items():
            setattr(row, key, value)
        session.add(row)
        session.flush()
        session.add(BuildingAuditEvent(
            entity_type="contact",
            entity_id=row.id,
            action="upserted",
            actor=payload.actor or "internal-api",
            before_json=before,
            after_json={"email": row.email, "status": row.status},
        ))
        relationships = session.execute(
            select(BuildingRelationship).where(BuildingRelationship.contact_id == row.id)
        ).scalars().all()
        preference = session.get(BuildingCommunicationPreference, row.id)
        suppressed = session.get(BuildingSuppression, row.email) is not None
        return {"ok": True, "contact": _contact_payload(row, relationships, preference, suppressed)}


@internal_router.get("/contacts/{contact_id}")
def get_contact(
    contact_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingContact, contact_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        relationships = session.execute(
            select(BuildingRelationship).where(BuildingRelationship.contact_id == row.id)
        ).scalars().all()
        preference = session.get(BuildingCommunicationPreference, row.id)
        return {
            "contact": _contact_payload(
                row,
                relationships,
                preference,
                session.get(BuildingSuppression, row.email) is not None,
            )
        }


@internal_router.post("/contacts/{contact_id}/relationships", status_code=201)
def add_relationship(
    contact_id: str,
    payload: RelationshipInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.ends_on and payload.starts_on and payload.ends_on < payload.starts_on:
        raise HTTPException(status_code=422, detail="Relationship end precedes start.")
    if (
        payload.status == "active"
        and payload.relationship_type in REVIEWED_RELATIONSHIP_TYPES
        and (not payload.list_owner.strip() or payload.review_due_on is None)
    ):
        raise HTTPException(
            status_code=422,
            detail="Active employee/community relationships require an owner and review date.",
        )
    with session_scope(request.app.state.session_factory) as session:
        if session.get(BuildingContact, contact_id) is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        if (
            payload.billing_account_id
            and session.get(BuildingBillingAccount, payload.billing_account_id) is None
        ):
            raise HTTPException(status_code=422, detail="Billing account not found.")
        metadata = dict(payload.metadata)
        if payload.relationship_type in REVIEWED_RELATIONSHIP_TYPES:
            metadata.update({
                "list_owner": payload.list_owner.strip(),
                "review_due_on": (
                    payload.review_due_on.isoformat() if payload.review_due_on else ""
                ),
                "reviewed_at": _now().isoformat(),
                "reviewed_by": payload.actor or "internal-api",
            })
        row = BuildingRelationship(
            id=payload.id or str(uuid4()),
            contact_id=contact_id,
            billing_account_id=payload.billing_account_id,
            relationship_type=payload.relationship_type,
            status=payload.status,
            organization=payload.organization,
            starts_on=payload.starts_on,
            ends_on=payload.ends_on,
            source_reference=payload.source_reference,
            metadata_json=metadata,
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="relationship",
            entity_id=row.id,
            action="created",
            actor=payload.actor or "internal-api",
            after_json={
                "contact_id": contact_id,
                "type": row.relationship_type,
                "status": row.status,
                "governance": metadata,
            },
        ))
        return {"ok": True, "relationship_id": row.id}


@internal_router.put("/contacts/{contact_id}/relationships/{relationship_id}/review")
def review_relationship(
    contact_id: str,
    relationship_id: str,
    payload: RelationshipReviewInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingRelationship, relationship_id)
        if row is None or row.contact_id != contact_id:
            raise HTTPException(status_code=404, detail="Relationship not found.")
        if row.relationship_type not in REVIEWED_RELATIONSHIP_TYPES:
            raise HTTPException(
                status_code=422,
                detail="This relationship type does not require periodic list review.",
            )
        before = {
            "status": row.status,
            "governance": dict(row.metadata_json or {}),
        }
        metadata = dict(row.metadata_json or {})
        metadata.update({
            "list_owner": payload.list_owner.strip(),
            "review_due_on": payload.review_due_on.isoformat(),
            "reviewed_at": _now().isoformat(),
            "reviewed_by": payload.actor,
        })
        row.metadata_json = metadata
        row.status = payload.status
        row.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="relationship",
            entity_id=row.id,
            action="list_reviewed",
            actor=payload.actor,
            before_json=before,
            after_json={"status": row.status, "governance": metadata},
        ))
        return {
            "ok": True,
            "relationship_id": row.id,
            "status": row.status,
            "list_owner": metadata["list_owner"],
            "review_due_on": metadata["review_due_on"],
        }


@internal_router.put("/contacts/{contact_id}/preference")
def set_preference(
    contact_id: str,
    payload: PreferenceInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        contact = session.get(BuildingContact, contact_id)
        if contact is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        row = session.get(BuildingCommunicationPreference, contact_id)
        before = {"marketing_status": row.marketing_status} if row else {}
        if row is None:
            row = BuildingCommunicationPreference(contact_id=contact_id)
        row.marketing_status = payload.marketing_status
        row.marketing_source = payload.source
        row.marketing_changed_at = _now()
        row.updated_by = payload.actor
        row.updated_at = _now()
        session.add(row)
        if payload.marketing_status == "unsubscribed":
            suppression = session.get(BuildingSuppression, contact.email)
            if suppression is None:
                session.add(BuildingSuppression(
                    email=contact.email,
                    scope="marketing",
                    reason="unsubscribe",
                    source=payload.source,
                ))
        elif payload.marketing_status == "subscribed":
            session.execute(
                delete(BuildingSuppression).where(
                    BuildingSuppression.email == contact.email,
                    BuildingSuppression.scope == "marketing",
                    BuildingSuppression.reason == "unsubscribe",
                )
            )
        session.add(BuildingAuditEvent(
            entity_type="preference",
            entity_id=contact_id,
            action="marketing_status_changed",
            actor=payload.actor or "internal-api",
            before_json=before,
            after_json={"marketing_status": row.marketing_status, "source": row.marketing_source},
        ))
        return {"ok": True, "marketing_status": row.marketing_status}


@internal_router.put("/contacts/{contact_id}/operational-preference")
def set_operational_preference(
    contact_id: str,
    payload: OperationalPreferenceInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Record operational-contact authority without changing marketing consent."""

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        contact = session.get(BuildingContact, contact_id)
        if contact is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        row = session.get(BuildingCommunicationPreference, contact_id)
        if row is None:
            row = BuildingCommunicationPreference(contact_id=contact_id)
        before = {
            "transactional_allowed": row.transactional_allowed,
            "operational_source": row.operational_source,
        }
        row.transactional_allowed = payload.transactional_allowed
        row.operational_source = payload.source.strip()
        row.operational_evidence_reference = payload.evidence_reference.strip()
        row.operational_changed_at = _now()
        row.updated_by = payload.actor
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="preference",
            entity_id=contact_id,
            action="operational_permission_recorded",
            actor=payload.actor,
            before_json=before,
            after_json={
                "transactional_allowed": row.transactional_allowed,
                "source": row.operational_source,
                "evidence_reference": row.operational_evidence_reference,
                "changed_at": row.operational_changed_at.isoformat(),
                "marketing_status_unchanged": row.marketing_status,
            },
        ))
        return {
            "ok": True,
            "transactional_allowed": row.transactional_allowed,
            "marketing_status": row.marketing_status,
        }


@internal_router.put("/segments/{segment_id}")
def upsert_segment(
    segment_id: str,
    payload: SegmentInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != segment_id:
        raise HTTPException(status_code=422, detail="Segment ID does not match route.")
    if not payload.relationship_types and payload.segment_type != "manual_approved_list":
        raise HTTPException(status_code=422, detail="Select at least one relationship type.")
    if (
        payload.segment_type == "manual_approved_list"
        and (not payload.manual_contact_ids or not payload.approval_evidence.strip())
    ):
        raise HTTPException(
            status_code=422,
            detail="Manual lists require contacts and explicit approval evidence.",
        )
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingSegment, segment_id)
        if row is None:
            row = BuildingSegment(id=segment_id, name=payload.name)
        row.name = payload.name
        row.description = payload.description
        row.rules_json = {
            "relationship_types": payload.relationship_types,
            "relationship_status": payload.relationship_status,
            "marketing_statuses": payload.marketing_statuses,
            "inquiry_kinds": payload.inquiry_kinds,
            "manual_contact_ids": sorted(set(payload.manual_contact_ids)),
        }
        row.segment_type = payload.segment_type
        row.purpose_scope = payload.purpose_scope
        row.approval_evidence = payload.approval_evidence.strip()
        row.is_active = payload.is_active
        row.created_by = row.created_by or payload.actor
        row.updated_at = _now()
        session.add(row)
        session.flush()
        session.add(BuildingAuditEvent(
            entity_type="segment",
            entity_id=row.id,
            action="upserted",
            actor=payload.actor or "internal-api",
            after_json={
                "name": row.name,
                "rules": row.rules_json,
                "segment_type": row.segment_type,
                "purpose_scope": row.purpose_scope,
                "approval_evidence": row.approval_evidence,
                "active": row.is_active,
            },
        ))
        preview_class = (
            "operational" if row.purpose_scope == "operational" else "marketing"
        )
        resolved = _resolve_segment(
            session, row, communication_class=preview_class
        )
        return {
            "ok": True,
            "segment_id": row.id,
            "included_count": sum(1 for item in resolved if item["included"]),
            "excluded_count": sum(1 for item in resolved if not item["included"]),
        }


@internal_router.post("/segments/bootstrap")
def bootstrap_standard_segments(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Idempotently provision empty canonical audience definitions only."""

    _require_internal_key(request, x_internal_api_key)
    definitions = [
        (
            "current-tenants",
            "Current tenants",
            "current_tenants",
            "both",
            ["tenant", "tenant_employee"],
            "active",
            [],
        ),
        (
            "former-tenants",
            "Former tenants",
            "former_tenants",
            "marketing",
            ["former_tenant"],
            "any",
            [],
        ),
        (
            "workspace-prospects",
            "Workspace prospects",
            "workspace_prospects",
            "marketing",
            ["prospect", "waitlist"],
            "active",
            ["workspace", "tour"],
        ),
        (
            "event-prospects-hosts",
            "Event prospects and hosts",
            "event_prospects_hosts",
            "marketing",
            ["prospect", "event_host"],
            "active",
            ["event"],
        ),
    ]
    with session_scope(request.app.state.session_factory) as session:
        created = 0
        preserved = 0
        for (
            segment_id,
            name,
            segment_type,
            purpose_scope,
            relationship_types,
            relationship_status,
            inquiry_kinds,
        ) in definitions:
            row = session.get(BuildingSegment, segment_id)
            if row is not None:
                preserved += 1
                continue
            row = BuildingSegment(
                id=segment_id,
                name=name,
                description=f"Canonical governed audience: {name}.",
                rules_json={
                    "relationship_types": relationship_types,
                    "relationship_status": relationship_status,
                    "marketing_statuses": ["subscribed"],
                    "inquiry_kinds": inquiry_kinds,
                    "manual_contact_ids": [],
                },
                segment_type=segment_type,
                purpose_scope=purpose_scope,
                approval_evidence="system:canonical-audience-definition-v1",
                is_active=True,
                created_by="system:canonical-segments",
            )
            session.add(row)
            session.add(BuildingAuditEvent(
                entity_type="segment",
                entity_id=row.id,
                action="canonical_segment_created",
                actor="system:canonical-segments",
                after_json={
                    "segment_type": segment_type,
                    "purpose_scope": purpose_scope,
                    "rules": row.rules_json,
                    "contacts_imported": 0,
                },
            ))
            created += 1
        return {
            "ok": True,
            "created": created,
            "preserved": preserved,
            "contacts_imported": 0,
            "segment_ids": [item[0] for item in definitions],
        }


@internal_router.get("/segments/{segment_id}/preview")
def preview_segment(
    segment_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        segment = session.get(BuildingSegment, segment_id)
        if segment is None:
            raise HTTPException(status_code=404, detail="Segment not found.")
        preview_class = (
            "operational"
            if segment.purpose_scope == "operational"
            else "marketing"
        )
        resolved = _resolve_segment(
            session, segment, communication_class=preview_class
        )
        included_count = sum(1 for item in resolved if item["included"])
        excluded_count = len(resolved) - included_count
        return {
            "segment_id": segment.id,
            "segment_type": segment.segment_type,
            "purpose_scope": segment.purpose_scope,
            "included_count": included_count,
            "excluded_count": excluded_count,
            "empty": included_count == 0,
            "message": (
                "No eligible contacts. Review relationships, consent evidence, and suppressions."
                if included_count == 0
                else "Audience preview is ready for review."
            ),
            "contacts": [
                {
                    "contact_id": item["contact"].id,
                    "email": item["contact"].email,
                    "included": item["included"],
                    "reason": item["inclusion_reason"] if item["included"] else item["exclusion_reason"],
                }
                for item in resolved
            ],
        }


@internal_router.put("/campaigns/{campaign_id}")
def upsert_campaign(
    campaign_id: str,
    payload: CampaignInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != campaign_id:
        raise HTTPException(status_code=422, detail="Campaign ID does not match route.")
    with session_scope(request.app.state.session_factory) as session:
        segment = session.get(BuildingSegment, payload.segment_id)
        if segment is None:
            raise HTTPException(status_code=422, detail="Unknown segment.")
        _validate_campaign_segment(segment, payload.communication_class)
        row = session.get(BuildingCampaign, campaign_id)
        if row and row.status not in {"draft", "previewed"}:
            raise HTTPException(status_code=409, detail="Approved or sent campaigns are immutable.")
        if row and all((
            row.name == payload.name,
            row.segment_id == payload.segment_id,
            row.communication_class == payload.communication_class,
            row.subject == payload.subject,
            row.body_text == payload.body_text,
            row.template_reference == payload.template_reference.strip(),
            row.content_classification == payload.content_classification,
            row.private_content_approval_evidence
            == payload.private_content_approval_evidence.strip(),
        )):
            return {
                "ok": True,
                "campaign_id": row.id,
                "status": row.status,
                "content_version": row.content_version,
                "content_checksum": row.content_checksum,
                "replayed": True,
            }
        content_changed = bool(
            row
            and any((
                row.communication_class != payload.communication_class,
                row.subject != payload.subject,
                row.body_text != payload.body_text,
                row.template_reference != payload.template_reference.strip(),
                row.content_classification != payload.content_classification,
                row.private_content_approval_evidence
                != payload.private_content_approval_evidence.strip(),
            ))
        )
        if row is None:
            row = BuildingCampaign(
                id=campaign_id,
                name=payload.name,
                segment_id=payload.segment_id,
                communication_class=payload.communication_class,
                subject=payload.subject,
                body_text=payload.body_text,
                content_version=1,
                created_by=payload.actor,
            )
        row.name = payload.name
        row.segment_id = payload.segment_id
        row.communication_class = payload.communication_class
        row.subject = payload.subject
        row.body_text = payload.body_text
        row.template_reference = payload.template_reference.strip()
        row.content_classification = payload.content_classification
        row.private_content_approval_evidence = (
            payload.private_content_approval_evidence.strip()
        )
        if content_changed:
            row.content_version += 1
        _validate_private_campaign(row, segment)
        row.content_checksum = _campaign_content_checksum(row)
        row.status = "draft"
        row.preview_hash = ""
        row.previewed_at = None
        row.test_sent_by = ""
        row.test_sent_at = None
        row.reviewed_by = ""
        row.reviewed_at = None
        row.sender_identity = ""
        row.sent_by = ""
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=row.id,
            action="draft_saved",
            actor=payload.actor or "internal-api",
            after_json={
                "name": row.name,
                "segment_id": row.segment_id,
                "communication_class": row.communication_class,
                "subject": row.subject,
                "content_version": row.content_version,
                "template_reference": row.template_reference,
                "content_checksum": row.content_checksum,
                "content_classification": row.content_classification,
            },
        ))
        return {
            "ok": True,
            "campaign_id": row.id,
            "status": row.status,
            "content_version": row.content_version,
            "content_checksum": row.content_checksum,
            "replayed": False,
        }


@internal_router.post("/campaigns/{campaign_id}/preview")
def preview_campaign(
    campaign_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign.status not in {"draft", "previewed"}:
            raise HTTPException(status_code=409, detail="Campaign can no longer be previewed.")
        preview = _preview_payload(
            session,
            campaign,
            sender_identity=request.app.state.settings.resend_from,
        )
        campaign.preview_hash = preview["preview_hash"]
        campaign.previewed_at = _now()
        campaign.status = "previewed"
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="previewed",
            actor="internal-api",
            after_json={
                "included_count": preview["included_count"],
                "excluded_count": preview["excluded_count"],
                "preview_hash": preview["preview_hash"],
            },
        ))
        return preview


@internal_router.post("/campaigns/{campaign_id}/test-send")
def test_send_campaign(
    campaign_id: str,
    payload: TestSendInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign.status not in {"draft", "previewed"}:
            raise HTTPException(status_code=409, detail="Campaign can no longer be test-sent.")
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
            raise HTTPException(status_code=503, detail="Email delivery is not configured.")
        client.send_message(
            to=payload.email,
            subject=f"[TEST] {campaign.subject}",
            text=(
                f"{campaign.body_text.rstrip()}\n\n"
                "This is a test message. No campaign recipient status was changed."
            ),
        )
        campaign.test_sent_by = payload.actor
        campaign.test_sent_at = _now()
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="test_sent",
            actor=payload.actor,
            after_json={"email": payload.email},
        ))
        return {"ok": True, "status": "test_sent", "email": payload.email}


@internal_router.post("/campaigns/{campaign_id}/review")
def review_campaign(
    campaign_id: str,
    payload: CampaignReviewInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Complete provider-free content/audience review."""

    _require_internal_key(request, x_internal_api_key)
    expected_confirmation = f"REVIEW CAMPAIGN {campaign_id}"
    if payload.confirmation.strip() != expected_confirmation:
        raise HTTPException(
            status_code=422, detail=f"Type exactly: {expected_confirmation}"
        )
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign.status == "reviewed" and campaign.preview_hash == payload.preview_hash:
            return {"ok": True, "status": "reviewed", "replayed": True}
        if campaign.status != "previewed" or campaign.preview_hash != payload.preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Preview changed; refresh the audience preview.",
            )
        preview = _preview_payload(
            session,
            campaign,
            sender_identity=request.app.state.settings.resend_from,
        )
        if preview["preview_hash"] != payload.preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Audience or content changed; preview the campaign again.",
            )
        campaign.status = "reviewed"
        campaign.reviewed_by = payload.actor
        campaign.reviewed_at = _now()
        campaign.content_checksum = preview["content_checksum"]
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="reviewed_provider_neutral",
            actor=payload.actor,
            after_json={
                "preview_hash": campaign.preview_hash,
                "content_version": campaign.content_version,
                "content_checksum": campaign.content_checksum,
                "included_count": preview["included_count"],
                "excluded_count": preview["excluded_count"],
                "provider_call": False,
            },
        ))
        return {
            "ok": True,
            "status": campaign.status,
            "replayed": False,
            "included_count": preview["included_count"],
            "excluded_count": preview["excluded_count"],
        }


@internal_router.post("/campaigns/{campaign_id}/approve")
def approve_campaign(
    campaign_id: str,
    payload: ApprovalInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if (
            campaign.status == "approved"
            and campaign.preview_hash == payload.preview_hash
        ):
            recipient_count = session.execute(
                select(BuildingCampaignRecipient).where(
                    BuildingCampaignRecipient.campaign_id == campaign.id
                )
            ).scalars().all()
            return {
                "ok": True,
                "status": "approved",
                "recipient_count": len(recipient_count),
                "outbox_status": "schedule_ready",
                "replayed": True,
            }
        if campaign.status not in {"previewed", "reviewed"} or campaign.preview_hash != payload.preview_hash:
            raise HTTPException(status_code=409, detail="Preview changed; preview the campaign again.")
        provider_free_review = campaign.status == "reviewed"
        if not provider_free_review and campaign.test_sent_at is None:
            raise HTTPException(status_code=409, detail="Send a test message before approval.")
        if provider_free_review:
            expected_confirmation = f"APPROVE CAMPAIGN {campaign.id}"
            if payload.confirmation.strip() != expected_confirmation:
                raise HTTPException(
                    status_code=422, detail=f"Type exactly: {expected_confirmation}"
                )
        preview = _preview_payload(
            session,
            campaign,
            sender_identity=request.app.state.settings.resend_from,
        )
        if preview["preview_hash"] != payload.preview_hash:
            raise HTTPException(status_code=409, detail="Audience changed; preview the campaign again.")
        if not preview["included"]:
            raise HTTPException(status_code=422, detail="Campaign has no eligible recipients.")
        session.execute(
            delete(BuildingCampaignRecipient).where(
                BuildingCampaignRecipient.campaign_id == campaign.id
            )
        )
        for item in preview["included"]:
            preference = session.get(
                BuildingCommunicationPreference, item["contact_id"]
            )
            permission_snapshot = {
                "communication_class": campaign.communication_class,
                "marketing_status": (
                    preference.marketing_status if preference else "unknown"
                ),
                "marketing_source": (
                    preference.marketing_source if preference else ""
                ),
                "marketing_changed_at": (
                    preference.marketing_changed_at.isoformat()
                    if preference
                    else None
                ),
                "operational_allowed": (
                    preference.transactional_allowed if preference else False
                ),
                "operational_source": (
                    preference.operational_source if preference else ""
                ),
                "operational_evidence_reference": (
                    preference.operational_evidence_reference if preference else ""
                ),
                "inclusion_reason": item["reason"],
                "suppression_checked_at": _now().isoformat(),
            }
            recipient_checksum = hashlib.sha256(
                json.dumps(
                    {
                        "campaign_id": campaign.id,
                        "campaign_version": campaign.content_version,
                        "contact_id": item["contact_id"],
                        "email": item["email"],
                        "content_checksum": preview["content_checksum"],
                        "permission": permission_snapshot,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            session.add(BuildingCampaignRecipient(
                campaign_id=campaign.id,
                contact_id=item["contact_id"],
                email=item["email"],
                full_name=item["full_name"],
                campaign_version=campaign.content_version,
                communication_class=campaign.communication_class,
                content_checksum=preview["content_checksum"],
                permission_snapshot_json=permission_snapshot,
                recipient_checksum=recipient_checksum,
                inclusion_reason=item["reason"],
            ))
        campaign.status = "approved"
        campaign.approved_by = payload.actor
        campaign.approved_at = _now()
        campaign.sender_identity = preview["sender_identity"]
        campaign.content_checksum = preview["content_checksum"]
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="approved",
            actor=payload.actor,
            after_json={
                "recipient_count": preview["included_count"],
                "preview_hash": payload.preview_hash,
                "sender_identity": campaign.sender_identity,
                "content_version": campaign.content_version,
                "content_checksum": campaign.content_checksum,
                "outbox_status": "schedule_ready",
                "provider_call": False,
            },
        ))
        return {
            "ok": True,
            "status": campaign.status,
            "recipient_count": preview["included_count"],
            "outbox_status": "schedule_ready",
        }


@internal_router.post("/campaigns/{campaign_id}/schedule")
def schedule_campaign(
    campaign_id: str,
    payload: ScheduleInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Schedule an approved, frozen recipient snapshot for hourly delivery."""

    _require_internal_key(request, x_internal_api_key)
    if payload.scheduled_at.tzinfo is None or payload.scheduled_at.utcoffset() is None:
        raise HTTPException(
            status_code=422,
            detail="scheduled_at must include a timezone offset.",
        )
    scheduled_at = _utc(payload.scheduled_at)
    if scheduled_at <= _now():
        raise HTTPException(status_code=422, detail="scheduled_at must be in the future.")
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign.status != "approved":
            raise HTTPException(
                status_code=409,
                detail="Campaign must be approved before scheduling.",
            )
        _approved_campaign_sender(campaign)
        campaign.status = "scheduled"
        campaign.scheduled_at = scheduled_at
        campaign.scheduled_by = payload.actor
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="scheduled",
            actor=payload.actor,
            after_json={"scheduled_at": scheduled_at.isoformat()},
        ))
        return {
            "ok": True,
            "status": campaign.status,
            "scheduled_at": scheduled_at.isoformat(),
        }


@internal_router.post("/campaigns/{campaign_id}/unschedule")
def unschedule_campaign(
    campaign_id: str,
    payload: SendInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return a scheduled campaign to its approved state without changing its snapshot."""

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign.status != "scheduled":
            raise HTTPException(status_code=409, detail="Campaign is not scheduled.")
        before = {
            "scheduled_at": (
                _utc(campaign.scheduled_at).isoformat()
                if campaign.scheduled_at
                else None
            ),
            "scheduled_by": campaign.scheduled_by,
        }
        campaign.status = "approved"
        campaign.scheduled_at = None
        campaign.scheduled_by = ""
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="unscheduled",
            actor=payload.actor,
            before_json=before,
            after_json={"status": campaign.status},
        ))
        return {"ok": True, "status": campaign.status}


@internal_router.post("/scheduled-campaigns/run")
def run_scheduled_campaigns(
    payload: ScheduledRunInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Deliver due campaign snapshots while rechecking current permission rules."""

    _require_internal_key(request, x_internal_api_key)
    run_at = _now()
    with session_scope(request.app.state.session_factory) as session:
        due = session.execute(
            select(BuildingCampaign)
            .where(
                BuildingCampaign.status == "scheduled",
                BuildingCampaign.scheduled_at.is_not(None),
                BuildingCampaign.scheduled_at <= run_at,
            )
            .order_by(BuildingCampaign.scheduled_at, BuildingCampaign.id)
            .limit(payload.max_campaigns)
            .with_for_update(skip_locked=True)
        ).scalars().all()
        due_payload = [
            {
                "campaign_id": campaign.id,
                "scheduled_at": _utc(campaign.scheduled_at).isoformat(),
            }
            for campaign in due
            if campaign.scheduled_at is not None
        ]
        if payload.dry_run or not due:
            return {
                "ok": True,
                "dry_run": payload.dry_run,
                "due_count": len(due),
                "campaigns": due_payload,
                "sent": 0,
                "suppressed": 0,
                "failed": 0,
            }

        if any(campaign.communication_class == "marketing" for campaign in due):
            _campaign_secret(request)
        approved_senders = [
            _approved_campaign_sender(campaign)
            for campaign in due
        ]
        client = ResendClient(request.app.state.settings)
        if not client.is_configured(from_address=approved_senders[0]):
            raise HTTPException(status_code=503, detail="Email delivery is not configured.")

        totals = {"sent": 0, "suppressed": 0, "failed": 0}
        completed: list[dict[str, Any]] = []
        for campaign in due:
            campaign.status = "sending"
            counts = _deliver_campaign_recipients(
                session,
                request,
                campaign,
                client,
                eligible_statuses={"approved"},
            )
            campaign.status = "sent_with_errors" if counts["failed"] else "sent"
            campaign.sent_by = payload.actor
            campaign.sent_at = _now()
            campaign.updated_at = _now()
            for key in totals:
                totals[key] += counts[key]
            completed.append(
                {
                    "campaign_id": campaign.id,
                    "status": campaign.status,
                    **counts,
                }
            )
            session.add(BuildingAuditEvent(
                entity_type="campaign",
                entity_id=campaign.id,
                action="scheduled_send_completed",
                actor=payload.actor,
                after_json={
                    "scheduled_at": (
                        _utc(campaign.scheduled_at).isoformat()
                        if campaign.scheduled_at
                        else None
                    ),
                    **counts,
                },
            ))
        return {
            "ok": totals["failed"] == 0,
            "dry_run": False,
            "due_count": len(due),
            "campaigns": completed,
            **totals,
        }


@internal_router.post("/campaigns/{campaign_id}/send")
def send_campaign(
    campaign_id: str,
    payload: SendInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign.status != "approved":
            raise HTTPException(status_code=409, detail="Campaign must be approved before sending.")
        if campaign.communication_class == "marketing":
            _campaign_secret(request)
        approved_sender = _approved_campaign_sender(campaign)
        client = ResendClient(request.app.state.settings)
        if not client.is_configured(from_address=approved_sender):
            raise HTTPException(status_code=503, detail="Email delivery is not configured.")
        campaign.status = "sending"
        counts = _deliver_campaign_recipients(
            session,
            request,
            campaign,
            client,
            eligible_statuses={"approved"},
        )
        sent = counts["sent"]
        suppressed = counts["suppressed"]
        failed = counts["failed"]
        campaign.status = "sent_with_errors" if failed else "sent"
        campaign.sent_by = payload.actor
        campaign.sent_at = _now()
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="sent",
            actor=payload.actor,
            after_json={"sent": sent, "suppressed": suppressed, "failed": failed},
        ))
        return {
            "ok": failed == 0,
            "status": campaign.status,
            "sent": sent,
            "suppressed": suppressed,
            "failed": failed,
        }


@internal_router.post("/campaigns/{campaign_id}/retry")
def retry_campaign_failures(
    campaign_id: str,
    payload: SendInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign.status != "sent_with_errors":
            raise HTTPException(
                status_code=409,
                detail="Only a campaign with failed recipients can be retried.",
            )
        if campaign.communication_class == "marketing":
            _campaign_secret(request)
        approved_sender = _approved_campaign_sender(campaign)
        client = ResendClient(request.app.state.settings)
        if not client.is_configured(from_address=approved_sender):
            raise HTTPException(status_code=503, detail="Email delivery is not configured.")
        campaign.status = "sending"
        counts = _deliver_campaign_recipients(
            session,
            request,
            campaign,
            client,
            eligible_statuses={"failed"},
        )
        campaign.status = "sent_with_errors" if counts["failed"] else "sent"
        campaign.sent_by = campaign.sent_by or payload.actor
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="failed_recipients_retried",
            actor=payload.actor,
            after_json=counts,
        ))
        return {
            "ok": counts["failed"] == 0,
            "status": campaign.status,
            **counts,
        }


@admin_router.post(
    "/catalog/arena/prepare",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def prepare_verified_arena_catalog_from_control_room(
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Prepare the approved Arena identity without publishing or pricing it."""

    # Creates private, unpublished records only. Nothing is sent, charged or
    # published, so a typed passphrase bought nothing.
    actor = str(user.get("email") or "building-operator")
    try:
        with session_scope(request.app.state.session_factory) as session:
            result = _prepare_verified_arena_catalog(session, actor=actor)
    except ValueError as exc:
        return _building_redirect(error=str(exc))
    if (
        not result["created_space"]
        and not result["updated_space"]
        and not result["created_offering"]
    ):
        return _building_redirect(
            notice="The verified Arena catalog is already prepared; no records changed."
        )
    return _building_redirect(
        notice=(
            "Verified Arena catalog prepared as private and unavailable. "
            "Pricing, publication, and booking remain blocked pending approval."
        )
    )


@admin_router.post(
    "/spaces",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def save_space_from_control_room(
    request: Request,
    space_id: str = Form(...),
    slug: str = Form(...),
    name: str = Form(...),
    space_type: str = Form(...),
    floor: str = Form(""),
    capacity: int = Form(0),
    status: str = Form("unavailable"),
    public_description: str = Form(""),
    internal_notes: str = Form(""),
    features: str = Form(""),
    is_public: bool = Form(False),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        payload = SpaceInput(
            id=space_id.strip(),
            slug=slug.strip().lower(),
            name=name.strip(),
            space_type=space_type.strip().lower(),
            floor=floor.strip(),
            capacity=capacity,
            status=status,
            public_description=public_description.strip(),
            internal_notes=internal_notes.strip(),
            features=[item.strip() for item in features.split(",") if item.strip()],
            is_public=is_public,
        )
    except ValidationError as exc:
        return _building_redirect(error=exc.errors()[0].get("msg", "Invalid space."))
    with session_scope(request.app.state.session_factory) as session:
        slug_owner = session.execute(
            select(BuildingSpace).where(BuildingSpace.slug == payload.slug)
        ).scalar_one_or_none()
        if slug_owner is not None and slug_owner.id != payload.id:
            return _building_redirect(error="That public space URL is already in use.")
        row = session.get(BuildingSpace, payload.id)
        before = (
            {"name": row.name, "status": row.status, "is_public": row.is_public}
            if row
            else {}
        )
        if row is None:
            row = BuildingSpace(
                id=payload.id,
                slug=payload.slug,
                name=payload.name,
                space_type=payload.space_type,
            )
        row.slug = payload.slug
        row.name = payload.name
        row.space_type = payload.space_type
        row.floor = payload.floor
        row.capacity = payload.capacity
        row.status = payload.status
        row.public_description = payload.public_description
        row.internal_notes = payload.internal_notes
        row.features_json = payload.features
        row.is_public = payload.is_public
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="space",
            entity_id=row.id,
            action="upserted_from_control_room",
            actor=user.get("email") or "building-operator",
            before_json=before,
            after_json={
                "name": row.name,
                "status": row.status,
                "is_public": row.is_public,
                "capacity": row.capacity,
            },
        ))
    return _building_redirect(notice=f"{payload.name} saved.")


@admin_router.post(
    "/spaces/{space_id}/media",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def save_space_media_from_control_room(
    space_id: str,
    request: Request,
    media_id: str = Form(...),
    src: str = Form(...),
    kind: str = Form("image"),
    alt: str = Form(""),
    placement: str = Form("gallery"),
    caption: str = Form(""),
    sort_order: int = Form(0),
    approved: bool = Form(False),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = str(user.get("email") or "building-operator")
    try:
        payload = SpaceMediaInput(
            id=media_id.strip().lower(),
            src=src,
            kind=kind,
            alt=alt,
            placement=placement,
            caption=caption,
            sort_order=sort_order,
            approved=approved,
            actor=actor,
        )
        stored = payload.as_storage_dict()
    except (ValidationError, ValueError) as exc:
        message = (
            exc.errors()[0].get("msg", "Invalid media assignment.")
            if isinstance(exc, ValidationError)
            else str(exc)
        )
        return _building_redirect(error=message)
    with session_scope(request.app.state.session_factory) as session:
        space = session.get(BuildingSpace, space_id)
        if space is None:
            return _building_redirect(error="Space not found.")
        current = [item for item in list(space.media_json or []) if isinstance(item, dict)]
        before = next((dict(item) for item in current if item.get("id") == payload.id), {})
        space.media_json = [
            *[item for item in current if item.get("id") != payload.id],
            stored,
        ]
        space.updated_at = _now()
        session.add(space)
        session.add(BuildingAuditEvent(
            entity_type="space_media",
            entity_id=f"{space.id}:{payload.id}",
            action="upserted_from_control_room",
            actor=actor,
            before_json=before,
            after_json=stored,
        ))
    state = "approved for public use" if payload.approved else "saved as draft"
    return _building_redirect(notice=f"{payload.id} {state} on {space_id}.")


@admin_router.post(
    "/spaces/{space_id}/media/{media_id}/remove",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def remove_space_media_from_control_room(
    space_id: str,
    media_id: str,
    request: Request,
    reason: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    cleaned_reason = reason.strip()
    if len(cleaned_reason) < 5:
        return _building_redirect(error="Give a short reason for removing the media assignment.")
    with session_scope(request.app.state.session_factory) as session:
        space = session.get(BuildingSpace, space_id)
        if space is None:
            return _building_redirect(error="Space not found.")
        current = [item for item in list(space.media_json or []) if isinstance(item, dict)]
        before = next((dict(item) for item in current if item.get("id") == media_id), None)
        if before is None:
            return _building_redirect(error="Media assignment not found.")
        space.media_json = [item for item in current if item.get("id") != media_id]
        space.updated_at = _now()
        session.add(space)
        session.add(BuildingAuditEvent(
            entity_type="space_media",
            entity_id=f"{space.id}:{media_id}",
            action="removed_from_control_room",
            actor=user.get("email") or "building-operator",
            before_json=before,
            after_json={"reason": cleaned_reason},
        ))
    return _building_redirect(notice=f"{media_id} removed from {space_id}.")


@admin_router.post(
    "/offerings",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def save_offering_from_control_room(
    request: Request,
    offering_id: str = Form(...),
    slug: str = Form(...),
    name: str = Form(...),
    offering_type: str = Form(...),
    space_id: str = Form(""),
    public_description: str = Form(""),
    price_display: str = Form(""),
    booking_unit: str = Form("custom"),
    call_to_action: str = Form("inquire"),
    features: str = Form(""),
    is_published: bool = Form(False),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        payload = OfferingInput(
            id=offering_id.strip(),
            slug=slug.strip().lower(),
            name=name.strip(),
            offering_type=offering_type.strip().lower(),
            space_id=space_id.strip() or None,
            public_description=public_description.strip(),
            price_display=price_display.strip(),
            booking_unit=booking_unit.strip().lower(),
            call_to_action=call_to_action.strip().lower(),
            features=[item.strip() for item in features.split(",") if item.strip()],
            is_published=is_published,
        )
    except ValidationError as exc:
        return _building_redirect(error=exc.errors()[0].get("msg", "Invalid offering."))
    with session_scope(request.app.state.session_factory) as session:
        if payload.space_id and session.get(BuildingSpace, payload.space_id) is None:
            return _building_redirect(error="Choose a saved space before linking an offering.")
        slug_owner = session.execute(
            select(BuildingOffering).where(BuildingOffering.slug == payload.slug)
        ).scalar_one_or_none()
        if slug_owner is not None and slug_owner.id != payload.id:
            return _building_redirect(error="That public offering URL is already in use.")
        row = session.get(BuildingOffering, payload.id)
        before = (
            {"name": row.name, "is_published": row.is_published}
            if row
            else {}
        )
        if row is None:
            row = BuildingOffering(
                id=payload.id,
                slug=payload.slug,
                name=payload.name,
                offering_type=payload.offering_type,
            )
        row.slug = payload.slug
        row.name = payload.name
        row.offering_type = payload.offering_type
        row.space_id = payload.space_id
        row.public_description = payload.public_description
        row.price_display = payload.price_display
        row.booking_unit = payload.booking_unit
        row.call_to_action = payload.call_to_action
        row.features_json = payload.features
        row.is_published = payload.is_published
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="offering",
            entity_id=row.id,
            action="upserted_from_control_room",
            actor=user.get("email") or "building-operator",
            before_json=before,
            after_json={
                "name": row.name,
                "is_published": row.is_published,
                "space_id": row.space_id,
                "price_display": row.price_display,
            },
        ))
    return _building_redirect(notice=f"{payload.name} saved.")


@admin_router.post(
    "/launch-readiness/decisions/{decision_key}",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def record_arena_launch_decision(
    decision_key: str,
    request: Request,
    offering_id: str = Form(...),
    decision_status: str = Form(...),
    value: str = Form(...),
    evidence: str = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.pricing.approve")),
) -> RedirectResponse:
    """Record one explicit Arena launch decision without calling a provider."""

    definition = ARENA_LAUNCH_DECISIONS.get(decision_key)
    if definition is None:
        return _building_redirect(error="Unknown launch-readiness decision.")
    label, required_status = definition
    if decision_status.strip() != required_status:
        return _building_redirect(
            error=f"{label} requires status {required_status}."
        )
    if len(value.strip()) < 3:
        return _building_redirect(error="Write down what the rule is.")
    actor = user.get("email") or "building-launch-approver"
    # A typed passphrase and a written justification were ceremony for a
    # business where one person decides. Signing in, holding the permission,
    # and clicking the button is the decision. Who and when are recorded
    # automatically, so the audit trail is unchanged.
    evidence = evidence.strip() or f"Recorded by {actor} on {_now():%B %d, %Y}"
    with session_scope(request.app.state.session_factory) as session:
        offering = session.get(BuildingOffering, offering_id.strip())
        space = (
            session.get(BuildingSpace, offering.space_id)
            if offering is not None and offering.space_id
            else None
        )
        if (
            offering is None
            or offering.offering_type != "event"
            or space is None
            or space.name.strip().casefold() != "the arena"
        ):
            return _building_redirect(error="Choose The Arena event offering.")
        decision_id = launch_decision_id(offering.id, decision_key)
        row = session.get(BuildingLaunchDecision, decision_id)
        before = (
            {
                "status": row.status,
                "value": row.value,
                "evidence": row.evidence,
            }
            if row
            else {"status": "unresolved"}
        )
        if row is None:
            row = BuildingLaunchDecision(
                id=decision_id,
                offering_id=offering.id,
                decision_key=decision_key,
            )
        row.status = decision_status.strip()
        row.value = value.strip()
        row.evidence = evidence.strip()
        row.decided_by = actor
        row.decided_at = _now()
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="launch_decision",
            entity_id=row.id,
            action="arena_launch_decision_recorded",
            actor=actor,
            before_json=before,
            after_json={
                "decision_key": decision_key,
                "status": row.status,
                "value": row.value,
                "evidence": row.evidence,
                "external_write": False,
            },
        ))
    return _building_redirect(
        notice=f"{label} decision recorded. No provider was changed."
    )


@admin_router.post(
    "/rate-plans",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def save_rate_plan_from_control_room(
    request: Request,
    offering_id: str = Form(...),
    rate_plan_id: str = Form(""),
    version: int = Form(1),
    name: str = Form(...),
    status: str = Form("draft"),
    currency: str = Form("USD"),
    # Dollars is what the form now asks for. The cents fields stay accepted so
    # existing callers and tests keep working; a filled dollars field wins.
    unit_amount: str = Form(""),
    unit_amount_cents: int = Form(0),
    public_price_display: str = Form(""),
    booking_unit: str = Form("custom"),
    minimum_units: int = Form(1),
    deposit_type: str = Form("none"),
    deposit_amount: str = Form(""),
    deposit_amount_cents: int = Form(0),
    deposit_percent: float = Form(0),
    cancellation_policy: str = Form(""),
    included: str = Form(""),
    addons: str = Form(""),
    addons_json: str = Form("[]"),
    tax_status: str = Form("review_required"),
    tax_rate_percent: float = Form(0),
    tax_note: str = Form(""),
    effective_from: date = Form(...),
    effective_until: date | None = Form(None),
    user: dict = Depends(require_tool("building.pricing.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    if status not in {"draft", "in_review"}:
        return _building_redirect(
            error="Save the plan as draft or in review; approval is a separate action."
        )
    try:
        # Add-ons are one per line now. JSON is still read when the plain field
        # is empty, so anything that posted JSON before still saves.
        addon_items: list = parse_lines(addons)
        if not addon_items:
            decoded = json.loads(addons_json or "[]")
            if not isinstance(decoded, list):
                raise ValueError("Add-ons must be one per line.")
            addon_items = decoded

        unit_cents = (
            dollars_to_cents(unit_amount) if unit_amount.strip() else unit_amount_cents
        )
        deposit_cents = (
            dollars_to_cents(deposit_amount)
            if deposit_amount.strip()
            else deposit_amount_cents
        )
        # An operator should never have to invent an id.
        plan_id = rate_plan_id.strip() or suggested_rate_plan_id(offering_id, version)

        payload = RatePlanInput(
            id=plan_id,
            version=version,
            name=name.strip(),
            status=status,
            currency=currency,
            unit_amount_cents=unit_cents,
            public_price_display=public_price_display.strip(),
            booking_unit=booking_unit.strip(),
            minimum_units=minimum_units,
            deposit_type=deposit_type,
            deposit_amount_cents=deposit_cents,
            deposit_percent_bps=round(deposit_percent * 100),
            cancellation_policy=cancellation_policy.strip(),
            included=parse_lines(included) or [
                item.strip() for item in included.split(",") if item.strip()
            ],
            addons=addon_items,
            tax_status=tax_status,
            tax_rate_bps=round(tax_rate_percent * 100),
            tax_note=tax_note.strip(),
            effective_from=effective_from,
            effective_until=effective_until,
            approved_by=actor if status == "approved" else "",
            actor=actor,
        )
    except (ValidationError, ValueError, json.JSONDecodeError) as exc:
        detail = (
            exc.errors()[0].get("msg", "Invalid rate plan.")
            if isinstance(exc, ValidationError)
            else str(exc)
        )
        return _building_redirect(error=detail)
    with session_scope(request.app.state.session_factory) as session:
        if session.get(BuildingOffering, offering_id) is None:
            return _building_redirect(error="Offering not found.")
        row = session.get(BuildingRatePlan, payload.id)
        before = (
            {"status": row.status, "version": row.version, "name": row.name}
            if row
            else {}
        )
        if row is not None and row.offering_id != offering_id:
            return _building_redirect(error="Rate plan belongs to another offering.")
        if row is not None and row.status in {"approved", "retired"}:
            return _building_redirect(
                error="Approved or retired terms are locked; create a new version."
            )
        conflict = session.execute(
            select(BuildingRatePlan).where(
                BuildingRatePlan.offering_id == offering_id,
                BuildingRatePlan.version == payload.version,
                BuildingRatePlan.id != payload.id,
            )
        ).scalar_one_or_none()
        if conflict is not None:
            return _building_redirect(error="That version already exists.")
        if row is None:
            row = BuildingRatePlan(
                id=payload.id,
                offering_id=offering_id,
                version=payload.version,
                name=payload.name,
                effective_from=payload.effective_from,
                created_by=actor,
            )
        for key, value in {
            "version": payload.version,
            "name": payload.name,
            "status": payload.status,
            "currency": payload.currency,
            "unit_amount_cents": payload.unit_amount_cents,
            "public_price_display": payload.public_price_display,
            "booking_unit": payload.booking_unit,
            "minimum_units": payload.minimum_units,
            "deposit_type": payload.deposit_type,
            "deposit_amount_cents": payload.deposit_amount_cents,
            "deposit_percent_bps": payload.deposit_percent_bps,
            "cancellation_policy": payload.cancellation_policy,
            "included_json": payload.included,
            "addons_json": payload.addons,
            "tax_status": payload.tax_status,
            "tax_rate_bps": payload.tax_rate_bps,
            "tax_note": payload.tax_note,
            "effective_from": payload.effective_from,
            "effective_until": payload.effective_until,
            "updated_at": _now(),
        }.items():
            setattr(row, key, value)
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="rate_plan",
            entity_id=row.id,
            action=(
                "submitted_for_review_from_control_room"
                if payload.status == "in_review"
                else "draft_saved_from_control_room"
            ),
            actor=actor,
            before_json=before,
            after_json={
                "offering_id": offering_id,
                "version": row.version,
                "status": row.status,
                "unit_amount_cents": row.unit_amount_cents,
                "deposit_type": row.deposit_type,
                "effective_from": row.effective_from.isoformat(),
            },
        ))
    return _building_redirect(notice=f"{payload.name} saved as {payload.status}.")


@admin_router.post(
    "/rate-plans/arena-commercial-baseline",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def prepare_arena_commercial_baseline(
    request: Request,
    offering_id: str = Form(...),
    effective_from: date = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.pricing.manage")),
) -> RedirectResponse:
    """Create a reviewable Arena draft from verified, conflicting evidence."""

    actor = user.get("email") or "building-pricing-operator"
    # A draft. Approval is a separate, checked step below.
    with session_scope(request.app.state.session_factory) as session:
        offering = session.get(BuildingOffering, offering_id.strip())
        if offering is None or offering.offering_type != "event":
            return _building_redirect(error="Choose an existing event offering.")
        space = session.get(BuildingSpace, offering.space_id) if offering.space_id else None
        if space is None or space.name.strip().casefold() != "the arena":
            return _building_redirect(
                error="The selected event offering must be linked to The Arena."
            )
        existing = session.execute(
            select(BuildingRatePlan)
            .where(BuildingRatePlan.offering_id == offering.id)
            .order_by(BuildingRatePlan.version.desc())
        ).scalars().all()
        version = (existing[0].version if existing else 0) + 1
        rate_plan_id = f"{offering.id}-commercial-baseline-v{version}"
        if session.get(BuildingRatePlan, rate_plan_id) is not None:
            return _building_redirect(
                error="That reconciliation draft already exists; review it below."
            )
        row = build_arena_commercial_draft(
            offering_id=offering.id,
            version=version,
            effective_from=effective_from,
            actor=actor,
            rate_plan_id=rate_plan_id,
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="rate_plan",
            entity_id=row.id,
            action="commercial_baseline_draft_prepared",
            actor=actor,
            after_json={
                "offering_id": offering.id,
                "version": version,
                "status": "draft",
                "source_count": len(row.source_evidence_json),
                "conflict_count": len(row.conflicts_json),
                "provider_write": False,
            },
        ))
    return _building_redirect(
        notice=f"{rate_plan_id} prepared as a draft. No terms were approved or published."
    )


@admin_router.post(
    "/rate-plans/{rate_plan_id}/reconcile-source-conflicts",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def reconcile_rate_plan_source_conflicts(
    rate_plan_id: str,
    request: Request,
    conflict_id: str = Form(...),
    resolution_status: str = Form(...),
    resolution_note: str = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.pricing.manage")),
) -> RedirectResponse:
    """Acknowledge stale-source conflicts without writing to those providers."""

    actor = user.get("email") or "building-pricing-operator"
    # Writes no provider. The note is what matters, so it stays required, but
    # retyping the plan id on top of it did not add safety.
    if len(resolution_note.strip()) < 10:
        return _building_redirect(error="Say what you reconciled.")
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingRatePlan, rate_plan_id)
        if row is None:
            return _building_redirect(error="Rate plan not found.")
        if row.status not in {"draft", "in_review"}:
            return _building_redirect(error="Only a draft or in-review plan may be reconciled.")
        before = list(row.conflicts_json or [])
        conflicts = []
        matched = False
        for raw in before:
            item = dict(raw)
            if item.get("id") == conflict_id.strip():
                matched = True
                allowed = set(item.get("allowed_resolution_statuses") or [])
                if resolution_status.strip() not in allowed:
                    return _building_redirect(
                        error="That disposition is not valid for this conflict."
                    )
                item["status"] = resolution_status.strip()
                item["resolution_note"] = resolution_note.strip()
                item["resolved_by"] = actor
                item["resolved_at"] = _now().isoformat()
            conflicts.append(item)
        if not matched:
            return _building_redirect(error="Source conflict not found.")
        row.conflicts_json = conflicts
        row.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="rate_plan",
            entity_id=row.id,
            action="source_conflicts_reconciled",
            actor=actor,
            before_json={"conflicts": before},
            after_json={
                "conflicts": conflicts,
                "conflict_id": conflict_id.strip(),
                "resolution_status": resolution_status.strip(),
                "provider_write": False,
            },
        ))
    return _building_redirect(
        notice="Source conflicts reconciled in Agent only. No provider copy was changed."
    )


@admin_router.post(
    "/rate-plans/{rate_plan_id}/approve",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def approve_rate_plan_from_control_room(
    rate_plan_id: str,
    request: Request,
    approval_evidence: str = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.pricing.approve")),
) -> RedirectResponse:
    actor = user.get("email") or "building-pricing-approver"
    # Clicking approve, while signed in and holding the pricing permission, is
    # the approval. The overlap and effective-date checks below are the real
    # protection here, not a retyped plan id.
    approval_evidence = (
        approval_evidence.strip() or f"Approved by {actor} on {_now():%B %d, %Y}"
    )
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingRatePlan, rate_plan_id)
        if row is None:
            return _building_redirect(error="Rate plan not found.")
        if row.status != "in_review":
            return _building_redirect(error="Only an in-review rate plan may be approved.")
        overlapping = session.execute(
            select(BuildingRatePlan).where(
                BuildingRatePlan.offering_id == row.offering_id,
                BuildingRatePlan.status == "approved",
                BuildingRatePlan.id != row.id,
            )
        ).scalars().all()
        if any(
            _date_ranges_overlap(
                row.effective_from,
                row.effective_until,
                item.effective_from,
                item.effective_until,
            )
            for item in overlapping
        ):
            return _building_redirect(
                error="Approved rate-plan effective dates may not overlap."
            )
        try:
            RatePlanInput(
                id=row.id,
                version=row.version,
                name=row.name,
                status="approved",
                currency=row.currency,
                unit_amount_cents=row.unit_amount_cents,
                public_price_display=row.public_price_display,
                booking_unit=row.booking_unit,
                minimum_units=row.minimum_units,
                deposit_type=row.deposit_type,
                deposit_amount_cents=row.deposit_amount_cents,
                deposit_percent_bps=row.deposit_percent_bps,
                cancellation_policy=row.cancellation_policy,
                included=list(row.included_json or []),
                addons=list(row.addons_json or []),
                commercial_terms=dict(row.commercial_terms_json or {}),
                source_evidence=list(row.source_evidence_json or []),
                conflicts=list(row.conflicts_json or []),
                tax_status=row.tax_status,
                tax_rate_bps=row.tax_rate_bps,
                tax_note=row.tax_note,
                approval_evidence=approval_evidence.strip(),
                effective_from=row.effective_from,
                effective_until=row.effective_until,
                approved_by=actor,
                actor=actor,
            )
        except ValidationError as exc:
            return _building_redirect(error=exc.errors()[0].get("msg", "Invalid rate plan."))
        launch_blockers = arena_rate_plan_decision_blockers(
            session, row.offering_id
        )
        if launch_blockers:
            labels = ", ".join(
                ARENA_LAUNCH_DECISIONS[key][0] for key in launch_blockers
            )
            return _building_redirect(
                error=f"Resolve Arena launch decisions before approval: {labels}."
            )
        before = {"status": row.status}
        row.status = "approved"
        row.approved_by = actor
        row.approved_at = _now()
        row.approval_evidence = approval_evidence.strip()
        row.updated_at = _now()
        sync_arena_effective_date_decision(
            session,
            rate_plan=row,
            actor=actor,
        )
        session.add(BuildingAuditEvent(
            entity_type="rate_plan",
            entity_id=row.id,
            action="approved_from_control_room",
            actor=actor,
            before_json=before,
            after_json={
                "status": "approved",
                "version": row.version,
                "approval_evidence": row.approval_evidence,
            },
        ))
    return _building_redirect(notice=f"{rate_plan_id} approved and locked.")


@admin_router.post(
    "/rate-plans/{rate_plan_id}/retire",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def retire_rate_plan_from_control_room(
    rate_plan_id: str,
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.pricing.approve")),
) -> RedirectResponse:
    actor = user.get("email") or "building-pricing-approver"
    if confirmation.strip() != f"RETIRE {rate_plan_id}":
        return _building_redirect(error=f"Type RETIRE {rate_plan_id} to retire.")
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingRatePlan, rate_plan_id)
        if row is None:
            return _building_redirect(error="Rate plan not found.")
        if row.status != "approved":
            return _building_redirect(error="Only an approved rate plan may be retired.")
        row.status = "retired"
        row.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="rate_plan",
            entity_id=row.id,
            action="retired_from_control_room",
            actor=actor,
            before_json={"status": "approved"},
            after_json={"status": "retired"},
        ))
    return _building_redirect(notice=f"{rate_plan_id} retired.")


@admin_router.post(
    "/roster-imports/preview",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def preview_roster_import_from_control_room(
    request: Request,
    csv_text: str = Form(...),
    relationship_type: str = Form(...),
    organization: str = Form(""),
    list_owner: str = Form(""),
    review_due_on: str = Form(""),
    filename: str = Form("pasted-roster.csv"),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Stage a normalized roster snapshot without changing CRM records."""

    actor = user.get("email") or "building-operator"
    relationship_type = relationship_type.strip()
    organization = organization.strip()[:255]
    list_owner = list_owner.strip()[:255]
    if relationship_type not in ROSTER_RELATIONSHIP_TYPES:
        return _building_redirect(error="Choose a supported roster relationship.")
    try:
        review_date = date.fromisoformat(review_due_on) if review_due_on else None
    except ValueError:
        return _building_redirect(error="Choose a valid review-through date.")
    if relationship_type in REVIEWED_RELATIONSHIP_TYPES:
        if not list_owner or review_date is None:
            return _building_redirect(
                error="Tenant employee and community lists need an owner and review date."
            )
        if review_date < date.today():
            return _building_redirect(error="Review-through date cannot be in the past.")
    if relationship_type == "tenant_employee" and not organization:
        return _building_redirect(
            error="Tenant employee rosters need the tenant organization."
        )
    try:
        rows = _parse_roster_csv(csv_text)
    except ValueError as exc:
        return _building_redirect(error=str(exc))

    emails = [row["email"] for row in rows]
    with session_scope(request.app.state.session_factory) as session:
        existing_emails = set(
            session.execute(
                select(BuildingContact.email).where(BuildingContact.email.in_(emails))
            ).scalars().all()
        )
        import_id = str(uuid4())
        preview_hash = _roster_preview_hash(
            rows=rows,
            relationship_type=relationship_type,
            organization=organization,
            list_owner=list_owner,
            review_due_on=review_date,
        )
        session.add(
            BuildingRosterImport(
                id=import_id,
                filename=(filename.strip() or "pasted-roster.csv")[:255],
                relationship_type=relationship_type,
                organization=organization,
                list_owner=list_owner,
                review_due_on=review_date,
                rows_json=rows,
                preview_hash=preview_hash,
                status="previewed",
                row_count=len(rows),
                new_contact_count=len(rows) - len(existing_emails),
                existing_contact_count=len(existing_emails),
                created_by=actor,
            )
        )
        session.add(
            BuildingAuditEvent(
                entity_type="roster_import",
                entity_id=import_id,
                action="previewed_from_control_room",
                actor=actor,
                after_json={
                    "relationship_type": relationship_type,
                    "organization": organization,
                    "row_count": len(rows),
                    "new_contact_count": len(rows) - len(existing_emails),
                    "existing_contact_count": len(existing_emails),
                    "preview_hash": preview_hash,
                },
            )
        )
    return _building_redirect(
        notice=(
            f"Roster preview ready: {len(rows)} rows, "
            f"{len(rows) - len(existing_emails)} new contacts, "
            f"{len(existing_emails)} existing contacts. Review and confirm below."
        )
    )


@admin_router.post(
    "/roster-imports/{import_id}/apply",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def apply_roster_import_from_control_room(
    import_id: str,
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Apply a staged roster while preserving opt-outs and existing profile data."""

    actor = user.get("email") or "building-operator"
    expected_confirmation = f"IMPORT {import_id}"
    if confirmation.strip() != expected_confirmation:
        return _building_redirect(error=f"Type {expected_confirmation} to confirm.")
    with session_scope(request.app.state.session_factory) as session:
        roster = session.execute(
            select(BuildingRosterImport)
            .where(BuildingRosterImport.id == import_id)
            .with_for_update()
        ).scalar_one_or_none()
        if roster is None:
            return _building_redirect(error="Roster preview not found.")
        if roster.status != "previewed":
            return _building_redirect(error="This roster preview is no longer pending.")
        rows = list(roster.rows_json or [])
        current_hash = _roster_preview_hash(
            rows=rows,
            relationship_type=roster.relationship_type,
            organization=roster.organization,
            list_owner=roster.list_owner,
            review_due_on=roster.review_due_on,
        )
        if not hmac.compare_digest(current_hash, roster.preview_hash):
            return _building_redirect(
                error="Roster preview integrity check failed; create a new preview."
            )

        counts = {
            "created": 0,
            "updated": 0,
            "relationships_created": 0,
            "opt_outs_preserved": 0,
        }
        for row in rows:
            contact = session.execute(
                select(BuildingContact).where(BuildingContact.email == row["email"])
            ).scalar_one_or_none()
            before: dict[str, Any] = {}
            if contact is None:
                contact = BuildingContact(
                    id=str(uuid4()),
                    email=row["email"],
                    full_name=row["full_name"],
                    phone=row["phone"],
                    company_name=row["company_name"],
                    source="roster_import",
                    metadata_json={"roster_import_id": roster.id},
                )
                session.add(contact)
                session.flush()
                counts["created"] += 1
            else:
                before = {
                    "full_name": contact.full_name,
                    "phone": contact.phone,
                    "company_name": contact.company_name,
                }
                for field in ("full_name", "phone", "company_name"):
                    if not str(getattr(contact, field) or "").strip() and row[field]:
                        setattr(contact, field, row[field])
                contact.updated_at = _now()
                counts["updated"] += 1

            relationship = session.execute(
                select(BuildingRelationship).where(
                    BuildingRelationship.contact_id == contact.id,
                    BuildingRelationship.relationship_type
                    == roster.relationship_type,
                    BuildingRelationship.organization == roster.organization,
                )
            ).scalars().first()
            governance = {}
            if roster.relationship_type in REVIEWED_RELATIONSHIP_TYPES:
                governance = {
                    "list_owner": roster.list_owner,
                    "review_due_on": roster.review_due_on.isoformat(),
                    "reviewed_at": _now().isoformat(),
                    "reviewed_by": actor,
                    "roster_import_id": roster.id,
                }
            if relationship is None:
                relationship = BuildingRelationship(
                    id=str(uuid4()),
                    contact_id=contact.id,
                    relationship_type=roster.relationship_type,
                    status="active",
                    organization=roster.organization,
                    source_reference=(
                        row["source_reference"] or f"roster-import:{roster.id}"
                    ),
                    metadata_json=governance,
                )
                session.add(relationship)
                counts["relationships_created"] += 1
            else:
                relationship.status = "active"
                if governance:
                    metadata = dict(relationship.metadata_json or {})
                    metadata.update(governance)
                    relationship.metadata_json = metadata
                relationship.updated_at = _now()

            preference = session.get(BuildingCommunicationPreference, contact.id)
            if preference is None:
                preference = BuildingCommunicationPreference(
                    contact_id=contact.id,
                    marketing_status="unknown",
                    marketing_source="roster_import",
                    updated_by=actor,
                )
                session.add(preference)
            requested_status = row["marketing_status"]
            if preference.marketing_status == "unsubscribed":
                if requested_status != "unsubscribed":
                    counts["opt_outs_preserved"] += 1
            elif requested_status in {"subscribed", "unsubscribed"}:
                preference.marketing_status = requested_status
                preference.marketing_source = (
                    row["marketing_source"] or "roster_import"
                )
                preference.marketing_changed_at = _now()
                preference.updated_by = actor
                preference.updated_at = _now()

            session.add(
                BuildingAuditEvent(
                    entity_type="contact",
                    entity_id=contact.id,
                    action="roster_import_applied",
                    actor=actor,
                    before_json=before,
                    after_json={
                        "roster_import_id": roster.id,
                        "relationship_type": roster.relationship_type,
                        "organization": roster.organization,
                        "requested_marketing_status": requested_status,
                        "effective_marketing_status": preference.marketing_status,
                    },
                )
            )

        roster.status = "applied"
        roster.applied_by = actor
        roster.applied_at = _now()
        session.add(
            BuildingAuditEvent(
                entity_type="roster_import",
                entity_id=roster.id,
                action="applied_from_control_room",
                actor=actor,
                before_json={"status": "previewed"},
                after_json={"status": roster.status, **counts},
            )
        )
        roster_filename = roster.filename
    return _building_redirect(
        notice=(
            f"{roster_filename} imported: {counts['created']} created, "
            f"{counts['updated']} matched, "
            f"{counts['opt_outs_preserved']} opt-outs preserved."
        )
    )


@admin_router.post(
    "/roster-imports/{import_id}/cancel",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def cancel_roster_import_from_control_room(
    import_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    with session_scope(request.app.state.session_factory) as session:
        roster = session.execute(
            select(BuildingRosterImport)
            .where(BuildingRosterImport.id == import_id)
            .with_for_update()
        ).scalar_one_or_none()
        if roster is None:
            return _building_redirect(error="Roster preview not found.")
        if roster.status != "previewed":
            return _building_redirect(error="This roster preview is no longer pending.")
        roster.status = "cancelled"
        session.add(
            BuildingAuditEvent(
                entity_type="roster_import",
                entity_id=roster.id,
                action="cancelled_from_control_room",
                actor=actor,
                before_json={"status": "previewed"},
                after_json={"status": roster.status},
            )
        )
    return _building_redirect(notice="Roster preview cancelled; no contacts changed.")


@admin_router.post(
    "/contacts",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def save_contact_from_control_room(
    request: Request,
    contact_id: str = Form(""),
    email: str = Form(...),
    full_name: str = Form(""),
    phone: str = Form(""),
    company_name: str = Form(""),
    relationship_type: str = Form(...),
    organization: str = Form(""),
    source_reference: str = Form(""),
    list_owner: str = Form(""),
    review_due_on: date | None = Form(None),
    marketing_status: str = Form("unknown"),
    consent_confirmed: bool = Form(False),
    user: dict = Depends(require_tool("building.crm.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    try:
        contact_payload = ContactInput(
            email=email,
            full_name=full_name,
            phone=phone,
            company_name=company_name,
            source="control_room",
            actor=actor,
        )
        relationship_payload = RelationshipInput(
            relationship_type=relationship_type,
            organization=organization.strip(),
            source_reference=source_reference.strip(),
            list_owner=list_owner.strip(),
            review_due_on=review_due_on,
            actor=actor,
        )
        PreferenceInput(
            marketing_status=marketing_status,
            source="operator_confirmed" if consent_confirmed else "operator",
            actor=actor,
        )
    except ValidationError as exc:
        return _building_redirect(error=exc.errors()[0].get("msg", "Invalid contact."))
    if marketing_status == "subscribed" and not consent_confirmed:
        return _building_redirect(
            error="Confirm documented marketing consent before subscribing a contact."
        )
    if (
        relationship_payload.relationship_type in REVIEWED_RELATIONSHIP_TYPES
        and (
            not relationship_payload.list_owner.strip()
            or relationship_payload.review_due_on is None
        )
    ):
        return _building_redirect(
            error="Tenant employee and community relationships need an owner and review date."
        )
    with session_scope(request.app.state.session_factory) as session:
        normalized_id = contact_id.strip()
        existing_email = session.execute(
            select(BuildingContact).where(BuildingContact.email == contact_payload.email)
        ).scalar_one_or_none()
        if existing_email and normalized_id and existing_email.id != normalized_id:
            return _building_redirect(error="That email already belongs to another contact.")
        row = existing_email or (
            session.get(BuildingContact, normalized_id) if normalized_id else None
        )
        before = {"email": row.email, "status": row.status} if row else {}
        if row is None:
            row = BuildingContact(
                id=normalized_id or str(uuid4()),
                email=contact_payload.email,
            )
        row.email = contact_payload.email
        row.full_name = contact_payload.full_name.strip()
        row.phone = contact_payload.phone.strip()
        row.company_name = contact_payload.company_name.strip()
        row.source = "control_room"
        row.status = "active"
        row.updated_at = _now()
        session.add(row)
        session.flush()
        duplicate_relationship = session.execute(
            select(BuildingRelationship).where(
                BuildingRelationship.contact_id == row.id,
                BuildingRelationship.relationship_type
                == relationship_payload.relationship_type,
                BuildingRelationship.organization == relationship_payload.organization,
                BuildingRelationship.source_reference
                == relationship_payload.source_reference,
                BuildingRelationship.status == "active",
            )
        ).scalar_one_or_none()
        if duplicate_relationship is None:
            relationship_metadata: dict[str, Any] = {}
            if (
                relationship_payload.relationship_type
                in REVIEWED_RELATIONSHIP_TYPES
            ):
                relationship_metadata = {
                    "list_owner": relationship_payload.list_owner.strip(),
                    "review_due_on": relationship_payload.review_due_on.isoformat(),
                    "reviewed_at": _now().isoformat(),
                    "reviewed_by": actor,
                }
            session.add(BuildingRelationship(
                id=str(uuid4()),
                contact_id=row.id,
                relationship_type=relationship_payload.relationship_type,
                status="active",
                organization=relationship_payload.organization,
                source_reference=relationship_payload.source_reference,
                metadata_json=relationship_metadata,
            ))
        preference = session.get(BuildingCommunicationPreference, row.id)
        if preference is None:
            preference = BuildingCommunicationPreference(contact_id=row.id)
        preference.marketing_status = marketing_status
        preference.marketing_source = (
            "operator_confirmed" if consent_confirmed else "operator"
        )
        preference.marketing_changed_at = _now()
        preference.updated_by = actor
        preference.updated_at = _now()
        session.add(preference)
        session.add(BuildingAuditEvent(
            entity_type="contact",
            entity_id=row.id,
            action="upserted_from_control_room",
            actor=actor,
            before_json=before,
            after_json={
                "email": row.email,
                "relationship_type": relationship_payload.relationship_type,
                "marketing_status": marketing_status,
                "consent_confirmed": consent_confirmed,
            },
        ))
    return _building_redirect(notice=f"{contact_payload.full_name or contact_payload.email} saved.")


@admin_router.post(
    "/contacts/{contact_id}/relationships/{relationship_id}/review",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def review_relationship_from_control_room(
    contact_id: str,
    relationship_id: str,
    request: Request,
    list_owner: str = Form(...),
    review_due_on: date = Form(...),
    status: str = Form("active"),
    user: dict = Depends(require_tool("building.crm.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    try:
        payload = RelationshipReviewInput(
            list_owner=list_owner.strip(),
            review_due_on=review_due_on,
            status=status,
            actor=actor,
        )
    except ValidationError as exc:
        return _building_redirect(
            error=exc.errors()[0].get("msg", "Invalid relationship review.")
        )
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingRelationship, relationship_id)
        if row is None or row.contact_id != contact_id:
            return _building_redirect(error="Relationship not found.")
        if row.relationship_type not in REVIEWED_RELATIONSHIP_TYPES:
            return _building_redirect(
                error="This relationship does not require periodic review."
            )
        before = {"status": row.status, "governance": dict(row.metadata_json or {})}
        metadata = dict(row.metadata_json or {})
        metadata.update({
            "list_owner": payload.list_owner,
            "review_due_on": payload.review_due_on.isoformat(),
            "reviewed_at": _now().isoformat(),
            "reviewed_by": actor,
        })
        row.metadata_json = metadata
        row.status = payload.status
        row.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="relationship",
            entity_id=row.id,
            action="list_reviewed_from_control_room",
            actor=actor,
            before_json=before,
            after_json={"status": row.status, "governance": metadata},
        ))
    return _building_redirect(
        notice=f"Relationship reviewed through {payload.review_due_on.isoformat()}."
    )


@admin_router.post(
    "/segments",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def save_segment_from_control_room(
    request: Request,
    segment_id: str = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    relationship_types: list[str] = Form(...),
    marketing_statuses: list[str] = Form(...),
    relationship_status: str = Form("active"),
    is_active: bool = Form(False),
    user: dict = Depends(require_tool("building.crm.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    try:
        payload = SegmentInput(
            id=segment_id.strip(),
            name=name.strip(),
            description=description.strip(),
            relationship_types=relationship_types,
            relationship_status=relationship_status,
            marketing_statuses=marketing_statuses,
            is_active=is_active,
            actor=actor,
        )
    except ValidationError as exc:
        return _building_redirect(error=exc.errors()[0].get("msg", "Invalid audience."))
    if not payload.relationship_types:
        return _building_redirect(error="Select at least one relationship type.")
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingSegment, payload.id)
        if row is None:
            row = BuildingSegment(id=payload.id, name=payload.name)
        row.name = payload.name
        row.description = payload.description
        row.rules_json = {
            "relationship_types": payload.relationship_types,
            "relationship_status": payload.relationship_status,
            "marketing_statuses": payload.marketing_statuses,
        }
        row.is_active = payload.is_active
        row.created_by = row.created_by or actor
        row.updated_at = _now()
        session.add(row)
        session.flush()
        resolved = _resolve_segment(session, row)
        included = sum(1 for item in resolved if item["included"])
        excluded = sum(1 for item in resolved if not item["included"])
        session.add(BuildingAuditEvent(
            entity_type="segment",
            entity_id=row.id,
            action="upserted_from_control_room",
            actor=actor,
            after_json={
                "name": row.name,
                "rules": row.rules_json,
                "included_count": included,
                "excluded_count": excluded,
            },
        ))
    return _building_redirect(
        notice=f"{payload.name} saved: {included} eligible, {excluded} excluded."
    )


@admin_router.post(
    "/campaigns",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def save_campaign_from_control_room(
    request: Request,
    campaign_id: str = Form(...),
    name: str = Form(...),
    segment_id: str = Form(...),
    communication_class: str = Form("marketing"),
    subject: str = Form(...),
    body_text: str = Form(...),
    template_reference: str = Form(""),
    content_classification: str = Form("standard"),
    private_content_approval_evidence: str = Form(""),
    user: dict = Depends(require_tool("building.campaigns.prepare")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    try:
        payload = CampaignInput(
            id=campaign_id.strip(),
            name=name.strip(),
            segment_id=segment_id.strip(),
            communication_class=communication_class.strip(),
            subject=subject.strip(),
            body_text=body_text.strip(),
            template_reference=template_reference.strip(),
            content_classification=content_classification.strip(),
            private_content_approval_evidence=private_content_approval_evidence.strip(),
            actor=actor,
        )
    except ValidationError as exc:
        return _building_redirect(error=exc.errors()[0].get("msg", "Invalid campaign."))
    with session_scope(request.app.state.session_factory) as session:
        segment = session.get(BuildingSegment, payload.segment_id)
        if segment is None:
            return _building_redirect(error="Choose a saved audience.")
        try:
            _validate_campaign_segment(segment, payload.communication_class)
        except HTTPException as exc:
            return _building_redirect(error=str(exc.detail))
        row = session.get(BuildingCampaign, payload.id)
        if row and row.status not in {"draft", "previewed"}:
            return _building_redirect(error="Approved or sent campaigns are immutable.")
        content_changed = bool(
            row
            and any((
                row.communication_class != payload.communication_class,
                row.subject != payload.subject,
                row.body_text != payload.body_text,
                row.template_reference != payload.template_reference,
                row.content_classification != payload.content_classification,
                row.private_content_approval_evidence
                != payload.private_content_approval_evidence,
            ))
        )
        if row is None:
            row = BuildingCampaign(
                id=payload.id,
                name=payload.name,
                segment_id=payload.segment_id,
                communication_class=payload.communication_class,
                subject=payload.subject,
                body_text=payload.body_text,
                content_version=1,
                created_by=actor,
            )
        row.name = payload.name
        row.segment_id = payload.segment_id
        row.communication_class = payload.communication_class
        row.subject = payload.subject
        row.body_text = payload.body_text
        row.template_reference = payload.template_reference
        row.content_classification = payload.content_classification
        row.private_content_approval_evidence = (
            payload.private_content_approval_evidence
        )
        if content_changed:
            row.content_version += 1
        try:
            _validate_private_campaign(row, segment)
        except HTTPException as exc:
            return _building_redirect(error=str(exc.detail))
        row.content_checksum = _campaign_content_checksum(row)
        row.status = "draft"
        row.preview_hash = ""
        row.previewed_at = None
        row.test_sent_by = ""
        row.test_sent_at = None
        row.sender_identity = ""
        row.sent_by = ""
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=row.id,
            action="draft_saved_from_control_room",
            actor=actor,
            after_json={
                "name": row.name,
                "segment_id": row.segment_id,
                "communication_class": row.communication_class,
                "subject": row.subject,
                "content_version": row.content_version,
                "template_reference": row.template_reference,
                "content_checksum": row.content_checksum,
                "content_classification": row.content_classification,
            },
        ))
    return _building_redirect(notice=f"{payload.name} saved as a draft.")


@admin_router.post(
    "/campaigns/{campaign_id}/preview",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def preview_campaign_from_control_room(
    campaign_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.campaigns.prepare")),
) -> RedirectResponse:
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status not in {"draft", "previewed"}:
            return _building_redirect(error="This campaign can no longer be previewed.")
        preview = _preview_payload(
            session,
            campaign,
            sender_identity=request.app.state.settings.resend_from,
        )
        campaign.preview_hash = preview["preview_hash"]
        campaign.previewed_at = _now()
        campaign.status = "previewed"
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="previewed_from_control_room",
            actor=user.get("email") or "building-operator",
            after_json={
                "included_count": preview["included_count"],
                "excluded_count": preview["excluded_count"],
                "preview_hash": preview["preview_hash"],
            },
        ))
    return _building_redirect(
        notice=(
            f"{campaign.name} previewed: {preview['included_count']} eligible, "
            f"{preview['excluded_count']} excluded. No email was sent."
        )
    )


@admin_router.post(
    "/campaigns/{campaign_id}/review",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def review_campaign_from_control_room(
    campaign_id: str,
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.campaigns.prepare")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    expected = f"REVIEW CAMPAIGN {campaign_id}"
    if confirmation.strip() != expected:
        return _building_redirect(error=f"Type exactly: {expected}")
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status == "reviewed":
            return _building_redirect(notice=f"{campaign.name} is already reviewed.")
        if campaign.status != "previewed" or not campaign.preview_hash:
            return _building_redirect(error="Refresh the audience preview first.")
        preview = _preview_payload(
            session,
            campaign,
            sender_identity=request.app.state.settings.resend_from,
        )
        if preview["preview_hash"] != campaign.preview_hash:
            return _building_redirect(
                error="Audience or content changed; refresh the preview again."
            )
        campaign.status = "reviewed"
        campaign.reviewed_by = actor
        campaign.reviewed_at = _now()
        campaign.content_checksum = preview["content_checksum"]
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="reviewed_provider_neutral_from_control_room",
            actor=actor,
            after_json={
                "preview_hash": campaign.preview_hash,
                "content_version": campaign.content_version,
                "content_checksum": campaign.content_checksum,
                "included_count": preview["included_count"],
                "excluded_count": preview["excluded_count"],
                "provider_call": False,
            },
        ))
    return _building_redirect(
        notice=f"{campaign.name} reviewed. No email was sent."
    )


@admin_router.post(
    "/campaigns/{campaign_id}/test-send",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def test_send_campaign_from_control_room(
    campaign_id: str,
    request: Request,
    test_email: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        recipient = _normalize_email(test_email)
    except ValueError as exc:
        return _building_redirect(error=str(exc))
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status not in {"draft", "previewed"}:
            return _building_redirect(error="This campaign can no longer be test-sent.")
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
            return _building_redirect(error="Email delivery is not configured.")
        try:
            client.send_message(
                to=recipient,
                subject=f"[TEST] {campaign.subject}",
                text=(
                    f"{campaign.body_text.rstrip()}\n\n"
                    "This is a test message. No campaign recipient status was changed."
                ),
            )
        except Exception as exc:  # noqa: BLE001 - surface provider-safe failure
            return _building_redirect(error=f"Test delivery failed: {str(exc)[:180]}")
        campaign.test_sent_by = user.get("email") or "building-operator"
        campaign.test_sent_at = _now()
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="test_sent_from_control_room",
            actor=campaign.test_sent_by,
            after_json={"email": recipient},
        ))
    return _building_redirect(notice=f"Test message sent to {recipient}.")


@admin_router.post(
    "/campaigns/{campaign_id}/approve",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def approve_campaign_from_control_room(
    campaign_id: str,
    request: Request,
    confirmation: str = Form(""),
    user: dict = Depends(require_tool("building.campaigns.approve")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status not in {"previewed", "reviewed"} or not campaign.preview_hash:
            return _building_redirect(error="Refresh the final audience preview first.")
        provider_free_review = campaign.status == "reviewed"
        if provider_free_review:
            expected = f"APPROVE CAMPAIGN {campaign_id}"
            if confirmation.strip() != expected:
                return _building_redirect(error=f"Type exactly: {expected}")
        elif campaign.test_sent_at is None:
            return _building_redirect(
                error="Complete provider-free review or send a test message first."
            )
        preview = _preview_payload(
            session,
            campaign,
            sender_identity=request.app.state.settings.resend_from,
        )
        if preview["preview_hash"] != campaign.preview_hash:
            return _building_redirect(error="Audience changed; refresh the preview again.")
        if not preview["included"]:
            return _building_redirect(error="Campaign has no eligible recipients.")
        session.execute(
            delete(BuildingCampaignRecipient).where(
                BuildingCampaignRecipient.campaign_id == campaign.id
            )
        )
        for item in preview["included"]:
            preference = session.get(
                BuildingCommunicationPreference, item["contact_id"]
            )
            permission_snapshot = {
                "communication_class": campaign.communication_class,
                "marketing_status": (
                    preference.marketing_status if preference else "unknown"
                ),
                "marketing_source": (
                    preference.marketing_source if preference else ""
                ),
                "operational_allowed": (
                    preference.transactional_allowed if preference else False
                ),
                "operational_source": (
                    preference.operational_source if preference else ""
                ),
                "inclusion_reason": item["reason"],
                "suppression_checked_at": _now().isoformat(),
            }
            recipient_checksum = hashlib.sha256(json.dumps({
                "campaign_id": campaign.id,
                "campaign_version": campaign.content_version,
                "contact_id": item["contact_id"],
                "email": item["email"],
                "content_checksum": preview["content_checksum"],
                "permission": permission_snapshot,
            }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            session.add(BuildingCampaignRecipient(
                campaign_id=campaign.id,
                contact_id=item["contact_id"],
                email=item["email"],
                full_name=item["full_name"],
                campaign_version=campaign.content_version,
                communication_class=campaign.communication_class,
                content_checksum=preview["content_checksum"],
                permission_snapshot_json=permission_snapshot,
                recipient_checksum=recipient_checksum,
                inclusion_reason=item["reason"],
            ))
        campaign.status = "approved"
        campaign.approved_by = actor
        campaign.approved_at = _now()
        campaign.sender_identity = preview["sender_identity"]
        campaign.content_checksum = preview["content_checksum"]
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="approved_from_control_room",
            actor=actor,
            after_json={
                "recipient_count": preview["included_count"],
                "preview_hash": campaign.preview_hash,
                "sender_identity": campaign.sender_identity,
                "content_version": campaign.content_version,
                "content_checksum": campaign.content_checksum,
                "outbox_status": "schedule_ready",
                "provider_call": False,
            },
        ))
    return _building_redirect(
        notice=(
            f"{campaign.name} approved as schedule-ready for "
            f"{preview['included_count']} recipients. No email was sent."
        )
    )


@admin_router.post(
    "/campaigns/{campaign_id}/schedule",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def schedule_campaign_from_control_room(
    campaign_id: str,
    request: Request,
    scheduled_at: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    try:
        scheduled_utc = _local_mountain_datetime(scheduled_at)
    except ValueError:
        return _building_redirect(error="Choose a valid Mountain Time delivery date.")
    if scheduled_utc <= _now():
        return _building_redirect(error="Choose a future delivery time.")
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status != "approved":
            return _building_redirect(
                error="Campaign must be approved before scheduling."
            )
        try:
            approved_sender = _approved_campaign_sender(campaign)
        except HTTPException as exc:
            return _building_redirect(error=str(exc.detail))
        campaign.status = "scheduled"
        campaign.scheduled_at = scheduled_utc
        campaign.scheduled_by = actor
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="scheduled_from_control_room",
            actor=actor,
            after_json={"scheduled_at": scheduled_utc.isoformat()},
        ))
        campaign_name = campaign.name
    local_label = _mountain(scheduled_utc).strftime("%b %d, %Y at %I:%M %p MT")
    return _building_redirect(notice=f"{campaign_name} scheduled for {local_label}.")


@admin_router.post(
    "/campaigns/{campaign_id}/unschedule",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def unschedule_campaign_from_control_room(
    campaign_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status != "scheduled":
            return _building_redirect(error="Campaign is not scheduled.")
        before = {
            "scheduled_at": (
                _utc(campaign.scheduled_at).isoformat()
                if campaign.scheduled_at
                else None
            ),
            "scheduled_by": campaign.scheduled_by,
        }
        campaign.status = "approved"
        campaign.scheduled_at = None
        campaign.scheduled_by = ""
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="unscheduled_from_control_room",
            actor=actor,
            before_json=before,
            after_json={"status": campaign.status},
        ))
        campaign_name = campaign.name
    return _building_redirect(notice=f"{campaign_name} schedule cancelled.")


@admin_router.post(
    "/campaigns/{campaign_id}/send",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def send_campaign_from_control_room(
    campaign_id: str,
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status != "approved":
            return _building_redirect(error="Campaign must be approved before sending.")
        if (
            campaign.communication_class == "marketing"
            and not str(
                getattr(
                    request.app.state.settings,
                    "building_campaign_token_secret",
                    "",
                )
                or ""
            ).strip()
        ):
            return _building_redirect(
                error="Campaign unsubscribe signing is not configured."
            )
        try:
            approved_sender = _approved_campaign_sender(campaign)
        except HTTPException as exc:
            return _building_redirect(error=str(exc.detail))
        expected_confirmation = f"SEND {campaign.id}"
        if confirmation.strip() != expected_confirmation:
            return _building_redirect(error=f"Type {expected_confirmation} to confirm delivery.")
        client = ResendClient(request.app.state.settings)
        if not client.is_configured(from_address=approved_sender):
            return _building_redirect(error="Email delivery is not configured.")
        campaign.status = "sending"
        counts = _deliver_campaign_recipients(
            session,
            request,
            campaign,
            client,
            eligible_statuses={"approved"},
        )
        sent = counts["sent"]
        suppressed = counts["suppressed"]
        failed = counts["failed"]
        campaign.status = "sent_with_errors" if failed else "sent"
        campaign.sent_by = actor
        campaign.sent_at = _now()
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="sent_from_control_room",
            actor=actor,
            after_json={"sent": sent, "suppressed": suppressed, "failed": failed},
        ))
    return _building_redirect(
        notice=(
            f"{campaign.name}: {sent} sent, {suppressed} suppressed, {failed} failed."
        )
    )


@admin_router.post(
    "/campaigns/{campaign_id}/retry",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def retry_campaign_failures_from_control_room(
    campaign_id: str,
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status != "sent_with_errors":
            return _building_redirect(error="This campaign has no retryable failures.")
        expected_confirmation = f"RETRY {campaign.id}"
        if confirmation.strip() != expected_confirmation:
            return _building_redirect(error=f"Type {expected_confirmation} to retry delivery.")
        if (
            campaign.communication_class == "marketing"
            and not str(
                getattr(request.app.state.settings, "building_campaign_token_secret", "")
                or ""
            ).strip()
        ):
            return _building_redirect(error="Campaign unsubscribe signing is not configured.")
        try:
            approved_sender = _approved_campaign_sender(campaign)
        except HTTPException as exc:
            return _building_redirect(error=str(exc.detail))
        client = ResendClient(request.app.state.settings)
        if not client.is_configured(from_address=approved_sender):
            return _building_redirect(error="Email delivery is not configured.")
        campaign.status = "sending"
        counts = _deliver_campaign_recipients(
            session,
            request,
            campaign,
            client,
            eligible_statuses={"failed"},
        )
        campaign.status = "sent_with_errors" if counts["failed"] else "sent"
        campaign.sent_by = campaign.sent_by or actor
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="failed_recipients_retried_from_control_room",
            actor=actor,
            after_json=counts,
        ))
    return _building_redirect(
        notice=(
            f"{campaign.name} retry: {counts['sent']} sent, "
            f"{counts['suppressed']} suppressed, {counts['failed']} still failed."
        )
    )


@admin_router.get("/settings", response_class=HTMLResponse)
@admin_router.get("/catalog", response_class=HTMLResponse)
@admin_router.get("/contacts", response_class=HTMLResponse)
@admin_router.get("/billing", response_class=HTMLResponse)
@admin_router.get("/operations", response_class=HTMLResponse)
@admin_router.get("/bookings", response_class=HTMLResponse)
@admin_router.get("/sales", response_class=HTMLResponse)
@admin_router.get("", response_class=HTMLResponse)
def building_control_room(
    request: Request,
    notice: str = "",
    error: str = "",
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    requested_view = request.url.path.rstrip("/").rsplit("/", 1)[-1]
    view = (
        requested_view
        if requested_view in {
            "sales",
            "bookings",
            "operations",
            "billing",
            "contacts",
            "catalog",
            "settings",
        }
        else "today"
    )
    with session_scope(request.app.state.session_factory) as session:
        space_rows = session.execute(
            select(BuildingSpace).order_by(BuildingSpace.name)
        ).scalars().all()
        offering_rows = session.execute(
            select(BuildingOffering).order_by(BuildingOffering.name)
        ).scalars().all()
        rate_plan_rows = session.execute(
            select(BuildingRatePlan).order_by(
                BuildingRatePlan.offering_id,
                BuildingRatePlan.version.desc(),
            )
        ).scalars().all()
        launch_decision_rows = session.execute(
            select(BuildingLaunchDecision).order_by(
                BuildingLaunchDecision.offering_id,
                BuildingLaunchDecision.decision_key,
            )
        ).scalars().all()
        agreement_template_rows = session.execute(
            select(BuildingAgreementTemplate).order_by(
                BuildingAgreementTemplate.template_key,
                BuildingAgreementTemplate.version.desc(),
            )
        ).scalars().all()
        contact_rows = session.execute(
            select(BuildingContact).order_by(BuildingContact.full_name, BuildingContact.email)
        ).scalars().all()
        contact_merge_rows = session.execute(
            select(BuildingContactMerge)
            .order_by(BuildingContactMerge.completed_at.desc())
            .limit(50)
        ).scalars().all()
        roster_import_rows = session.execute(
            select(BuildingRosterImport)
            .order_by(BuildingRosterImport.created_at.desc())
            .limit(25)
        ).scalars().all()
        contact_ids = [item.id for item in contact_rows]
        relationships: dict[str, list[BuildingRelationship]] = {}
        preferences: dict[str, BuildingCommunicationPreference] = {}
        if contact_ids:
            for item in session.execute(
                select(BuildingRelationship).where(
                    BuildingRelationship.contact_id.in_(contact_ids)
                )
            ).scalars().all():
                relationships.setdefault(item.contact_id, []).append(item)
            preferences = {
                item.contact_id: item
                for item in session.execute(
                    select(BuildingCommunicationPreference).where(
                        BuildingCommunicationPreference.contact_id.in_(contact_ids)
                    )
                ).scalars().all()
            }
        suppressions = {
            item.email: item.reason
            for item in session.execute(select(BuildingSuppression)).scalars().all()
        }
        segment_rows = session.execute(
            select(BuildingSegment).order_by(BuildingSegment.name)
        ).scalars().all()
        campaign_rows = session.execute(
            select(BuildingCampaign).order_by(BuildingCampaign.created_at.desc())
        ).scalars().all()
        segment_names = {item.id: item.name for item in segment_rows}
        recipient_counts: dict[str, int] = {}
        failed_recipient_counts: dict[str, int] = {}
        if campaign_rows:
            for recipient in session.execute(
                select(BuildingCampaignRecipient).where(
                    BuildingCampaignRecipient.campaign_id.in_(
                        [item.id for item in campaign_rows]
                    )
                )
            ).scalars().all():
                recipient_counts[recipient.campaign_id] = (
                    recipient_counts.get(recipient.campaign_id, 0) + 1
                )
                if recipient.status == "failed":
                    failed_recipient_counts[recipient.campaign_id] = (
                        failed_recipient_counts.get(recipient.campaign_id, 0) + 1
                    )
        inquiry_rows = session.execute(
            select(BuildingInquiry)
            .order_by(BuildingInquiry.created_at.desc())
            .limit(50)
        ).scalars().all()
        conversion_dispatch_inquiry_ids = {
            event.entity_id
            for event in session.execute(
                select(BuildingAuditEvent).where(
                    BuildingAuditEvent.entity_type == "inquiry",
                    BuildingAuditEvent.action
                    == "google_ads_browser_conversion_dispatched",
                    BuildingAuditEvent.entity_id.in_([item.id for item in inquiry_rows]),
                )
            ).scalars().all()
        }
        reservation_rows = session.execute(
            select(BuildingReservation)
            .order_by(BuildingReservation.starts_at)
            .limit(100)
        ).scalars().all()
        proposal_rows = session.execute(
            select(BuildingProposal)
            .order_by(BuildingProposal.reservation_id, BuildingProposal.version.desc())
        ).scalars().all()
        latest_proposals: dict[str, BuildingProposal] = {}
        for proposal in proposal_rows:
            latest_proposals.setdefault(proposal.reservation_id, proposal)
        tour_rows = session.execute(
            select(BuildingTour)
            .order_by(BuildingTour.scheduled_at.desc())
            .limit(200)
        ).scalars().all()
        invoice_rows = session.execute(
            select(BuildingInvoice)
            .order_by(BuildingInvoice.created_at.desc())
            .limit(100)
        ).scalars().all()
        collection_case_rows = session.execute(
            select(BuildingCollectionCase)
            .order_by(
                BuildingCollectionCase.status,
                BuildingCollectionCase.next_action_at,
                BuildingCollectionCase.created_at,
            )
            .limit(200)
        ).scalars().all()
        billing_account_rows = session.execute(
            select(BuildingBillingAccount)
            .order_by(BuildingBillingAccount.account_name)
        ).scalars().all()
        billing_accounts_by_id = {
            item.id: item for item in billing_account_rows
        }
        invoices_by_id = {item.id: item for item in invoice_rows}
        billing_schedule_rows = session.execute(
            select(BuildingBillingSchedule)
            .order_by(BuildingBillingSchedule.created_at.desc())
            .limit(100)
        ).scalars().all()
        can_finance = bool(
            user.get("is_superadmin")
            or "finance" in set(user.get("permissions") or ())
        )
        adjustment_rows = (
            session.execute(
                select(BuildingBillingAdjustment)
                .order_by(BuildingBillingAdjustment.created_at.desc())
                .limit(100)
            ).scalars().all()
            if can_finance
            else []
        )
        calendar_projection_rows = session.execute(
            select(BuildingCalendarProjection)
            .order_by(BuildingCalendarProjection.updated_at.desc())
            .limit(100)
        ).scalars().all()
        checklist_rows = session.execute(
            select(BuildingOperationalChecklist)
            .order_by(
                BuildingOperationalChecklist.status,
                BuildingOperationalChecklist.due_at,
            )
            .limit(100)
        ).scalars().all()
        checklist_item_rows = session.execute(
            select(BuildingOperationalChecklistItem)
            .where(
                BuildingOperationalChecklistItem.checklist_id.in_(
                    [item.id for item in checklist_rows]
                )
            )
            .order_by(
                BuildingOperationalChecklistItem.checklist_id,
                BuildingOperationalChecklistItem.sort_order,
            )
        ).scalars().all() if checklist_rows else []
        service_request_rows = session.execute(
            select(BuildingServiceRequest)
            .order_by(
                BuildingServiceRequest.status,
                BuildingServiceRequest.due_at,
                BuildingServiceRequest.created_at,
            )
            .limit(200)
        ).scalars().all()
        privacy_request_rows = session.execute(
            select(BuildingPrivacyRequest)
            .order_by(BuildingPrivacyRequest.status, BuildingPrivacyRequest.due_at)
            .limit(100)
        ).scalars().all()
        space_names = {item.id: item.name for item in space_rows}
        reservations_by_id = {item.id: item for item in reservation_rows}
        analytics = build_building_analytics(session)
        analytics.setdefault("campaigns", {})["delivery_feedback"] = (
            "configured"
            if str(
                getattr(request.app.state.settings, "resend_webhook_secret", "")
                or ""
            ).strip()
            else "not_configured"
        )

        contacts = [
            {
                "id": item.id,
                "email": item.email,
                "full_name": item.full_name,
                "phone": item.phone,
                "company_name": item.company_name,
                "status": item.status,
                "relationships": [
                    {
                        "id": rel.id,
                        "type": rel.relationship_type,
                        "status": rel.status,
                        "source_reference": rel.source_reference,
                        "starts_on": (
                            rel.starts_on.isoformat() if rel.starts_on else ""
                        ),
                        "ends_on": (
                            rel.ends_on.isoformat() if rel.ends_on else ""
                        ),
                        "list_owner": str(
                            (rel.metadata_json or {}).get("list_owner") or ""
                        ),
                        "review_due_on": (
                            rel.metadata_json or {}
                        ).get("review_due_on"),
                        "review_current": _relationship_review_is_current(rel),
                    }
                    for rel in relationships.get(item.id, [])
                ],
                "marketing_status": (
                    preferences[item.id].marketing_status
                    if item.id in preferences
                    else "unknown"
                ),
                "suppressed": item.email in suppressions,
                "suppression_reason": suppressions.get(item.email, ""),
            }
            for item in contact_rows
        ]
        segments = []
        for item in segment_rows:
            resolved = _resolve_segment(session, item)
            segments.append({
                "id": item.id,
                "name": item.name,
                "description": item.description,
                "relationship_types": list(
                    (item.rules_json or {}).get("relationship_types") or []
                ),
                "included_count": sum(1 for row in resolved if row["included"]),
                "is_active": item.is_active,
            })
        campaigns = [
            {
                "id": item.id,
                "name": item.name,
                "subject": item.subject,
                "communication_class": item.communication_class,
                "sender_identity": (
                    item.sender_identity
                    or (
                        request.app.state.settings.resend_from
                        if item.status in {"draft", "previewed"}
                        else "Approval missing sender snapshot"
                    )
                ),
                "segment_name": segment_names.get(item.segment_id, ""),
                "recipient_count": recipient_counts.get(item.id, 0),
                "failed_recipient_count": failed_recipient_counts.get(item.id, 0),
                "status": item.status,
                "scheduled_at": (
                    _mountain(item.scheduled_at).strftime(
                        "%b %d, %Y · %I:%M %p MT"
                    )
                    if item.scheduled_at
                    else ""
                ),
                "scheduled_by": item.scheduled_by,
                "sent_by": item.sent_by,
            }
            for item in campaign_rows
        ]
        service_requests = []
        for item in service_request_rows:
            due_at = item.due_at
            comparable_due = due_at
            if comparable_due is not None and comparable_due.tzinfo is None:
                comparable_due = comparable_due.replace(tzinfo=timezone.utc)
            service_requests.append({
                "id": item.id,
                "category": item.category,
                "priority": item.priority,
                "status": item.status,
                "title": item.title,
                "description": item.description,
                "space_id": item.space_id,
                "space_name": space_names.get(item.space_id or "", ""),
                "contact_id": item.contact_id,
                "reservation_id": item.reservation_id,
                "source": item.source,
                "source_reference": item.source_reference,
                "assigned_owner": item.assigned_owner,
                "due_at": due_at.strftime("%b %d, %Y · %I:%M %p") if due_at else "",
                "overdue": bool(
                    comparable_due
                    and comparable_due < _now()
                    and item.status not in {"completed", "cancelled"}
                ),
                "resolution": item.resolution,
                "allowed_next": sorted(
                    SERVICE_REQUEST_TRANSITIONS.get(item.status, set())
                ),
            })
        rate_plan_payloads = [
            {
                "id": item.id,
                "offering_id": item.offering_id,
                "version": item.version,
                "name": item.name,
                "status": item.status,
                "currency": item.currency,
                "unit_amount_cents": item.unit_amount_cents,
                "public_price_display": item.public_price_display,
                "booking_unit": item.booking_unit,
                "minimum_units": item.minimum_units,
                "deposit_type": item.deposit_type,
                "deposit_amount_cents": item.deposit_amount_cents,
                "deposit_percent_bps": item.deposit_percent_bps,
                "cancellation_policy": item.cancellation_policy,
                "addons": list(item.addons_json or []),
                "commercial_terms": dict(item.commercial_terms_json or {}),
                "source_evidence": list(item.source_evidence_json or []),
                "conflicts": list(item.conflicts_json or []),
                "tax_status": item.tax_status,
                "tax_rate_bps": item.tax_rate_bps,
                "tax_note": item.tax_note,
                "effective_from": item.effective_from.isoformat(),
                "effective_until": (
                    item.effective_until.isoformat()
                    if item.effective_until
                    else ""
                ),
                "approved_by": item.approved_by,
            }
            for item in rate_plan_rows
        ]
        launch_decision_payloads = [
            {
                "offering_id": item.offering_id,
                "decision_key": item.decision_key,
                "status": item.status,
                "value": item.value,
                "evidence": item.evidence,
                "decided_by": item.decided_by,
                "decided_at": (
                    item.decided_at.isoformat() if item.decided_at else ""
                ),
            }
            for item in launch_decision_rows
        ]
        calendar_adapter = BuildingGoogleCalendarClient()
        launch_status = build_arena_launch_status(
            launch_decisions=launch_decision_payloads,
            rate_plans=rate_plan_payloads,
            agreement_templates=[
                {
                    "id": item.id,
                    "template_key": item.template_key,
                    "version": item.version,
                    "status": item.status,
                    "name": item.name,
                }
                for item in agreement_template_rows
            ],
            provider_readiness={
                # Was hardcoded False, so this row could never go green however
                # much setup was done. The agreement_template decision is the
                # record that names the approved template and the e-sign
                # provider, so it is what "verified" actually means here.
                "esign_verified": any(
                    item.decision_key == "agreement_template"
                    and item.status == "approved_reference"
                    for item in launch_decision_rows
                ),
                # The rail Anata bills on. Stripe below is only the optional
                # automatic-confirmation path.
                "quickbooks_connected": BuildingQuickBooksClient().is_configured,
                "payment_credentials": bool(
                    str(request.app.state.settings.stripe_secret_key or "").strip()
                ),
                "payment_webhook": bool(
                    str(request.app.state.settings.stripe_webhook_secret or "").strip()
                ),
                "calendar_configured": calendar_adapter.configured,
                # Shown on the page so an operator asked to record the calendar
                # as verified can see whether it actually is, rather than
                # attesting to something invisible.
                "calendar_target_id": calendar_adapter.target_calendar_id,
                "calendar_readiness_error": calendar_adapter.readiness_error,
                "calendar_writes_enabled": os.getenv(
                    "BUILDING_GOOGLE_CALENDAR_WRITES_ENABLED", ""
                ).strip().lower() in {"1", "true", "yes", "on"},
                "sender_credentials": bool(
                    str(request.app.state.settings.resend_api_key or "").strip()
                ),
                "sender_webhook": bool(
                    str(request.app.state.settings.resend_webhook_secret or "").strip()
                ),
                # Building mail sends from its own address regardless of the
                # agent-wide default, so this checks the Building sender rather
                # than forcing every other part of the agent onto building@.
                "sender_matches_owner_choice": (
                    building_from_address().strip().lower() == "building@anatainc.com"
                ),
            },
        )
        html_body = render_building_page(
            user=user,
            view=view,
            spaces=[
                {
                    "id": item.id,
                    "name": item.name,
                    "space_type": item.space_type,
                    "floor": item.floor,
                    "capacity": item.capacity,
                    "status": item.status,
                    "is_public": item.is_public,
                    "media": list(item.media_json or []),
                }
                for item in space_rows
            ],
            offerings=[
                {
                    "id": item.id,
                    "name": item.name,
                    "space_id": item.space_id,
                    "offering_id": item.id,
                    "offering_type": item.offering_type,
                    "is_published": item.is_published,
                }
                for item in offering_rows
            ],
            rate_plans=rate_plan_payloads,
            launch_decisions=launch_decision_payloads,
            launch_status=launch_status,
            contacts=contacts,
            segments=segments,
            campaigns=campaigns,
            roster_imports=[
                {
                    "id": item.id,
                    "filename": item.filename,
                    "relationship_type": item.relationship_type,
                    "organization": item.organization,
                    "list_owner": item.list_owner,
                    "review_due_on": (
                        item.review_due_on.isoformat()
                        if item.review_due_on
                        else ""
                    ),
                    "status": item.status,
                    "row_count": item.row_count,
                    "new_contact_count": item.new_contact_count,
                    "existing_contact_count": item.existing_contact_count,
                    "created_by": item.created_by,
                    "created_at": _mountain(item.created_at).strftime(
                        "%b %d, %Y · %I:%M %p MT"
                    ),
                    "applied_by": item.applied_by,
                    "rows": (
                        list(item.rows_json or [])
                        if item.status == "previewed"
                        else []
                    ),
                }
                for item in roster_import_rows
            ],
            inquiries=[
                {
                    "id": item.id,
                    "name": item.name,
                    "email": item.email,
                    "kind": item.kind,
                    "preferred_date": (
                        item.preferred_date.isoformat() if item.preferred_date else ""
                    ),
                    "status": item.status,
                    "source": item.source,
                    "source_reference": item.source_reference,
                    "attribution": dict(
                        (item.payload_json or {}).get("_attribution") or {}
                    ),
                    "conversion_dispatch_recorded": (
                        item.id in conversion_dispatch_inquiry_ids
                    ),
                    "hubspot_contact_id": item.hubspot_contact_id,
                    "hubspot_attempt_count": int(
                        (
                            (item.payload_json or {}).get("_hubspot_sync") or {}
                        ).get("attempt_count")
                        or 0
                    ),
                    "hubspot_error": str(
                        (
                            (item.payload_json or {}).get("_hubspot_sync") or {}
                        ).get("last_error")
                        or ""
                    ),
                    "lifecycle": dict(
                        (item.payload_json or {}).get("_lifecycle") or {}
                    ),
                    "tour_handoff": dict(
                        (item.payload_json or {}).get("_tour_handoff") or {}
                    ),
                    "event_interview": dict(
                        (item.payload_json or {}).get("_event_interview") or {}
                    ),
                    "event_interview_meta": dict(
                        (item.payload_json or {}).get("_event_interview_meta") or {}
                    ),
                    "follow_up_sequence": [
                        _follow_up_step_payload(dict(step))
                        for step in list(
                            (item.payload_json or {}).get("_follow_up_sequence") or []
                        )
                        if isinstance(step, dict)
                    ],
                    "lead_notification": dict(
                        (item.payload_json or {}).get("_lead_notification") or {}
                    ),
                    "customer_receipt": dict(
                        (item.payload_json or {}).get("_customer_receipt") or {}
                    ),
                    "phone": item.phone,
                    "details": {
                        key: value
                        for key, value in dict(item.payload_json or {}).items()
                        if not str(key).startswith("_")
                    },
                    "assigned_owner": item.assigned_owner,
                    "response_due_at": (
                        _mountain(item.response_due_at).strftime(
                            "%b %d, %Y · %I:%M %p MT"
                        )
                        if item.response_due_at
                        else ""
                    ),
                    "response_overdue": bool(
                        item.response_due_at
                        and str(
                            (
                                (item.payload_json or {}).get("_lifecycle") or {}
                            ).get("stage")
                            or "new"
                        )
                        == "new"
                        and (
                            item.response_due_at.replace(tzinfo=timezone.utc)
                            if item.response_due_at.tzinfo is None
                            else item.response_due_at
                        )
                        < _now()
                    ),
                    "id": item.id,
                }
                for item in inquiry_rows
            ],
            reservations=[
                {
                    "id": item.id,
                    "space_id": item.space_id,
                    "space_name": space_names.get(item.space_id, item.space_id),
                    "kind": item.kind,
                    "starts_at": _mountain(item.starts_at).strftime(
                        "%b %d, %Y · %I:%M %p MT"
                    ),
                    "status": item.status,
                    "agreement_status": item.agreement_status,
                    "deposit_status": item.deposit_status,
                    "proposal": (
                        {
                            "version": latest_proposals[item.id].version,
                            "proposal_type": latest_proposals[item.id].proposal_type,
                            "status": latest_proposals[item.id].status,
                            "currency": latest_proposals[item.id].currency,
                            "amount_cents": latest_proposals[item.id].amount_cents,
                            "rate_plan_id": latest_proposals[item.id].rate_plan_id,
                            "rate_plan_snapshot": dict(
                                latest_proposals[item.id].rate_plan_snapshot_json or {}
                            ),
                            "pricing_adjustment": dict(
                                (
                                    latest_proposals[item.id].rate_plan_snapshot_json
                                    or {}
                                ).get("pricing_adjustment")
                                or {}
                            ),
                            "line_item": str(
                                (
                                    list(latest_proposals[item.id].line_items_json or [{}])[0]
                                    or {}
                                ).get("description")
                                or ""
                            ),
                            "terms_summary": latest_proposals[item.id].terms_summary,
                            "valid_until": (
                                latest_proposals[item.id].valid_until.isoformat()
                                if latest_proposals[item.id].valid_until
                                else ""
                            ),
                            "document_url": latest_proposals[item.id].document_url,
                        }
                        if item.id in latest_proposals
                        else {}
                    ),
                    "allowed_next": sorted(
                        (
                            EVENT_TRANSITIONS
                            if item.kind == "event"
                            else WORKSPACE_TRANSITIONS
                        ).get(item.status, set())
                    ),
                }
                for item in reservation_rows
            ],
            invoices=[
                {
                    "id": item.id,
                    "description": item.description,
                    "status": item.status,
                    "accounting_status": item.accounting_status,
                    "amount_due_cents": item.amount_due_cents,
                    "amount_paid_cents": item.amount_paid_cents,
                    "currency": item.currency,
                    "hosted_invoice_url": item.hosted_invoice_url,
                }
                for item in invoice_rows
            ],
            collections=[
                {
                    "id": item.id,
                    "invoice_id": item.invoice_id,
                    "status": item.status,
                    "assigned_owner": item.assigned_owner,
                    "next_action_at": (
                        item.next_action_at.strftime("%b %d, %Y · %I:%M %p")
                        if item.next_action_at
                        else ""
                    ),
                    "notes": item.notes,
                    "reminder_count": item.reminder_count,
                    "last_reminder_at": (
                        item.last_reminder_at.strftime("%b %d, %Y · %I:%M %p")
                        if item.last_reminder_at
                        else ""
                    ),
                    "resolution": item.resolution,
                    "account_name": (
                        billing_accounts_by_id[
                            invoices_by_id[item.invoice_id].billing_account_id
                        ].account_name
                        if item.invoice_id in invoices_by_id
                        and invoices_by_id[item.invoice_id].billing_account_id
                        in billing_accounts_by_id
                        else "Unknown account"
                    ),
                    "billing_email": (
                        billing_accounts_by_id[
                            invoices_by_id[item.invoice_id].billing_account_id
                        ].billing_email
                        if item.invoice_id in invoices_by_id
                        and invoices_by_id[item.invoice_id].billing_account_id
                        in billing_accounts_by_id
                        else ""
                    ),
                    "currency": (
                        invoices_by_id[item.invoice_id].currency
                        if item.invoice_id in invoices_by_id
                        else "usd"
                    ),
                    "outstanding_cents": (
                        max(
                            0,
                            invoices_by_id[item.invoice_id].amount_due_cents
                            - invoices_by_id[item.invoice_id].amount_paid_cents,
                        )
                        if item.invoice_id in invoices_by_id
                        else 0
                    ),
                    "due_at": (
                        invoices_by_id[item.invoice_id].due_at.strftime("%b %d, %Y")
                        if item.invoice_id in invoices_by_id
                        and invoices_by_id[item.invoice_id].due_at
                        else ""
                    ),
                    "hosted_invoice_url": (
                        invoices_by_id[item.invoice_id].hosted_invoice_url
                        if item.invoice_id in invoices_by_id
                        else ""
                    ),
                }
                for item in collection_case_rows
                if can_finance
            ],
            adjustments=[
                {
                    "id": item.id,
                    "invoice_id": item.invoice_id,
                    "adjustment_type": item.adjustment_type,
                    "amount_cents": item.amount_cents,
                    "currency": item.currency,
                    "status": item.status,
                    "reason": item.reason,
                    "provider_reference": item.provider_reference,
                    "qbo_reference": item.qbo_reference,
                    "requested_by": item.requested_by,
                    "approved_by": item.approved_by,
                }
                for item in adjustment_rows
            ],
            can_finance=can_finance,
            billing_accounts=[
                {
                    "id": item.id,
                    "account_name": item.account_name,
                    "billing_email": item.billing_email,
                    "status": item.status,
                    "stripe_customer_id": item.stripe_customer_id,
                    "qbo_customer_id": item.qbo_customer_id,
                }
                for item in billing_account_rows
            ],
            billing_schedules=[
                {
                    "id": item.id,
                    "billing_account_id": item.billing_account_id,
                    "reservation_id": item.reservation_id,
                    "schedule_type": item.schedule_type,
                    "description": item.description,
                    "amount_cents": item.amount_cents,
                    "currency": item.currency,
                    "status": item.status,
                    "next_invoice_on": (
                        item.next_invoice_on.isoformat()
                        if item.next_invoice_on
                        else ""
                    ),
                }
                for item in billing_schedule_rows
            ],
            calendar_projections=[
                {
                    "reservation_id": item.reservation_id,
                    "space_name": space_names.get(
                        reservations_by_id[item.reservation_id].space_id
                        if item.reservation_id in reservations_by_id
                        else "",
                        "",
                    ),
                    "desired_action": item.desired_action,
                    "status": item.status,
                    "provider_event_id": item.provider_event_id,
                    "last_error": item.last_error,
                    "updated_at": item.updated_at.strftime("%b %d, %Y · %I:%M %p"),
                }
                for item in calendar_projection_rows
            ],
            checklists=[
                {
                    "id": item.id,
                    "reservation_id": item.reservation_id,
                    "space_name": space_names.get(
                        reservations_by_id[item.reservation_id].space_id
                        if item.reservation_id in reservations_by_id
                        else "",
                        "",
                    ),
                    "title": item.title,
                    "checklist_type": item.checklist_type,
                    "status": item.status,
                    "assigned_owner": item.assigned_owner,
                    "due_at": (
                        item.due_at.strftime("%b %d, %Y · %I:%M %p")
                        if item.due_at
                        else ""
                    ),
                    "items": [
                        {
                            "id": checklist_item.id,
                            "label": checklist_item.label,
                            "status": checklist_item.status,
                            "is_required": checklist_item.is_required,
                            "completion_reason": checklist_item.completion_reason,
                        }
                        for checklist_item in checklist_item_rows
                        if checklist_item.checklist_id == item.id
                    ],
                }
                for item in checklist_rows
            ],
            service_requests=service_requests,
            tours=[
                {
                    "id": item.id,
                    "reservation_id": item.reservation_id,
                    "space_name": space_names.get(
                        reservations_by_id[item.reservation_id].space_id
                        if item.reservation_id in reservations_by_id
                        else "",
                        "",
                    ),
                    "scheduled_at": _mountain(item.scheduled_at).strftime("%Y-%m-%dT%H:%M"),
                    "scheduled_label": _mountain(item.scheduled_at).strftime("%b %d, %Y · %I:%M %p MT"),
                    "duration_minutes": item.duration_minutes,
                    "status": item.status,
                    "host": item.host,
                    "meeting_location": item.meeting_location,
                    "notes": item.notes,
                    "outcome": item.outcome,
                    "next_step": item.next_step,
                }
                for item in tour_rows
            ],
            contact_merges=[
                {
                    "id": item.id,
                    "survivor_contact_id": item.survivor_contact_id,
                    "merged_contact_id": item.merged_contact_id,
                    "reason": item.reason,
                    "actor": item.actor,
                    "completed_at": _mountain(item.completed_at).strftime(
                        "%b %d, %Y · %I:%M %p MT"
                    ),
                    "consent_result": dict(item.consent_result_json or {}),
                }
                for item in contact_merge_rows
            ],
            privacy_requests=[
                {
                    "id": item.id,
                    "contact_id": item.contact_id,
                    "request_type": item.request_type,
                    "status": item.status,
                    "requestor_email": item.requestor_email,
                    "details": item.details,
                    "due_at": item.due_at.strftime("%b %d, %Y"),
                    "assigned_owner": item.assigned_owner,
                    "resolution": item.resolution,
                }
                for item in privacy_request_rows
            ],
            analytics=analytics,
            csrf_token=building_csrf_token(user),
            notice=notice[:300],
            error=error[:300],
        )
        return HTMLResponse(html_body)


@public_router.get("/unsubscribe", response_class=HTMLResponse)
def unsubscribe(
    contact_id: str,
    token: str,
    request: Request,
) -> HTMLResponse:
    secret = _campaign_secret(request)
    with session_scope(request.app.state.session_factory) as session:
        contact = session.get(BuildingContact, contact_id)
        if contact is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        expected = _unsubscribe_token(secret, contact.id, contact.email)
        if not hmac.compare_digest(expected, str(token or "")):
            raise HTTPException(status_code=401, detail="Invalid unsubscribe link.")
        preference = session.get(BuildingCommunicationPreference, contact.id)
        if preference is None:
            preference = BuildingCommunicationPreference(contact_id=contact.id)
        preference.marketing_status = "unsubscribed"
        preference.marketing_source = "campaign_link"
        preference.marketing_changed_at = _now()
        preference.updated_by = contact.email
        preference.updated_at = _now()
        session.add(preference)
        if session.get(BuildingSuppression, contact.email) is None:
            session.add(BuildingSuppression(
                email=contact.email,
                scope="marketing",
                reason="unsubscribe",
                source="campaign_link",
            ))
        session.add(BuildingAuditEvent(
            entity_type="preference",
            entity_id=contact.id,
            action="unsubscribed",
            actor=contact.email,
            after_json={"scope": "marketing", "source": "campaign_link"},
        ))
    return HTMLResponse(
        """
        <!doctype html>
        <html lang="en">
          <head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
          <title>Unsubscribed · The Anata Building</title></head>
          <body style="margin:0;background:#f4f2ed;color:#151719;font:16px/1.6 Arial,sans-serif">
            <main style="max-width:680px;margin:12vh auto;padding:32px">
              <p style="letter-spacing:.14em;text-transform:uppercase;font-size:12px">The Anata Building</p>
              <h1 style="font-size:42px;line-height:1.05">You’re unsubscribed.</h1>
              <p>You will no longer receive optional building news and promotions. Required messages about an active tenancy, booking, invoice, or safety issue remain separate.</p>
            </main>
          </body>
        </html>
        """
    )
