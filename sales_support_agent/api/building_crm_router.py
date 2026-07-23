"""Tenant/community CRM, segmentation, preferences, and campaign delivery."""

from __future__ import annotations

import csv
import hashlib
import hmac
import html
import io
import json
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
from sales_support_agent.api.building_router import (
    OfferingInput,
    RatePlanInput,
    SpaceInput,
    SpaceMediaInput,
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
from sales_support_agent.services.building_page import render_building_page


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
MOUNTAIN = ZoneInfo("America/Denver")


def _building_redirect(*, notice: str = "", error: str = "") -> RedirectResponse:
    query = urlencode({"notice": notice} if notice else {"error": error})
    return RedirectResponse(f"/admin/building?{query}", status_code=303)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _mountain(value: datetime) -> datetime:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(MOUNTAIN)


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


class CampaignInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    segment_id: str = Field(min_length=1, max_length=64)
    communication_class: Literal["marketing", "operational"] = "marketing"
    subject: str = Field(min_length=1, max_length=255)
    body_text: str = Field(min_length=1, max_length=20000)
    actor: str = Field(default="", max_length=255)


class ApprovalInput(BaseModel):
    preview_hash: str = Field(min_length=64, max_length=128)
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
        "suppressed": suppressed,
        "updated_at": contact.updated_at.isoformat(),
    }


def _validate_campaign_segment(
    segment: BuildingSegment,
    communication_class: str,
) -> None:
    if communication_class not in CAMPAIGN_COMMUNICATION_CLASSES:
        raise HTTPException(status_code=422, detail="Unsupported communication class.")
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
        elif eligible_relationships:
            reasons.append(
                ", ".join(sorted({item.relationship_type for item in eligible_relationships}))
            )
        if communication_class == "marketing":
            if marketing_status not in wanted_marketing:
                exclusions.append(f"marketing status is {marketing_status}")
            else:
                reasons.append(f"marketing status is {marketing_status}")
            if suppression_scopes.get(contact.email) in {"marketing", "all"}:
                exclusions.append("email is suppressed for marketing")
        else:
            if preference is not None and not preference.transactional_allowed:
                exclusions.append("required operational email is disabled")
            else:
                reasons.append("required operational email is allowed")
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
    if not payload.relationship_types:
        raise HTTPException(status_code=422, detail="Select at least one relationship type.")
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
        }
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
            after_json={"name": row.name, "rules": row.rules_json, "active": row.is_active},
        ))
        resolved = _resolve_segment(session, row)
        return {
            "ok": True,
            "segment_id": row.id,
            "included_count": sum(1 for item in resolved if item["included"]),
            "excluded_count": sum(1 for item in resolved if not item["included"]),
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
        resolved = _resolve_segment(session, segment)
        return {
            "segment_id": segment.id,
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
        if row is None:
            row = BuildingCampaign(
                id=campaign_id,
                name=payload.name,
                segment_id=payload.segment_id,
                communication_class=payload.communication_class,
                subject=payload.subject,
                body_text=payload.body_text,
                created_by=payload.actor,
            )
        row.name = payload.name
        row.segment_id = payload.segment_id
        row.communication_class = payload.communication_class
        row.subject = payload.subject
        row.body_text = payload.body_text
        row.status = "draft"
        row.preview_hash = ""
        row.previewed_at = None
        row.test_sent_by = ""
        row.test_sent_at = None
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
            },
        ))
        return {"ok": True, "campaign_id": row.id, "status": row.status}


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
        if campaign.status != "previewed" or campaign.preview_hash != payload.preview_hash:
            raise HTTPException(status_code=409, detail="Preview changed; preview the campaign again.")
        if campaign.test_sent_at is None:
            raise HTTPException(status_code=409, detail="Send a test message before approval.")
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
            session.add(BuildingCampaignRecipient(
                campaign_id=campaign.id,
                contact_id=item["contact_id"],
                email=item["email"],
                full_name=item["full_name"],
                inclusion_reason=item["reason"],
            ))
        campaign.status = "approved"
        campaign.approved_by = payload.actor
        campaign.approved_at = _now()
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="approved",
            actor=payload.actor,
            after_json={"recipient_count": preview["included_count"], "preview_hash": payload.preview_hash},
        ))
        return {
            "ok": True,
            "status": campaign.status,
            "recipient_count": preview["included_count"],
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
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
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
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
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
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
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
    "/rate-plans",
    dependencies=[Depends(require_building_form_security)],
    response_class=RedirectResponse,
)
def save_rate_plan_from_control_room(
    request: Request,
    offering_id: str = Form(...),
    rate_plan_id: str = Form(...),
    version: int = Form(1),
    name: str = Form(...),
    status: str = Form("draft"),
    currency: str = Form("USD"),
    unit_amount_cents: int = Form(0),
    public_price_display: str = Form(""),
    booking_unit: str = Form("custom"),
    minimum_units: int = Form(1),
    deposit_type: str = Form("none"),
    deposit_amount_cents: int = Form(0),
    deposit_percent: float = Form(0),
    cancellation_policy: str = Form(""),
    included: str = Form(""),
    addons_json: str = Form("[]"),
    effective_from: date = Form(...),
    effective_until: date | None = Form(None),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    try:
        addons = json.loads(addons_json or "[]")
        if not isinstance(addons, list):
            raise ValueError("Add-ons must be a JSON list.")
        payload = RatePlanInput(
            id=rate_plan_id.strip(),
            version=version,
            name=name.strip(),
            status=status,
            currency=currency,
            unit_amount_cents=unit_amount_cents,
            public_price_display=public_price_display.strip(),
            booking_unit=booking_unit.strip(),
            minimum_units=minimum_units,
            deposit_type=deposit_type,
            deposit_amount_cents=deposit_amount_cents,
            deposit_percent_bps=round(deposit_percent * 100),
            cancellation_policy=cancellation_policy.strip(),
            included=[item.strip() for item in included.split(",") if item.strip()],
            addons=addons,
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
            if row.status == "approved" and payload.status == "retired":
                row.status = "retired"
                row.updated_at = _now()
                session.add(BuildingAuditEvent(
                    entity_type="rate_plan",
                    entity_id=row.id,
                    action="retired_from_control_room",
                    actor=actor,
                    before_json=before,
                    after_json={"status": "retired"},
                ))
                return _building_redirect(notice=f"{row.name} retired.")
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
        if payload.status == "approved":
            current = session.execute(
                select(BuildingRatePlan).where(
                    BuildingRatePlan.offering_id == offering_id,
                    BuildingRatePlan.status == "approved",
                    BuildingRatePlan.id != payload.id,
                )
            ).scalar_one_or_none()
            if current is not None:
                return _building_redirect(
                    error="Retire the current approved rate plan first."
                )
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
            "effective_from": payload.effective_from,
            "effective_until": payload.effective_until,
            "updated_at": _now(),
        }.items():
            setattr(row, key, value)
        if payload.status == "approved":
            row.approved_by = actor
            row.approved_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="rate_plan",
            entity_id=row.id,
            action=(
                "approved_from_control_room"
                if payload.status == "approved"
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
    user: dict = Depends(require_tool("building.manage")),
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
    user: dict = Depends(require_tool("building.manage")),
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
    user: dict = Depends(require_tool("building.manage")),
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
    user: dict = Depends(require_tool("building.manage")),
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
        if row is None:
            row = BuildingCampaign(
                id=payload.id,
                name=payload.name,
                segment_id=payload.segment_id,
                communication_class=payload.communication_class,
                subject=payload.subject,
                body_text=payload.body_text,
                created_by=actor,
            )
        row.name = payload.name
        row.segment_id = payload.segment_id
        row.communication_class = payload.communication_class
        row.subject = payload.subject
        row.body_text = payload.body_text
        row.status = "draft"
        row.preview_hash = ""
        row.previewed_at = None
        row.test_sent_by = ""
        row.test_sent_at = None
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
    user: dict = Depends(require_tool("building.manage")),
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
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = user.get("email") or "building-operator"
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            return _building_redirect(error="Campaign not found.")
        if campaign.status != "previewed" or not campaign.preview_hash:
            return _building_redirect(error="Refresh the final audience preview first.")
        if campaign.test_sent_at is None:
            return _building_redirect(error="Send a test message before approval.")
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
            session.add(BuildingCampaignRecipient(
                campaign_id=campaign.id,
                contact_id=item["contact_id"],
                email=item["email"],
                full_name=item["full_name"],
                inclusion_reason=item["reason"],
            ))
        campaign.status = "approved"
        campaign.approved_by = actor
        campaign.approved_at = _now()
        campaign.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="campaign",
            entity_id=campaign.id,
            action="approved_from_control_room",
            actor=actor,
            after_json={
                "recipient_count": preview["included_count"],
                "preview_hash": campaign.preview_hash,
            },
        ))
    return _building_redirect(
        notice=f"{campaign.name} approved for {preview['included_count']} recipients."
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
        expected_confirmation = f"SEND {campaign.id}"
        if confirmation.strip() != expected_confirmation:
            return _building_redirect(error=f"Type {expected_confirmation} to confirm delivery.")
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
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
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
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


@admin_router.get("", response_class=HTMLResponse)
def building_control_room(
    request: Request,
    notice: str = "",
    error: str = "",
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
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
                "sender_identity": request.app.state.settings.resend_from,
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
        html_body = render_building_page(
            user=user,
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
                    "offering_id": item.offering_id,
                    "is_published": item.is_published,
                }
                for item in offering_rows
            ],
            rate_plans=[
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
                    "effective_from": item.effective_from.isoformat(),
                    "effective_until": (
                        item.effective_until.isoformat()
                        if item.effective_until
                        else ""
                    ),
                    "approved_by": item.approved_by,
                }
                for item in rate_plan_rows
            ],
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
                    "name": item.name,
                    "email": item.email,
                    "kind": item.kind,
                    "preferred_date": (
                        item.preferred_date.isoformat() if item.preferred_date else ""
                    ),
                    "status": item.status,
                    "source": item.source,
                    "source_reference": item.source_reference,
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
                    "starts_at": item.starts_at.strftime("%b %d, %Y · %I:%M %p"),
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
