"""Tenant/community CRM, segmentation, preferences, and campaign delivery."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import date, datetime, timezone
from typing import Any, Literal, Optional
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import delete, select

from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingCampaign,
    BuildingCampaignRecipient,
    BuildingCommunicationPreference,
    BuildingContact,
    BuildingRelationship,
    BuildingSegment,
    BuildingSuppression,
    BuildingInquiry,
    BuildingOffering,
    BuildingSpace,
)
from sales_support_agent.services.auth_deps import require_tool
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


def _now() -> datetime:
    return datetime.now(timezone.utc)


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
    subject: str = Field(min_length=1, max_length=255)
    body_text: str = Field(min_length=1, max_length=20000)
    actor: str = Field(default="", max_length=255)


class ApprovalInput(BaseModel):
    preview_hash: str = Field(min_length=64, max_length=128)
    actor: str = Field(min_length=1, max_length=255)


class SendInput(BaseModel):
    actor: str = Field(min_length=1, max_length=255)


class TestSendInput(BaseModel):
    email: str = Field(max_length=255)
    actor: str = Field(min_length=1, max_length=255)

    @field_validator("email")
    @classmethod
    def valid_email(cls, value: str) -> str:
        return _normalize_email(value)


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
            }
            for item in relationships
        ],
        "marketing_status": preference.marketing_status if preference else "unknown",
        "marketing_source": preference.marketing_source if preference else "",
        "suppressed": suppressed,
        "updated_at": contact.updated_at.isoformat(),
    }


def _resolve_segment(session, segment: BuildingSegment) -> list[dict[str, Any]]:
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
    suppressions = {
        item.email
        for item in session.execute(select(BuildingSuppression)).scalars().all()
        if item.scope in {"marketing", "all"}
    }

    resolved: list[dict[str, Any]] = []
    for contact in contacts:
        contact_relationships = relationships.get(contact.id, [])
        eligible_relationships = [
            item
            for item in contact_relationships
            if (not wanted_types or item.relationship_type in wanted_types)
            and (
                wanted_relationship_status == "any"
                or item.status == wanted_relationship_status
            )
        ]
        preference = preferences.get(contact.id)
        marketing_status = preference.marketing_status if preference else "unknown"
        reasons: list[str] = []
        exclusions: list[str] = []
        if wanted_types and not eligible_relationships:
            exclusions.append("relationship does not match")
        elif eligible_relationships:
            reasons.append(
                ", ".join(sorted({item.relationship_type for item in eligible_relationships}))
            )
        if marketing_status not in wanted_marketing:
            exclusions.append(f"marketing status is {marketing_status}")
        else:
            reasons.append(f"marketing status is {marketing_status}")
        if contact.email in suppressions:
            exclusions.append("email is suppressed")
        resolved.append(
            {
                "contact": contact,
                "included": not exclusions,
                "inclusion_reason": "; ".join(reasons),
                "exclusion_reason": "; ".join(exclusions),
            }
        )
    return resolved


def _preview_payload(session, campaign: BuildingCampaign) -> dict[str, Any]:
    segment = session.get(BuildingSegment, campaign.segment_id)
    if segment is None or not segment.is_active:
        raise HTTPException(status_code=422, detail="Campaign segment is unavailable.")
    resolved = _resolve_segment(session, segment)
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
            "subject": campaign.subject,
            "body_text": campaign.body_text,
            "recipients": included,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "campaign_id": campaign.id,
        "included": included,
        "excluded": excluded,
        "included_count": len(included),
        "excluded_count": len(excluded),
        "preview_hash": hashlib.sha256(canonical.encode()).hexdigest(),
    }


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
    with session_scope(request.app.state.session_factory) as session:
        if session.get(BuildingContact, contact_id) is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        row = BuildingRelationship(
            id=payload.id or str(uuid4()),
            contact_id=contact_id,
            relationship_type=payload.relationship_type,
            status=payload.status,
            organization=payload.organization,
            starts_on=payload.starts_on,
            ends_on=payload.ends_on,
            source_reference=payload.source_reference,
            metadata_json=payload.metadata,
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="relationship",
            entity_id=row.id,
            action="created",
            actor=payload.actor or "internal-api",
            after_json={"contact_id": contact_id, "type": row.relationship_type, "status": row.status},
        ))
        return {"ok": True, "relationship_id": row.id}


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
        if session.get(BuildingSegment, payload.segment_id) is None:
            raise HTTPException(status_code=422, detail="Unknown segment.")
        row = session.get(BuildingCampaign, campaign_id)
        if row and row.status not in {"draft", "previewed"}:
            raise HTTPException(status_code=409, detail="Approved or sent campaigns are immutable.")
        if row is None:
            row = BuildingCampaign(
                id=campaign_id,
                name=payload.name,
                segment_id=payload.segment_id,
                subject=payload.subject,
                body_text=payload.body_text,
                created_by=payload.actor,
            )
        row.name = payload.name
        row.segment_id = payload.segment_id
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
            after_json={"name": row.name, "segment_id": row.segment_id, "subject": row.subject},
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
        preview = _preview_payload(session, campaign)
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
        preview = _preview_payload(session, campaign)
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


@internal_router.post("/campaigns/{campaign_id}/send")
def send_campaign(
    campaign_id: str,
    payload: SendInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    secret = _campaign_secret(request)
    with session_scope(request.app.state.session_factory) as session:
        campaign = session.get(BuildingCampaign, campaign_id)
        if campaign is None:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign.status != "approved":
            raise HTTPException(status_code=409, detail="Campaign must be approved before sending.")
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
            raise HTTPException(status_code=503, detail="Email delivery is not configured.")
        recipients = session.execute(
            select(BuildingCampaignRecipient)
            .where(BuildingCampaignRecipient.campaign_id == campaign.id)
            .order_by(BuildingCampaignRecipient.id)
        ).scalars().all()
        sent = 0
        suppressed = 0
        failed = 0
        campaign.status = "sending"
        for recipient in recipients:
            if recipient.status != "approved":
                continue
            preference = session.get(BuildingCommunicationPreference, recipient.contact_id)
            suppression = session.get(BuildingSuppression, recipient.email)
            if (
                preference is None
                or preference.marketing_status != "subscribed"
                or (suppression is not None and suppression.scope in {"marketing", "all"})
            ):
                recipient.status = "suppressed"
                recipient.exclusion_reason = "No current marketing permission or email is suppressed."
                suppressed += 1
                continue
            token = _unsubscribe_token(secret, recipient.contact_id, recipient.email)
            unsubscribe_url = (
                f"{str(request.base_url).rstrip('/')}/api/public/building/unsubscribe?"
                + urlencode({"contact_id": recipient.contact_id, "token": token})
            )
            try:
                client.send_message(
                    to=recipient.email,
                    subject=campaign.subject,
                    text=(
                        f"{campaign.body_text.rstrip()}\n\n"
                        f"Stop receiving optional Anata Building news: {unsubscribe_url}"
                    ),
                )
                recipient.status = "sent"
                recipient.provider_message_id = "resend"
                recipient.sent_at = _now()
                sent += 1
            except Exception as exc:  # noqa: BLE001 - record and preserve retry evidence
                recipient.status = "failed"
                recipient.exclusion_reason = str(exc)[:500]
                failed += 1
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


@admin_router.get("", response_class=HTMLResponse)
def building_control_room(
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    with session_scope(request.app.state.session_factory) as session:
        space_rows = session.execute(
            select(BuildingSpace).order_by(BuildingSpace.name)
        ).scalars().all()
        offering_rows = session.execute(
            select(BuildingOffering).order_by(BuildingOffering.name)
        ).scalars().all()
        contact_rows = session.execute(
            select(BuildingContact).order_by(BuildingContact.full_name, BuildingContact.email)
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
            item.email
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
        inquiry_rows = session.execute(
            select(BuildingInquiry)
            .order_by(BuildingInquiry.created_at.desc())
            .limit(50)
        ).scalars().all()

        contacts = [
            {
                "id": item.id,
                "email": item.email,
                "full_name": item.full_name,
                "relationships": [
                    {
                        "type": rel.relationship_type,
                        "status": rel.status,
                    }
                    for rel in relationships.get(item.id, [])
                ],
                "marketing_status": (
                    preferences[item.id].marketing_status
                    if item.id in preferences
                    else "unknown"
                ),
                "suppressed": item.email in suppressions,
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
                "segment_name": segment_names.get(item.segment_id, ""),
                "recipient_count": recipient_counts.get(item.id, 0),
                "status": item.status,
            }
            for item in campaign_rows
        ]
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
                }
                for item in space_rows
            ],
            offerings=[{"id": item.id} for item in offering_rows],
            contacts=contacts,
            segments=segments,
            campaigns=campaigns,
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
                }
                for item in inquiry_rows
            ],
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
