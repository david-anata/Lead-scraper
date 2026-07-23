"""Permissioned CRM data governance for Anata Building."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal, Optional
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingCampaignRecipient,
    BuildingCommunicationPreference,
    BuildingContact,
    BuildingInquiry,
    BuildingPrivacyRequest,
    BuildingRelationship,
    BuildingReservation,
    BuildingSuppression,
)
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_security import require_building_form_security


internal_router = APIRouter(
    prefix="/api/internal/building/privacy", tags=["building-privacy"]
)
admin_router = APIRouter(prefix="/admin/building/privacy", tags=["building-privacy-admin"])

REQUEST_TYPES = {
    "access_export",
    "correction",
    "suppression",
    "deletion_review",
    "retention_review",
}
TRANSITIONS = {
    "new": {"in_review"},
    "in_review": {"completed", "denied"},
    "completed": set(),
    "denied": set(),
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(
        getattr(request.app.state.settings, "internal_api_key", "") or ""
    ).strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _redirect(*, notice: str = "", error: str = "") -> RedirectResponse:
    query = urlencode({"notice": notice} if notice else {"error": error})
    return RedirectResponse(f"/admin/building?{query}", status_code=303)


def _actor(user: dict) -> str:
    return str(user.get("email") or "building-operator")


def _contact_export(session, contact: BuildingContact) -> dict:
    preference = session.get(BuildingCommunicationPreference, contact.id)
    relationships = session.execute(
        select(BuildingRelationship).where(BuildingRelationship.contact_id == contact.id)
    ).scalars().all()
    inquiries = session.execute(
        select(BuildingInquiry).where(BuildingInquiry.email == contact.email)
    ).scalars().all()
    reservations = session.execute(
        select(BuildingReservation).where(BuildingReservation.contact_id == contact.id)
    ).scalars().all()
    recipients = session.execute(
        select(BuildingCampaignRecipient).where(
            BuildingCampaignRecipient.contact_id == contact.id
        )
    ).scalars().all()
    suppression = session.get(BuildingSuppression, contact.email)
    return {
        "generated_at": _now().isoformat(),
        "contact": {
            "id": contact.id,
            "email": contact.email,
            "full_name": contact.full_name,
            "phone": contact.phone,
            "company_name": contact.company_name,
            "source": contact.source,
            "status": contact.status,
            "created_at": contact.created_at.isoformat() if contact.created_at else None,
            "updated_at": contact.updated_at.isoformat() if contact.updated_at else None,
        },
        "communication_preferences": {
            "marketing_status": preference.marketing_status if preference else "unknown",
            "marketing_source": preference.marketing_source if preference else "",
            "transactional_allowed": (
                preference.transactional_allowed if preference else True
            ),
        },
        "suppression": (
            {"scope": suppression.scope, "reason": suppression.reason, "source": suppression.source}
            if suppression else None
        ),
        "relationships": [
            {
                "type": row.relationship_type,
                "status": row.status,
                "organization": row.organization,
                "starts_on": row.starts_on.isoformat() if row.starts_on else None,
                "ends_on": row.ends_on.isoformat() if row.ends_on else None,
            }
            for row in relationships
        ],
        "inquiries": [
            {
                "id": row.id,
                "kind": row.kind,
                "source": row.source,
                "preferred_date": row.preferred_date.isoformat() if row.preferred_date else None,
                "status": row.status,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
            for row in inquiries
        ],
        "reservations": [
            {
                "id": row.id,
                "kind": row.kind,
                "status": row.status,
                "starts_at": row.starts_at.isoformat() if row.starts_at else None,
                "ends_at": row.ends_at.isoformat() if row.ends_at else None,
            }
            for row in reservations
        ],
        "campaign_history": [
            {
                "campaign_id": row.campaign_id,
                "status": row.status,
                "exclusion_reason": row.exclusion_reason,
                "sent_at": row.sent_at.isoformat() if row.sent_at else None,
            }
            for row in recipients
        ],
    }


class PrivacyRequestInput(BaseModel):
    contact_id: str | None = None
    request_type: Literal[
        "access_export", "correction", "suppression", "deletion_review", "retention_review"
    ]
    requestor_email: str = Field(min_length=3, max_length=255)
    details: str = Field(default="", max_length=4000)
    assigned_owner: str = Field(default="", max_length=255)


class TransitionInput(BaseModel):
    status: Literal["in_review", "completed", "denied"]
    resolution: str = Field(default="", max_length=4000)
    evidence: dict = Field(default_factory=dict)


class CorrectionInput(BaseModel):
    full_name: str | None = Field(default=None, max_length=255)
    phone: str | None = Field(default=None, max_length=128)
    company_name: str | None = Field(default=None, max_length=255)
    reason: str = Field(min_length=5, max_length=1000)


class SuppressionInput(BaseModel):
    scope: Literal["marketing", "all"] = "marketing"
    reason: str = Field(min_length=5, max_length=255)


@internal_router.post("/requests")
def create_request(
    payload: PrivacyRequestInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        if payload.contact_id and session.get(BuildingContact, payload.contact_id) is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        row = BuildingPrivacyRequest(
            id=f"privacy-{uuid4().hex}",
            contact_id=payload.contact_id,
            request_type=payload.request_type,
            requestor_email=payload.requestor_email.strip().lower(),
            details=payload.details.strip(),
            due_at=_now() + timedelta(days=30),
            assigned_owner=payload.assigned_owner.strip(),
            created_by="internal-api",
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="privacy_request", entity_id=row.id, action="created",
            actor="internal-api", after_json={"type": row.request_type, "due_at": row.due_at.isoformat()},
        ))
        return {"id": row.id, "status": row.status, "due_at": row.due_at.isoformat()}


@internal_router.get("/requests")
def list_requests(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        rows = session.execute(
            select(BuildingPrivacyRequest).order_by(
                BuildingPrivacyRequest.status, BuildingPrivacyRequest.due_at
            )
        ).scalars().all()
        return {"requests": [
            {
                "id": row.id, "contact_id": row.contact_id,
                "request_type": row.request_type, "status": row.status,
                "requestor_email": row.requestor_email,
                "due_at": row.due_at.isoformat(), "assigned_owner": row.assigned_owner,
                "resolution": row.resolution,
            } for row in rows
        ]}


def _transition(session, row: BuildingPrivacyRequest, payload: TransitionInput, actor: str) -> None:
    if payload.status not in TRANSITIONS.get(row.status, set()):
        raise HTTPException(
            status_code=409, detail=f"Cannot move {row.status} to {payload.status}."
        )
    if payload.status in {"completed", "denied"} and (
        len(payload.resolution.strip()) < 5 or not payload.evidence
    ):
        raise HTTPException(
            status_code=422,
            detail="A resolution and evidence are required to close a privacy request.",
        )
    before = {"status": row.status}
    row.status = payload.status
    row.resolution = payload.resolution.strip()
    row.evidence_json = dict(payload.evidence)
    row.updated_at = _now()
    if payload.status in {"completed", "denied"}:
        row.completed_at = _now()
        row.completed_by = actor
    session.add(row)
    session.add(BuildingAuditEvent(
        entity_type="privacy_request", entity_id=row.id, action="transitioned",
        actor=actor, before_json=before,
        after_json={"status": row.status, "resolution": row.resolution, "evidence": row.evidence_json},
    ))


@internal_router.post("/requests/{request_id}/transition")
def transition_request(
    request_id: str,
    payload: TransitionInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingPrivacyRequest, request_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Privacy request not found.")
        _transition(session, row, payload, "internal-api")
        return {"id": row.id, "status": row.status}


@internal_router.get("/contacts/{contact_id}/export")
def export_contact_internal(
    contact_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        contact = session.get(BuildingContact, contact_id)
        if contact is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        session.add(BuildingAuditEvent(
            entity_type="contact", entity_id=contact.id, action="privacy_exported",
            actor="internal-api", after_json={"fields": "allow-listed"},
        ))
        return _contact_export(session, contact)


def _correct_contact(session, contact: BuildingContact, payload: CorrectionInput, actor: str) -> None:
    before = {
        "full_name": contact.full_name, "phone": contact.phone,
        "company_name": contact.company_name,
    }
    updates = payload.model_dump(exclude={"reason"}, exclude_none=True)
    if not updates:
        raise HTTPException(status_code=422, detail="Provide at least one corrected field.")
    for field, value in updates.items():
        setattr(contact, field, value.strip())
    contact.updated_at = _now()
    session.add(contact)
    session.add(BuildingAuditEvent(
        entity_type="contact", entity_id=contact.id, action="privacy_corrected",
        actor=actor, before_json=before,
        after_json={**updates, "reason": payload.reason.strip()},
    ))


def _suppress_contact(session, contact: BuildingContact, payload: SuppressionInput, actor: str) -> None:
    existing = session.get(BuildingSuppression, contact.email)
    before = (
        {"scope": existing.scope, "reason": existing.reason} if existing else {}
    )
    row = existing or BuildingSuppression(email=contact.email)
    row.scope = payload.scope
    row.reason = payload.reason.strip()
    row.source = "privacy_workflow"
    session.add(row)
    preference = session.get(BuildingCommunicationPreference, contact.id)
    if preference is None:
        preference = BuildingCommunicationPreference(contact_id=contact.id)
    preference.marketing_status = "unsubscribed"
    preference.marketing_source = "privacy_workflow"
    preference.marketing_changed_at = _now()
    preference.transactional_allowed = payload.scope != "all"
    preference.updated_by = actor
    preference.updated_at = _now()
    session.add(preference)
    session.add(BuildingAuditEvent(
        entity_type="contact", entity_id=contact.id, action="privacy_suppressed",
        actor=actor, before_json=before,
        after_json={
            "scope": row.scope, "reason": row.reason,
            "transactional_allowed": preference.transactional_allowed,
        },
    ))


@internal_router.post("/contacts/{contact_id}/correct")
def correct_contact_internal(
    contact_id: str, payload: CorrectionInput, request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        contact = session.get(BuildingContact, contact_id)
        if contact is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        _correct_contact(session, contact, payload, "internal-api")
        return {"id": contact.id, "updated": True}


@internal_router.post("/contacts/{contact_id}/suppress")
def suppress_contact_internal(
    contact_id: str, payload: SuppressionInput, request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        contact = session.get(BuildingContact, contact_id)
        if contact is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        _suppress_contact(session, contact, payload, "internal-api")
        return {"id": contact.id, "scope": payload.scope, "suppressed": True}


@admin_router.get("/contacts/{contact_id}/export")
def export_contact_admin(
    contact_id: str, request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> JSONResponse:
    with session_scope(request.app.state.session_factory) as session:
        contact = session.get(BuildingContact, contact_id)
        if contact is None:
            raise HTTPException(status_code=404, detail="Contact not found.")
        session.add(BuildingAuditEvent(
            entity_type="contact", entity_id=contact.id, action="privacy_exported",
            actor=_actor(user), after_json={"fields": "allow-listed"},
        ))
        response = JSONResponse(_contact_export(session, contact))
        response.headers["Content-Disposition"] = (
            f'attachment; filename="anata-contact-{contact.id}.json"'
        )
        return response


@admin_router.post(
    "/contacts/{contact_id}/correct",
    dependencies=[Depends(require_building_form_security)],
)
def correct_contact_admin(
    contact_id: str, request: Request,
    full_name: str = Form(""), phone: str = Form(""),
    company_name: str = Form(""), reason: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        payload = CorrectionInput(
            full_name=full_name, phone=phone, company_name=company_name, reason=reason
        )
        with session_scope(request.app.state.session_factory) as session:
            contact = session.get(BuildingContact, contact_id)
            if contact is None:
                return _redirect(error="Contact not found.")
            _correct_contact(session, contact, payload, _actor(user))
        return _redirect(notice="Contact correction saved with audit evidence.")
    except (ValueError, HTTPException) as exc:
        detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
        return _redirect(error=str(detail))


@admin_router.post(
    "/contacts/{contact_id}/suppress",
    dependencies=[Depends(require_building_form_security)],
)
def suppress_contact_admin(
    contact_id: str, request: Request,
    scope: str = Form("marketing"), reason: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        payload = SuppressionInput(scope=scope, reason=reason)
        with session_scope(request.app.state.session_factory) as session:
            contact = session.get(BuildingContact, contact_id)
            if contact is None:
                return _redirect(error="Contact not found.")
            _suppress_contact(session, contact, payload, _actor(user))
        message = (
            "All email suppressed."
            if payload.scope == "all"
            else "Marketing suppressed; required transactional email remains available."
        )
        return _redirect(notice=message)
    except ValueError as exc:
        return _redirect(error=str(exc))


@admin_router.post(
    "/requests",
    dependencies=[Depends(require_building_form_security)],
)
def create_request_admin(
    request: Request, request_type: str = Form(...),
    requestor_email: str = Form(...), contact_id: str = Form(""),
    details: str = Form(""), assigned_owner: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        payload = PrivacyRequestInput(
            contact_id=contact_id or None, request_type=request_type,
            requestor_email=requestor_email, details=details,
            assigned_owner=assigned_owner,
        )
    except ValueError as exc:
        return _redirect(error=str(exc))
    with session_scope(request.app.state.session_factory) as session:
        if payload.contact_id and session.get(BuildingContact, payload.contact_id) is None:
            return _redirect(error="Contact not found.")
        row = BuildingPrivacyRequest(
            id=f"privacy-{uuid4().hex}", contact_id=payload.contact_id,
            request_type=payload.request_type,
            requestor_email=payload.requestor_email.strip().lower(),
            details=payload.details.strip(), due_at=_now() + timedelta(days=30),
            assigned_owner=payload.assigned_owner.strip(), created_by=_actor(user),
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="privacy_request", entity_id=row.id, action="created",
            actor=_actor(user), after_json={"type": row.request_type, "due_at": row.due_at.isoformat()},
        ))
    return _redirect(notice="Privacy request added with a 30-day review deadline.")


@admin_router.post(
    "/requests/{request_id}/transition",
    dependencies=[Depends(require_building_form_security)],
)
def transition_request_admin(
    request_id: str, request: Request, status: str = Form(...),
    resolution: str = Form(""), evidence_note: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        payload = TransitionInput(
            status=status, resolution=resolution,
            evidence={"note": evidence_note.strip()} if evidence_note.strip() else {},
        )
        with session_scope(request.app.state.session_factory) as session:
            row = session.get(BuildingPrivacyRequest, request_id)
            if row is None:
                return _redirect(error="Privacy request not found.")
            _transition(session, row, payload, _actor(user))
        return _redirect(notice=f"Privacy request moved to {payload.status.replace('_', ' ')}.")
    except (ValueError, HTTPException) as exc:
        detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
        return _redirect(error=str(detail))
