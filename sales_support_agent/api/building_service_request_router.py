"""Deterministic maintenance and tenant-service request workflows."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingContact,
    BuildingReservation,
    BuildingServiceRequest,
    BuildingSpace,
)


router = APIRouter(
    prefix="/api/internal/building/service-requests",
    tags=["building-service-requests"],
)

CATEGORIES = {
    "maintenance",
    "cleaning",
    "access",
    "internet",
    "furniture",
    "safety",
    "billing_question",
    "event_support",
    "other",
}
TRANSITIONS = {
    "new": {"triaged", "cancelled"},
    "triaged": {"in_progress", "waiting", "cancelled"},
    "in_progress": {"waiting", "completed", "cancelled"},
    "waiting": {"in_progress", "completed", "cancelled"},
    "completed": {"in_progress"},
    "cancelled": set(),
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


class ServiceRequestInput(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    category: str
    priority: Literal["low", "normal", "high", "urgent"] = "normal"
    title: str = Field(min_length=3, max_length=255)
    description: str = Field(default="", max_length=5000)
    space_id: str | None = Field(default=None, max_length=64)
    contact_id: str | None = Field(default=None, max_length=64)
    reservation_id: str | None = Field(default=None, max_length=64)
    source: str = Field(default="operator", max_length=32)
    source_reference: str = Field(default="", max_length=255)
    assigned_owner: str = Field(default="", max_length=255)
    due_at: datetime | None = None
    reported_by: str = Field(min_length=1, max_length=255)

    @model_validator(mode="after")
    def urgent_work_is_owned(self) -> "ServiceRequestInput":
        if self.category not in CATEGORIES:
            raise ValueError("Unsupported service-request category.")
        if self.priority in {"high", "urgent"} and not self.assigned_owner.strip():
            raise ValueError("High and urgent requests require an assigned owner.")
        if self.priority == "urgent" and self.due_at is None:
            raise ValueError("Urgent requests require a response due time.")
        return self


class ServiceRequestTransitionInput(BaseModel):
    target_status: str = Field(min_length=1, max_length=32)
    assigned_owner: str = Field(default="", max_length=255)
    due_at: datetime | None = None
    resolution: str = Field(default="", max_length=5000)
    reason: str = Field(min_length=3, max_length=2000)
    actor: str = Field(min_length=1, max_length=255)


def _payload(row: BuildingServiceRequest) -> dict[str, Any]:
    now = _now()
    due_at = row.due_at
    if due_at is not None and due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=timezone.utc)
    return {
        "id": row.id,
        "category": row.category,
        "priority": row.priority,
        "status": row.status,
        "title": row.title,
        "description": row.description,
        "space_id": row.space_id,
        "contact_id": row.contact_id,
        "reservation_id": row.reservation_id,
        "source": row.source,
        "source_reference": row.source_reference,
        "assigned_owner": row.assigned_owner,
        "due_at": due_at.isoformat() if due_at else None,
        "overdue": bool(
            due_at
            and due_at < now
            and row.status not in {"completed", "cancelled"}
        ),
        "resolution": row.resolution,
        "reported_by": row.reported_by,
        "completed_by": row.completed_by,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "allowed_next": sorted(TRANSITIONS.get(row.status, set())),
    }


@router.get("")
def list_service_requests(
    request: Request,
    status: str = "",
    priority: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        query = select(BuildingServiceRequest).order_by(
            BuildingServiceRequest.due_at,
            BuildingServiceRequest.created_at,
        )
        if status:
            query = query.where(BuildingServiceRequest.status == status)
        if priority:
            query = query.where(BuildingServiceRequest.priority == priority)
        rows = session.execute(query).scalars().all()
        return {"service_requests": [_payload(row) for row in rows]}


@router.post("", status_code=201)
def create_service_request(
    payload: ServiceRequestInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        if payload.space_id and session.get(BuildingSpace, payload.space_id) is None:
            raise HTTPException(status_code=422, detail="Unknown space.")
        if payload.contact_id and session.get(BuildingContact, payload.contact_id) is None:
            raise HTTPException(status_code=422, detail="Unknown contact.")
        if (
            payload.reservation_id
            and session.get(BuildingReservation, payload.reservation_id) is None
        ):
            raise HTTPException(status_code=422, detail="Unknown reservation.")
        now = _now()
        row = BuildingServiceRequest(
            id=payload.id or str(uuid4()),
            category=payload.category,
            priority=payload.priority,
            title=payload.title.strip(),
            description=payload.description.strip(),
            space_id=payload.space_id,
            contact_id=payload.contact_id,
            reservation_id=payload.reservation_id,
            source=payload.source.strip() or "operator",
            source_reference=payload.source_reference.strip(),
            assigned_owner=payload.assigned_owner.strip(),
            due_at=payload.due_at,
            reported_by=payload.reported_by,
            created_at=now,
            updated_at=now,
        )
        session.add(row)
        session.add(
            BuildingAuditEvent(
                entity_type="service_request",
                entity_id=row.id,
                action="service_request_created",
                actor=payload.reported_by,
                after_json={
                    "category": row.category,
                    "priority": row.priority,
                    "status": row.status,
                    "space_id": row.space_id,
                    "assigned_owner": row.assigned_owner,
                    "due_at": row.due_at.isoformat() if row.due_at else None,
                },
            )
        )
        return {"ok": True, "service_request": _payload(row)}


@router.post("/{request_id}/transition")
def transition_service_request(
    request_id: str,
    payload: ServiceRequestTransitionInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingServiceRequest, request_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Service request not found.")
        allowed = TRANSITIONS.get(row.status, set())
        if payload.target_status not in allowed:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot move service request from {row.status} to {payload.target_status}.",
            )
        if payload.target_status == "completed" and not payload.resolution.strip():
            raise HTTPException(
                status_code=422,
                detail="Completing a service request requires a resolution.",
            )
        owner = payload.assigned_owner.strip() or row.assigned_owner
        if payload.target_status in {"triaged", "in_progress", "waiting"} and not owner:
            raise HTTPException(
                status_code=422,
                detail="Active service work requires an assigned owner.",
            )
        before = {
            "status": row.status,
            "assigned_owner": row.assigned_owner,
            "due_at": row.due_at.isoformat() if row.due_at else None,
            "resolution": row.resolution,
        }
        row.status = payload.target_status
        row.assigned_owner = owner
        if payload.due_at is not None:
            row.due_at = payload.due_at
        if payload.resolution.strip():
            row.resolution = payload.resolution.strip()
        row.updated_at = _now()
        if row.status == "completed":
            row.completed_by = payload.actor
            row.completed_at = _now()
        elif before["status"] == "completed":
            row.completed_by = ""
            row.completed_at = None
        session.add(
            BuildingAuditEvent(
                entity_type="service_request",
                entity_id=row.id,
                action=f"service_request_{row.status}",
                actor=payload.actor,
                before_json=before,
                after_json={
                    "status": row.status,
                    "assigned_owner": row.assigned_owner,
                    "due_at": row.due_at.isoformat() if row.due_at else None,
                    "resolution": row.resolution,
                    "reason": payload.reason.strip(),
                },
            )
        )
        return {"ok": True, "service_request": _payload(row)}
