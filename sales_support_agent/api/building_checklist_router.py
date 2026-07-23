"""Internal APIs for event and tenant operational checklists."""

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
    BuildingOperationalChecklist,
    BuildingOperationalChecklistItem,
)


router = APIRouter(
    prefix="/api/internal/building/checklists",
    tags=["building-checklists"],
)


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


class ChecklistItemInput(BaseModel):
    label: str = Field(min_length=1, max_length=512)
    is_required: bool = True
    actor: str = Field(min_length=1, max_length=255)


class ChecklistItemStatusInput(BaseModel):
    status: Literal["pending", "completed", "waived"]
    reason: str = Field(default="", max_length=2000)
    actor: str = Field(min_length=1, max_length=255)

    @model_validator(mode="after")
    def waiver_requires_reason(self) -> "ChecklistItemStatusInput":
        if self.status == "waived" and not self.reason.strip():
            raise ValueError("Waiving an operational item requires a reason.")
        return self


def _item_payload(row: BuildingOperationalChecklistItem) -> dict[str, Any]:
    return {
        "id": row.id,
        "label": row.label,
        "status": row.status,
        "is_required": row.is_required,
        "sort_order": row.sort_order,
        "completion_reason": row.completion_reason,
        "completed_by": row.completed_by,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
    }


def _checklist_payload(
    row: BuildingOperationalChecklist,
    items: list[BuildingOperationalChecklistItem],
) -> dict[str, Any]:
    return {
        "id": row.id,
        "reservation_id": row.reservation_id,
        "checklist_type": row.checklist_type,
        "title": row.title,
        "status": row.status,
        "assigned_owner": row.assigned_owner,
        "due_at": row.due_at.isoformat() if row.due_at else None,
        "completed_by": row.completed_by,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "items": [_item_payload(item) for item in items],
    }


def _refresh_checklist_status(
    checklist: BuildingOperationalChecklist,
    items: list[BuildingOperationalChecklistItem],
    *,
    actor: str,
) -> None:
    required = [item for item in items if item.is_required]
    complete = bool(required) and all(
        item.status in {"completed", "waived"} for item in required
    )
    checklist.status = "completed" if complete else "open"
    checklist.updated_at = _now()
    if complete:
        checklist.completed_by = actor
        checklist.completed_at = checklist.completed_at or _now()
    else:
        checklist.completed_by = ""
        checklist.completed_at = None


@router.get("")
def list_checklists(
    request: Request,
    reservation_id: str = "",
    status: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        query = select(BuildingOperationalChecklist).order_by(
            BuildingOperationalChecklist.due_at,
            BuildingOperationalChecklist.created_at,
        )
        if reservation_id:
            query = query.where(
                BuildingOperationalChecklist.reservation_id == reservation_id
            )
        if status:
            query = query.where(BuildingOperationalChecklist.status == status)
        rows = session.execute(query).scalars().all()
        result = []
        for row in rows:
            items = session.execute(
                select(BuildingOperationalChecklistItem)
                .where(BuildingOperationalChecklistItem.checklist_id == row.id)
                .order_by(BuildingOperationalChecklistItem.sort_order)
            ).scalars().all()
            result.append(_checklist_payload(row, items))
        return {"checklists": result}


@router.post("/{checklist_id}/items", status_code=201)
def add_checklist_item(
    checklist_id: str,
    payload: ChecklistItemInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        checklist = session.get(BuildingOperationalChecklist, checklist_id)
        if checklist is None:
            raise HTTPException(status_code=404, detail="Checklist not found.")
        max_order = max(
            (
                item.sort_order
                for item in session.execute(
                    select(BuildingOperationalChecklistItem).where(
                        BuildingOperationalChecklistItem.checklist_id == checklist_id
                    )
                ).scalars().all()
            ),
            default=0,
        )
        row = BuildingOperationalChecklistItem(
            id=str(uuid4()),
            checklist_id=checklist_id,
            label=payload.label.strip(),
            is_required=payload.is_required,
            sort_order=max_order + 1,
        )
        if payload.is_required:
            checklist.status = "open"
            checklist.completed_by = ""
            checklist.completed_at = None
        checklist.updated_at = _now()
        session.add(row)
        session.add(
            BuildingAuditEvent(
                entity_type="operational_checklist_item",
                entity_id=row.id,
                action="checklist_item_added",
                actor=payload.actor,
                after_json={
                    "checklist_id": checklist_id,
                    "label": row.label,
                    "is_required": row.is_required,
                },
            )
        )
        return {"ok": True, "item": _item_payload(row)}


@router.post("/items/{item_id}/status")
def update_checklist_item_status(
    item_id: str,
    payload: ChecklistItemStatusInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingOperationalChecklistItem, item_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Checklist item not found.")
        checklist = session.get(BuildingOperationalChecklist, row.checklist_id)
        if checklist is None:
            raise HTTPException(status_code=409, detail="Checklist record is missing.")
        before = {
            "status": row.status,
            "completion_reason": row.completion_reason,
            "completed_by": row.completed_by,
        }
        row.status = payload.status
        row.completion_reason = payload.reason.strip()
        row.updated_at = _now()
        if payload.status in {"completed", "waived"}:
            row.completed_by = payload.actor
            row.completed_at = _now()
        else:
            row.completed_by = ""
            row.completed_at = None
        items = session.execute(
            select(BuildingOperationalChecklistItem).where(
                BuildingOperationalChecklistItem.checklist_id == checklist.id
            )
        ).scalars().all()
        _refresh_checklist_status(checklist, items, actor=payload.actor)
        session.add(
            BuildingAuditEvent(
                entity_type="operational_checklist_item",
                entity_id=row.id,
                action=f"checklist_item_{payload.status}",
                actor=payload.actor,
                before_json=before,
                after_json={
                    "status": row.status,
                    "completion_reason": row.completion_reason,
                    "checklist_status": checklist.status,
                },
            )
        )
        return {
            "ok": True,
            "item": _item_payload(row),
            "checklist_status": checklist.status,
        }
