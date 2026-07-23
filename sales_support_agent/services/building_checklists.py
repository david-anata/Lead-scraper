"""Default operational checklist creation for confirmed building workflows."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingOperationalChecklist,
    BuildingOperationalChecklistItem,
    BuildingReservation,
)


DEFAULT_ITEMS = {
    "event_readiness": [
        "Confirm final schedule, attendance, setup, and teardown window",
        "Confirm room layout, furniture, and included equipment",
        "Review signed requirements, insurance, and approved exceptions",
        "Confirm vendor, guest, and building-access plan",
        "Assign opening, onsite, and closing responsibilities",
        "Record post-event inspection and closeout notes",
    ],
    "move_in": [
        "Confirm signed agreement, deposit, and billing readiness",
        "Confirm the space is clean, furnished, and ready as agreed",
        "Record access and key handoff",
        "Confirm billing, operational, and community contacts",
        "Complete the welcome walkthrough and record exceptions",
    ],
    "move_out": [
        "Confirm the final occupancy date and access-return plan",
        "Complete the final space condition walkthrough",
        "Review final balance, credits, deposits, and open exceptions",
        "Update occupancy and reviewed public availability",
        "Close, renew, or update the tenant relationship",
    ],
    "renewal": [
        "Confirm the tenant's renewal, expansion, transfer, or move-out decision",
        "Review term, pricing, space, and approved exceptions",
        "Prepare and complete the correct agreement or amendment",
        "Review deposit, billing schedule, and accounting handoff",
        "Update occupancy dates, relationships, and future availability",
    ],
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _target_type(reservation: BuildingReservation) -> str:
    if reservation.kind == "event" and reservation.status in {
        "confirmed",
        "pre_event",
        "completed",
    }:
        return "event_readiness"
    if reservation.kind == "workspace" and reservation.status == "renewal":
        return "renewal"
    if reservation.kind == "workspace" and reservation.status in {
        "confirmed",
        "occupied",
    }:
        return "move_in"
    if reservation.kind == "workspace" and reservation.status in {
        "move_out",
        "completed",
    }:
        return "move_out"
    return ""


def ensure_operational_checklist(
    session,
    reservation: BuildingReservation,
    *,
    actor: str,
) -> BuildingOperationalChecklist | None:
    """Create one idempotent default checklist when a workflow reaches operations."""

    checklist_type = _target_type(reservation)
    if not checklist_type:
        return None
    existing = session.execute(
        select(BuildingOperationalChecklist).where(
            BuildingOperationalChecklist.reservation_id == reservation.id,
            BuildingOperationalChecklist.checklist_type == checklist_type,
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    title_map = {
        "event_readiness": "Event readiness and closeout",
        "move_in": "Workspace move-in",
        "move_out": "Workspace move-out",
        "renewal": "Workspace renewal",
    }
    row = BuildingOperationalChecklist(
        id=str(uuid4()),
        reservation_id=reservation.id,
        checklist_type=checklist_type,
        title=title_map[checklist_type],
        assigned_owner=reservation.assigned_owner,
        due_at=reservation.starts_at,
        created_by=actor,
        updated_at=_now(),
    )
    session.add(row)
    for position, label in enumerate(DEFAULT_ITEMS[checklist_type], start=1):
        session.add(
            BuildingOperationalChecklistItem(
                id=str(uuid4()),
                checklist_id=row.id,
                label=label,
                sort_order=position,
            )
        )
    session.add(
        BuildingAuditEvent(
            entity_type="operational_checklist",
            entity_id=row.id,
            action="checklist_created",
            actor=actor,
            after_json={
                "reservation_id": reservation.id,
                "checklist_type": checklist_type,
                "item_count": len(DEFAULT_ITEMS[checklist_type]),
            },
        )
    )
    return row
