"""Internal calendar projection queue and controlled Google Calendar sync."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from sales_support_agent.integrations.building_google_calendar import (
    BuildingGoogleCalendarClient,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingCalendarProjection,
    BuildingReservation,
)


router = APIRouter(
    prefix="/api/internal/building/calendar",
    tags=["building-calendar"],
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


class CalendarSyncInput(BaseModel):
    execute: bool = False
    max_items: int = Field(default=25, ge=1, le=100)
    actor: str = Field(min_length=1, max_length=255)


def _projection_payload(row: BuildingCalendarProjection) -> dict[str, Any]:
    return {
        "id": row.id,
        "reservation_id": row.reservation_id,
        "provider": row.provider,
        "desired_action": row.desired_action,
        "status": row.status,
        "provider_event_id": row.provider_event_id,
        "attempt_count": row.attempt_count,
        "last_error": row.last_error,
        "last_attempt_at": (
            row.last_attempt_at.isoformat() if row.last_attempt_at else None
        ),
        "synced_at": row.synced_at.isoformat() if row.synced_at else None,
        "updated_at": row.updated_at.isoformat(),
    }


@router.get("/projections")
def list_calendar_projections(
    request: Request,
    status: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        query = select(BuildingCalendarProjection).order_by(
            BuildingCalendarProjection.updated_at.desc()
        )
        if status:
            query = query.where(BuildingCalendarProjection.status == status)
        rows = session.execute(query).scalars().all()
        return {
            "configured": BuildingGoogleCalendarClient().configured,
            "projections": [_projection_payload(row) for row in rows],
        }


@router.post("/sync")
def sync_calendar_projections(
    payload: CalendarSyncInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    client = BuildingGoogleCalendarClient()
    with session_scope(request.app.state.session_factory) as session:
        rows = session.execute(
            select(BuildingCalendarProjection)
            .where(BuildingCalendarProjection.status.in_(("pending", "error")))
            .order_by(BuildingCalendarProjection.updated_at)
            .limit(payload.max_items)
        ).scalars().all()
        preview = [_projection_payload(row) for row in rows]
        if not payload.execute:
            return {
                "ok": True,
                "execute": False,
                "configured": client.configured,
                "pending_count": len(rows),
                "projections": preview,
            }
        if not client.configured:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Building Google Calendar is not configured. No external "
                    "calendar writes were attempted."
                ),
            )

        synced = 0
        failed = 0
        results: list[dict[str, Any]] = []
        for row in rows:
            reservation = session.get(BuildingReservation, row.reservation_id)
            row.attempt_count += 1
            row.last_attempt_at = _now()
            before = {
                "status": row.status,
                "desired_action": row.desired_action,
                "provider_event_id": row.provider_event_id,
            }
            try:
                if row.desired_action == "delete":
                    client.delete_event(
                        row.provider_event_id
                        or (reservation.calendar_event_id if reservation else "")
                    )
                    row.provider_event_id = ""
                    if reservation is not None:
                        reservation.calendar_event_id = ""
                        reservation.updated_at = _now()
                else:
                    if reservation is None:
                        raise RuntimeError("The linked Agent reservation is missing.")
                    event_id = client.upsert_event(
                        reservation_id=row.reservation_id,
                        payload=dict(row.payload_json or {}),
                        provider_event_id=(
                            row.provider_event_id or reservation.calendar_event_id
                        ),
                    )
                    row.provider_event_id = event_id
                    reservation.calendar_event_id = event_id
                    reservation.updated_at = _now()
                row.status = "synced"
                row.last_error = ""
                row.synced_at = _now()
                synced += 1
            except Exception as exc:  # provider errors belong in the retry queue
                row.status = "error"
                row.last_error = str(exc)[:2000]
                failed += 1
            row.updated_at = _now()
            session.add(
                BuildingAuditEvent(
                    entity_type="calendar_projection",
                    entity_id=row.id,
                    action=f"calendar_{row.desired_action}_{row.status}",
                    actor=payload.actor,
                    before_json=before,
                    after_json={
                        "status": row.status,
                        "provider_event_id": row.provider_event_id,
                        "error": row.last_error,
                    },
                )
            )
            results.append(_projection_payload(row))
        return {
            "ok": failed == 0,
            "execute": True,
            "synced_count": synced,
            "failed_count": failed,
            "projections": results,
        }
