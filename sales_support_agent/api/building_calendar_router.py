"""Internal calendar projection queue and controlled Google Calendar sync."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from typing import Any, Optional
from uuid import uuid4

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
    dry_run: bool = True
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
        "target_calendar_id": row.target_calendar_id,
        "operation_key": row.operation_key,
        "payload_checksum": row.payload_checksum,
        "attempt_count": row.attempt_count,
        "claim_token": row.claim_token,
        "claimed_at": row.claimed_at.isoformat() if row.claimed_at else None,
        "next_attempt_at": (
            row.next_attempt_at.isoformat() if row.next_attempt_at else None
        ),
        "last_error": row.last_error,
        "last_attempt_at": (
            row.last_attempt_at.isoformat() if row.last_attempt_at else None
        ),
        "synced_at": row.synced_at.isoformat() if row.synced_at else None,
        "delivered_at": (
            row.delivered_at.isoformat() if row.delivered_at else None
        ),
        "reconciled_at": (
            row.reconciled_at.isoformat() if row.reconciled_at else None
        ),
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
        adapter = BuildingGoogleCalendarClient()
        return {
            "configured": adapter.configured,
            "dry_run_default": True,
            "writes_enabled": _writes_enabled(),
            "provider": adapter.provider,
            "target_calendar_id": adapter.target_calendar_id,
            "readiness_error": adapter.readiness_error,
            "projections": [_projection_payload(row) for row in rows],
        }


def _writes_enabled() -> bool:
    return os.getenv("BUILDING_GOOGLE_CALENDAR_WRITES_ENABLED", "").strip().lower() in {
        "1", "true", "yes", "on",
    }


@router.get("/readiness")
def calendar_readiness(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Expose non-secret delivery readiness and queue counts."""

    _require_internal_key(request, x_internal_api_key)
    adapter = BuildingGoogleCalendarClient()
    with session_scope(request.app.state.session_factory) as session:
        counts = {
            status: len(session.execute(
                select(BuildingCalendarProjection).where(
                    BuildingCalendarProjection.status == status
                )
            ).scalars().all())
            for status in ("pending", "claimed", "error", "synced")
        }
    return {
        "provider": adapter.provider,
        "configured": adapter.configured,
        "target_calendar_id": adapter.target_calendar_id,
        "dedicated_calendar": bool(
            adapter.target_calendar_id
            and adapter.target_calendar_id.lower() != "primary"
        ),
        "dry_run_default": True,
        "writes_enabled": _writes_enabled(),
        "readiness_error": adapter.readiness_error,
        "counts": counts,
    }


@router.post("/sync")
def sync_calendar_projections(
    payload: CalendarSyncInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    client = BuildingGoogleCalendarClient()
    execute = bool(payload.execute and not payload.dry_run)
    with session_scope(request.app.state.session_factory) as session:
        stale_before = _now() - timedelta(minutes=10)
        rows = session.execute(
            select(BuildingCalendarProjection)
            .where(
                (
                    BuildingCalendarProjection.status.in_(("pending", "error"))
                )
                | (
                    (BuildingCalendarProjection.status == "claimed")
                    & (
                        BuildingCalendarProjection.claimed_at.is_(None)
                        | (BuildingCalendarProjection.claimed_at < stale_before)
                    )
                ),
                (
                    BuildingCalendarProjection.next_attempt_at.is_(None)
                    | (BuildingCalendarProjection.next_attempt_at <= _now())
                ),
            )
            .order_by(BuildingCalendarProjection.updated_at)
            .limit(payload.max_items)
            .with_for_update(skip_locked=True)
        ).scalars().all()
        preview = [_projection_payload(row) for row in rows]
        if not execute:
            return {
                "ok": True,
                "execute": False,
                "dry_run": True,
                "configured": client.configured,
                "writes_enabled": _writes_enabled(),
                "target_calendar_id": client.target_calendar_id,
                "readiness_error": client.readiness_error,
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
        if not _writes_enabled():
            raise HTTPException(
                status_code=503,
                detail=(
                    "Building calendar writes are disabled. Dry-run remains "
                    "available and no external write was attempted."
                ),
            )
        mismatched = [
            row for row in rows
            if row.target_calendar_id
            and row.target_calendar_id != client.target_calendar_id
        ]
        if mismatched:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Pending projections target a different calendar. Requeue "
                    "them explicitly before delivery; no write was attempted."
                ),
            )

        claim_token = uuid4().hex
        claimed_ids: list[str] = []
        for row in rows:
            row.status = "claimed"
            row.claim_token = claim_token
            row.claimed_at = _now()
            row.target_calendar_id = client.target_calendar_id
            row.updated_at = _now()
            claimed_ids.append(row.id)
        session.flush()

    # The claim commits before any adapter call. Each claimed row then reconciles
    # independently so a crash leaves a reclaimable, auditable state.
    synced = 0
    failed = 0
    results: list[dict[str, Any]] = []
    for projection_id in claimed_ids:
        with session_scope(request.app.state.session_factory) as session:
            row = session.get(BuildingCalendarProjection, projection_id)
            if row is None or row.status != "claimed" or row.claim_token != claim_token:
                continue
            reservation = session.get(BuildingReservation, row.reservation_id)
            row.attempt_count += 1
            row.last_attempt_at = _now()
            before = {
                "status": row.status,
                "desired_action": row.desired_action,
                "provider_event_id": row.provider_event_id,
                "claim_token": row.claim_token,
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
                row.delivered_at = _now()
                row.synced_at = _now()
                row.reconciled_at = _now()
                row.next_attempt_at = None
                synced += 1
            except Exception as exc:
                row.status = "error"
                row.last_error = str(exc)[:2000]
                row.next_attempt_at = _now() + timedelta(
                    minutes=min(60, 2 ** min(row.attempt_count, 5))
                )
                failed += 1
            row.claim_token = ""
            row.claimed_at = None
            row.updated_at = _now()
            session.add(BuildingAuditEvent(
                entity_type="calendar_projection",
                entity_id=row.id,
                action=f"calendar_{row.desired_action}_{row.status}",
                actor=payload.actor,
                before_json=before,
                after_json={
                    "status": row.status,
                    "provider": client.provider,
                    "target_calendar_id": client.target_calendar_id,
                    "operation_key": row.operation_key,
                    "provider_event_id": row.provider_event_id,
                    "error": row.last_error,
                },
            ))
            results.append(_projection_payload(row))
    return {
        "ok": failed == 0,
        "execute": True,
        "dry_run": False,
        "claim_token": claim_token,
        "synced_count": synced,
        "failed_count": failed,
        "projections": results,
    }
