"""Deterministic expiration for temporary Anata Building holds."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingAvailabilityBlock,
    BuildingReservation,
)
from sales_support_agent.services.building_calendar import queue_calendar_projection


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def expire_building_holds(
    session_factory,
    *,
    as_of: datetime | None = None,
    dry_run: bool = False,
    actor: str = "job:building-hold-expiration",
) -> dict[str, Any]:
    """Release every soft hold whose approved expiration has passed."""

    now = _aware(as_of or datetime.now(timezone.utc))
    with session_scope(session_factory) as session:
        rows = session.execute(
            select(BuildingReservation)
            .where(BuildingReservation.status == "soft_hold")
            .order_by(BuildingReservation.hold_expires_at, BuildingReservation.id)
        ).scalars().all()
        expired = [
            row
            for row in rows
            if row.hold_expires_at is not None
            and _aware(row.hold_expires_at) <= now
        ]
        preview = [
            {
                "reservation_id": row.id,
                "space_id": row.space_id,
                "hold_expires_at": _aware(row.hold_expires_at).isoformat(),
            }
            for row in expired
        ]
        if dry_run:
            return {
                "ok": True,
                "dry_run": True,
                "expired_count": len(expired),
                "expired": preview,
            }

        for row in expired:
            before = {
                "status": row.status,
                "hold_expires_at": _aware(row.hold_expires_at).isoformat(),
            }
            session.execute(
                delete(BuildingAvailabilityBlock).where(
                    BuildingAvailabilityBlock.source_reference
                    == f"reservation:{row.id}"
                )
            )
            row.status = "expired"
            row.hold_expires_at = None
            row.updated_at = now
            queue_calendar_projection(session, row)
            session.add(BuildingAuditEvent(
                entity_type="reservation",
                entity_id=row.id,
                action="hold_expired_automatically",
                actor=actor,
                before_json=before,
                after_json={
                    "status": "expired",
                    "availability_released": True,
                    "expired_at": now.isoformat(),
                },
            ))
        return {
            "ok": True,
            "dry_run": False,
            "expired_count": len(expired),
            "expired": preview,
        }
