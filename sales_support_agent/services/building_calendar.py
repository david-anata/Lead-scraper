"""Calendar projection outbox helpers for building reservations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAvailabilityBlock,
    BuildingCalendarProjection,
    BuildingReservation,
    BuildingSpace,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def projection_payload(
    reservation: BuildingReservation,
    space: BuildingSpace,
) -> dict[str, Any]:
    """Build an operator-safe Google Calendar event from an Agent reservation."""

    status = reservation.status.replace("_", " ").title()
    return {
        "summary": f"Anata Building — {space.name}",
        "description": (
            f"Agent reservation: {reservation.id}\n"
            f"Status: {status}\n"
            f"Kind: {reservation.kind.title()}\n"
            f"Attendance: {reservation.attendance}\n"
            f"Owner: {reservation.assigned_owner or 'Unassigned'}\n\n"
            "Agent is the booking source of truth. Calendar edits do not change "
            "the reservation."
        ),
        "location": f"{space.name}{f', {space.floor}' if space.floor else ''}",
        "start": {
            "dateTime": reservation.starts_at.isoformat(),
            "timeZone": "America/Denver",
        },
        "end": {
            "dateTime": reservation.ends_at.isoformat(),
            "timeZone": "America/Denver",
        },
        "extendedProperties": {
            "private": {
                "anataReservationId": reservation.id,
                "anataReservationStatus": reservation.status,
            }
        },
        "transparency": "opaque",
    }


def queue_calendar_projection(session, reservation: BuildingReservation) -> None:
    """Queue an upsert/delete without making an external write in the transaction."""

    existing = session.execute(
        select(BuildingCalendarProjection).where(
            BuildingCalendarProjection.reservation_id == reservation.id
        )
    ).scalar_one_or_none()
    terminal_delete = reservation.status in {"cancelled", "expired"}
    block = session.execute(
        select(BuildingAvailabilityBlock).where(
            BuildingAvailabilityBlock.source_reference
            == f"reservation:{reservation.id}"
        )
    ).scalar_one_or_none()
    should_project = bool(
        block
        or reservation.status
        in {
            "soft_hold",
            "quote_sent",
            "contract_pending",
            "deposit_due",
            "confirmed",
            "pre_event",
            "occupied",
            "renewal",
            "move_out",
            "completed",
        }
        or existing
    )
    if not should_project:
        return

    row = existing or BuildingCalendarProjection(
        id=str(uuid4()),
        reservation_id=reservation.id,
    )
    row.desired_action = "delete" if terminal_delete else "upsert"
    if row.desired_action == "upsert":
        space = session.get(BuildingSpace, reservation.space_id)
        if space is None:
            return
        row.payload_json = projection_payload(reservation, space)
    row.status = "pending"
    row.last_error = ""
    row.updated_at = _now()
    session.add(row)
