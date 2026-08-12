"""Calendar projection outbox helpers for building reservations."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAvailabilityBlock,
    BuildingCalendarProjection,
    BuildingContact,
    BuildingReservation,
    BuildingSpace,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def configured_target_calendar_id() -> str:
    """Return the dedicated projection target without falling back to primary."""

    return os.getenv("BUILDING_GOOGLE_CALENDAR_ID", "").strip()


def projection_checksum(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def projection_payload(
    reservation: BuildingReservation,
    space: BuildingSpace,
    contact: BuildingContact | None = None,
) -> dict[str, Any]:
    """Build an operator-safe Google Calendar event from an Agent reservation."""

    status = reservation.status.replace("_", " ").title()
    payload = {
        "summary": f"Anata Building — {space.name}",
        "description": (
            f"Agent reservation: {reservation.id}\n"
            f"Status: {status}\n"
            f"Kind: {reservation.kind.title()}\n"
            f"Attendance: {reservation.attendance}\n"
            f"Owner: {reservation.assigned_owner or 'Unassigned'}\n\n"
            "The Anata Events calendar is authoritative for date occupancy. "
            "Agent is authoritative for customer, quote, agreement, and payment evidence."
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
    if (
        contact is not None
        and reservation.status in {"confirmed", "pre_event", "completed"}
        and contact.status == "active"
        and contact.email
    ):
        payload["attendees"] = [{
            "email": contact.email,
            "displayName": contact.full_name,
        }]
    return payload


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
        contact = (
            session.get(BuildingContact, reservation.contact_id)
            if reservation.contact_id
            else None
        )
        row.payload_json = projection_payload(reservation, space, contact)
    row.target_calendar_id = configured_target_calendar_id()
    row.payload_checksum = projection_checksum(dict(row.payload_json or {}))
    row.operation_key = hashlib.sha256(
        (
            f"{reservation.id}:{row.desired_action}:{row.payload_checksum}"
        ).encode()
    ).hexdigest()
    row.status = "pending"
    row.claim_token = ""
    row.claimed_at = None
    row.next_attempt_at = None
    row.last_error = ""
    row.updated_at = _now()
    session.add(row)
