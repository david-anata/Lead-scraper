"""Privacy-safe candidate-date checks for the public Arena inquiry."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from sales_support_agent.integrations.building_google_calendar import (
    BuildingCalendarAdapter,
)
from sales_support_agent.models.entities import BuildingAvailabilityBlock


MOUNTAIN = ZoneInfo("America/Denver")
ARENA_SPACE_ID = "arena"


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _clock(value: str) -> time | None:
    try:
        return time.fromisoformat(str(value or "").strip())
    except ValueError:
        return None


def _window(
    candidate: date,
    *,
    guest_start_time: str,
    guest_end_time: str,
) -> tuple[datetime, datetime, bool]:
    start_clock = _clock(guest_start_time)
    end_clock = _clock(guest_end_time)
    if start_clock is None or end_clock is None:
        start = datetime.combine(candidate, time.min, MOUNTAIN)
        return start, start + timedelta(days=1), False
    start = datetime.combine(candidate, start_clock, MOUNTAIN)
    end = datetime.combine(candidate, end_clock, MOUNTAIN)
    if end <= start:
        end += timedelta(days=1)
    return start, end, True


def _active_blocks(
    session: Session,
    *,
    starts_at: datetime,
    ends_at: datetime,
) -> list[BuildingAvailabilityBlock]:
    now = datetime.now(timezone.utc)
    rows = session.execute(
        select(BuildingAvailabilityBlock).where(
            BuildingAvailabilityBlock.space_id == ARENA_SPACE_ID,
            BuildingAvailabilityBlock.starts_at < ends_at.astimezone(timezone.utc),
            or_(
                BuildingAvailabilityBlock.ends_at.is_(None),
                BuildingAvailabilityBlock.ends_at
                > starts_at.astimezone(timezone.utc),
            ),
        )
    ).scalars().all()
    return [
        row
        for row in rows
        if not (
            row.state == "soft_hold"
            and row.expires_at
            and _aware(row.expires_at) <= now
        )
    ]


def _covers_window(
    start_value: datetime,
    end_value: datetime | None,
    *,
    starts_at: datetime,
    ends_at: datetime,
) -> bool:
    start = _aware(start_value).astimezone(MOUNTAIN)
    end = (
        _aware(end_value).astimezone(MOUNTAIN)
        if end_value is not None
        else datetime.max.replace(tzinfo=MOUNTAIN)
    )
    return start <= starts_at and end >= ends_at


def candidate_date_availability(
    session: Session,
    *,
    calendar: BuildingCalendarAdapter,
    candidates: Iterable[date],
    guest_start_time: str = "",
    guest_end_time: str = "",
) -> dict[str, Any]:
    """Return only available/limited/unavailable/unknown, never event details."""

    unique = list(dict.fromkeys(candidates))[:3]
    checked_at = datetime.now(timezone.utc)
    if not calendar.configured:
        return {
            "calendar_status": "unavailable",
            "checked_at": checked_at.isoformat(),
            "dates": [
                {
                    "date": item.isoformat(),
                    "status": "unknown",
                    "message": "We’ll review this date manually.",
                }
                for item in unique
            ],
        }

    results: list[dict[str, str]] = []
    for candidate in unique:
        starts_at, ends_at, exact_window = _window(
            candidate,
            guest_start_time=guest_start_time,
            guest_end_time=guest_end_time,
        )
        blocks = _active_blocks(
            session, starts_at=starts_at, ends_at=ends_at
        )
        try:
            calendar_conflicts = calendar.find_conflicts(
                starts_at=starts_at,
                ends_at=ends_at,
            )
        except Exception:
            results.append({
                "date": candidate.isoformat(),
                "status": "unknown",
                "message": "We’ll review this date manually.",
            })
            continue

        if exact_window and (blocks or calendar_conflicts):
            status = "unavailable"
            message = "That time is already blocked. Please choose another date or time."
        elif blocks or calendar_conflicts:
            fully_blocked = any(
                _covers_window(
                    row.starts_at,
                    row.ends_at,
                    starts_at=starts_at,
                    ends_at=ends_at,
                )
                for row in blocks
            ) or any(
                "date" in dict(conflict.get("start") or {})
                for conflict in calendar_conflicts
            )
            status = "unavailable" if fully_blocked else "limited"
            message = (
                "That date is already blocked. Please choose another date."
                if fully_blocked
                else "Some time is already blocked; you can still request this date."
            )
        else:
            status = "available"
            message = "No calendar conflict found. Final confirmation follows staff review."
        results.append({
            "date": candidate.isoformat(),
            "status": status,
            "message": message,
        })
    return {
        "calendar_status": "connected",
        "checked_at": checked_at.isoformat(),
        "dates": results,
    }
