"""Month availability for the Arena, read from the records that already decide it.

The lead page needs to show which dates are open before anyone types a date into
a box. This reads the same two sources the hold path checks — Agent's own
reservations and the Anata Events calendar — so the calendar can never say a
date is open that the hold would then refuse.

Nothing here writes. Nothing here exposes what an outside calendar event
actually is; an occupied day is occupied and that is all a lead page needs to
know.
"""

from __future__ import annotations

import calendar as _calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from sales_support_agent.integrations.building_google_calendar import (
    BuildingCalendarAdapter,
)
from sales_support_agent.models.entities import BuildingReservation

MOUNTAIN = ZoneInfo("America/Denver")

#: Hours of building time added before guests arrive and after they leave.
#: Approved Arena commercial terms include two self-service hours on each side.
SETUP_BUFFER_HOURS = 2
TEARDOWN_BUFFER_HOURS = 2

#: Setup through teardown at or beyond this length is a full day, and a full day
#: makes its neighbours a judgement call rather than a free date.
FULL_DAY_HOURS = 12

#: A date is still being won. It holds the room but nothing is signed.
PENDING_STATUSES = frozenset({
    "requirements_review", "soft_hold", "quote_sent", "contract_pending",
    "deposit_due",
})
#: A date is won. Moving it costs a conversation with a customer.
BOOKED_STATUSES = frozenset({
    "confirmed", "pre_event", "occupied", "renewal", "completed",
})
#: Neither holds the room.
RELEASED_STATUSES = frozenset({"cancelled", "expired", "move_out"})

#: Guest arrival and departure are chosen from whole hours.
FIRST_GUEST_HOUR = 8
LAST_GUEST_HOUR = 23


def access_window(
    day: date, guest_start: time, guest_end: time
) -> tuple[datetime, datetime, datetime, datetime]:
    """Return setup, guests-in, guests-out and teardown for a chosen day.

    Setup and teardown are derived, not asked for. An event that runs past
    midnight carries its guest end and teardown into the following day rather
    than folding back onto itself.
    """

    guests_in = datetime.combine(day, guest_start, tzinfo=MOUNTAIN)
    guests_out = datetime.combine(day, guest_end, tzinfo=MOUNTAIN)
    if guests_out <= guests_in:
        guests_out += timedelta(days=1)
    setup = guests_in - timedelta(hours=SETUP_BUFFER_HOURS)
    teardown = guests_out + timedelta(hours=TEARDOWN_BUFFER_HOURS)
    return setup, guests_in, guests_out, teardown


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _days_touched(starts_at: datetime, ends_at: datetime) -> list[date]:
    """Every Mountain-time calendar day an occupied window actually occupies.

    A window that ends exactly at midnight stops at the previous day. Counting
    the boundary would let a nine o'clock event close the whole day after it.
    """

    start = _aware(starts_at).astimezone(MOUNTAIN).date()
    finish = _aware(ends_at).astimezone(MOUNTAIN)
    end = finish.date()
    if finish.timetz().replace(tzinfo=None) == time.min and end > start:
        end -= timedelta(days=1)
    span = (end - start).days
    return [start + timedelta(days=offset) for offset in range(max(span, 0) + 1)]


def _is_full_day(starts_at: datetime, ends_at: datetime) -> bool:
    hours = (_aware(ends_at) - _aware(starts_at)).total_seconds() / 3600
    return hours >= FULL_DAY_HOURS


def _google_span(payload: dict[str, Any]) -> Optional[tuple[date, date]]:
    """Read a Google start/end pair as Mountain-time dates, all-day included."""

    start = dict(payload.get("start") or {})
    end = dict(payload.get("end") or {})
    if start.get("date"):
        try:
            first = date.fromisoformat(str(start["date"]))
            # Google's all-day end date is exclusive.
            last = date.fromisoformat(str(end.get("date") or start["date"]))
        except ValueError:
            return None
        return first, max(first, last - timedelta(days=1))
    if start.get("dateTime"):
        try:
            first_dt = datetime.fromisoformat(str(start["dateTime"]))
            last_dt = datetime.fromisoformat(
                str(end.get("dateTime") or start["dateTime"])
            )
        except ValueError:
            return None
        return (
            _aware(first_dt).astimezone(MOUNTAIN).date(),
            _aware(last_dt).astimezone(MOUNTAIN).date(),
        )
    return None


def month_grid(month: date) -> list[date]:
    """Whole weeks covering the month, Monday first, as the page renders them."""

    first = month.replace(day=1)
    last = first.replace(day=_calendar.monthrange(first.year, first.month)[1])
    start = first - timedelta(days=first.weekday())
    end = last + timedelta(days=(6 - last.weekday()))
    return [
        start + timedelta(days=offset) for offset in range((end - start).days + 1)
    ]


def _reservation_rows(
    session: Session, *, space_id: str, first: date, last: date
) -> list[BuildingReservation]:
    """Reservations touching the window, plus a day either side for neighbours."""

    window_start = datetime.combine(
        first - timedelta(days=2), time.min, tzinfo=MOUNTAIN
    )
    window_end = datetime.combine(last + timedelta(days=2), time.max, tzinfo=MOUNTAIN)
    rows = session.execute(
        select(BuildingReservation).where(
            BuildingReservation.space_id == space_id,
            BuildingReservation.starts_at < window_end.astimezone(timezone.utc),
            or_(
                BuildingReservation.ends_at.is_(None),
                BuildingReservation.ends_at > window_start.astimezone(timezone.utc),
            ),
        )
    ).scalars().all()
    return [row for row in rows if str(row.status or "") not in RELEASED_STATUSES]


def month_availability(
    session: Session,
    *,
    calendar: Optional[BuildingCalendarAdapter],
    month: date,
    space_id: str,
    exclude_inquiry_id: str = "",
    requested: Iterable[date] = (),
) -> dict[str, Any]:
    """Describe every day on the month grid.

    Each day carries one of ``booked``, ``pending``, ``external``, ``heads_up``
    or ``open``. ``heads_up`` is the neighbour of a full day: still selectable,
    because the owner decided that rule is a warning rather than a wall.
    """

    days = month_grid(month)
    first, last = days[0], days[-1]
    states: dict[date, str] = {day: "open" for day in days}
    notes: dict[date, str] = {}
    busy: dict[date, list[tuple[datetime, datetime]]] = {day: [] for day in days}
    full_days: set[date] = set()

    for row in _reservation_rows(
        session, space_id=space_id, first=first, last=last
    ):
        if exclude_inquiry_id and str(row.inquiry_id or "") == exclude_inquiry_id:
            continue
        if row.ends_at is None:
            continue
        occupied = "booked" if str(row.status or "") in BOOKED_STATUSES else "pending"
        if _is_full_day(row.starts_at, row.ends_at):
            full_days.update(_days_touched(row.starts_at, row.ends_at))
        for day in _days_touched(row.starts_at, row.ends_at):
            if day in busy:
                busy[day].append((
                    _aware(row.starts_at).astimezone(MOUNTAIN),
                    _aware(row.ends_at).astimezone(MOUNTAIN),
                ))
            if day in states and not (states[day] == "booked" and occupied == "pending"):
                states[day] = occupied
                notes[day] = (
                    "Booked" if occupied == "booked" else "Held, not yet signed"
                )

    calendar_status = "unavailable"
    if calendar is not None and getattr(calendar, "configured", False):
        span_start = datetime.combine(first, time.min, tzinfo=MOUNTAIN)
        span_end = datetime.combine(last, time.max, tzinfo=MOUNTAIN)
        try:
            conflicts = calendar.find_conflicts(
                starts_at=span_start, ends_at=span_end
            )
            calendar_status = "connected"
        except Exception:
            conflicts = []
            calendar_status = "unknown"
        for conflict in conflicts:
            span = _google_span(conflict)
            if span is None:
                continue
            begin, finish = span
            cursor = begin
            while cursor <= finish:
                if cursor in states and states[cursor] == "open":
                    states[cursor] = "external"
                    notes[cursor] = "Busy on the Anata Events calendar"
                cursor += timedelta(days=1)

    # Neighbours of a full day stay selectable and say why they are worth a look.
    for day in sorted(full_days):
        for neighbour in (day - timedelta(days=1), day + timedelta(days=1)):
            if neighbour in full_days or neighbour not in states:
                continue
            if states[neighbour] == "open":
                states[neighbour] = "heads_up"
                notes[neighbour] = "Next to a full-day event"

    today = datetime.now(MOUNTAIN).date()
    # The dates the prospect actually asked for stay visible. Replacing the
    # three-date form with a calendar must not lose their alternates.
    asked_for = {item for item in requested if item is not None}
    cells = [
        {
            "date": day,
            "iso": day.isoformat(),
            "day": day.day,
            "in_month": day.month == month.month,
            "is_today": day == today,
            "is_past": day < today,
            "state": states[day],
            "note": notes.get(day, ""),
            "requested": day in asked_for,
            "occupied": states[day] in {"pending", "booked", "external"},
            # Any future day can be chosen, including an occupied one. Taking it
            # is the owner's call; the picker names the clash before they do.
            "selectable": day >= today,
        }
        for day in days
    ]
    return {
        "month": month.replace(day=1),
        "label": month.strftime("%B %Y"),
        "previous": (month.replace(day=1) - timedelta(days=1)).replace(day=1),
        "next": (
            month.replace(day=1) + timedelta(days=32)
        ).replace(day=1),
        "cells": cells,
        "calendar_status": calendar_status,
        "busy": {day.isoformat(): spans for day, spans in busy.items()},
    }


def guest_hour_options(
    busy_spans: Iterable[tuple[datetime, datetime]], day: date
) -> list[dict[str, Any]]:
    """Whole-hour arrival and departure options, marking hours already taken."""

    taken: set[int] = set()
    for begin, finish in busy_spans:
        cursor = begin
        while cursor < finish:
            if cursor.date() == day:
                taken.add(cursor.hour)
            cursor += timedelta(hours=1)
    return [
        {
            "value": f"{hour:02d}:00",
            "label": clock_label(hour),
            "taken": hour in taken,
        }
        for hour in range(FIRST_GUEST_HOUR, LAST_GUEST_HOUR + 1)
    ]


def clock_label(hour: int) -> str:
    """"14" reads as "2:00 PM". Written out because %-I is not portable."""

    suffix = "AM" if hour < 12 else "PM"
    twelve = hour % 12 or 12
    return f"{twelve}:00 {suffix}"
