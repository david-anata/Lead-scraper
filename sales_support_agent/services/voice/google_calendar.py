"""Tour calendar via a Google OAuth refresh token (free/busy + event insert).

Scopes: https://www.googleapis.com/auth/calendar.events and
https://www.googleapis.com/auth/calendar.freebusy
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests

from sales_support_agent.services.voice.config import VoiceConfig

TOKEN_URL = "https://oauth2.googleapis.com/token"
API = "https://www.googleapis.com/calendar/v3"
SCOPES = (
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/calendar.freebusy",
)


class CalendarError(RuntimeError):
    pass


def _access_token(config: VoiceConfig) -> str:
    if not config.calendar_ready:
        raise CalendarError("calendar_not_configured")
    response = requests.post(
        TOKEN_URL,
        data={
            "client_id": config.google_client_id,
            "client_secret": config.google_client_secret,
            "refresh_token": config.google_refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=10,
    )
    if response.status_code >= 400:
        raise CalendarError(f"token_http_{response.status_code}")
    return response.json()["access_token"]


def busy_intervals(config: VoiceConfig, start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    token = _access_token(config)
    response = requests.post(
        f"{API}/freeBusy",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "timeMin": start.astimezone(timezone.utc).isoformat(),
            "timeMax": end.astimezone(timezone.utc).isoformat(),
            "items": [{"id": config.tour_calendar_id}],
        },
        timeout=10,
    )
    if response.status_code >= 400:
        raise CalendarError(f"freebusy_http_{response.status_code}")
    busy = response.json().get("calendars", {}).get(config.tour_calendar_id, {}).get("busy", [])
    return [
        (datetime.fromisoformat(b["start"].replace("Z", "+00:00")), datetime.fromisoformat(b["end"].replace("Z", "+00:00")))
        for b in busy
    ]


def candidate_slots(config: VoiceConfig, start_day: date, days: int) -> list[datetime]:
    zone = ZoneInfo(config.tour_timezone)
    step = timedelta(minutes=config.tour_minutes)
    slots: list[datetime] = []
    for offset in range(days):
        day = start_day + timedelta(days=offset)
        if day.weekday() >= 5:
            continue
        cursor = datetime.combine(day, time(config.tour_start_hour), zone)
        close = datetime.combine(day, time(config.tour_end_hour), zone)
        while cursor + step <= close:
            slots.append(cursor)
            cursor += step
    return slots


def free_slots(
    config: VoiceConfig,
    *,
    start_day: Optional[date] = None,
    days: int = 5,
    limit: int = 6,
    now: Optional[datetime] = None,
    busy: Optional[list[tuple[datetime, datetime]]] = None,
) -> list[datetime]:
    zone = ZoneInfo(config.tour_timezone)
    now = now or datetime.now(timezone.utc)
    first = start_day or (now.astimezone(zone).date() + timedelta(days=1))
    candidates = [s for s in candidate_slots(config, first, max(1, min(days, 14))) if s > now + timedelta(hours=2)]
    if not candidates:
        return []
    step = timedelta(minutes=config.tour_minutes)
    if busy is None:
        busy = busy_intervals(config, candidates[0], candidates[-1] + step)
    open_slots = [s for s in candidates if not any(s < b_end and s + step > b_start for b_start, b_end in busy)]
    return open_slots[:limit]


def insert_tour_event(config: VoiceConfig, *, start: datetime, summary: str, description: str) -> dict:
    token = _access_token(config)
    end = start + timedelta(minutes=config.tour_minutes)
    response = requests.post(
        f"{API}/calendars/{requests.utils.quote(config.tour_calendar_id, safe='')}/events",
        params={"sendUpdates": "none"},
        headers={"Authorization": f"Bearer {token}"},
        json={
            "summary": summary,
            "description": description,
            "location": config.tour_location,
            "start": {"dateTime": start.isoformat(), "timeZone": config.tour_timezone},
            "end": {"dateTime": end.isoformat(), "timeZone": config.tour_timezone},
        },
        timeout=10,
    )
    if response.status_code >= 400:
        raise CalendarError(f"insert_http_{response.status_code}")
    return response.json()
