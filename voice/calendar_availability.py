"""Read-only check against the Anata events Google Calendar.

Event titles stay on the server. The model only hears whether the day has a
conflict, or that the calendar could not be read. A clear day is not a booking.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Callable
from urllib.parse import quote

from voice.knowledge import DENVER, hours_guidance, parse_iso_date

logger = logging.getLogger(__name__)

CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"
FetchEvents = Callable[[str, str], list[dict[str, Any]]]


class CalendarAvailability:
    def __init__(
        self,
        *,
        calendar_id: str,
        service_account_json: str,
        delegated_subject: str = "",
        fetch_events: FetchEvents | None = None,
    ) -> None:
        self.calendar_id = (calendar_id or "").strip()
        self.service_account_json = (service_account_json or "").strip()
        self.delegated_subject = (delegated_subject or "").strip()
        self._fetch = fetch_events
        self._session: Any | None = None

    @property
    def configured(self) -> bool:
        return bool(self.calendar_id and (self.service_account_json or self._fetch))

    def check(self, raw_date: str, style: str = "unsure") -> dict[str, object]:
        parsed = parse_iso_date(raw_date)
        if parsed is None:
            return {
                "status": "invalid_date",
                "speak": "Ask for the date as a calendar day. Do not guess a date.",
            }
        today = datetime.now(DENVER).date()
        guidance = hours_guidance(parsed, style)
        if parsed < today:
            return {
                "status": "past",
                "date": parsed.isoformat(),
                "hours": guidance,
                "speak": "That date is in the past. Ask for another date. Do not say it is available.",
            }
        if parsed > today + timedelta(days=730):
            return {
                "status": "too_far",
                "date": parsed.isoformat(),
                "hours": guidance,
                "speak": "That date is too far out to check. Take it down and say the team will confirm.",
            }
        if not self.configured:
            return {
                "status": "unavailable",
                "date": parsed.isoformat(),
                "hours": guidance,
                "speak": (
                    "You cannot see the events calendar. Take the date and say the team "
                    "will confirm. Do not say the date is open or booked."
                ),
            }
        start, end = _day_window(parsed)
        try:
            events = self._events(start, end)
        except Exception:
            logger.exception("Event calendar check failed")
            return {
                "status": "unavailable",
                "date": parsed.isoformat(),
                "hours": guidance,
                "speak": (
                    "The calendar check failed. Take the date and say the team will confirm. "
                    "Do not say the date is open or booked."
                ),
            }
        blocking = [event for event in events if _blocks(event)]
        if blocking:
            return {
                "status": "busy",
                "date": parsed.isoformat(),
                "conflict_count": len(blocking),
                "hours": guidance,
                "speak": (
                    "That date already has something on the events calendar. Do not describe "
                    "the other event. Offer to take a backup date for the team to confirm."
                ),
            }
        return {
            "status": "clear",
            "date": parsed.isoformat(),
            "conflict_count": 0,
            "hours": guidance,
            "speak": (
                "Nothing else is on the events calendar that day. Say that it looks clear "
                "and that it is not reserved until the team confirms the agreement and the "
                "50% payment. Do not say you booked it."
            ),
        }

    def _events(self, start: datetime, end: datetime) -> list[dict[str, Any]]:
        if self._fetch is not None:
            return self._fetch(start.isoformat(), end.isoformat())
        return self._google_fetch(start, end)

    def _google_fetch(self, start: datetime, end: datetime) -> list[dict[str, Any]]:
        session = self._authorized()
        url = (
            "https://www.googleapis.com/calendar/v3/calendars/"
            f"{quote(self.calendar_id, safe='')}/events"
        )
        params: dict[str, Any] = {
            "timeMin": start.isoformat(),
            "timeMax": end.isoformat(),
            "singleEvents": "true",
            "showDeleted": "false",
            "maxResults": 250,
            "fields": "items(status,transparency,start,end),nextPageToken",
        }
        found: list[dict[str, Any]] = []
        while True:
            response = session.get(url, params=params, timeout=20)
            response.raise_for_status()
            body = response.json()
            found.extend(body.get("items") or [])
            page = body.get("nextPageToken")
            if not page:
                return found
            params["pageToken"] = page

    def _authorized(self) -> Any:
        if self._session is not None:
            return self._session
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2 import service_account

        info = json.loads(self.service_account_json)
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=[CALENDAR_SCOPE]
        )
        if self.delegated_subject:
            credentials = credentials.with_subject(self.delegated_subject)
        self._session = AuthorizedSession(credentials)
        return self._session


def _day_window(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=DENVER)
    return start, start + timedelta(days=1)


def _blocks(event: dict[str, Any]) -> bool:
    if event.get("status") == "cancelled":
        return False
    if event.get("transparency") == "transparent":
        return False
    return True
