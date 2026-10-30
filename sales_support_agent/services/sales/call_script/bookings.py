"""Booked sales calls from calendars and inboxes the app can already read.

Google appointment-schedule events land on the Sales calendar
(abby@anatainc.com). The event title is the schedule name, for example
"Schedule a Meeting with anata (Nicolas Manderfield)". The notification
subject is "Appointment booked". The description starts with "Booked by"
and the form answer, usually Purpose.

The app has no dedicated sales-calendar credential. This module tries the
service accounts it already has, then Appointment booked mail in a configured
Gmail mailbox. ClickUp's Meeting Set checkbox and Zapier are not booking
ledgers here; when both readers fail, the caller reports that gap.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html import unescape
from typing import Any, Optional
from urllib.parse import quote
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

ORGANIZER_EMAIL = "abby@anatainc.com"
CALENDAR_ID = "abby@anatainc.com"
CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar"
DENVER = ZoneInfo("America/Denver")
LOOKBACK = timedelta(hours=18)
LOOKAHEAD = timedelta(days=21)

_CREDENTIALS = (
    ("GOOGLE_SERVICE_ACCOUNT_JSON", "google_service_account_json"),
    ("BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", None),
    ("HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", None),
)
_TAG = re.compile(r"<[^>]+>")
_BREAK = re.compile(r"(?i)<br\s*/?>|</p>|</div>|</li>|</tr>|</h[1-6]>")
_PURPOSE = re.compile(
    r"Purpose\s*(.+?)(?:Have questions about our services|What to expect|Cancel or book|$)",
    re.I | re.S,
)
_BOOKED = re.compile(r"Booked by\s+(.+?)\s+([^\s<>]+@[^\s<>]+)", re.I | re.S)
_SUBJECT_WHEN = re.compile(
    r"@\s+[A-Za-z]{3}\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2}):(\d{2})\s*([ap]m)",
    re.I,
)
_SUBJECT_NAME = re.compile(r"\(([^)]+)\)")
_INTERNAL_DOMAIN = "anatainc.com"


@dataclass(frozen=True)
class Booking:
    """One prospect appointment, independent of which reader found it."""

    event_id: str
    summary: str
    prospect_name: str
    prospect_email: str
    purpose: str
    starts_at: Optional[datetime]
    ends_at: Optional[datetime]
    meet_url: str
    source: str


@dataclass(frozen=True)
class SourceLoad:
    """What the readers returned, plus a gap when nothing could be read."""

    bookings: list[Booking]
    source: str
    gap: str = ""
    attempts: tuple[str, ...] = field(default_factory=tuple)


def html_to_text(value: str) -> str:
    """Turn an appointment description into plain lines."""

    text = _BREAK.sub("\n", value or "")
    text = _TAG.sub(" ", text)
    return unescape(text)


def _clean_line(value: str, limit: int = 500) -> str:
    return " ".join(str(value or "").split())[:limit]


def _purpose(text: str) -> str:
    match = _PURPOSE.search(text or "")
    if not match:
        return ""
    return _clean_line(match.group(1), 800)


def _external_attendee(event: dict) -> tuple[str, str]:
    for person in event.get("attendees") or []:
        if not isinstance(person, dict):
            continue
        email = str(person.get("email") or "").strip().lower()
        if not email or email.endswith("@" + _INTERNAL_DOMAIN):
            continue
        name = _clean_line(person.get("displayName") or "", 255)
        return name, email
    return "", ""


def _meet_url(event: dict) -> str:
    data = event.get("conferenceData") if isinstance(event.get("conferenceData"), dict) else {}
    for entry in data.get("entryPoints") or []:
        if isinstance(entry, dict) and entry.get("entryPointType") == "video":
            return str(entry.get("uri") or "").strip()
    return ""


def _google_time(value: Any) -> Optional[datetime]:
    if not isinstance(value, dict):
        return None
    raw = str(value.get("dateTime") or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def booking_from_event(event: dict, *, source: str = "google_calendar") -> Optional[Booking]:
    """Return a booking when this calendar event is an appointment-schedule hold."""

    if not isinstance(event, dict) or str(event.get("status") or "") == "cancelled":
        return None
    organizer = str((event.get("organizer") or {}).get("email") or "").strip().lower()
    if organizer and organizer != ORGANIZER_EMAIL:
        return None
    description = html_to_text(str(event.get("description") or ""))
    summary = _clean_line(event.get("summary") or "", 300)
    booked = "booked by" in description.lower() or "appointment booked" in summary.lower()
    if not booked:
        return None
    booked_match = _BOOKED.search(description)
    attendee_name, attendee_email = _external_attendee(event)
    if booked_match:
        name = _clean_line(booked_match.group(1), 255)
        email = booked_match.group(2).strip().lower()
    else:
        name, email = attendee_name, attendee_email
    if not name:
        name = attendee_name
    if not email:
        email = attendee_email
    if not name and not email:
        return None
    event_id = str(event.get("id") or "").strip()
    if not event_id:
        return None
    return Booking(
        event_id=event_id,
        summary=summary,
        prospect_name=name,
        prospect_email=email,
        purpose=_purpose(description),
        starts_at=_google_time(event.get("start")),
        ends_at=_google_time(event.get("end")),
        meet_url=_meet_url(event),
        source=source,
    )


def _subject_start(subject: str) -> Optional[datetime]:
    match = _SUBJECT_WHEN.search(subject or "")
    if not match:
        return None
    month_name, day, year, hour, minute, ampm = match.groups()
    try:
        month = datetime.strptime(month_name, "%b").month
        hour_num = int(hour)
        minute_num = int(minute)
    except ValueError:
        return None
    if ampm.lower() == "pm" and hour_num != 12:
        hour_num += 12
    if ampm.lower() == "am" and hour_num == 12:
        hour_num = 0
    try:
        naive = datetime(int(year), month, int(day), hour_num, minute_num)
    except ValueError:
        return None
    return naive.replace(tzinfo=DENVER)


def booking_from_gmail(message: dict) -> Optional[Booking]:
    """Read one Appointment booked notification. The message is not sent onward."""

    from sales_support_agent.integrations.gmail_payloads import extract_body_text, extract_header

    if not isinstance(message, dict):
        return None
    payload = message.get("payload") if isinstance(message.get("payload"), dict) else message
    headers = payload.get("headers") if isinstance(payload.get("headers"), list) else []
    subject = extract_header(headers, "subject") or str(message.get("subject") or "")
    if "appointment booked" not in subject.lower():
        return None
    body = extract_body_text(payload) or str(message.get("snippet") or "")
    text = html_to_text(body)
    booked_match = _BOOKED.search(text)
    name_match = _SUBJECT_NAME.search(subject)
    name = _clean_line(booked_match.group(1), 255) if booked_match else ""
    email = booked_match.group(2).strip().lower() if booked_match else ""
    if not name and name_match:
        candidate = name_match.group(1).strip()
        if "@" not in candidate and candidate.upper() not in {"MDT", "MST", "PDT", "PST", "UTC"}:
            name = _clean_line(candidate, 255)
    message_id = str(message.get("id") or "").strip()
    if not message_id or not name:
        return None
    summary_match = re.search(r"Appointment booked:\s*(.+?)\s+@", subject, re.I)
    summary = _clean_line(summary_match.group(1) if summary_match else subject, 300)
    return Booking(
        event_id="gmail:" + message_id,
        summary=summary,
        prospect_name=name,
        prospect_email=email,
        purpose=_purpose(text),
        starts_at=_subject_start(subject),
        ends_at=None,
        meet_url="",
        source="gmail",
    )


def _credential_blobs(settings: Any) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for env_name, attr in _CREDENTIALS:
        raw = os.getenv(env_name, "").strip()
        if not raw and attr and settings is not None:
            raw = str(getattr(settings, attr, "") or "").strip()
        if raw:
            found.append((env_name, raw))
    return found


def _list_events(session: Any, now: datetime) -> list[dict]:
    calendar = quote(CALENDAR_ID, safe="")
    base = os.getenv(
        "BUILDING_GOOGLE_CALENDAR_API_BASE_URL",
        "https://www.googleapis.com/calendar/v3",
    ).rstrip("/")
    start = (now - LOOKBACK).astimezone(timezone.utc).isoformat()
    end = (now + LOOKAHEAD).astimezone(timezone.utc).isoformat()
    response = session.get(
        f"{base}/calendars/{calendar}/events",
        params={
            "singleEvents": "true",
            "orderBy": "startTime",
            "timeMin": start,
            "timeMax": end,
            "maxResults": 50,
            "showDeleted": "false",
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    items = payload.get("items") if isinstance(payload, dict) else None
    return [item for item in (items or []) if isinstance(item, dict)]


def _calendar_bookings(settings: Any, now: datetime) -> tuple[list[Booking], tuple[str, ...]]:
    blobs = _credential_blobs(settings)
    if not blobs:
        return [], ("no service-account JSON in the existing calendar env vars",)
    try:
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2 import service_account
    except ModuleNotFoundError:
        return [], ("google-auth is not installed",)

    attempts: list[str] = []
    import json

    for env_name, raw in blobs:
        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            attempts.append(f"{env_name}: invalid JSON")
            continue
        if not isinstance(info, dict):
            attempts.append(f"{env_name}: not an object")
            continue
        for subject in (ORGANIZER_EMAIL, ""):
            label = f"{env_name} subject={subject or 'none'}"
            try:
                credentials = service_account.Credentials.from_service_account_info(
                    info, scopes=[CALENDAR_SCOPE]
                )
                if subject:
                    credentials = credentials.with_subject(subject)
                session = AuthorizedSession(credentials)
                events = _list_events(session, now)
            except Exception as exc:  # auth, sharing, or HTTP
                attempts.append(f"{label}: {type(exc).__name__}")
                logger.info("[call-prep] calendar read failed for %s", label)
                continue
            bookings = []
            for event in events:
                booking = booking_from_event(event)
                if booking is not None:
                    bookings.append(booking)
            attempts.append(f"{label}: ok ({len(bookings)} bookings)")
            return bookings, tuple(attempts)
    return [], tuple(attempts)


def _gmail_bookings(clients: list[Any]) -> tuple[list[Booking], str]:
    bookings: list[Booking] = []
    seen: set[str] = set()
    readable = False
    for client in clients:
        if client is None or not getattr(client, "is_configured", lambda: False)():
            continue
        try:
            messages = client.list_messages(
                query='subject:"Appointment booked" newer_than:14d',
                max_results=20,
            )
        except Exception as exc:  # mailbox missing or scope rejected
            logger.info("[call-prep] gmail list failed: %s", type(exc).__name__)
            continue
        readable = True
        for item in messages:
            message_id = str((item or {}).get("id") or "")
            if not message_id or message_id in seen:
                continue
            seen.add(message_id)
            try:
                full = client.get_message(message_id)
            except Exception as exc:
                logger.info("[call-prep] gmail read failed: %s", type(exc).__name__)
                continue
            booking = booking_from_gmail(full)
            if booking is not None:
                bookings.append(booking)
    if not readable:
        return [], "Gmail is not configured, so Appointment booked mail cannot be read."
    return bookings, ""


def load_bookings(settings: Any, now: datetime, *, gmail_clients: Optional[list[Any]] = None) -> SourceLoad:
    """Prefer the Sales calendar. Fall back to Appointment booked mail."""

    bookings, attempts = _calendar_bookings(settings, now)
    if any(item.endswith(")") and ": ok" in item for item in attempts):
        return SourceLoad(bookings=bookings, source="google_calendar", gap="", attempts=attempts)

    mail, mail_gap = _gmail_bookings(list(gmail_clients or []))
    calendar_gap = (
        "Google Calendar for abby@anatainc.com is not readable with the credentials "
        "already on this app (GOOGLE_SERVICE_ACCOUNT_JSON, "
        "BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON, "
        "HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON). "
        + (mail_gap or "Appointment booked mail was checked instead.")
    )
    if mail:
        return SourceLoad(bookings=mail, source="gmail", gap=calendar_gap, attempts=attempts)
    gap = (
        calendar_gap
        + " ClickUp Sales CRM only has a Meeting Set checkbox, not a booking event id. "
        "This app has no Zapier booking record."
    )
    return SourceLoad(bookings=[], source="none", gap=gap, attempts=attempts)
