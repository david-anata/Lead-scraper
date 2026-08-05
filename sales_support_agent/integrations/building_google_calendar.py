"""Google Calendar adapter for Anata Building reservation projections."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import quote


CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar"
UNSAFE_CALENDAR_IDS = {"primary"}


class BuildingCalendarAdapter(Protocol):
    """Provider-neutral delivery boundary for Building calendar projections."""

    provider: str
    target_calendar_id: str
    configured: bool
    readiness_error: str

    def upsert_event(
        self, *, reservation_id: str, payload: dict[str, Any],
        provider_event_id: str = "",
    ) -> str: ...

    def delete_event(self, provider_event_id: str) -> None: ...

    def find_conflicts(
        self, *, starts_at: datetime, ends_at: datetime,
        exclude_reservation_id: str = "",
    ) -> list[dict[str, Any]]: ...


def deterministic_event_id(reservation_id: str) -> str:
    """Return a Google-compatible stable event ID so retries cannot duplicate events."""

    digest = hashlib.sha256(reservation_id.encode("utf-8")).hexdigest()
    return f"anata{digest[:40]}"


class BuildingGoogleCalendarClient:
    """Small authenticated client that only manages the configured building calendar."""

    provider = "google_calendar"

    def __init__(
        self,
        *,
        calendar_id: str | None = None,
        service_account_json: str | None = None,
        delegated_subject: str | None = None,
        api_base_url: str | None = None,
    ) -> None:
        self.calendar_id = (
            calendar_id or os.getenv("BUILDING_GOOGLE_CALENDAR_ID", "")
        ).strip()
        self.service_account_json = (
            service_account_json
            or os.getenv("BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", "")
        ).strip()
        self.delegated_subject = (
            delegated_subject
            if delegated_subject is not None
            else os.getenv("BUILDING_GOOGLE_CALENDAR_DELEGATED_SUBJECT", "")
        ).strip()
        self.api_base_url = (
            api_base_url
            or os.getenv(
                "BUILDING_GOOGLE_CALENDAR_API_BASE_URL",
                "https://www.googleapis.com/calendar/v3",
            )
        ).rstrip("/")
        self._session: Any | None = None

    @property
    def target_calendar_id(self) -> str:
        return self.calendar_id

    @property
    def readiness_error(self) -> str:
        if not self.calendar_id:
            return "Dedicated Building calendar ID is missing."
        if self.calendar_id.lower() in UNSAFE_CALENDAR_IDS:
            return "The primary calendar alias is prohibited; configure a dedicated calendar ID."
        if not self.service_account_json:
            return "Google Calendar service-account credentials are missing."
        return ""

    @property
    def configured(self) -> bool:
        return not self.readiness_error

    def _authorized_session(self) -> Any:
        if not self.configured:
            raise RuntimeError(
                "Building Google Calendar is not configured. Set the calendar ID "
                "and service-account JSON, then share the calendar with that account."
            )
        if self._session is None:
            try:
                from google.auth.transport.requests import AuthorizedSession
                from google.oauth2 import service_account
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "google-auth is required to synchronize Building Google Calendar."
                ) from exc
            try:
                info = json.loads(self.service_account_json)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    "BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON is not valid JSON."
                ) from exc
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=[CALENDAR_SCOPE]
            )
            if self.delegated_subject:
                credentials = credentials.with_subject(self.delegated_subject)
            self._session = AuthorizedSession(credentials)
        return self._session

    def upsert_event(
        self,
        *,
        reservation_id: str,
        payload: dict[str, Any],
        provider_event_id: str = "",
    ) -> str:
        """Insert or update one deterministic calendar event."""

        session = self._authorized_session()
        event_id = provider_event_id or deterministic_event_id(reservation_id)
        calendar_id = quote(self.calendar_id, safe="")
        event_url = f"{self.api_base_url}/calendars/{calendar_id}/events/{event_id}"
        delivery_params = {"sendUpdates": "all"} if payload.get("attendees") else None
        response = session.patch(
            event_url, params=delivery_params, json=payload, timeout=20
        )
        if response.status_code == 404:
            collection_url = (
                f"{self.api_base_url}/calendars/{calendar_id}/events"
            )
            body = {"id": event_id, **payload}
            response = session.post(
                collection_url, params=delivery_params, json=body, timeout=20
            )
            if response.status_code == 409:
                response = session.patch(
                    event_url, params=delivery_params, json=payload, timeout=20
                )
        response.raise_for_status()
        result = response.json()
        return str(result.get("id") or event_id)

    def delete_event(self, provider_event_id: str) -> None:
        """Delete a projected event; an already-missing event is a successful result."""

        if not provider_event_id:
            return
        session = self._authorized_session()
        calendar_id = quote(self.calendar_id, safe="")
        event_url = f"{self.api_base_url}/calendars/{calendar_id}/events/{provider_event_id}"
        response = session.delete(event_url, timeout=20)
        if response.status_code != 404:
            response.raise_for_status()

    def find_conflicts(
        self,
        *,
        starts_at: datetime,
        ends_at: datetime,
        exclude_reservation_id: str = "",
    ) -> list[dict[str, Any]]:
        """Return opaque events that occupy any part of the requested window."""

        session = self._authorized_session()
        collection_url = (
            f"{self.api_base_url}/calendars/{quote(self.calendar_id, safe='')}/events"
        )
        params: dict[str, Any] = {
            "timeMin": starts_at.isoformat(),
            "timeMax": ends_at.isoformat(),
            "singleEvents": "true",
            "showDeleted": "false",
            "maxResults": 2500,
        }
        conflicts: list[dict[str, Any]] = []
        while True:
            response = session.get(collection_url, params=params, timeout=20)
            response.raise_for_status()
            body = response.json()
            for event in body.get("items") or []:
                private = ((event.get("extendedProperties") or {}).get("private") or {})
                if (
                    event.get("status") == "cancelled"
                    or event.get("transparency") == "transparent"
                    or (
                        exclude_reservation_id
                        and private.get("anataReservationId") == exclude_reservation_id
                    )
                ):
                    continue
                conflicts.append({
                    "id": str(event.get("id") or ""),
                    "summary": str(event.get("summary") or "Busy"),
                    "start": dict(event.get("start") or {}),
                    "end": dict(event.get("end") or {}),
                })
            page_token = body.get("nextPageToken")
            if not page_token:
                break
            params["pageToken"] = page_token
        return conflicts
