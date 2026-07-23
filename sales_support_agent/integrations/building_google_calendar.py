"""Google Calendar adapter for Anata Building reservation projections."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any


CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar"


def deterministic_event_id(reservation_id: str) -> str:
    """Return a Google-compatible stable event ID so retries cannot duplicate events."""

    digest = hashlib.sha256(reservation_id.encode("utf-8")).hexdigest()
    return f"anata{digest[:40]}"


class BuildingGoogleCalendarClient:
    """Small authenticated client that only manages the configured building calendar."""

    def __init__(
        self,
        *,
        calendar_id: str | None = None,
        service_account_json: str | None = None,
        api_base_url: str | None = None,
    ) -> None:
        self.calendar_id = (
            calendar_id or os.getenv("BUILDING_GOOGLE_CALENDAR_ID", "")
        ).strip()
        self.service_account_json = (
            service_account_json
            or os.getenv("BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", "")
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
    def configured(self) -> bool:
        return bool(self.calendar_id and self.service_account_json)

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
        event_url = (
            f"{self.api_base_url}/calendars/{self.calendar_id}/events/{event_id}"
        )
        response = session.patch(event_url, json=payload, timeout=20)
        if response.status_code == 404:
            collection_url = (
                f"{self.api_base_url}/calendars/{self.calendar_id}/events"
            )
            body = {"id": event_id, **payload}
            response = session.post(collection_url, json=body, timeout=20)
            if response.status_code == 409:
                response = session.patch(event_url, json=payload, timeout=20)
        response.raise_for_status()
        result = response.json()
        return str(result.get("id") or event_id)

    def delete_event(self, provider_event_id: str) -> None:
        """Delete a projected event; an already-missing event is a successful result."""

        if not provider_event_id:
            return
        session = self._authorized_session()
        event_url = (
            f"{self.api_base_url}/calendars/{self.calendar_id}/events/"
            f"{provider_event_id}"
        )
        response = session.delete(event_url, timeout=20)
        if response.status_code != 404:
            response.raise_for_status()
