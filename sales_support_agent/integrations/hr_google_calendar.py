"""Dedicated Google Calendar client for approved HR out-of-office events."""

from __future__ import annotations

import json
import os
from typing import Any

from sales_support_agent.integrations.building_google_calendar import deterministic_event_id


CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar"


class HRGoogleCalendarClient:
    """Manage only the explicitly configured Anata OOO calendar."""

    def __init__(self, *, calendar_id: str | None = None,
                 service_account_json: str | None = None) -> None:
        self.calendar_id = (calendar_id or os.getenv("HR_OOO_GOOGLE_CALENDAR_ID", "")).strip()
        self.service_account_json = (
            service_account_json
            or os.getenv("HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", "")
        ).strip()
        self.api_base_url = os.getenv(
            "HR_OOO_GOOGLE_CALENDAR_API_BASE_URL",
            "https://www.googleapis.com/calendar/v3",
        ).rstrip("/")
        self._session: Any | None = None

    @property
    def readiness_error(self) -> str:
        if not self.calendar_id:
            return "The Anata OOO calendar ID is not configured."
        if self.calendar_id.lower() == "primary":
            return "Use the dedicated Anata OOO calendar ID, not the primary alias."
        if not self.service_account_json:
            return "The OOO calendar service account is not configured."
        return ""

    @property
    def configured(self) -> bool:
        return not self.readiness_error

    @property
    def service_account_email(self) -> str:
        """Return only the non-secret account identity used for calendar sharing."""
        if not self.service_account_json:
            return ""
        try:
            return str(json.loads(self.service_account_json).get("client_email") or "")
        except (json.JSONDecodeError, TypeError):
            return ""

    def _authorized_session(self) -> Any:
        if not self.configured:
            raise RuntimeError(self.readiness_error)
        if self._session is None:
            from google.auth.transport.requests import AuthorizedSession
            from google.oauth2 import service_account
            try:
                info = json.loads(self.service_account_json)
            except json.JSONDecodeError as exc:
                raise RuntimeError("The OOO calendar service account JSON is invalid.") from exc
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=[CALENDAR_SCOPE]
            )
            self._session = AuthorizedSession(credentials)
        return self._session

    def upsert_event(self, *, request_id: int, payload: dict[str, Any],
                     provider_event_id: str = "") -> str:
        session = self._authorized_session()
        event_id = provider_event_id or deterministic_event_id(f"hr-pto-{request_id}")
        event_url = f"{self.api_base_url}/calendars/{self.calendar_id}/events/{event_id}"
        response = session.patch(event_url, json=payload, timeout=20)
        if response.status_code == 404:
            response = session.post(
                f"{self.api_base_url}/calendars/{self.calendar_id}/events",
                json={"id": event_id, **payload}, timeout=20,
            )
            if response.status_code == 409:
                response = session.patch(event_url, json=payload, timeout=20)
        response.raise_for_status()
        return str(response.json().get("id") or event_id)

    def delete_event(self, provider_event_id: str) -> None:
        if not provider_event_id:
            return
        response = self._authorized_session().delete(
            f"{self.api_base_url}/calendars/{self.calendar_id}/events/{provider_event_id}",
            timeout=20,
        )
        if response.status_code != 404:
            response.raise_for_status()
