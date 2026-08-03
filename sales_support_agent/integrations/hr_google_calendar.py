"""Dedicated Google Calendar client for approved HR out-of-office events."""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import quote

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
        try:
            info = json.loads(self.service_account_json)
        except (json.JSONDecodeError, TypeError):
            return "The OOO calendar credential is invalid. Replace it in Render."
        if not info.get("client_email") or not info.get("private_key"):
            return "The OOO calendar credential is incomplete. Replace it in Render."
        return ""

    @property
    def readiness_state(self) -> str:
        if not self.calendar_id:
            return "calendar_id_missing"
        if self.calendar_id.lower() == "primary":
            return "calendar_id_invalid"
        if not self.service_account_json:
            return "credential_missing"
        if self.readiness_error:
            return "credential_invalid"
        return "credential_detected"

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

    def check_connection(self) -> tuple[bool, str, str]:
        """Verify calendar visibility and write role without creating an event."""
        if not self.configured:
            return False, self.readiness_state, self.readiness_error
        try:
            response = self._authorized_session().get(
                f"{self.api_base_url}/users/me/calendarList/{quote(self.calendar_id, safe='')}",
                timeout=20,
            )
        except Exception:
            return False, "api_unavailable", "Google Calendar could not be reached. Try again."
        if response.status_code == 401:
            return False, "credential_invalid", (
                "Google rejected the protected service-account credential. "
                "Replace it in Render, deploy, and test again."
            )
        if response.status_code == 403:
            return False, "permission_missing", (
                "Share the Anata OOO calendar with the service account using "
                "‘Make changes to events,’ then test again."
            )
        if response.status_code == 404:
            return False, "calendar_not_found", (
                "The configured calendar was not found for this service account. "
                "Check the Calendar ID and sharing permission."
            )
        if response.status_code >= 400:
            return False, "api_unavailable", "Google Calendar returned an error. Try again."
        access_role = str((response.json() or {}).get("accessRole") or "").lower()
        if access_role not in {"writer", "owner"}:
            return False, "permission_missing", (
                "The service account can see the calendar but cannot change events. "
                "Grant ‘Make changes to events,’ then test again."
            )
        return True, "ready", "Calendar connection and event-write permission confirmed."
