"""Minimal Google Sheets API client for read-only deck generation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from sales_support_agent.config import Settings

try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials

    GOOGLE_AUTH_AVAILABLE = True
except ModuleNotFoundError:
    GOOGLE_AUTH_AVAILABLE = False
    GoogleAuthRequest = None
    ServiceAccountCredentials = None


class GoogleSheetsClient:
    READONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"

    def __init__(self, settings: Settings):
        self.settings = settings
        self._cached_token = ""

    def is_configured(self) -> bool:
        return bool(
            self.settings.google_sheets_spreadsheet_id
            and self.settings.google_sheets_sales_range
            and self.settings.google_service_account_json
        )

    def get_values(self, *, value_range: str | None = None) -> dict[str, Any]:
        access_token = self._get_access_token()
        selected_range = value_range or self.settings.google_sheets_sales_range
        encoded_range = quote(selected_range, safe="")
        response = requests.get(
            f"{self.settings.google_sheets_api_base_url.rstrip('/')}/spreadsheets/{self.settings.google_sheets_spreadsheet_id}/values/{encoded_range}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(
                f"Google Sheets request failed ({response.status_code}): {response.text}"
            )
        return response.json()

    def _get_access_token(self) -> str:
        if self._cached_token:
            return self._cached_token
        if not GOOGLE_AUTH_AVAILABLE:
            raise RuntimeError("google-auth must be installed to read Google Sheets with a service account.")

        credentials_info = self._load_service_account_info()
        credentials = ServiceAccountCredentials.from_service_account_info(
            credentials_info,
            scopes=[self.READONLY_SCOPE],
        )
        credentials.refresh(GoogleAuthRequest())
        token = str(credentials.token or "").strip()
        if not token:
            raise RuntimeError("Google service account refresh did not return an access token.")
        self._cached_token = token
        return token

    def _load_service_account_info(self) -> dict[str, Any]:
        raw = self.settings.google_service_account_json.strip()
        if not raw:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")
        if raw.startswith("{"):
            payload = json.loads(raw)
        else:
            payload = json.loads(Path(raw).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON must resolve to a JSON object.")
        return payload
