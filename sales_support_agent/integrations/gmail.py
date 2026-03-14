"""Minimal Gmail API client for polling and sending digest mail."""

from __future__ import annotations

import base64
from email.message import EmailMessage
from typing import Any

import requests

from sales_support_agent.config import Settings


class GmailClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._cached_access_token = settings.gmail_access_token

    def is_configured(self) -> bool:
        if self.settings.gmail_access_token:
            return True
        return bool(
            self.settings.gmail_client_id
            and self.settings.gmail_client_secret
            and self.settings.gmail_refresh_token
        )

    def list_messages(self, *, query: str, max_results: int) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"users/{self.settings.gmail_user_id}/messages",
            params={"q": query, "maxResults": max_results},
        )
        return list(payload.get("messages", []) or [])

    def get_message(self, message_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"users/{self.settings.gmail_user_id}/messages/{message_id}",
            params={"format": "full"},
        )

    def send_message(self, *, to: tuple[str, ...], subject: str, text: str, cc: tuple[str, ...] = ()) -> dict[str, Any]:
        if not to:
            return {"ok": False, "skipped": True, "reason": "missing_recipients"}

        msg = EmailMessage()
        msg["To"] = ", ".join(to)
        if cc:
            msg["Cc"] = ", ".join(cc)
        msg["Subject"] = subject
        msg.set_content(text)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        return self._request(
            "POST",
            f"users/{self.settings.gmail_user_id}/messages/send",
            json_body={"raw": raw},
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        access_token = self._get_access_token()
        response = requests.request(
            method,
            f"{self.settings.gmail_api_base_url.rstrip('/')}/{path.lstrip('/')}",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            params=params,
            json=json_body,
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(
                f"Gmail API request failed ({response.status_code}) for {path}: {response.text}"
            )
        if not response.content:
            return {}
        return response.json()

    def _get_access_token(self) -> str:
        if self._cached_access_token:
            return self._cached_access_token
        response = requests.post(
            self.settings.gmail_oauth_token_url,
            data={
                "client_id": self.settings.gmail_client_id,
                "client_secret": self.settings.gmail_client_secret,
                "refresh_token": self.settings.gmail_refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(
                f"Gmail token refresh failed ({response.status_code}): {response.text}"
            )
        payload = response.json()
        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise RuntimeError(f"Gmail token refresh failed: {payload}")
        self._cached_access_token = access_token
        return access_token
