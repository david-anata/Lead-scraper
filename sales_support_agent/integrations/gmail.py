"""Minimal Gmail API client for polling and sending digest mail."""

from __future__ import annotations

import base64
import json
from email.message import EmailMessage
from typing import Any

import requests

from sales_support_agent.config import GmailMailboxAccount, Settings


class GmailIntegrationError(RuntimeError):
    def __init__(
        self,
        *,
        stage: str,
        message: str,
        code: str = "gmail_error",
        http_status: int | None = None,
        hint: str = "",
        provider_payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.code = code
        self.http_status = http_status
        self.hint = hint
        self.provider_payload = provider_payload or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "error_code": self.code,
            "http_status": self.http_status,
            "error": str(self),
            "hint": self.hint,
            "provider_payload": self.provider_payload,
        }


class GmailClient:
    def __init__(self, settings: Settings, mailbox_account: GmailMailboxAccount | None = None):
        self.settings = settings
        self.mailbox_account = mailbox_account
        self.account_key = mailbox_account.account_key if mailbox_account else "primary"
        self.account_label = mailbox_account.label if mailbox_account else "Primary inbox"
        self.user_id = mailbox_account.user_id if mailbox_account else settings.gmail_user_id
        self.poll_query = mailbox_account.poll_query if mailbox_account else settings.gmail_poll_query
        self.poll_max_messages = mailbox_account.poll_max_messages if mailbox_account else settings.gmail_poll_max_messages
        self.source_domains = mailbox_account.source_domains if mailbox_account else settings.gmail_source_domains
        self._static_access_token = mailbox_account.access_token if mailbox_account else settings.gmail_access_token
        self._client_id = mailbox_account.client_id if mailbox_account else settings.gmail_client_id
        self._client_secret = mailbox_account.client_secret if mailbox_account else settings.gmail_client_secret
        self._refresh_token = mailbox_account.refresh_token if mailbox_account else settings.gmail_refresh_token
        self._cached_access_token = self._static_access_token

    def is_configured(self) -> bool:
        if self._static_access_token:
            return True
        return not self.missing_configuration()

    def missing_configuration(self) -> tuple[str, ...]:
        if self._static_access_token:
            return ()

        missing: list[str] = []
        if not self._client_id:
            missing.append("GMAIL_CLIENT_ID")
        if not self._client_secret:
            missing.append("GMAIL_CLIENT_SECRET")
        if not self._refresh_token:
            missing.append("GMAIL_REFRESH_TOKEN")
        return tuple(missing)

    def get_profile(self) -> dict[str, Any]:
        return self._request("GET", f"users/{self.user_id}/profile", stage="profile_lookup")

    def debug_preflight(self) -> dict[str, Any]:
        access_token = self._get_access_token()
        profile = self.get_profile()
        return {
            "auth_ok": True,
            "account_key": self.account_key,
            "account_label": self.account_label,
            "token_source": "static_access_token" if self._static_access_token else "refresh_token",
            "access_token_preview": access_token[:12] + "..." if access_token else "",
            "gmail_address": str(profile.get("emailAddress") or ""),
            "messages_total": int(profile.get("messagesTotal") or 0),
            "threads_total": int(profile.get("threadsTotal") or 0),
            "history_id": str(profile.get("historyId") or ""),
        }

    def list_messages(self, *, query: str, max_results: int) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"users/{self.user_id}/messages",
            params={"q": query, "maxResults": max_results},
            stage="list_messages",
        )
        return list(payload.get("messages", []) or [])

    def get_message(self, message_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"users/{self.user_id}/messages/{message_id}",
            params={"format": "full"},
            stage="get_message",
        )

    def create_draft(self, *, to: tuple[str, ...], subject: str, text: str, cc: tuple[str, ...] = ()) -> dict[str, Any]:
        if not to:
            return {"ok": False, "skipped": True, "reason": "missing_recipients"}

        raw = self._build_raw_message(to=to, subject=subject, text=text, cc=cc)
        return self._request(
            "POST",
            f"users/{self.user_id}/drafts",
            json_body={"message": {"raw": raw}},
            stage="create_draft",
        )

    def send_message(self, *, to: tuple[str, ...], subject: str, text: str, cc: tuple[str, ...] = ()) -> dict[str, Any]:
        if not to:
            return {"ok": False, "skipped": True, "reason": "missing_recipients"}

        raw = self._build_raw_message(to=to, subject=subject, text=text, cc=cc)
        return self._request(
            "POST",
            f"users/{self.user_id}/messages/send",
            json_body={"raw": raw},
            stage="send_message",
        )

    def _build_raw_message(self, *, to: tuple[str, ...], subject: str, text: str, cc: tuple[str, ...] = ()) -> str:
        msg = EmailMessage()
        msg["To"] = ", ".join(to)
        if cc:
            msg["Cc"] = ", ".join(cc)
        msg["Subject"] = subject
        msg.set_content(text)
        return base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    def _request(
        self,
        method: str,
        path: str,
        *,
        stage: str,
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
            provider_payload = self._parse_response_payload(response.text)
            raise GmailIntegrationError(
                stage=stage,
                code=self._infer_error_code(provider_payload),
                http_status=response.status_code,
                message=f"Gmail API request failed for {path}: {response.text}",
                hint=self._build_hint(self._infer_error_code(provider_payload), stage=stage),
                provider_payload=provider_payload,
            )
        if not response.content:
            return {}
        return response.json()

    def _get_access_token(self) -> str:
        if self._cached_access_token:
            return self._cached_access_token
        missing = self.missing_configuration()
        if missing:
            raise GmailIntegrationError(
                stage="configuration",
                code="missing_configuration",
                message=f"Gmail is not configured. Missing: {', '.join(missing)}",
                hint="Set the missing GMAIL_* environment variables on the sales-support-agent service and redeploy.",
                provider_payload={"missing": list(missing)},
            )
        response = requests.post(
            self.settings.gmail_oauth_token_url,
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=30,
        )
        if not response.ok:
            provider_payload = self._parse_response_payload(response.text)
            code = self._infer_error_code(provider_payload)
            raise GmailIntegrationError(
                stage="token_refresh",
                code=code,
                http_status=response.status_code,
                message=f"Gmail token refresh failed ({response.status_code}): {response.text}",
                hint=self._build_hint(code, stage="token_refresh"),
                provider_payload=provider_payload,
            )
        payload = response.json()
        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise GmailIntegrationError(
                stage="token_refresh",
                code="missing_access_token",
                message=f"Gmail token refresh succeeded but no access token was returned: {payload}",
                hint="Re-authorize the Gmail OAuth client and generate a fresh refresh token using the same Web application client.",
                provider_payload=payload,
            )
        self._cached_access_token = access_token
        return access_token

    def _parse_response_payload(self, raw_text: str) -> dict[str, Any]:
        text = (raw_text or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
        return {"raw_text": text}

    def _infer_error_code(self, payload: dict[str, Any]) -> str:
        code = str(payload.get("error") or "").strip()
        if code:
            return code
        if isinstance(payload.get("error"), dict):
            nested = str((payload.get("error") or {}).get("status") or "").strip()
            if nested:
                return nested.lower()
        return "gmail_error"

    def _build_hint(self, code: str, *, stage: str) -> str:
        if code == "invalid_client":
            return "The Gmail OAuth client ID and client secret do not match the Web application client used to create the refresh token."
        if code == "invalid_grant":
            return "The Gmail refresh token is invalid, revoked, expired, or was generated for a different OAuth client."
        if code == "insufficient_scope":
            return "The connected Google account authorized the wrong Gmail scope. Re-authorize with https://www.googleapis.com/auth/gmail.modify."
        if code == "unauthorized_client":
            return "The Google OAuth client is not allowed to use this grant or redirect URI. Confirm it is a Web application client with the OAuth Playground redirect URI."
        if code == "missing_configuration":
            return "Set the missing GMAIL_* environment variables on the sales-support-agent service and redeploy."
        if stage == "send_message":
            return "Inbound sync can still be validated first. Confirm DAILY_DIGEST_EMAIL_TO only after Gmail auth succeeds."
        return "Check the Gmail OAuth client, refresh token, and Render environment variables on the sales-support-agent service."
