"""Minimal Resend client for transactional email (invites, approvals).

Resend is the preferred sender for access-flow email: a single API key plus a
verified `from` domain — no OAuth dance like Gmail. Configure with:
  * RESEND_API_KEY  — secret API key from the Resend dashboard
  * RESEND_FROM     — verified sender, e.g. "Anata Agent <noreply@anatainc.com>"

`is_configured()` is False when the key is missing, so callers degrade to the
copyable-link fallback exactly as they do today — adding this file changes no
behavior until RESEND_API_KEY is set.
"""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

_RESEND_ENDPOINT = "https://api.resend.com/emails"


class ResendClient:
    def __init__(self, settings):
        self.settings = settings
        self.api_key = (getattr(settings, "resend_api_key", "") or "").strip()
        self.from_address = (getattr(settings, "resend_from", "") or "").strip()

    def is_configured(self) -> bool:
        return bool(self.api_key and self.from_address)

    def send_message(self, *, to, subject: str, text: str, reply_to: str = "") -> str:
        """Send a plain-text email. Raises on transport/HTTP error so the caller
        (notify.py) can log and fall through to the next sender."""
        recipients = [to] if isinstance(to, str) else list(to)
        payload = {
            "from": self.from_address,
            "to": recipients,
            "subject": subject,
            "text": text,
        }
        if reply_to:
            payload["reply_to"] = reply_to

        response = requests.post(
            _RESEND_ENDPOINT,
            json=payload,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        if response.status_code >= 300:
            raise RuntimeError(
                f"Resend send failed ({response.status_code}): {response.text[:300]}"
            )
        try:
            return str((response.json() or {}).get("id") or "")
        except ValueError:
            return ""
