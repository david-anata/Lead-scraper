"""Production relay contract for content destinations.

The deployed Agent cannot call a developer-only MCP session. A provider API,
Zapier action, or MCP relay must implement this small HTTPS contract and return
durable evidence that can be verified independently.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


ALLOWED_ACTIONS = {
    "linkedin_company": "linkedin_create_company_update",
    "linkedin_personal": "linkedin_create_share_update",
    "instagram_video": "instagram_for_business_publish_video",
    "instagram_photo": "instagram_for_business_publish_photo_s",
    "youtube_upload": "youtube_upload_video",
    "youtube_report": "youtube_get_report",
}


@dataclass(frozen=True)
class RelayResult:
    """Safe provider result retained by the content audit ledger."""

    accepted: bool
    verified: bool
    status: str
    provider_receipt: str = ""
    public_url: str = ""
    safe_message: str = ""
    retryable: bool = False


class RelayTransport(Protocol):
    """Injectable HTTPS transport used by provider contract tests."""

    def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        payload: dict[str, Any],
        timeout_seconds: float,
    ) -> tuple[int, dict[str, Any]]:
        """Return an HTTP status and decoded JSON response."""


class UrllibRelayTransport:
    """Small standard-library transport with bounded payload reads."""

    def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        payload: dict[str, Any],
        timeout_seconds: float,
    ) -> tuple[int, dict[str, Any]]:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = Request(url, data=body, headers=headers, method="POST")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
                raw = response.read(256_000)
                return int(response.status), json.loads(raw or b"{}")
        except HTTPError as exc:
            raw = exc.read(256_000)
            try:
                decoded = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                decoded = {}
            return int(exc.code), decoded
        except (URLError, TimeoutError) as exc:
            raise ConnectionError("Content relay is unavailable.") from exc


def _validated_https_url(value: str) -> str:
    parsed = urlsplit(str(value or "").strip())
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("Content relay URL must be HTTPS.")
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("Content relay URL must not contain credentials or fragments.")
    return parsed.geturl()


class ContentRelayClient:
    """Fail-closed client for allowlisted provider relay actions."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        transport: RelayTransport | None = None,
        timeout_seconds: float = 12.0,
        max_attempts: int = 2,
    ) -> None:
        self.base_url = _validated_https_url(base_url).rstrip("/")
        self.api_key = str(api_key or "").strip()
        if not self.api_key:
            raise ValueError("Content relay key is required.")
        self.transport = transport or UrllibRelayTransport()
        self.timeout_seconds = max(1.0, min(float(timeout_seconds), 30.0))
        self.max_attempts = max(1, min(int(max_attempts), 3))

    def execute(
        self,
        *,
        action_key: str,
        destination_identity: str,
        idempotency_key: str,
        payload: dict[str, Any],
        allow_write: bool,
    ) -> RelayResult:
        """Execute one allowlisted action only after an explicit write gate."""

        if action_key not in ALLOWED_ACTIONS.values():
            return RelayResult(
                False,
                False,
                "blocked",
                safe_message="This content action is not allowlisted.",
            )
        if not allow_write:
            return RelayResult(
                False,
                False,
                "staged",
                safe_message="Publishing is disabled. The artifact remains staged.",
            )
        if not destination_identity.strip():
            return RelayResult(
                False,
                False,
                "blocked",
                safe_message="Destination identity is not verified.",
            )
        if len(idempotency_key.strip()) < 16:
            return RelayResult(
                False,
                False,
                "blocked",
                safe_message="A durable idempotency key is required.",
            )

        safe_payload = {
            "action_key": action_key,
            "destination_identity": destination_identity.strip()[:255],
            "idempotency_key": idempotency_key.strip()[:160],
            "payload": payload,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key.strip()[:160],
            "User-Agent": "Anata-Agent-Content/1.0",
        }
        last_status = 0
        last_body: dict[str, Any] = {}
        for attempt in range(1, self.max_attempts + 1):
            try:
                last_status, last_body = self.transport.post(
                    f"{self.base_url}/v1/content/actions",
                    headers=headers,
                    payload=safe_payload,
                    timeout_seconds=self.timeout_seconds,
                )
            except ConnectionError:
                if attempt >= self.max_attempts:
                    return RelayResult(
                        False,
                        False,
                        "failed",
                        safe_message="Content relay is unavailable.",
                        retryable=True,
                    )
                time.sleep(0.05)
                continue
            if last_status not in {429, 500, 502, 503, 504}:
                break
            if attempt < self.max_attempts:
                time.sleep(0.05)

        accepted = 200 <= last_status < 300 and bool(last_body.get("accepted"))
        verified = accepted and bool(last_body.get("verified"))
        receipt = str(last_body.get("provider_receipt") or "")[:500]
        public_url = str(last_body.get("public_url") or "")[:4000]
        if verified and (not receipt or not public_url):
            return RelayResult(
                accepted,
                False,
                "needs_review",
                provider_receipt=receipt,
                public_url=public_url,
                safe_message="Provider accepted the write but verification evidence is incomplete.",
            )
        if verified:
            return RelayResult(
                True,
                True,
                "delivered",
                provider_receipt=receipt,
                public_url=public_url,
                safe_message="Destination publication was independently verified.",
            )
        if accepted:
            return RelayResult(
                True,
                False,
                "running",
                provider_receipt=receipt,
                safe_message="Provider accepted the write; live verification is pending.",
            )
        return RelayResult(
            False,
            False,
            "failed",
            safe_message="The destination rejected the content action.",
            retryable=last_status in {429, 500, 502, 503, 504},
        )
