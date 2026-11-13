"""Twilio request signature checks.

The algorithm is the one documented by Twilio: HMAC-SHA1 of the full URL
plus the POST parameters sorted by name, compared to `X-Twilio-Signature`.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time


def build_signature_payload(url: str, params: list[tuple[str, str]]) -> str:
    ordered = sorted(params, key=lambda item: item[0])
    return url + "".join(key + value for key, value in ordered)


def compute_signature(url: str, params: list[tuple[str, str]], auth_token: str) -> str:
    payload = build_signature_payload(url, params)
    digest = hmac.new(auth_token.encode("utf-8"), payload.encode("utf-8"), hashlib.sha1)
    return base64.b64encode(digest.digest()).decode("utf-8")


def valid_signature(
    url: str,
    params: list[tuple[str, str]],
    signature: str,
    auth_token: str,
) -> bool:
    if not auth_token or not signature:
        return False
    expected = compute_signature(url, params, auth_token)
    return hmac.compare_digest(expected, signature)


def public_request_url(base: str, path: str, query: str) -> str:
    origin = base.rstrip("/")
    url = f"{origin}{path}"
    if query:
        url = f"{url}?{query}"
    return url


def mint_stream_token(call_sid: str, resume: str, auth_token: str, ttl_seconds: int = 7200) -> str:
    """Mint an unguessable token so only TwiML we generated can open the socket."""

    expires = int(time.time()) + ttl_seconds
    message = f"{call_sid}.{expires}.{resume}"
    digest = hmac.new(
        auth_token.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return f"{message}.{digest}"


def valid_stream_token(token: str, auth_token: str, now: float | None = None) -> tuple[bool, str, str]:
    """Return (ok, call_sid, resume). Expired or tampered tokens fail closed."""

    parts = (token or "").split(".")
    if len(parts) != 4 or not auth_token:
        return False, "", ""
    call_sid, expires_raw, resume, digest = parts
    message = f"{call_sid}.{expires_raw}.{resume}"
    expected = hmac.new(
        auth_token.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, digest):
        return False, "", ""
    try:
        expires = int(expires_raw)
    except ValueError:
        return False, "", ""
    current = time.time() if now is None else now
    if expires < current:
        return False, "", ""
    return True, call_sid, resume
