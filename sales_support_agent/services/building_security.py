"""Session-bound form protection for Building Control browser writes."""

from __future__ import annotations

import hashlib
import hmac
import os


def csrf_token(user: dict | None) -> str:
    user = user or {}
    secret = (
        os.getenv("BUILDING_CAMPAIGN_TOKEN_SECRET", "").strip()
        or os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
    )
    if not secret:
        return ""
    payload = "|".join((
        str(user.get("email") or "").strip().lower(),
        str(user.get("session_issued_at") or ""),
        "anata-building-csrf-v1",
    ))
    return hmac.new(
        secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def valid_csrf_token(user: dict | None, supplied: str) -> bool:
    expected = csrf_token(user)
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))
