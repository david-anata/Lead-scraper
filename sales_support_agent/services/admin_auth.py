"""Simple single-user admin auth helpers."""

from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime, timedelta, timezone

from sales_support_agent.config import Settings


def admin_login_enabled(settings: Settings) -> bool:
    return bool(settings.admin_password and settings.admin_session_secret)


def verify_admin_password(settings: Settings, supplied_password: str) -> bool:
    expected = settings.admin_password.encode("utf-8")
    actual = (supplied_password or "").encode("utf-8")
    return bool(expected) and hmac.compare_digest(actual, expected)


def create_admin_session_token(settings: Settings, *, now: datetime | None = None) -> str:
    issued_at = now or datetime.now(timezone.utc)
    payload = f"{settings.admin_username}|{int(issued_at.timestamp())}"
    signature = hmac.new(
        settings.admin_session_secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    token = f"{payload}|{signature}"
    return base64.urlsafe_b64encode(token.encode("utf-8")).decode("utf-8")


def validate_admin_session_token(
    settings: Settings,
    token: str,
    *,
    now: datetime | None = None,
) -> bool:
    if not token:
        return False
    try:
        decoded = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
        username, issued_ts_text, provided_signature = decoded.split("|", 2)
        issued_ts = int(issued_ts_text)
    except Exception:
        return False

    if username != settings.admin_username:
        return False

    payload = f"{username}|{issued_ts}"
    expected_signature = hmac.new(
        settings.admin_session_secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(provided_signature, expected_signature):
        return False

    issued_at = datetime.fromtimestamp(issued_ts, tz=timezone.utc)
    current_time = now or datetime.now(timezone.utc)
    if current_time > issued_at + timedelta(hours=settings.admin_session_ttl_hours):
        return False
    return True
