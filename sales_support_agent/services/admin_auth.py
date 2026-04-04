"""Single-user admin auth helpers — supports legacy password tokens and Google SSO tokens."""

from __future__ import annotations

import base64
import json
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from typing import Optional

from sales_support_agent.config import Settings


def admin_login_enabled(settings: Settings) -> bool:
    return bool(settings.admin_password and settings.admin_session_secret)


def verify_admin_password(settings: Settings, supplied_password: str) -> bool:
    expected = settings.admin_password.encode("utf-8")
    actual = (supplied_password or "").encode("utf-8")
    return bool(expected) and hmac.compare_digest(actual, expected)


def create_admin_session_token(settings: Settings, *, now: Optional[datetime] = None) -> str:
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
    now: Optional[datetime] = None,
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


# ---------------------------------------------------------------------------
# Google SSO session tokens (5-part: email|name|role|timestamp|signature)
# Legacy password tokens are 3-part: username|timestamp|signature
# ---------------------------------------------------------------------------

def create_user_session_token(settings: Settings, *, email: str, name: str, role: str, now: Optional[datetime] = None) -> str:
    """Create a signed session token for a Google-authenticated user."""
    issued_at = now or datetime.now(timezone.utc)
    payload = f"{email}|{name}|{role}|{int(issued_at.timestamp())}"
    signature = hmac.new(
        settings.admin_session_secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    token = f"{payload}|{signature}"
    return base64.urlsafe_b64encode(token.encode("utf-8")).decode("utf-8")


def get_session_user(settings: Settings, token: str, *, now: Optional[datetime] = None) -> Optional[dict[str, str]]:
    """Validate any session token (legacy or Google SSO) and return user dict or None."""
    if not token:
        return None
    try:
        decoded = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
        parts = decoded.split("|")
    except Exception:
        return None

    current_time = now or datetime.now(timezone.utc)

    if len(parts) == 5:
        # Google SSO token: email|name|role|timestamp|signature
        email, name, role, issued_ts_text, provided_signature = parts
        try:
            issued_ts = int(issued_ts_text)
        except ValueError:
            return None
        payload = f"{email}|{name}|{role}|{issued_ts}"
        expected_signature = hmac.new(
            settings.admin_session_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(provided_signature, expected_signature):
            return None
        issued_at = datetime.fromtimestamp(issued_ts, tz=timezone.utc)
        if current_time > issued_at + timedelta(hours=settings.admin_session_ttl_hours):
            return None
        return {"email": email, "name": name, "role": role}

    if len(parts) == 3:
        # Legacy password token: username|timestamp|signature
        username, issued_ts_text, provided_signature = parts
        try:
            issued_ts = int(issued_ts_text)
        except ValueError:
            return None
        if username != settings.admin_username:
            return None
        payload = f"{username}|{issued_ts}"
        expected_signature = hmac.new(
            settings.admin_session_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(provided_signature, expected_signature):
            return None
        issued_at = datetime.fromtimestamp(issued_ts, tz=timezone.utc)
        if current_time > issued_at + timedelta(hours=settings.admin_session_ttl_hours):
            return None
        return {"email": username, "name": username, "role": "admin"}

    return None


def create_signed_state_token(secret: str, payload: dict[str, str]) -> str:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    signature = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    token = f"{body}|{signature}"
    return base64.urlsafe_b64encode(token.encode("utf-8")).decode("utf-8")


def read_signed_state_token(secret: str, token: str) -> Optional[dict[str, str]]:
    if not token:
        return None
    try:
        decoded = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
        body, provided_signature = decoded.rsplit("|", 1)
        expected_signature = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(provided_signature, expected_signature):
            return None
        payload = json.loads(body)
        if not isinstance(payload, dict):
            return None
        return {str(key): str(value) for key, value in payload.items()}
    except Exception:
        return None
