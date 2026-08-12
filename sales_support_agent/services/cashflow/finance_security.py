"""Session-bound protection for browser writes in Finance Control."""

from __future__ import annotations

import hashlib
import hmac
import os
from urllib.parse import urlparse

from fastapi import HTTPException, Request

from sales_support_agent.services.auth_deps import get_current_user


def csrf_token(user: dict | None) -> str:
    """Return a stable token bound to the current authenticated session."""
    user = user or {}
    secret = os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
    if not secret:
        return ""
    payload = "|".join((
        str(user.get("email") or "").strip().lower(),
        str(user.get("session_issued_at") or ""),
        "anata-finance-csrf-v1",
    ))
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def valid_csrf_token(user: dict | None, supplied: str) -> bool:
    expected = csrf_token(user)
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


async def require_finance_write_security(request: Request) -> None:
    """Reject cross-site browser writes and require a session token for them."""
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    if (request.headers.get("sec-fetch-site") or "").lower() == "cross-site":
        raise HTTPException(status_code=403, detail="Cross-site Finance write rejected.")
    origin = request.headers.get("origin")
    if origin and urlparse(origin).netloc.lower() != request.url.netloc.lower():
        raise HTTPException(status_code=403, detail="Finance write origin does not match.")
    if origin or request.headers.get("sec-fetch-mode"):
        supplied = request.headers.get("x-csrf-token", "")
        if not supplied and "application/x-www-form-urlencoded" in request.headers.get("content-type", ""):
            supplied = str((await request.form()).get("_csrf_token") or "")
        if not valid_csrf_token(get_current_user(request), supplied):
            raise HTTPException(status_code=403, detail="Finance security token is invalid.")
