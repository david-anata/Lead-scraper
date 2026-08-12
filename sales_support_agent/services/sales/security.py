"""Sales-specific browser form security helpers.

Mirrors the Building, Finance, and HR modules: a session-bound token that never
exposes the session cookie, plus a dependency that rejects cross-site browser
writes. Sales writes reach ClickUp, HubSpot, and Slack, so they need the same
protection those surfaces already had.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from urllib.parse import urlparse

from fastapi import HTTPException, Request

from sales_support_agent.services.auth_deps import get_current_user


def csrf_token(user: dict | None) -> str:
    """Return a session-bound token without exposing the session cookie."""
    user = user or {}
    secret = (
        os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
        or os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip()
    )
    if not secret:
        return ""
    payload = "|".join((
        str(user.get("email") or "").strip().lower(),
        str(user.get("session_issued_at") or ""),
        "anata-sales-csrf-v1",
    ))
    return hmac.new(
        secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def valid_csrf_token(user: dict | None, supplied: str) -> bool:
    expected = csrf_token(user)
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


async def require_sales_form_security(request: Request) -> None:
    """Reject cross-site browser writes and require a session-bound form token."""

    if (request.headers.get("sec-fetch-site") or "").lower() == "cross-site":
        raise HTTPException(status_code=403, detail="Cross-site sales write rejected.")
    origin = request.headers.get("origin")
    if origin and urlparse(origin).netloc.lower() != request.url.netloc.lower():
        raise HTTPException(status_code=403, detail="Sales form origin does not match.")
    if origin or request.headers.get("sec-fetch-mode"):
        form = await request.form()
        if not valid_csrf_token(
            get_current_user(request), str(form.get("_csrf_token") or "")
        ):
            raise HTTPException(
                status_code=403,
                detail="Sales form security token is invalid.",
            )
