"""Session-bound protection for Content Operations browser writes."""

from __future__ import annotations

import hashlib
import hmac
import os
from urllib.parse import urlparse

from fastapi import HTTPException, Request

from sales_support_agent.services.auth_deps import get_current_user


def csrf_token(user: dict | None) -> str:
    user = user or {}
    secret = os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
    if not secret:
        return ""
    payload = "|".join(
        (
            str(user.get("email") or "").strip().lower(),
            str(user.get("session_issued_at") or ""),
            "anata-content-csrf-v1",
        )
    )
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def valid_csrf_token(user: dict | None, supplied: str) -> bool:
    expected = csrf_token(user)
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


async def require_content_form_security(request: Request) -> None:
    if (request.headers.get("sec-fetch-site") or "").lower() == "cross-site":
        raise HTTPException(status_code=403, detail="Cross-site content write rejected.")
    origin = request.headers.get("origin")
    if origin and urlparse(origin).netloc.lower() != request.url.netloc.lower():
        raise HTTPException(status_code=403, detail="Content form origin does not match.")
    if origin or request.headers.get("sec-fetch-mode"):
        form = await request.form()
        if not valid_csrf_token(get_current_user(request), str(form.get("_csrf_token") or "")):
            raise HTTPException(status_code=403, detail="Content form security token is invalid.")
