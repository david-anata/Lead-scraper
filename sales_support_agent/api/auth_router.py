"""Google OAuth 2.0 callback routes: /admin/auth/google and /admin/auth/callback."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sales_support_agent.services.admin_auth import (
    create_signed_state_token,
    create_user_session_token,
    read_signed_state_token,
)
from sales_support_agent.services.admin_auth_google import (
    exchange_google_code,
    get_user_role,
    google_auth_url,
    google_oauth_enabled,
)

router = APIRouter()
logger = logging.getLogger(__name__)

_OAUTH_STATE_COOKIE = "oauth_state"


def _callback_uri(request: Request) -> str:
    # Always use HTTPS in production; fall back to request URL scheme in dev.
    base = str(request.base_url).rstrip("/")
    if "localhost" not in base and "127.0.0.1" not in base:
        base = base.replace("http://", "https://")
    return f"{base}/admin/auth/callback"


def _cookie_opts(request: Request) -> dict:
    secure = "localhost" not in str(request.base_url)
    return {
        "key": request.app.state.settings.admin_cookie_name,
        "httponly": True,
        "samesite": "lax",
        "path": "/",
        "secure": secure,
    }


@router.get("/admin/auth/google")
def google_login_start(request: Request) -> RedirectResponse:
    settings = request.app.state.settings
    if not google_oauth_enabled(settings):
        return RedirectResponse("/admin/login", status_code=302)

    state = create_signed_state_token(settings.admin_session_secret, {"action": "login"})
    url = google_auth_url(settings, redirect_uri=_callback_uri(request), state=state)
    response = RedirectResponse(url, status_code=302)
    secure = "localhost" not in str(request.base_url)
    response.set_cookie(
        _OAUTH_STATE_COOKIE,
        state,
        httponly=True,
        samesite="lax",
        path="/",
        secure=secure,
        max_age=600,
    )
    return response


@router.get("/admin/auth/callback")
def google_callback(request: Request, code: str = "", state: str = "", error: str = "") -> HTMLResponse | RedirectResponse:
    settings = request.app.state.settings

    if error:
        logger.warning("Google OAuth error: %s", error)
        return RedirectResponse("/admin/login?error=oauth_denied", status_code=302)

    # Validate CSRF state
    stored_state = request.cookies.get(_OAUTH_STATE_COOKIE, "")
    if not stored_state or state != stored_state:
        logger.warning("OAuth state mismatch — possible CSRF")
        return RedirectResponse("/admin/login?error=state_mismatch", status_code=302)

    payload = read_signed_state_token(settings.admin_session_secret, state)
    if not payload or payload.get("action") != "login":
        return RedirectResponse("/admin/login?error=invalid_state", status_code=302)

    if not code:
        return RedirectResponse("/admin/login?error=no_code", status_code=302)

    try:
        userinfo = exchange_google_code(settings, code=code, redirect_uri=_callback_uri(request))
    except Exception as exc:
        logger.exception("Google token exchange failed: %s", exc)
        return RedirectResponse("/admin/login?error=token_exchange", status_code=302)

    email: str = (userinfo.get("email") or "").strip().lower()
    name: str = (userinfo.get("name") or userinfo.get("given_name") or email).strip()
    hd: str = (userinfo.get("hd") or "").strip().lower()

    allowed_domain = settings.google_oauth_allowed_domain.lower()
    if not email.endswith(f"@{allowed_domain}") and hd != allowed_domain:
        logger.warning("OAuth login rejected — domain not allowed: %s", email)
        return RedirectResponse("/admin/login?error=domain_not_allowed", status_code=302)

    role = get_user_role(settings, email)
    token = create_user_session_token(settings, email=email, name=name, role=role)

    response = RedirectResponse("/admin", status_code=302)
    response.delete_cookie(_OAUTH_STATE_COOKIE, path="/")
    response.set_cookie(value=token, **_cookie_opts(request))
    return response
