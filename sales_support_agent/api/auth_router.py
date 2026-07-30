"""Google OAuth 2.0 callback routes: /admin/auth/google and /admin/auth/callback."""

from __future__ import annotations

import hashlib
import logging
import secrets

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

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


def _auth_settings(request: Request):
    """Settings carrying Google OAuth config (client id/secret, allowed domain).
    On the standalone app that's app.state.settings; on the root app it's
    app.state.agent_settings. Used for the OAuth handshake + domain check."""
    return getattr(request.app.state, "agent_settings", None) or request.app.state.settings


def _session_settings(request: Request):
    """Settings used to MINT the session cookie — must match whatever the host
    app validates `/admin` against, or the user gets bounced to login.

    On the root app (main.py) every @app /admin route validates against
    `admin_dashboard_settings` (that's what password login mints with), so we
    must mint with the same one — agent_settings has a different cookie
    name/secret and would be rejected at /admin. On the standalone app there's no
    admin_dashboard_settings, so fall back to the OAuth/session settings."""
    return getattr(request.app.state, "admin_dashboard_settings", None) or _auth_settings(request)


def _callback_uri(request: Request) -> str:
    # Always use HTTPS in production; fall back to request URL scheme in dev.
    base = str(request.base_url).rstrip("/")
    if "localhost" not in base and "127.0.0.1" not in base:
        base = base.replace("http://", "https://")
    return f"{base}/admin/auth/callback"


def _public_base(request: Request) -> str:
    """Build authentication links without trusting an arbitrary Host header."""

    hostname = (request.url.hostname or "").lower()
    if hostname in {"localhost", "127.0.0.1", "::1", "testserver"}:
        return str(request.base_url).rstrip("/")
    return "https://agent.anatainc.com"


def _request_fingerprint(request: Request) -> str:
    settings = _session_settings(request)
    client_host = request.client.host if request.client else ""
    user_agent = request.headers.get("user-agent", "")[:500]
    material = f"{settings.admin_session_secret}|{client_host}|{user_agent}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _is_seed_superadmin(settings, email: str) -> bool:
    normalized = (email or "").strip().lower()
    superadmins = {str(value or "").strip().lower() for value in (getattr(settings, "rbac_superadmin_emails", ()) or ())}
    return normalized in superadmins


def _cookie_opts(request: Request) -> dict:
    secure = "localhost" not in str(request.base_url)
    return {
        "key": _session_settings(request).admin_cookie_name,
        "httponly": True,
        "samesite": "lax",
        "path": "/",
        "secure": secure,
    }


@router.get("/admin/auth/google")
def google_login_start(request: Request) -> RedirectResponse:
    settings = _auth_settings(request)
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
def google_callback(request: Request, code: str = "", state: str = "", error: str = "") -> Response:
    settings = _auth_settings(request)

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
    picture: str = (userinfo.get("picture") or "").strip()
    hd: str = (userinfo.get("hd") or "").strip().lower()

    allowed_domain = settings.google_oauth_allowed_domain.lower()
    domain_ok = email.endswith(f"@{allowed_domain}") or hd == allowed_domain
    if not domain_ok and not _external_login_allowed(request, email):
        logger.warning("OAuth login rejected — domain not allowed: %s", email)
        return RedirectResponse("/admin/login?error=domain_not_allowed", status_code=302)

    # --- RBAC-aware login resolution ---
    rbac_enabled = getattr(settings, "rbac_enabled", True)
    if rbac_enabled:
        try:
            result = _rbac_login(request, settings, email, name, picture=picture)
            if result is not None:
                return result
        except Exception:  # noqa: BLE001 — make the outage explicit; do not bypass RBAC
            logger.exception("RBAC login resolution failed for %s", email)
            if _is_seed_superadmin(settings, email):
                logger.warning("RBAC store unavailable during Google login; allowing configured super-admin %s", email)
                return _mint_session(request, settings, email, name)
            from sales_support_agent.services.access.pages import render_access_unavailable_page

            return HTMLResponse(render_access_unavailable_page(email), status_code=503)

    # Legacy / RBAC disabled: mint cookie and redirect
    role = get_user_role(settings, email)
    token = create_user_session_token(settings, email=email, name=name, role=role)
    response = RedirectResponse("/admin", status_code=302)
    response.delete_cookie(_OAUTH_STATE_COOKIE, path="/")
    response.set_cookie(value=token, **_cookie_opts(request))
    return response


@router.post("/admin/auth/email", response_class=HTMLResponse)
def email_login_request(
    request: Request,
    email: str = Form(""),
) -> HTMLResponse:
    """Email a passwordless link without revealing whether an account exists."""

    from sales_support_agent.services.access import store
    from sales_support_agent.services.access.notify import (
        email_delivery_configured,
        send_email_login_link,
    )
    from sales_support_agent.services.access.pages import render_email_login_sent_page

    normalized = (email or "").strip().lower()
    settings = _auth_settings(request)
    if (
        store.valid_email(normalized)
        and email_delivery_configured(settings)
    ):
        user = store.get_user_by_email(normalized)
        if user and user.get("status") == "active":
            token = secrets.token_urlsafe(32)
            created, _ = store.create_email_login_token(
                normalized,
                token=token,
                request_fingerprint=_request_fingerprint(request),
            )
            if created:
                link = f"{_public_base(request)}/admin/auth/email/{token}"
                send_email_login_link(
                    settings,
                    to_email=normalized,
                    login_link=link,
                )
    # The response is deliberately identical for unknown, suspended,
    # rate-limited, and deliverable addresses.
    return HTMLResponse(render_email_login_sent_page())


@router.get("/admin/auth/email/{token}", response_class=HTMLResponse)
def email_login_accept(request: Request, token: str) -> Response:
    """Consume one passwordless link and mint the ordinary signed session."""

    from sales_support_agent.services.access import store
    from sales_support_agent.services.access.pages import (
        render_email_login_invalid_page,
        render_suspended_page,
    )

    email = store.consume_email_login_token(token)
    if not email:
        return HTMLResponse(render_email_login_invalid_page(), status_code=410)
    user = store.get_user_by_email(email)
    if not user:
        return HTMLResponse(render_email_login_invalid_page(), status_code=410)
    if user.get("status") != "active":
        return HTMLResponse(render_suspended_page(email), status_code=403)
    store.record_login(email)
    try:
        from sales_support_agent.services.hr import store as hr_store

        employee = hr_store.get_employee_by_email(email)
        is_hr_login = bool(
            employee
            and (employee.get("hr_login_email") or "").strip().lower() == email
        )
        if is_hr_login and employee.get("status") != "active":
            return HTMLResponse(render_suspended_page(email), status_code=403)
        redirect_to = (
            "/app"
            if is_hr_login and employee.get("status") == "active"
            else "/admin"
        )
    except Exception:
        redirect_to = "/admin"
    return _mint_session(
        request,
        _auth_settings(request),
        email,
        user.get("name") or email,
        redirect_to=redirect_to,
    )


def _external_login_allowed(request: Request, email: str) -> bool:
    """Whether a non-allowed-domain Google account may sign in.

    Externals get in only when explicitly provisioned by an admin: either a
    pending invite for exactly this email (carried by the invite-link cookie)
    or an existing user row from an earlier invite. Uninvited externals are
    rejected outright and never file access requests.
    """
    try:
        from sales_support_agent.services.access import store
        token = request.cookies.get("pending_invite", "").strip()
        if token:
            invite = store.get_pending_invite_by_token(token)
            if invite and (invite.get("email") or "").lower() == email:
                return True
        return store.get_user_by_email(email) is not None
    except Exception:  # noqa: BLE001 — fail closed
        logger.exception("External-login check failed for %s", email)
        return False


def _mint_session(request: Request, settings, email: str, name: str, *, redirect_to: str = "/admin") -> RedirectResponse:
    """Mint a session cookie and redirect to /admin.

    The cookie MUST be signed with the host app's session settings (see
    _session_settings) so /admin accepts it; `settings` here is the OAuth config
    and is only used for the (cosmetic) role label."""
    sess = _session_settings(request)
    try:
        role = get_user_role(settings, email)
    except Exception:  # noqa: BLE001 — role label is non-critical; never block login
        role = "member"
    token = create_user_session_token(sess, email=email, name=name, role=role)
    response = RedirectResponse(redirect_to, status_code=302)
    response.delete_cookie(_OAUTH_STATE_COOKIE, path="/")
    response.set_cookie(value=token, **_cookie_opts(request))
    return response


@router.get("/admin/pending", response_class=HTMLResponse)
def access_pending(request: Request) -> Response:
    from sales_support_agent.services.access import store as _store
    from sales_support_agent.services.access.pages import render_access_pending_page
    from sales_support_agent.services.auth_deps import get_session_user_from_request

    identity = get_session_user_from_request(request)
    if not identity:
        return RedirectResponse("/admin/login", status_code=302)

    email = (identity.get("email") or "").strip().lower()
    name = (identity.get("name") or email).strip()
    try:
        user = _store.get_user_by_email(email)
        if user and (user.get("is_superadmin") or user.get("permissions")):
            return RedirectResponse("/admin", status_code=302)

        pending = _store.get_pending_access_request_for_email(email)
        if not pending:
            request_id = _store.create_access_request(email, name)
            pending = _store.get_access_request(request_id)
    except Exception:  # noqa: BLE001 — show an explicit outage state instead of a 500
        logger.exception("Access pending page failed while resolving %s", email)
        from sales_support_agent.services.access.pages import render_access_unavailable_page

        return HTMLResponse(render_access_unavailable_page(email), status_code=503)
    return HTMLResponse(render_access_pending_page(email, request_record=pending), status_code=200)


def _rbac_login(request: Request, settings, email: str, name: str, picture: str = ""):
    """RBAC decision tree after Google confirms the email.

    Returns a Response to send immediately, or None to fall through to legacy.
    """
    from fastapi.responses import HTMLResponse as _HTML
    from sales_support_agent.services.access import store as _store
    from sales_support_agent.services.access.pages import (
        render_suspended_page,
    )

    superadmins = {e.lower() for e in (getattr(settings, "rbac_superadmin_emails", ()) or ())}

    # 1. Super-admin — always allow; ensure seeded.
    if email in superadmins:
        _store.upsert_user(email, name, is_superadmin=True, status="active", picture_url=picture)
        _store.record_login(email)
        return _mint_session(request, settings, email, name)

    # 2. Pending invite cookie (set when user visited /admin/access/invite/{token}).
    invite_token = request.cookies.get("pending_invite", "").strip()
    if invite_token:
        invite = _store.get_pending_invite_by_token(invite_token)
        if invite and invite.get("email") == email:
            _store.upsert_user(email, name, role_id=invite["role_id"], status="active", picture_url=picture)
            _store.accept_invite(invite["id"])
            _store.record_login(email)
            resp = _mint_session(request, settings, email, name)
            resp.delete_cookie("pending_invite", path="/")
            return resp

    allowed_domain = (getattr(settings, "google_oauth_allowed_domain", "") or "").strip().lower()
    default_tools = tuple(getattr(settings, "rbac_auto_provision_domain_tools", ()) or ())
    should_grant_default_tools = bool(allowed_domain and email.endswith(f"@{allowed_domain}") and default_tools)

    # 3. Existing provisioned user.
    existing = _store.get_user_by_email(email)
    if existing:
        if existing.get("status") == "suspended":
            return _HTML(render_suspended_page(email), status_code=403)
        employee_self_service_only = set(existing.get("permissions") or set()) == {
            "hr.access"
        }
        if (
            should_grant_default_tools
            and not existing.get("is_superadmin")
            and not employee_self_service_only
        ):
            _store.set_user_permissions(
                existing["id"],
                sorted(set(existing.get("permissions") or set()).union(default_tools)),
            )
        if picture:
            _store.upsert_user(email, name, picture_url=picture)
        _store.record_login(email)
        return _mint_session(request, settings, email, name)

    # 4. Allowed-domain reviewer — provision narrow default tools so internal
    # users are not blocked on a manual DB grant before they can review work.
    if should_grant_default_tools:
        uid = _store.upsert_user(email, name, status="active", picture_url=picture)
        _store.set_user_permissions(uid, default_tools)
        _store.record_login(email)
        return _mint_session(request, settings, email, name)

    # 5. Unprovisioned — create an access request and show the pending page.
    _store.create_access_request(email, name)
    return _mint_session(request, settings, email, name, redirect_to="/admin/pending")
