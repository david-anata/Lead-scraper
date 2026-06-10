"""Shared auth helpers for FastAPI request handlers.

Identity comes from the signed session cookie (email); *authorization* is
resolved from the RBAC tables on every request (super-admins and the legacy
break-glass admin bypass per-tool checks). `require_tool(key)` is the single
dependency used by every guard.
"""
from __future__ import annotations

from typing import Optional

from fastapi import HTTPException, Request

from sales_support_agent.services.admin_auth import get_session_user
from sales_support_agent.services.access.catalog import ALL_TOOL_KEYS, label_for


def _get_auth_settings(request: Request):
    """Return the settings object that has admin_cookie_name / admin_session_secret.

    Priority:
      1. app.state.agent_settings  — full sales_support_agent Settings (preferred)
      2. app.state.admin_dashboard_settings — AdminDashboardSettings stored at startup
      3. app.state.settings — lean root Settings (last resort, may lack admin fields)
    """
    agent = getattr(request.app.state, "agent_settings", None)
    if agent is not None and hasattr(agent, "admin_cookie_name"):
        return agent
    admin_ds = getattr(request.app.state, "admin_dashboard_settings", None)
    if admin_ds is not None and hasattr(admin_ds, "admin_cookie_name"):
        return admin_ds
    return request.app.state.settings


def get_session_user_from_request(request: Request) -> Optional[dict]:
    """Return the *identity* dict ({email,name,role}) from the cookie, or None.
    This validates the signed token only — it does NOT decide authorization."""
    try:
        settings = _get_auth_settings(request)
        named = request.cookies.get(settings.admin_cookie_name, "")
        if named:
            user = get_session_user(settings, named)
            if user:
                return user
        for token in request.cookies.values():
            if token == named:
                continue
            user = get_session_user(settings, token)
            if user:
                return user
    except AttributeError:
        pass
    return None


def is_authenticated(request: Request) -> bool:
    return get_session_user_from_request(request) is not None


# ---------------------------------------------------------------------------
# RBAC: resolve effective permissions for the current request
# ---------------------------------------------------------------------------


def _superadmin_dict(email: str, name: str = "") -> dict:
    return {
        "email": email,
        "name": name or email,
        "role_id": None,
        "role_name": "Super-admin",
        "status": "active",
        "is_superadmin": True,
        "permissions": set(ALL_TOOL_KEYS),
    }


def get_current_user(request: Request) -> Optional[dict]:
    """Identity -> enriched access dict, or None if there is no valid session.

    Returns a dict with `permissions: set`, `is_superadmin`, `status`. An
    authenticated but un-provisioned user resolves to empty permissions (status
    "unprovisioned") so callers can offer the request-access flow.
    """
    identity = get_session_user_from_request(request)
    if not identity:
        return None
    email = (identity.get("email") or "").strip().lower()
    name = identity.get("name") or email
    settings = _get_auth_settings(request)

    # Kill-switch: if RBAC is disabled, any authenticated user has full access
    # (restores the pre-RBAC single-admin behaviour).
    if not getattr(settings, "rbac_enabled", True):
        return _superadmin_dict(email, name)

    # Break-glass: configured super-admins and the legacy password admin are
    # always full-access and can never be locked out, even if the DB is empty.
    superadmins = {e.lower() for e in getattr(settings, "rbac_superadmin_emails", ()) or ()}
    admin_username = (getattr(settings, "admin_username", "") or "").strip().lower()
    if email in superadmins or (admin_username and email == admin_username):
        return _superadmin_dict(email, name)

    try:
        from sales_support_agent.services.access import store
        access = store.resolve_access(email)
    except Exception:  # noqa: BLE001 — never 500 the whole app on a lookup hiccup
        access = None
    if access:
        if access.get("is_superadmin"):
            return _superadmin_dict(email, access.get("name") or name)
        return access

    # Authenticated, domain-allowed, but not provisioned -> default deny.
    return {
        "email": email, "name": name, "role_id": None, "role_name": "",
        "status": "unprovisioned", "is_superadmin": False, "permissions": set(),
    }


def has_tool(request: Request, key: str) -> bool:
    user = get_current_user(request)
    if not user:
        return False
    if user.get("is_superadmin"):
        return True
    return key in user.get("permissions", set())


def has_finance_access(request: Request) -> bool:
    """Backward-compatible helper — now role-driven via the `finance` tool."""
    return has_tool(request, "finance")


class ToolForbidden(Exception):
    """Raised by require_tool when an authenticated user lacks a tool. An
    exception handler (registered on both apps) renders a friendly 403 page."""

    def __init__(self, user: Optional[dict], tool_key: str) -> None:
        self.user = user or {}
        self.tool_key = tool_key
        super().__init__(f"forbidden: {tool_key}")


def require_tool(key: str):
    """FastAPI dependency factory. No session -> 303 to login; session without
    the tool -> ToolForbidden (friendly 403). Returns the user dict on success."""

    def _dep(request: Request) -> dict:
        identity = get_session_user_from_request(request)
        if not identity:
            raise HTTPException(status_code=303, headers={"Location": "/admin/login"})
        user = get_current_user(request)
        if user and (user.get("is_superadmin") or key in user.get("permissions", set())):
            return user
        raise ToolForbidden(user, key)

    return _dep


def require_tool_inline(request: Request, key: str):
    """Imperative variant for handlers that render their own pages. Returns
    (user, None) when allowed, or (None, Response) with the redirect/403 to
    return immediately."""
    identity = get_session_user_from_request(request)
    if not identity:
        from fastapi.responses import RedirectResponse
        return None, RedirectResponse(url="/admin/login", status_code=302)
    user = get_current_user(request)
    if user and (user.get("is_superadmin") or key in user.get("permissions", set())):
        return user, None
    return None, render_forbidden_response(request, ToolForbidden(user, key))


def render_forbidden_response(request: Request, exc: "ToolForbidden"):
    from fastapi.responses import HTMLResponse
    from sales_support_agent.services.access.pages import render_forbidden_page

    user = exc.user if isinstance(exc, ToolForbidden) else {}
    key = exc.tool_key if isinstance(exc, ToolForbidden) else ""
    return HTMLResponse(
        render_forbidden_page(user=user, tool_label=label_for(key)),
        status_code=403,
    )
