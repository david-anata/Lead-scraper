"""Shared auth helpers for FastAPI request handlers.

Identity comes from the signed session cookie (email); *authorization* is
resolved from the RBAC tables on every request (super-admins and the legacy
break-glass admin bypass per-tool checks). `require_tool(key)` is the single
dependency used by every guard.
"""
from __future__ import annotations

import sys
import os
from types import SimpleNamespace
from typing import Optional
from datetime import datetime, timezone

from fastapi import HTTPException, Request

from sales_support_agent.services.admin_auth import get_session_user
from sales_support_agent.services.access.catalog import ALL_TOOL_KEYS, label_for


def _all_auth_settings(request: Request) -> list:
    """Return every settings object on app.state that can validate a session token.

    main.py and sales_support_agent/main.py each store a DIFFERENT settings object
    with potentially different admin_session_secret defaults.  Collecting all of
    them lets get_session_user_from_request try every secret so a password-login
    cookie (minted by main.py's AdminDashboardSettings) is always found even when
    ADMIN_DASHBOARD_SESSION_SECRET is not set in the environment.
    """
    seen_settings: set = set()
    result = []
    candidates = [getattr(request.app.state, attr, None) for attr in ("agent_settings", "admin_dashboard_settings", "settings")]

    if getattr(request.app.state, "admin_dashboard_settings", None) is None:
        root_main = sys.modules.get("main")
        loader = getattr(root_main, "load_admin_dashboard_settings", None)
        if callable(loader):
            try:
                candidates.append(loader())
            except Exception:
                pass
        candidates.append(SimpleNamespace(
            admin_username=os.getenv("ADMIN_DASHBOARD_USERNAME", "admin").strip() or "admin",
            admin_session_secret=(
                os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
                or os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip()
                or "lead-scraper-admin-session-secret"
            ),
            admin_cookie_name=os.getenv("ADMIN_DASHBOARD_COOKIE_NAME", "lead_scraper_admin_session").strip() or "lead_scraper_admin_session",
            admin_session_ttl_hours=int((os.getenv("ADMIN_DASHBOARD_SESSION_TTL_HOURS", "24") or "24").strip()),
            rbac_enabled=True,
        ))

    for s in candidates:
        if s is None:
            continue
        if not (hasattr(s, "admin_cookie_name") and hasattr(s, "admin_session_secret")):
            continue
        key = (
            getattr(s, "admin_cookie_name", ""),
            getattr(s, "admin_session_secret", ""),
            getattr(s, "admin_username", ""),
        )
        if key in seen_settings:
            continue
        seen_settings.add(key)
        result.append(s)
    return result


def _get_auth_settings(request: Request):
    """Return the primary settings object for the current request (first in list)."""
    candidates = _all_auth_settings(request)
    if candidates:
        return candidates[0]
    return getattr(request.app.state, "settings", None)


def get_session_user_from_request(request: Request) -> Optional[dict]:
    """Return the *identity* dict ({email,name,role}) from the cookie, or None.

    Tries every settings object on app.state so a password-login cookie
    (minted by main.py, which may use a different default secret) is found
    even when ADMIN_DASHBOARD_SESSION_SECRET is not explicitly set.
    """
    try:
        for settings in _all_auth_settings(request):
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
        "session_issued_at": "",
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
        out = _superadmin_dict(email, name)
        out["session_issued_at"] = identity.get("session_issued_at", "")
        try:
            from sales_support_agent.services.access import store
            row = store.get_user_by_email(email)
            if row:
                out["name"] = row.get("name") or out["name"]
                out["picture"] = row.get("picture") or ""
        except Exception:  # noqa: BLE001 — enrichment only; never block a super-admin
            pass
        return out

    try:
        from sales_support_agent.services.access import store
        access = store.resolve_access(email)
    except Exception:  # noqa: BLE001 — never 500 the whole app on a lookup hiccup
        access = None
    if access:
        if access.get("is_superadmin"):
            sa = _superadmin_dict(email, access.get("name") or name)
            sa["picture"] = access.get("picture") or ""  # keep the Google avatar
            sa["session_issued_at"] = identity.get("session_issued_at", "")
            return sa
        access["session_issued_at"] = identity.get("session_issued_at", "")
        return access

    # Authenticated, domain-allowed, but not provisioned -> default deny.
    return {
        "email": email, "name": name, "role_id": None, "role_name": "",
        "status": "unprovisioned", "is_superadmin": False, "permissions": set(),
        "session_issued_at": identity.get("session_issued_at", ""),
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


def require_any_tool(*keys: str):
    """Allow any named capability; useful while splitting legacy broad roles."""
    if not keys:
        raise ValueError("At least one tool key is required")

    def _dep(request: Request) -> dict:
        identity = get_session_user_from_request(request)
        if not identity:
            raise HTTPException(status_code=303, headers={"Location": "/admin/login"})
        user = get_current_user(request)
        permissions = set((user or {}).get("permissions") or ())
        if user and (user.get("is_superadmin") or any(key in permissions for key in keys)):
            return user
        raise ToolForbidden(user, keys[0])

    return _dep


def require_all_tools(*keys: str, legacy_keys: tuple[str, ...] = ()):
    """Require all narrow capabilities, or one explicitly named legacy grant."""
    if not keys:
        raise ValueError("At least one tool key is required")

    def _dep(request: Request) -> dict:
        identity = get_session_user_from_request(request)
        if not identity:
            raise HTTPException(status_code=303, headers={"Location": "/admin/login"})
        user = get_current_user(request)
        permissions = set((user or {}).get("permissions") or ())
        if user and (
            user.get("is_superadmin")
            or all(key in permissions for key in keys)
            or any(key in permissions for key in legacy_keys)
        ):
            return user
        raise ToolForbidden(user, keys[0])

    return _dep


def require_recent_tool(
    *keys: str, legacy_keys: tuple[str, ...] = (), max_age_minutes: int = 30
):
    """Require capability plus a session minted within the sensitive-action window."""
    base = require_all_tools(*keys, legacy_keys=legacy_keys)

    def _dep(request: Request) -> dict:
        user = base(request)
        try:
            issued_at = datetime.fromtimestamp(
                int(user.get("session_issued_at") or 0), tz=timezone.utc
            )
        except (TypeError, ValueError, OSError):
            issued_at = datetime.fromtimestamp(0, tz=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - issued_at).total_seconds()
        if age_seconds > max_age_minutes * 60:
            next_path = request.url.path
            raise HTTPException(
                status_code=303,
                headers={
                    "Location":
                        f"/admin/login?err=reauth_required&next={next_path}"
                },
            )
        return user

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
