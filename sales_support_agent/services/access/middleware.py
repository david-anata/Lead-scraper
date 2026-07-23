"""Authorization middleware — the single, comprehensive per-tool gate.

Runs on every request: if the path maps to a catalog tool, it enforces that the
current user holds that tool (super-admins/break-glass bypass). This covers
inline-guarded pages and router-guarded prefixes uniformly, so we don't have to
touch ~30 individual route handlers. Authentication-only routes that map to no
specific tool fall through to their existing guards.

Bypass (never gated here): login/logout/Google-auth, the public invite-accept
link, the QBO OAuth callbacks (Intuit reviewer), `/static`, `/health`, and any
non-`/admin` path. Internal `/api/...` routes keep their API-key guards.
"""

from __future__ import annotations

import logging
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import RedirectResponse

from sales_support_agent.services.access.catalog import TOOL_CATALOG, Tool

logger = logging.getLogger(__name__)

# Paths under /admin that must NOT be tool-gated here.
_BYPASS_PREFIXES = (
    "/admin/login",
    "/admin/logout",
    "/admin/auth",                 # Google OAuth start/callback
    "/admin/access/invite",        # public invite-accept link
    "/admin/finances/qbo",         # QBO OAuth (Intuit reviewer) — intentionally open
)


def _normalize(path: str) -> str:
    p = path.rstrip("/")
    return p or "/"


def _resolve_tool(path: str) -> Optional[Tool]:
    """Most-specific catalog tool whose url prefix matches the path, or None."""
    norm = _normalize(path)
    best: Optional[Tool] = None
    best_len = -1
    for t in TOOL_CATALOG:
        for prefix in t.url_prefixes:
            np = _normalize(prefix)
            matched = (norm == np) if t.exact else (norm == np or norm.startswith(np + "/"))
            if matched and len(np) > best_len:
                best, best_len = t, len(np)
    return best


def _is_bypass(path: str) -> bool:
    if not path.startswith("/admin"):
        return True
    return any(path == p or path.startswith(p + "/") or path == p + "" for p in _BYPASS_PREFIXES) \
        or any(path.startswith(p) for p in _BYPASS_PREFIXES)


class AccessControlMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        path = request.url.path
        if _is_bypass(path):
            return await call_next(request)

        tool = _resolve_tool(path)
        if tool is None:
            # No specific tool for this /admin path — leave it to the route's
            # own authentication guard (e.g. internal sync/lead-build APIs).
            return await call_next(request)

        # Lazy import to avoid import cycles at module load.
        from sales_support_agent.services.auth_deps import (
            ToolForbidden,
            get_current_user,
            get_session_user_from_request,
            render_forbidden_response,
        )
        from sales_support_agent.services.access.catalog import grants_tool

        try:
            if get_session_user_from_request(request) is None:
                return RedirectResponse(url="/admin/login", status_code=302)
            user = get_current_user(request)
            if user and (
                user.get("is_superadmin")
                or grants_tool(set(user.get("permissions") or ()), tool.key)
            ):
                return await call_next(request)
            return render_forbidden_response(request, ToolForbidden(user, tool.key))
        except Exception:  # noqa: BLE001 — a guard error must never 500 every page; fail closed to login
            logger.exception("[access] authorization middleware error on %s", path)
            return RedirectResponse(url="/admin/login", status_code=302)


def install_access_middleware(app) -> None:
    app.add_middleware(AccessControlMiddleware)
