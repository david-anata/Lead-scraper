"""Shared auth helpers for FastAPI request handlers."""
from __future__ import annotations
from typing import Optional
from fastapi import Request
from sales_support_agent.services.admin_auth import get_session_user


def _get_auth_settings(request: Request):
    """Return the settings object that has admin_cookie_name / admin_session_secret.

    Root main.py stores a lean Settings (Apollo/Slack only) at app.state.settings
    and the full sales_support_agent Settings at app.state.agent_settings.  Prefer
    agent_settings; fall back to settings for legacy compatibility.
    """
    agent = getattr(request.app.state, "agent_settings", None)
    if agent is not None and hasattr(agent, "admin_cookie_name"):
        return agent
    return request.app.state.settings


def get_session_user_from_request(request: Request) -> Optional[dict]:
    """Return the authenticated user dict or None. Tries all cookie values."""
    settings = _get_auth_settings(request)
    # Try named cookie first for efficiency, then fall back to all values
    named = request.cookies.get(settings.admin_cookie_name, "")
    if named:
        user = get_session_user(settings, named)
        if user:
            return user
    # Fallback: iterate all cookies (handles cookie-name mismatches)
    for token in request.cookies.values():
        if token == named:
            continue
        user = get_session_user(settings, token)
        if user:
            return user
    return None

def is_authenticated(request: Request) -> bool:
    return get_session_user_from_request(request) is not None

def has_finance_access(request: Request) -> bool:
    user = get_session_user_from_request(request)
    if not user:
        return False
    return user.get("role", "") in ("admin", "finance")
