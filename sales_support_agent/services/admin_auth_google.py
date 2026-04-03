"""Google OAuth 2.0 helpers for admin SSO."""

from __future__ import annotations

from urllib.parse import urlencode

import requests

from sales_support_agent.config import Settings

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"


def google_oauth_enabled(settings: Settings) -> bool:
    return bool(settings.google_oauth_client_id and settings.google_oauth_client_secret)


def google_auth_url(settings: Settings, *, redirect_uri: str, state: str) -> str:
    params = {
        "client_id": settings.google_oauth_client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "hd": settings.google_oauth_allowed_domain,
        "access_type": "online",
        "prompt": "select_account",
    }
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def exchange_google_code(settings: Settings, *, code: str, redirect_uri: str) -> dict:
    """Exchange auth code for tokens, then fetch userinfo. Returns userinfo dict."""
    token_resp = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "code": code,
            "client_id": settings.google_oauth_client_id,
            "client_secret": settings.google_oauth_client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=15,
    )
    token_resp.raise_for_status()
    access_token = token_resp.json().get("access_token", "")

    userinfo_resp = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    userinfo_resp.raise_for_status()
    return userinfo_resp.json()


def get_user_role(settings: Settings, email: str) -> str:
    """Look up role for email in admin_role_map; fall back to admin_default_role."""
    return settings.admin_role_map.get(email.lower(), settings.admin_default_role)
