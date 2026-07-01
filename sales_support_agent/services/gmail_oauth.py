"""Gmail OAuth helpers for per-user inbox self-connect."""

from __future__ import annotations

from urllib.parse import urlencode

import requests

from sales_support_agent.config import Settings

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"

GMAIL_CONNECT_SCOPE = "openid email profile https://www.googleapis.com/auth/gmail.modify"


def gmail_oauth_enabled(settings: Settings) -> bool:
    return bool(settings.google_oauth_client_id and settings.google_oauth_client_secret)


def gmail_auth_url(settings: Settings, *, redirect_uri: str, state: str, login_hint: str = "") -> str:
    params = {
        "client_id": settings.google_oauth_client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": GMAIL_CONNECT_SCOPE,
        "state": state,
        "access_type": "offline",
        "prompt": "consent select_account",
        "include_granted_scopes": "true",
    }
    if login_hint:
        params["login_hint"] = login_hint
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def exchange_gmail_code(settings: Settings, *, code: str, redirect_uri: str) -> dict:
    token_resp = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "code": code,
            "client_id": settings.google_oauth_client_id,
            "client_secret": settings.google_oauth_client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=20,
    )
    token_resp.raise_for_status()
    token_payload = token_resp.json() or {}
    access_token = str(token_payload.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("Google OAuth completed without an access token.")

    userinfo_resp = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    userinfo_resp.raise_for_status()
    return {
        "tokens": token_payload,
        "userinfo": userinfo_resp.json() or {},
    }
