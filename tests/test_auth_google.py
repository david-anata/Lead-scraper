"""Tests for the Google OAuth callback in auth_router.py."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from sales_support_agent.api.auth_router import router as auth_router
from sales_support_agent.services.admin_auth import (
    create_signed_state_token,
    get_session_user,
)


def _make_settings(
    *,
    secret: str = "test-secret",
    cookie_name: str = "admin_session",
    ttl_hours: int = 24,
    google_client_id: str = "client-id",
    google_client_secret: str = "client-secret",
    allowed_domain: str = "example.com",
    role_map: dict | None = None,
    default_role: str = "ops",
) -> SimpleNamespace:
    return SimpleNamespace(
        admin_session_secret=secret,
        admin_cookie_name=cookie_name,
        admin_session_ttl_hours=ttl_hours,
        google_oauth_client_id=google_client_id,
        google_oauth_client_secret=google_client_secret,
        google_oauth_allowed_domain=allowed_domain,
        admin_role_map=role_map or {},
        admin_default_role=default_role,
    )


def _build_app(settings: SimpleNamespace) -> FastAPI:
    app = FastAPI()
    app.state.settings = settings
    app.include_router(auth_router)
    return app


def _make_state_cookie(settings: SimpleNamespace) -> str:
    """Create a valid signed state token and return it."""
    return create_signed_state_token(settings.admin_session_secret, {"action": "login"})


class TestGoogleCallback(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = _make_settings()
        self.app = _build_app(self.settings)
        self.client = TestClient(self.app, follow_redirects=False)

    def test_csrf_state_mismatch_redirects_to_error(self) -> None:
        # No oauth_state cookie set
        resp = self.client.get("/admin/auth/callback?code=mycode&state=badstate")
        self.assertEqual(resp.status_code, 302)
        self.assertIn("state_mismatch", resp.headers["location"])

    def test_no_code_redirects_to_error(self) -> None:
        state = _make_state_cookie(self.settings)
        # Set the cookie manually via the client
        self.client.cookies.set("oauth_state", state)
        resp = self.client.get(f"/admin/auth/callback?state={state}")
        self.assertEqual(resp.status_code, 302)
        self.assertIn("no_code", resp.headers["location"])

    def test_token_exchange_raises_redirects_to_error(self) -> None:
        state = _make_state_cookie(self.settings)
        self.client.cookies.set("oauth_state", state)
        with patch(
            "sales_support_agent.api.auth_router.exchange_google_code",
            side_effect=Exception("Network error"),
        ):
            resp = self.client.get(f"/admin/auth/callback?code=mycode&state={state}")
        self.assertEqual(resp.status_code, 302)
        self.assertIn("token_exchange", resp.headers["location"])

    def test_domain_not_allowed_redirects_to_error(self) -> None:
        state = _make_state_cookie(self.settings)
        self.client.cookies.set("oauth_state", state)
        with patch(
            "sales_support_agent.api.auth_router.exchange_google_code",
            return_value={
                "email": "user@other.com",
                "name": "User",
                "hd": "other.com",
            },
        ):
            resp = self.client.get(f"/admin/auth/callback?code=mycode&state={state}")
        self.assertEqual(resp.status_code, 302)
        self.assertIn("domain_not_allowed", resp.headers["location"])

    def test_happy_path_sets_cookie_and_redirects_to_admin(self) -> None:
        state = _make_state_cookie(self.settings)
        self.client.cookies.set("oauth_state", state)
        with patch(
            "sales_support_agent.api.auth_router.exchange_google_code",
            return_value={
                "email": "alice@example.com",
                "name": "Alice Smith",
                "hd": "example.com",
            },
        ):
            resp = self.client.get(f"/admin/auth/callback?code=mycode&state={state}")
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.headers["location"], "/admin")
        # Cookie should be set
        self.assertIn(self.settings.admin_cookie_name, resp.cookies)
        # Validate the token
        token = resp.cookies[self.settings.admin_cookie_name]
        user = get_session_user(self.settings, token)
        self.assertIsNotNone(user)
        assert user is not None
        self.assertEqual(user["email"], "alice@example.com")

    def test_get_user_role_uses_admin_role_map(self) -> None:
        settings = _make_settings(
            role_map={"alice@example.com": "finance"},
            default_role="ops",
        )
        app = _build_app(settings)
        client = TestClient(app, follow_redirects=False)
        state = _make_state_cookie(settings)
        client.cookies.set("oauth_state", state)
        with patch(
            "sales_support_agent.api.auth_router.exchange_google_code",
            return_value={
                "email": "alice@example.com",
                "name": "Alice",
                "hd": "example.com",
            },
        ):
            resp = client.get(f"/admin/auth/callback?code=mycode&state={state}")
        self.assertEqual(resp.status_code, 302)
        token = resp.cookies[settings.admin_cookie_name]
        user = get_session_user(settings, token)
        self.assertIsNotNone(user)
        assert user is not None
        self.assertEqual(user["role"], "finance")

    def test_unknown_user_gets_default_role(self) -> None:
        settings = _make_settings(
            role_map={"other@example.com": "finance"},
            default_role="ops",
        )
        app = _build_app(settings)
        client = TestClient(app, follow_redirects=False)
        state = _make_state_cookie(settings)
        client.cookies.set("oauth_state", state)
        with patch(
            "sales_support_agent.api.auth_router.exchange_google_code",
            return_value={
                "email": "unknown@example.com",
                "name": "Unknown",
                "hd": "example.com",
            },
        ):
            resp = client.get(f"/admin/auth/callback?code=mycode&state={state}")
        self.assertEqual(resp.status_code, 302)
        token = resp.cookies[settings.admin_cookie_name]
        user = get_session_user(settings, token)
        self.assertIsNotNone(user)
        assert user is not None
        self.assertEqual(user["role"], "ops")


if __name__ == "__main__":
    unittest.main()
