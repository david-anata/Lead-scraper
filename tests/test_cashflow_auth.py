"""Parameterized tests for the finance role gate in cashflow_router.py."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.services.admin_auth import create_user_session_token


def _make_settings(
    *,
    secret: str = "test-secret",
    cookie_name: str = "admin_session",
    ttl_hours: int = 24,
) -> SimpleNamespace:
    return SimpleNamespace(
        admin_session_secret=secret,
        admin_cookie_name=cookie_name,
        admin_session_ttl_hours=ttl_hours,
    )


def _build_app(settings: SimpleNamespace) -> FastAPI:
    app = FastAPI()
    app.state.settings = settings
    app.include_router(cashflow_router)
    return app


_SETTINGS = _make_settings()


def _make_token(role: str) -> str:
    return create_user_session_token(
        _SETTINGS,
        email=f"{role}@example.com",
        name=role.title(),
        role=role,
        now=datetime.now(timezone.utc),
    )


def _get_finances_with_token(token: str | None) -> int:
    """Make a GET /admin/finances request with an optional session token cookie."""
    app = _build_app(_SETTINGS)
    client = TestClient(app, follow_redirects=False)
    if token:
        client.cookies.set(_SETTINGS.admin_cookie_name, token)
    with patch(
        "sales_support_agent.services.cashflow.overview.list_obligations",
        return_value=[],
    ):
        resp = client.get("/admin/finances")
    return resp.status_code


class TestFinanceRoleGate(unittest.TestCase):
    """
    Parametrize over roles and expected HTTP status codes.
    admin and finance → 200 (page rendered)
    sales, ops → 303 (redirect to login)
    unauthenticated → 303
    """

    def test_admin_role_gets_200(self) -> None:
        self.assertEqual(_get_finances_with_token(_make_token("admin")), 200)

    def test_finance_role_gets_200(self) -> None:
        self.assertEqual(_get_finances_with_token(_make_token("finance")), 200)

    def test_sales_role_gets_303(self) -> None:
        self.assertEqual(_get_finances_with_token(_make_token("sales")), 303)

    def test_ops_role_gets_303(self) -> None:
        self.assertEqual(_get_finances_with_token(_make_token("ops")), 303)

    def test_unauthenticated_gets_303(self) -> None:
        self.assertEqual(_get_finances_with_token(None), 303)

    def test_redirect_target_is_login_page(self) -> None:
        app = _build_app(_SETTINGS)
        client = TestClient(app, follow_redirects=False)
        client.cookies.set(_SETTINGS.admin_cookie_name, _make_token("sales"))
        resp = client.get("/admin/finances")
        self.assertEqual(resp.status_code, 303)
        self.assertIn("/admin/login", resp.headers["location"])


if __name__ == "__main__":
    unittest.main()
