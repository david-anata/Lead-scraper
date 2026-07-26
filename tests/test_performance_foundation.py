from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from starlette.requests import Request

from sales_support_agent.services import auth_deps
from sales_support_agent.services.admin_nav import (
    render_agent_nav_styles,
    render_agent_stylesheet_links,
)
from sales_support_agent.services.access.pages import (
    render_access_pending_page,
    render_forbidden_page,
)
from sales_support_agent.services.performance import install_performance_middleware


def _request(*, cookie: str = "") -> Request:
    app = SimpleNamespace(
        state=SimpleNamespace(
            settings=SimpleNamespace(
                admin_cookie_name="session",
                admin_session_secret="secret",
                admin_username="admin",
                rbac_enabled=True,
                rbac_superadmin_emails=(),
            )
        )
    )
    headers = [(b"cookie", cookie.encode())] if cookie else []
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "https",
            "path": "/admin",
            "raw_path": b"/admin",
            "query_string": b"",
            "headers": headers,
            "client": ("test", 1),
            "server": ("test", 443),
            "app": app,
        }
    )


def test_session_identity_is_parsed_once_per_request() -> None:
    request = _request(cookie="session=token")
    settings = request.app.state.settings
    identity = {"email": "operator@anatainc.com", "name": "Operator"}
    with (
        patch.object(auth_deps, "_all_auth_settings", return_value=[settings]),
        patch.object(auth_deps, "get_session_user", return_value=identity) as decode,
    ):
        assert auth_deps.get_session_user_from_request(request) == identity
        assert auth_deps.get_session_user_from_request(request) == identity
    assert decode.call_count == 1


def test_current_user_access_is_resolved_once_per_request() -> None:
    request = _request()
    identity = {"email": "operator@anatainc.com", "name": "Operator"}
    access = {
        "email": "operator@anatainc.com",
        "name": "Operator",
        "status": "active",
        "is_superadmin": False,
        "permissions": {"sales.deals"},
    }
    with (
        patch.object(auth_deps, "get_session_user_from_request", return_value=identity),
        patch("sales_support_agent.services.access.store.resolve_access", return_value=access) as resolve,
    ):
        assert auth_deps.get_current_user(request) == access
        assert auth_deps.get_current_user(request) == access
    assert resolve.call_count == 1


def test_performance_middleware_reports_sql_and_static_cache_policy() -> None:
    app = FastAPI()
    engine = create_engine("sqlite:///:memory:")
    install_performance_middleware(app, engine)

    @app.get("/read")
    def read() -> PlainTextResponse:
        with engine.connect() as connection:
            connection.execute(text("select 1"))
        return PlainTextResponse("ok")

    @app.get("/static/test.css")
    def static_asset() -> PlainTextResponse:
        return PlainTextResponse("body{}", media_type="text/css")

    client = TestClient(app)
    response = client.get("/read")
    assert response.status_code == 200
    assert "app;dur=" in response.headers["server-timing"]
    assert 'desc="1 queries"' in response.headers["server-timing"]

    versioned = client.get("/static/test.css?v=abc123")
    assert versioned.headers["cache-control"] == "public, max-age=31536000, immutable"

    unversioned = client.get("/static/test.css")
    assert unversioned.headers["cache-control"] == "public, max-age=3600, must-revalidate"


def test_navigation_stylesheet_links_are_available_for_migrated_pages() -> None:
    markup = render_agent_stylesheet_links()
    assert markup.startswith('<link rel="stylesheet"')
    assert "/admin/assets/navigation.css?v=" in markup
    assert "/static/admin.css?v=" in markup
    assert "<style" not in markup


def test_legacy_navigation_style_helper_remains_css_compatible() -> None:
    css = render_agent_nav_styles()
    assert ".topbar" in css
    assert "<link" not in css


def test_access_pages_use_canonical_operator_and_transition_shells() -> None:
    forbidden = render_forbidden_page(
        user={
            "email": "operator@anatainc.com",
            "permissions": set(),
            "is_superadmin": False,
        },
        tool_label="Finance",
    )
    assert 'class="app app--operator access-page"' in forbidden
    assert 'id="agent-main-content"' in forbidden
    assert "/admin/assets/navigation.css?v=" in forbidden
    assert forbidden.count("<h1") == 1

    pending = render_access_pending_page("operator@anatainc.com")
    assert 'class="app app--transition"' in pending
    assert 'class="app-container app-container--focused app-page"' in pending
    assert pending.count("<h1") == 1
