from __future__ import annotations

from sales_support_agent.services.admin_home import (
    render_admin_home_page,
    render_service_status_page,
)
from sales_support_agent.services.admin_auth import create_admin_session_token


def test_service_status_page_is_human_readable_and_links_to_probe() -> None:
    page = render_service_status_page(ready=True)
    assert "Agent is operational." in page
    assert 'href="/health/ready"' in page
    assert 'href="/admin"' in page
    assert "agent-staging.anatainc.com" not in page


def test_admin_home_shows_only_authorized_workspaces() -> None:
    page = render_admin_home_page(
        user={"name": "Alex Operator", "permissions": {"sales.deals"}}
    )
    assert "Welcome back, Alex." in page
    assert 'href="/admin/sales"' in page
    assert 'href="/admin/finances"' not in page
    assert 'href="/admin/hr"' not in page


def test_admin_home_superadmin_sees_all_workspaces() -> None:
    page = render_admin_home_page(user={"is_superadmin": True})
    assert 'href="/admin/sales"' in page
    assert 'href="/admin/finances"' in page
    assert 'href="/admin/hr"' in page


def test_vercel_entrypoint_serves_status_and_authenticated_home() -> None:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app

    with TestClient(app) as client:
        root = client.get("/")
        assert root.status_code == 200
        assert "Agent is operational." in root.text

        unauthenticated = client.get("/admin", follow_redirects=False)
        assert unauthenticated.status_code == 302
        assert unauthenticated.headers["location"] == "/admin/login"

        settings = app.state.settings
        client.cookies.set(
            settings.admin_cookie_name,
            create_admin_session_token(settings),
        )
        landing = client.get("/admin")
        assert landing.status_code == 200
        assert "Anata operating system" in landing.text
