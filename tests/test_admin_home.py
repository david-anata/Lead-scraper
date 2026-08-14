from __future__ import annotations

from sales_support_agent.services.admin_home import (
    _valid_recent_path,
    accessible_workspaces,
    get_home_preferences,
    record_recent_page,
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
        assert "Your Agent workspace" in landing.text
        assert "Needs you" in landing.text
        assert "Your shortcuts" in landing.text


def test_recent_paths_exclude_auth_and_home_actions() -> None:
    assert _valid_recent_path("/admin/sales/deals") is True
    assert _valid_recent_path("/admin/login") is False
    assert _valid_recent_path("/admin/home/clock") is False
    assert _valid_recent_path("/admin/sales?owner=me") is False


def test_recent_pages_are_filtered_to_authorized_workspaces(monkeypatch) -> None:
    stored = {
        "shortcuts": ["sales", "finance"],
        "recent": [
            {"path": "/admin/finances", "title": "Finance Today"},
            {"path": "/admin/sales/deals", "title": "Sales Deal Board"},
            {"path": "/admin/hr/reports", "title": "Reports"},
        ],
    }
    monkeypatch.setattr(
        "sales_support_agent.services.admin_home.kv_get_json",
        lambda _key, _default: stored,
    )
    workspaces = accessible_workspaces({"permissions": {"sales.deals"}})

    preferences = get_home_preferences("sales@example.com", workspaces)

    assert preferences["shortcuts"] == ["sales"]
    assert preferences["recent"] == [
        {"path": "/admin/sales/deals", "title": "Sales Deal Board"}
    ]


def test_recent_page_rejects_an_inaccessible_workspace(monkeypatch) -> None:
    monkeypatch.setattr(
        "sales_support_agent.services.admin_home.kv_get_json",
        lambda _key, _default: {},
    )
    monkeypatch.setattr(
        "sales_support_agent.services.admin_home.kv_set_json",
        lambda *_args: None,
    )
    workspaces = accessible_workspaces({"permissions": {"sales.deals"}})

    assert record_recent_page("sales@example.com", "/admin/finances", workspaces) is False
    assert record_recent_page("sales@example.com", "/admin/sales/deals", workspaces) is True


def test_workspace_directory_uses_revised_names() -> None:
    workspaces = accessible_workspaces({"is_superadmin": True})
    titles = {item["title"] for item in workspaces}
    assert "Owner Overview" in titles
    assert any(item["href"] == "/admin/fulfillment/sales" for item in workspaces)


def test_home_renders_compact_clock_and_priority_items() -> None:
    page = render_admin_home_page(
        user={"name": "Alex Operator", "permissions": {"hr.access", "sales.deals"}},
        preferences={"shortcuts": ["sales", "hr"], "recent": []},
        context={
            "clock": {"is_clocked_in": False, "last_event": "Last clock out 4:30 PM", "today_hours": 7.5},
            "needs_you": [{"workspace": "Sales", "title": "Review Acme", "detail": "Record the next action.", "href": "/admin/sales/deals/1", "priority": 20}],
        },
    )
    assert "Clocked out" in page
    assert "7.50 hours today" in page
    assert "Review Acme" in page
    assert 'action="/admin/home/shortcuts"' in page
