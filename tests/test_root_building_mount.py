from __future__ import annotations

import dataclasses
import os
import tempfile
import uuid
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

import main as production_main
from sales_support_agent import main as modular_main
from sales_support_agent.config import load_settings
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.services.admin_auth import create_user_session_token


def _route_paths(app) -> set[str]:
    paths: set[str] = set()

    def visit(routes) -> None:
        for route in routes:
            path = getattr(route, "path", None)
            if path:
                paths.add(path)
            original_router = getattr(route, "original_router", None)
            if original_router is not None:
                visit(original_router.routes)
            nested_routes = getattr(route, "routes", None)
            if nested_routes is not None:
                visit(nested_routes)

    visit(app.routes)
    return paths


def test_both_entrypoints_mount_complete_building_surface() -> None:
    expected = {
        "/admin/building",
        "/admin/building/content",
        "/admin/building/agreement-readiness",
        "/api/public/building/offerings",
        "/api/public/building/inquiries",
        "/api/public/building/event-estimates",
        "/api/internal/building/offerings/{offering_id}/rate-plans",
        "/api/internal/building/agreement-readiness/packages",
        "/api/internal/building/billing/schedules/{schedule_id}/approve",
        "/api/internal/building/calendar/readiness",
        "/api/integrations/stripe/webhook",
        "/api/integrations/resend/webhook",
    }

    assert expected <= _route_paths(production_main.app)
    assert expected <= _route_paths(modular_main.app)


def test_root_runtime_uses_complete_agent_settings_for_operational_routers() -> None:
    test_app = FastAPI()
    lead_builder = SimpleNamespace(apollo_api_key="lead-builder")
    agent_settings = SimpleNamespace(
        internal_api_key="agent-internal",
        building_campaign_token_secret="building-csrf",
    )

    with patch(
        "sales_support_agent.config.load_settings",
        return_value=agent_settings,
    ):
        production_main._configure_agent_runtime_settings(test_app, lead_builder)

    assert test_app.state.lead_builder_settings is lead_builder
    assert test_app.state.agent_settings is agent_settings
    assert test_app.state.settings is agent_settings


def test_root_entrypoint_serves_authenticated_building_control() -> None:
    database_path = os.path.join(
        tempfile.gettempdir(),
        f"root_building_mount_{uuid.uuid4().hex}.db",
    )
    factory = create_session_factory("sqlite:///" + database_path)
    init_database(factory)
    settings = dataclasses.replace(
        load_settings(),
        internal_api_key="root-building-internal",
        building_campaign_token_secret="root-building-csrf",
    )

    original_settings = getattr(production_main.app.state, "settings", None)
    original_agent_settings = getattr(
        production_main.app.state,
        "agent_settings",
        None,
    )
    original_factory = getattr(
        production_main.app.state,
        "session_factory",
        None,
    )
    try:
        production_main.app.state.settings = settings
        production_main.app.state.agent_settings = settings
        production_main.app.state.session_factory = factory
        client = TestClient(production_main.app)
        token = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David Narayan",
            role="admin",
        )
        client.cookies.set(settings.admin_cookie_name, token)

        page = client.get("/admin/building")
        assert page.status_code == 200, page.text
        assert "Building Control" in page.text
        assert "Arena launch readiness" in page.text

        public = client.get("/api/public/building/offerings")
        assert public.status_code == 200, public.text
        assert public.json()["offerings"] == []
    finally:
        production_main.app.state.settings = original_settings
        production_main.app.state.agent_settings = original_agent_settings
        production_main.app.state.session_factory = original_factory
        factory.kw["bind"].dispose()
        if os.path.exists(database_path):
            os.remove(database_path)
