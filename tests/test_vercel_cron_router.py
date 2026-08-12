from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from sales_support_agent.api.vercel_cron_router import router


def _client() -> TestClient:
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal")
    app.include_router(router)
    return TestClient(app)


def test_all_vercel_crons_require_bearer_secret(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    for path in (
        "website-ops",
        "content",
        "stale-leads",
        "gmail-sync",
        "daily-digest",
        "durable-tasks",
        "sales-operator",
        "hr-reminders",
        "building-operations",
    ):
        assert _client().get(f"/api/vercel-cron/{path}").status_code == 401


def test_all_vercel_crons_are_inert_before_cutover(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "false")
    headers = {"Authorization": "Bearer cron-secret"}
    for path in (
        "website-ops",
        "content",
        "stale-leads",
        "gmail-sync",
        "daily-digest",
        "durable-tasks",
        "sales-operator",
        "hr-reminders",
        "building-operations",
    ):
        response = _client().get(f"/api/vercel-cron/{path}", headers=headers)
        assert response.status_code == 200
        assert response.json()["status"] == "disabled"
