from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sales_support_agent.api.vercel_cron_router import router


def _client(*, ready: bool = False) -> TestClient:
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal")
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    app.state.session_factory = sessionmaker(bind=engine)
    app.state.ready = ready
    app.state.render_git_commit = "test-commit"
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
        "synthetic-health",
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


def test_synthetic_health_is_read_only_and_available_before_cutover(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "false")

    response = _client(ready=True).get(
        "/api/vercel-cron/synthetic-health",
        headers={"Authorization": "Bearer cron-secret"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "passed"
    assert response.json()["external_writes"] is False
    assert response.json()["checks"] == {"application_ready": True, "database": True}
