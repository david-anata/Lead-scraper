from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.api.vercel_cron_router import router


def _client(*, ready: bool = False) -> TestClient:
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal")
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
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
        "preflight",
        "durable-recovery-probe",
        "shadow-preflight",
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


def test_global_switch_only_runs_explicitly_allowlisted_job(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "true")
    monkeypatch.setenv("VERCEL_CRON_ENABLED_JOBS", "content")
    headers = {"Authorization": "Bearer cron-secret"}

    response = _client().get("/api/vercel-cron/website-ops", headers=headers)

    assert response.status_code == 200
    assert response.json() == {
        "status": "disabled",
        "job": "website-ops",
        "message": "This Vercel scheduled writer remains disabled until explicitly allowlisted at cutover.",
    }


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


def test_cron_preflight_proves_prerequisites_without_enabling_writes(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "false")
    client = _client(ready=True)
    with client.app.state.session_factory() as session:
        session.execute(
            __import__("sqlalchemy").text(
                "CREATE TABLE durable_task_queue (id INTEGER PRIMARY KEY)"
            )
        )
        session.commit()

    response = client.get(
        "/api/vercel-cron/preflight",
        headers={"Authorization": "Bearer cron-secret"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "passed"
    assert response.json()["external_writes"] is False
    assert response.json()["checks"] == {
        "application_ready": True,
        "database_ready": True,
        "durable_queue_ready": True,
        "writes_disabled": True,
    }
    assert response.json()["write_schedules"] == [
        "website-ops",
        "content",
        "stale-leads",
        "gmail-sync",
        "daily-digest",
        "durable-tasks",
        "sales-operator",
        "hr-reminders",
        "building-operations",
        "outbound-morning",
    ]


def test_cron_preflight_fails_closed_if_writes_are_enabled(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "true")

    response = _client(ready=True).get(
        "/api/vercel-cron/preflight",
        headers={"Authorization": "Bearer cron-secret"},
    )

    assert response.status_code == 503
    assert response.json()["checks"]["writes_disabled"] is False
    assert response.json()["external_writes"] is False


def test_durable_recovery_probe_is_staging_only_and_has_no_external_write(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    monkeypatch.setenv("VERCEL_STAGING", "true")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "false")

    response = _client(ready=True).get(
        "/api/vercel-cron/durable-recovery-probe",
        headers={"Authorization": "Bearer cron-secret"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "passed"
    assert response.json()["attempts"] == 2
    assert response.json()["overlap_blocked"] is True
    assert response.json()["replay_blocked"] is True
    assert response.json()["external_writes"] is False


def test_durable_recovery_probe_rejects_non_staging_and_enabled_writes(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    headers = {"Authorization": "Bearer cron-secret"}

    monkeypatch.delenv("VERCEL_STAGING", raising=False)
    assert _client().get(
        "/api/vercel-cron/durable-recovery-probe", headers=headers
    ).status_code == 404

    monkeypatch.setenv("VERCEL_STAGING", "true")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "true")
    assert _client().get(
        "/api/vercel-cron/durable-recovery-probe", headers=headers
    ).status_code == 409


def test_content_cron_uses_the_authenticated_scheduler_boundary(monkeypatch) -> None:
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "true")
    monkeypatch.setenv("VERCEL_CRON_ENABLED_JOBS", "content")
    observed = {}

    def run_cycle(session_factory, settings, *, mode, force):
        observed.update(
            session_factory=session_factory,
            settings=settings,
            mode=mode,
            force=force,
        )
        return {"status": "ok", "mode": mode}

    monkeypatch.setattr(
        "sales_support_agent.api.vercel_cron_router.run_content_cycle",
        run_cycle,
    )
    client = _client(ready=True)
    response = client.get(
        "/api/vercel-cron/content",
        headers={"Authorization": "Bearer cron-secret"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "mode": "scheduled"}
    assert observed["session_factory"] is client.app.state.session_factory
    assert observed["settings"] is client.app.state.settings
    assert observed["force"] is False
