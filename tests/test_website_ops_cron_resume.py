"""Cron persistence, invocation budget, and in-progress outcome classification."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient

from sales_support_agent.api.website_ops_jobs_router import (
    WebsiteOpsInvocationBudget,
    _daily_run_has_verified_outcome,
    _daily_run_is_fresh,
    _run_due_modes,
    _run_embedded_pulse,
    router as website_ops_jobs_router,
)
from sales_support_agent.models import database as database_module
from sales_support_agent.models.database import create_session_factory
from sales_support_agent.services.website_ops import (
    get_website_ops_run_state,
    render_dashboard_page,
    write_website_ops_run_state,
)
from sales_support_agent.services.website_ops_storage import (
    WebsiteOpsStorageMiddleware,
    _CACHE_SYNC_LOCK,
    ensure_website_ops_storage_schema,
    restore_website_ops_root,
    snapshot_website_ops_root,
)


def _pipeline_result(status: str, *, delta: int = 0, ok: bool = True, message: str = ""):
    return SimpleNamespace(
        ok=ok,
        message=message or status,
        report={
            "run_outcome": {
                "status": status,
                "summary": message or status,
                "production_delta_count": delta,
                "actual_output": message,
            }
        },
    )


def _settings(root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        website_ops_root=root,
        website_ops_site_urls=("https://example.com/",),
        website_ops_execute_approved=False,
        internal_api_key="internal-test-key",
    )


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        fixed = datetime(2026, 8, 3, 10, 5, tzinfo=ZoneInfo("America/Denver"))
        if tz is None:
            return fixed.replace(tzinfo=None)
        return fixed.astimezone(tz)


class _NightDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        fixed = datetime(2026, 8, 3, 3, 5, tzinfo=ZoneInfo("America/Denver"))
        if tz is None:
            return fixed.replace(tzinfo=None)
        return fixed.astimezone(tz)


def test_one_invocation_queues_the_next_mode_and_the_following_pulse_resumes_it(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    calls: list[str] = []

    def fake_run(settings, mode="daily"):
        calls.append(mode)
        return _pipeline_result("production_verified", delta=1, message="published")

    with mock.patch(
        "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
        side_effect=fake_run,
    ):
        first = _run_due_modes(
            settings,
            ["daily", "weekly", "monthly"],
            trigger="render_cron",
            force=True,
            pulse_slot="2026-08-03:08",
            business_date=date(2026, 8, 3),
        )
        second = _run_due_modes(
            settings,
            ["daily", "weekly", "monthly"],
            trigger="render_cron",
            force=True,
            pulse_slot="2026-08-03:09",
            business_date=date(2026, 8, 3),
        )

    assert calls == ["daily", "weekly"]
    assert first["daily"]["status"] == "succeeded"
    assert first["weekly"]["status"] == "queued"
    assert first["monthly"]["status"] == "queued"
    assert second["weekly"]["status"] == "succeeded"
    assert get_website_ops_run_state(settings, "weekly")["status"] == "succeeded"
    assert get_website_ops_run_state(settings, "monthly")["recovery_status"] == "deferred_for_budget"


def test_exhausted_invocation_budget_defers_work_for_the_next_pulse(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    clock = {"now": 0.0}

    def fake_run(settings, mode="daily"):
        clock["now"] = 10.0
        return _pipeline_result("production_verified", delta=1)

    budget = WebsiteOpsInvocationBudget(
        pipeline_slots=2,
        max_seconds=10,
        clock=lambda: clock["now"],
    )
    with mock.patch(
        "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
        side_effect=fake_run,
    ) as run:
        deferred = _run_due_modes(
            settings,
            ["daily", "weekly"],
            trigger="render_cron",
            force=True,
            pulse_slot="2026-08-03:08",
            business_date=date(2026, 8, 3),
            budget=budget,
        )
        resumed = _run_due_modes(
            settings,
            ["daily", "weekly"],
            trigger="render_cron",
            force=True,
            pulse_slot="2026-08-03:09",
            business_date=date(2026, 8, 3),
        )

    assert run.call_count == 2
    assert deferred["daily"]["status"] == "succeeded"
    assert deferred["weekly"]["status"] == "queued"
    assert resumed["weekly"]["status"] == "succeeded"


def test_failed_outcome_is_still_a_single_pass_failure(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    with mock.patch(
        "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
        return_value=_pipeline_result(
            "failed_outcome",
            ok=False,
            message="No verified production delta.",
        ),
    ) as run, mock.patch(
        "sales_support_agent.api.website_ops_jobs_router.send_website_ops_failure_email"
    ) as failure_email:
        result = _run_due_modes(
            settings,
            ["daily"],
            trigger="render_cron",
            force=True,
            business_date=date(2026, 8, 3),
        )

    assert run.call_count == 1
    assert result["daily"]["status"] == "failed_outcome"
    failure_email.assert_called_once()
    assert get_website_ops_run_state(settings, "daily")["status"] == "failed_outcome"


def test_in_progress_run_is_fresh_and_not_a_verified_outcome() -> None:
    local_now = datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)
    state = {
        "runs": {
            "daily": {
                "run_date": "2026-08-01",
                "status": "work_in_progress",
                "outcome_status": "work_in_progress",
                "production_delta_count": "0",
            }
        }
    }
    assert _daily_run_is_fresh(state, local_now)
    assert not _daily_run_has_verified_outcome(state, local_now)


def test_dashboard_calls_in_progress_work_running(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    write_website_ops_run_state(
        settings,
        "daily",
        {
            "status": "work_in_progress",
            "run_date": "2026-08-03",
            "outcome_status": "work_in_progress",
            "outcome_message": "Qualified work is actively queued.",
            "actual_output": "329 active qualified item(s); no production delta yet.",
            "production_delta_count": "0",
            "recovery_status": "resume_next_pulse",
        },
    )
    html = render_dashboard_page(settings)
    assert "Qualified work is still running" in html
    assert "Needs attention" not in html


def test_embedded_scheduler_waits_for_the_next_pulse_while_work_is_in_progress() -> None:
    settings = _settings(Path("runtime/test-website-ops"))
    local_now = datetime(2026, 8, 1, 11, 52, tzinfo=timezone.utc)
    with mock.patch(
        "sales_support_agent.api.website_ops_jobs_router.get_website_ops_run_state",
        return_value={
            "status": "work_in_progress",
            "last_pulse_slot": "2026-08-01:11",
            "run_date": "2026-08-01",
        },
    ), mock.patch(
        "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
    ) as run:
        result = _run_embedded_pulse(settings, local_now)

    assert result["status"] == "skipped"
    run.assert_not_called()


def test_cron_get_persists_in_progress_state_and_health_get_does_not(tmp_path: Path) -> None:
    previous_engine = database_module.engine
    factory = create_session_factory(f"sqlite:///{tmp_path / 'mirror.db'}")
    engine = factory.kw["bind"]
    ensure_website_ops_storage_schema(engine)
    database_module.engine = None
    root = tmp_path / "ops"
    root.mkdir()
    app = FastAPI()
    app.state.settings = _settings(root)
    app.state.session_factory = factory
    app.include_router(website_ops_jobs_router)
    app.add_middleware(WebsiteOpsStorageMiddleware)
    snapshots: list[Path] = []
    real_snapshot = snapshot_website_ops_root

    def counting_snapshot(bound_engine, bound_root):
        snapshots.append(Path(bound_root))
        return real_snapshot(bound_engine, bound_root)

    try:
        with mock.patch.dict(
            "os.environ",
            {
                "WEBSITE_OPS_DATABASE_MIRROR": "true",
                "CRON_SECRET": "cron-test-key",
            },
        ), mock.patch(
            "sales_support_agent.services.website_ops_storage.snapshot_website_ops_root",
            side_effect=counting_snapshot,
        ), mock.patch(
            "sales_support_agent.api.website_ops_jobs_router.datetime",
            _FrozenDateTime,
        ), mock.patch(
            "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
            return_value=_pipeline_result(
                "work_in_progress",
                message="329 active qualified item(s); no production delta yet.",
            ),
        ):
            client = TestClient(app)
            response = client.get(
                "/api/jobs/website-ops/run",
                headers={"Authorization": "Bearer cron-test-key"},
            )
            assert response.status_code == 200
            body = response.json()
            assert body["details"]["daily"]["status"] == "work_in_progress"
            assert body["details"]["weekly"]["status"] == "queued"
            assert snapshots

            restored = tmp_path / "restored"
            restore_website_ops_root(engine, restored)
            payload = json.loads(
                (restored / "state" / "website_ops_run_state.json").read_text()
            )
            assert payload["runs"]["daily"]["status"] == "work_in_progress"
            assert payload["runs"]["daily"]["outcome_status"] == "work_in_progress"
            assert payload["updated_at"]

            snapshots.clear()
            health = client.get("/api/jobs/website-ops/health")
            assert health.status_code == 200
            assert snapshots == []
            assert health.json()["state_updated_at"] == payload["updated_at"]
            assert health.json()["runs"]["daily"]["status"] == "work_in_progress"
    finally:
        database_module.engine = previous_engine
        engine.dispose()


def test_cron_get_outside_the_pulse_window_does_not_rewrite_the_mirror(tmp_path: Path) -> None:
    previous_engine = database_module.engine
    factory = create_session_factory(f"sqlite:///{tmp_path / 'mirror.db'}")
    engine = factory.kw["bind"]
    ensure_website_ops_storage_schema(engine)
    database_module.engine = None
    root = tmp_path / "ops"
    root.mkdir()
    app = FastAPI()
    app.state.settings = _settings(root)
    app.state.session_factory = factory
    app.include_router(website_ops_jobs_router)
    app.add_middleware(WebsiteOpsStorageMiddleware)
    try:
        with mock.patch.dict(
            "os.environ",
            {
                "WEBSITE_OPS_DATABASE_MIRROR": "true",
                "CRON_SECRET": "cron-test-key",
            },
        ), mock.patch(
            "sales_support_agent.services.website_ops_storage.snapshot_website_ops_root",
        ) as snapshot, mock.patch(
            "sales_support_agent.api.website_ops_jobs_router.datetime",
            _NightDateTime,
        ), mock.patch(
            "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
        ) as run:
            response = TestClient(app).get(
                "/api/jobs/website-ops/run",
                headers={"Authorization": "Bearer cron-test-key"},
            )

        assert response.status_code == 200
        assert response.json()["status"] == "skipped"
        run.assert_not_called()
        snapshot.assert_not_called()
    finally:
        database_module.engine = previous_engine
        engine.dispose()


def test_cron_get_waits_instead_of_starting_a_second_pass_while_the_cache_is_locked(tmp_path: Path) -> None:
    previous_engine = database_module.engine
    factory = create_session_factory(f"sqlite:///{tmp_path / 'mirror.db'}")
    engine = factory.kw["bind"]
    database_module.engine = None
    root = tmp_path / "ops"
    root.mkdir()
    app = FastAPI()
    app.state.settings = _settings(root)
    app.state.session_factory = factory
    app.include_router(website_ops_jobs_router)
    app.add_middleware(WebsiteOpsStorageMiddleware)
    _CACHE_SYNC_LOCK.acquire()
    try:
        with mock.patch.dict(
            "os.environ",
            {
                "WEBSITE_OPS_DATABASE_MIRROR": "true",
                "CRON_SECRET": "cron-test-key",
            },
        ), mock.patch(
            "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
        ) as run:
            response = TestClient(app).get(
                "/api/jobs/website-ops/run",
                headers={"Authorization": "Bearer cron-test-key"},
            )
        assert response.status_code == 200
        assert response.json()["status"] == "skipped"
        run.assert_not_called()
    finally:
        _CACHE_SYNC_LOCK.release()
        database_module.engine = previous_engine
        engine.dispose()
