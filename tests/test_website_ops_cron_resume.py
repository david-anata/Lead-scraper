"""Cron persistence, chunking, and in-progress outcome coverage for Website Ops."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from starlette.requests import Request

from sales_support_agent.api.website_ops_jobs_router import (
    WEBSITE_OPS_STEP_BUDGET_MAX_SECONDS,
    WEBSITE_OPS_STEP_BUDGET_SECONDS,
    _run_due_modes,
    _run_embedded_pulse,
    _website_ops_step_budget_seconds,
    router as website_ops_jobs_router,
    run_scheduled_website_ops,
)
from sales_support_agent.services import website_ops_vendor as website_ops
from sales_support_agent.services.job_lease import ensure_job_lease_schema
from sales_support_agent.services.website_ops import (
    WEBSITE_OPS_FINALIZE_MIN_SECONDS,
    WEBSITE_OPS_SNAPSHOT_HEADROOM_SECONDS,
    WEBSITE_OPS_STAGE_INNER_TIMEOUT_SECONDS,
    get_website_ops_run_state,
    load_feedback_records,
    load_website_ops_run_state,
    run_website_ops,
    save_feedback_record,
    write_website_ops_run_state,
)
from sales_support_agent.services.website_ops_github import PipelineBudgetExceeded
from sales_support_agent.services.website_ops_storage import (
    WebsiteOpsStorageMiddleware,
    ensure_website_ops_storage_schema,
    website_ops_request_persists_state,
)


def _settings(root: Path, *, execute_approved: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        website_ops_root=root,
        website_ops_site_urls=("https://example.com/",),
        website_ops_execute_approved=execute_approved,
        internal_api_key="internal-test-key",
        resend_api_key="",
        openai_api_key="",
    )


def _fake_report() -> dict[str, object]:
    return {
        "date": "2026-11-20",
        "generated_at": "2026-11-20T17:00:00Z",
        "title": "Anata Website Ops Daily Report",
        "scope": "agent-admin daily sweep",
        "status": "healthy",
        "pages_reviewed": 0,
        "pages_healthy": 0,
        "pages_with_issues": 0,
        "issues_found": 0,
        "issue_counts_by_priority": {"P0": 0, "P1": 0, "P2": 0, "P3": 0},
        "pages": [],
        "issues": [],
        "recommendations": [],
        "notes": [],
        "feedback_received": 0,
        "feedback_open": 0,
        "recent_feedback": [],
        "changes_applied": 0,
        "executed_actions": [],
    }


def _stored_file(engine, relative_path: str) -> bytes | None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT content
                  FROM website_ops_files
                 WHERE relative_path = :relative_path
                """
            ),
            {"relative_path": relative_path},
        ).first()
    if row is None:
        return None
    return bytes(row[0])


def test_cron_get_is_a_persisting_request_and_health_is_not() -> None:
    assert website_ops_request_persists_state("GET", "/api/jobs/website-ops/run") is True
    assert website_ops_request_persists_state("GET", "/api/jobs/website-ops/run/") is True
    assert website_ops_request_persists_state("POST", "/api/jobs/website-ops/run") is True
    assert website_ops_request_persists_state("GET", "/api/jobs/website-ops/health") is False
    assert website_ops_request_persists_state("GET", "/admin/website-ops") is False


def test_cron_get_mirrors_run_state_into_postgres(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WEBSITE_OPS_DATABASE_MIRROR", "true")
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    root = tmp_path / "ops"
    root.mkdir()
    engine = create_engine(f"sqlite:///{tmp_path / 'mirror.db'}")
    ensure_website_ops_storage_schema(engine)
    settings = _settings(root)
    app = FastAPI()
    app.state.settings = settings
    app.state.website_ops_settings = settings
    app.state.session_factory = SimpleNamespace(kw={"bind": engine})
    app.add_middleware(WebsiteOpsStorageMiddleware)
    app.include_router(website_ops_jobs_router)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            moment = datetime(2026, 11, 20, 17, 5, tzinfo=timezone.utc)
            if tz is None:
                return moment.replace(tzinfo=None)
            return moment.astimezone(tz)

    def run_and_write(*args, **kwargs):
        from sales_support_agent.services.website_ops import write_website_ops_run_state

        write_website_ops_run_state(
            settings,
            "daily",
            {
                "status": "work_in_progress",
                "run_date": "2026-11-20",
                "outcome_status": "work_in_progress",
                "outcome_message": "Qualified work is in progress.",
                "trigger": "render_cron",
            },
        )
        cursor = root / "state" / "website_ops_daily_pipeline.json"
        cursor.write_text(
            json.dumps({"run_date": "2026-11-20", "next_stage": "report"}),
            encoding="utf-8",
        )
        return {"daily": {"status": "work_in_progress"}}

    sentinel = root / "state"
    sentinel.mkdir(parents=True)
    (sentinel / "sentinel.txt").write_text("original", encoding="utf-8")
    from sales_support_agent.services.website_ops_storage import snapshot_website_ops_root

    snapshot_website_ops_root(engine, root)

    with (
        patch(
            "sales_support_agent.api.website_ops_jobs_router.datetime",
            FrozenDateTime,
        ),
        patch(
            "sales_support_agent.models.database.get_engine",
            side_effect=RuntimeError("isolated test"),
        ),
        patch(
            "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
            side_effect=run_and_write,
        ),
    ):
        client = TestClient(app)
        response = client.get(
            "/api/jobs/website-ops/run",
            headers={"Authorization": "Bearer cron-secret"},
        )
        (sentinel / "sentinel.txt").write_text("changed-by-health", encoding="utf-8")
        health = client.get("/api/jobs/website-ops/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert health.status_code == 200
    mirrored = _stored_file(engine, "state/website_ops_run_state.json")
    assert mirrored is not None
    payload = json.loads(mirrored)
    assert payload["runs"]["daily"]["status"] == "work_in_progress"
    assert payload["runs"]["daily"]["outcome_status"] == "work_in_progress"
    assert payload["updated_at"]
    assert payload["updated_at"] != "2026-09-11T14:47:53Z"
    loaded = load_website_ops_run_state(settings)
    assert loaded["updated_at"] == payload["updated_at"]
    assert get_website_ops_run_state(settings, "daily")["status"] == "work_in_progress"
    cursor = json.loads(_stored_file(engine, "state/website_ops_daily_pipeline.json") or b"{}")
    assert cursor["next_stage"] == "report"
    assert _stored_file(engine, "state/sentinel.txt") == b"original"
    engine.dispose()


def test_step_budget_stays_under_the_vercel_limit(monkeypatch) -> None:
    monkeypatch.setenv("WEBSITE_OPS_STEP_BUDGET_SECONDS", "5000")
    assert _website_ops_step_budget_seconds() == WEBSITE_OPS_STEP_BUDGET_MAX_SECONDS
    monkeypatch.setenv("WEBSITE_OPS_STEP_BUDGET_SECONDS", "1")
    assert _website_ops_step_budget_seconds() == 30


def test_pipeline_pauses_before_the_report_and_resumes_on_the_next_invocation(tmp_path) -> None:
    root = tmp_path / "ops"
    settings = _settings(root)
    business_date = date(2026, 11, 20)
    fake_pipeline = {"report": _fake_report(), "observations": [], "artifacts": {}}
    fake_overlay = {
        "goal": {"primary": "Increase qualified leads."},
        "action_queue": [],
        "analytics_status": {"search_console": False, "ga4": False, "notes": []},
        "support_requests": [],
        "page_insights": [],
    }
    with (
        patch(
            "sales_support_agent.services.website_ops.discover_website_ops_urls",
            return_value=("https://example.com/",),
        ),
        patch(
            "sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline",
            return_value=fake_pipeline,
        ) as pipeline,
        patch(
            "sales_support_agent.services.website_ops.build_autonomy_overlay",
            return_value=fake_overlay,
        ),
    ):
        paused = run_website_ops(
            settings,
            mode="daily",
            deadline=50,
            clock=lambda: 100,
            business_date=business_date,
        )
        cursor_path = root / "state" / "website_ops_daily_pipeline.json"
        cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
        resumed = run_website_ops(
            settings,
            mode="daily",
            deadline=400,
            clock=lambda: 0,
            business_date=business_date,
        )

    assert paused.ok is True
    assert paused.report["run_outcome"]["status"] == "work_in_progress"
    assert paused.report["run_outcome"]["resume_required"] is True
    assert paused.report["run_outcome"]["production_delta_count"] == 0
    assert cursor["next_stage"] == "report"
    assert cursor["run_date"] == "2026-11-20"
    assert pipeline.call_count == 1
    assert resumed.ok is True
    finished = json.loads(cursor_path.read_text(encoding="utf-8"))
    assert finished["next_stage"] == "complete"
    assert finished["run_date"] == "2026-11-20"


def test_execution_chunk_resumes_the_remaining_item_on_the_next_invocation(tmp_path) -> None:
    root = tmp_path / "ops"
    settings = _settings(root, execute_approved=True)
    report_dir = root / "reports" / "daily"
    report_dir.mkdir(parents=True)
    (report_dir / "2026-11-01-report.md").write_text(
        "# Report\n\nDate: 2026-11-01\n",
        encoding="utf-8",
    )
    first = save_feedback_record(
        settings,
        {
            "summary": "First metadata fix",
            "status": "approved",
            "page_url": "https://example.com/a",
            "action_type": "meta_update",
            "suggested_action_type": "meta_update",
            "execution_eligibility": "auto_execute",
            "action_value": "First title",
        },
    )
    second = save_feedback_record(
        settings,
        {
            "summary": "Second metadata fix",
            "status": "approved",
            "page_url": "https://example.com/b",
            "action_type": "meta_update",
            "suggested_action_type": "meta_update",
            "execution_eligibility": "auto_execute",
            "action_value": "Second title",
        },
    )
    calls: list[str] = []

    def execute(settings, config, record, raise_on_error=False, budget_deadline=None):
        feedback_id = str(record.get("feedback_id", ""))
        calls.append(feedback_id)
        website_ops.update_feedback_entry(record, {"status": "done", "execution_error": ""})
        return {
            "feedback_id": feedback_id,
            "verification_status": "verified",
            "commit_sha": "abc123",
            "production_url": str(record.get("page_url") or "https://example.com/"),
        }

    ticks = iter([0, 0, 100])

    def first_clock() -> float:
        return next(ticks)

    business_date = date(2026, 11, 20)
    fake_pipeline = {"report": _fake_report(), "observations": [], "artifacts": {}}
    fake_overlay = {
        "goal": {"primary": "Increase qualified leads."},
        "action_queue": [],
        "analytics_status": {"search_console": False, "ga4": False, "notes": []},
        "support_requests": [],
        "page_insights": [],
    }
    with (
        patch(
            "sales_support_agent.services.website_ops._execute_record",
            side_effect=execute,
        ),
        patch(
            "sales_support_agent.services.website_ops.discover_website_ops_urls",
            return_value=("https://example.com/",),
        ),
        patch(
            "sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline",
            return_value=fake_pipeline,
        ) as pipeline,
        patch(
            "sales_support_agent.services.website_ops.build_autonomy_overlay",
            return_value=fake_overlay,
        ),
    ):
        paused = run_website_ops(
            settings,
            mode="daily",
            deadline=50,
            clock=first_clock,
            business_date=business_date,
        )
        assert len(calls) == 1
        assert pipeline.call_count == 0
        resumed = run_website_ops(
            settings,
            mode="daily",
            deadline=400,
            clock=lambda: 0,
            business_date=business_date,
        )

    assert paused.report["run_outcome"]["status"] == "work_in_progress"
    assert calls == [second["feedback_id"], first["feedback_id"]] or calls == [
        first["feedback_id"],
        second["feedback_id"],
    ]
    assert len(set(calls)) == 2
    assert pipeline.call_count == 1
    assert resumed.ok is True


def test_paused_weekly_run_is_not_recorded_as_a_successful_period(tmp_path) -> None:
    root = tmp_path / "ops"
    settings = _settings(root)
    paused = SimpleNamespace(
        ok=True,
        message="Paused before the report stage.",
        report={
            "run_outcome": {
                "status": "work_in_progress",
                "summary": "Paused before the report stage.",
                "production_delta_count": 0,
                "resume_required": True,
            }
        },
    )
    with (
        patch(
            "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
            return_value=paused,
        ),
        patch(
            "sales_support_agent.api.website_ops_jobs_router.send_website_ops_failure_email"
        ) as failure_email,
    ):
        result = _run_due_modes(
            settings,
            ["weekly"],
            trigger="render_cron",
            force=True,
            business_date=date(2026, 11, 16),
        )

    state = get_website_ops_run_state(settings, "weekly")
    assert result["weekly"]["status"] == "work_in_progress"
    assert state["status"] == "work_in_progress"
    assert state["last_successful_date"] == ""
    assert state["last_error"] == ""
    failure_email.assert_not_called()


def test_execution_budget_pause_leaves_the_approved_record_in_place(tmp_path) -> None:
    root = tmp_path / "ops"
    settings = _settings(root, execute_approved=True)
    report_dir = root / "reports" / "daily"
    report_dir.mkdir(parents=True)
    (report_dir / "2026-11-01-report.md").write_text("# Report\n", encoding="utf-8")
    record = save_feedback_record(
        settings,
        {
            "summary": "Publish the waiting article",
            "status": "approved",
            "page_url": "https://anatainc.com/blog/waiting",
            "action_type": "publish_blog_article",
            "suggested_action_type": "publish_blog_article",
            "execution_eligibility": "auto_execute",
            "action_value": json.dumps({"slug": "waiting"}),
        },
    )
    with patch(
        "sales_support_agent.services.website_ops._execute_record",
        side_effect=PipelineBudgetExceeded("verification paused"),
    ):
        paused = run_website_ops(
            settings,
            mode="daily",
            deadline=100,
            clock=lambda: 0,
            business_date=date(2026, 11, 20),
        )

    assert paused.report["run_outcome"]["status"] == "work_in_progress"
    assert paused.report["run_outcome"]["resume_required"] is True
    stored = next(
        item
        for item in load_feedback_records(settings)
        if item["feedback_id"] == record["feedback_id"]
    )
    assert stored["status"] == "approved"
    assert stored.get("execution_error", "") == ""


def test_in_progress_run_persists_across_invocations_until_production_is_verified(tmp_path) -> None:
    root = tmp_path / "ops"
    settings = _settings(root)
    active = SimpleNamespace(
        ok=True,
        message="329 active qualified item(s); no production delta yet.",
        report={
            "run_outcome": {
                "status": "work_in_progress",
                "summary": "329 active qualified item(s); no production delta yet.",
                "actual_output": "329 active qualified item(s); no production delta yet.",
                "production_delta_count": 0,
                "resume_required": True,
            }
        },
    )
    verified = SimpleNamespace(
        ok=True,
        message="published",
        report={
            "run_outcome": {
                "status": "production_verified",
                "summary": "1 production change was independently verified.",
                "production_delta_count": 1,
            }
        },
    )
    with (
        patch(
            "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
            side_effect=[active, verified],
        ) as run,
        patch(
            "sales_support_agent.api.website_ops_jobs_router.send_website_ops_failure_email"
        ) as failure_email,
    ):
        first = _run_due_modes(
            settings,
            ["daily"],
            trigger="render_cron",
            force=True,
            business_date=date(2026, 11, 20),
        )
        midway = get_website_ops_run_state(settings, "daily")
        second = _run_due_modes(
            settings,
            ["daily"],
            trigger="render_cron",
            force=True,
            business_date=date(2026, 11, 20),
        )

    assert run.call_count == 2
    assert first["daily"]["status"] == "work_in_progress"
    assert first["daily"]["attempts"] == 1
    assert midway["status"] == "work_in_progress"
    assert midway["outcome_status"] == "work_in_progress"
    assert midway["last_error"] == ""
    assert "329" in midway["actual_output"]
    assert load_website_ops_run_state(settings)["updated_at"]
    assert second["daily"]["status"] == "succeeded"
    assert get_website_ops_run_state(settings, "daily")["last_successful_date"] == "2026-11-20"
    failure_email.assert_not_called()
    deadlines = [call.kwargs["deadline"] for call in run.call_args_list]
    assert len(deadlines) == 2
    assert all(item > 0 for item in deadlines)


class _SimulatedKill(BaseException):
    """Stand-in for a timeout or cancellation that is not an Exception."""


def _lease_rows(engine) -> list[dict[str, str]]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT run_key, status, details_json
                  FROM scheduled_job_runs
                 ORDER BY run_key
                """
            )
        ).mappings()
        return [
            {
                "run_key": str(row["run_key"]),
                "status": str(row["status"]),
                "details_json": str(row["details_json"]),
            }
            for row in rows
        ]


def _run_request(app, method: str = "GET", body: bytes = b"") -> Request:
    sent = {"done": False}

    async def receive():
        if sent["done"]:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent["done"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": "/api/jobs/website-ops/run",
        "raw_path": b"/api/jobs/website-ops/run",
        "query_string": b"",
        "headers": [(b"x-internal-api-key", b"internal-test-key")],
        "client": ("127.0.0.1", 123),
        "server": ("test", 80),
        "app": app,
    }
    return Request(scope, receive)


def _leased_app(root: Path, engine):
    app = FastAPI()
    app.state.settings = _settings(root)
    app.state.session_factory = SimpleNamespace(kw={"bind": engine})
    return app


def test_simulated_timeout_fails_the_claimed_http_lease(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "sales_support_agent.api.website_ops_jobs_router.WEBSITE_OPS_PULSE_HOURS",
        tuple(range(24)),
    )
    root = tmp_path / "ops"
    engine = create_engine(f"sqlite:///{tmp_path / 'lease.db'}")
    ensure_job_lease_schema(engine)
    app = _leased_app(root, engine)

    async def killed(*args, **kwargs):
        raise _SimulatedKill("vercel timeout")

    with (
        patch(
            "sales_support_agent.models.database.get_engine",
            return_value=engine,
        ),
        patch(
            "sales_support_agent.api.website_ops_jobs_router.run_in_threadpool",
            killed,
        ),
    ):
        try:
            asyncio.run(run_scheduled_website_ops(_run_request(app)))
        except _SimulatedKill:
            pass
        else:
            raise AssertionError("The simulated kill should propagate.")

    rows = _lease_rows(engine)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert "interrupted or timed out" in rows[0]["details_json"]
    engine.dispose()


def test_successful_pulse_does_not_overwrite_the_lease_as_interrupted(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sales_support_agent.api.website_ops_jobs_router.WEBSITE_OPS_PULSE_HOURS",
        tuple(range(24)),
    )
    root = tmp_path / "ops"
    engine = create_engine(f"sqlite:///{tmp_path / 'lease.db'}")
    ensure_job_lease_schema(engine)
    app = _leased_app(root, engine)
    with (
        patch(
            "sales_support_agent.models.database.get_engine",
            return_value=engine,
        ),
        patch(
            "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
            return_value={"daily": {"status": "succeeded"}},
        ),
    ):
        result = asyncio.run(run_scheduled_website_ops(_run_request(app, "POST")))

    assert result["status"] == "ok"
    rows = _lease_rows(engine)
    assert len(rows) == 1
    assert rows[0]["status"] == "succeeded"
    assert "interrupted or timed out" not in rows[0]["details_json"]
    engine.dispose()


def test_embedded_pulse_timeout_fails_the_claimed_lease(tmp_path) -> None:
    root = tmp_path / "ops"
    engine = create_engine(f"sqlite:///{tmp_path / 'lease.db'}")
    ensure_job_lease_schema(engine)
    settings = _settings(root)
    local_now = datetime(2026, 11, 26, 11, 5, tzinfo=timezone.utc)
    with (
        patch(
            "sales_support_agent.models.database.get_engine",
            return_value=engine,
        ),
        patch(
            "sales_support_agent.api.website_ops_jobs_router.database_mirror_enabled",
            return_value=False,
        ),
        patch(
            "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
            side_effect=_SimulatedKill("killed"),
        ),
    ):
        try:
            _run_embedded_pulse(settings, local_now)
        except _SimulatedKill:
            pass
        else:
            raise AssertionError("The simulated kill should propagate.")

    rows = _lease_rows(engine)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert "interrupted or timed out" in rows[0]["details_json"]
    engine.dispose()


def test_run_route_expires_stale_leases_and_leaves_live_ones(tmp_path, monkeypatch) -> None:
    root = tmp_path / "ops"
    engine = create_engine(f"sqlite:///{tmp_path / 'lease.db'}")
    ensure_job_lease_schema(engine)
    now = datetime.now(timezone.utc)
    past = (now - timedelta(hours=3)).isoformat()
    future = (now + timedelta(hours=2)).isoformat()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO scheduled_job_runs (
                    job_key, run_key, status, owner_token, started_at,
                    lease_expires_at, details_json
                ) VALUES
                    ('website_ops', 'expired-running', 'running', 'a', :started, :past, '{}'),
                    ('website_ops', 'live-running', 'running', 'b', :started, :future, '{}'),
                    ('other_job', 'other-expired', 'running', 'c', :started, :past, '{}')
                """
            ),
            {"started": past, "past": past, "future": future},
        )
    app = _leased_app(root, engine)
    with (
        patch(
            "sales_support_agent.models.database.get_engine",
            return_value=engine,
        ),
        patch(
            "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
            return_value={"daily": {"status": "succeeded"}},
        ),
    ):
        asyncio.run(run_scheduled_website_ops(_run_request(app)))

    by_key = {row["run_key"]: row["status"] for row in _lease_rows(engine)}
    assert by_key["expired-running"] == "failed"
    assert by_key["live-running"] == "running"
    assert by_key["other-expired"] == "running"
    engine.dispose()


def test_stale_weekly_and_daily_file_state_fails_and_the_next_pulse_can_run(tmp_path) -> None:
    root = tmp_path / "ops"
    settings = _settings(root)
    old = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    fresh = datetime.now(timezone.utc).isoformat()
    write_website_ops_run_state(
        settings,
        "weekly",
        {
            "status": "running",
            "last_started_at": old,
            "run_date": "2026-11-23",
            "last_successful_date": "2026-11-23",
        },
    )
    write_website_ops_run_state(
        settings,
        "daily",
        {
            "status": "queued",
            "last_started_at": old,
            "run_date": "2026-11-25",
        },
    )
    paused = SimpleNamespace(
        ok=True,
        message="Daily website ops run completed.",
        report={
            "run_outcome": {
                "status": "production_verified",
                "summary": "1 production change was independently verified.",
                "production_delta_count": 1,
            }
        },
    )
    with (
        patch(
            "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
            return_value=paused,
        ) as run,
        patch(
            "sales_support_agent.api.website_ops_jobs_router.send_website_ops_failure_email"
        ),
    ):
        skipped = _run_due_modes(
            settings,
            ["weekly"],
            trigger="vercel_cron",
            business_date=date(2026, 11, 26),
        )
        weekly = get_website_ops_run_state(settings, "weekly")
        daily_after_skip = get_website_ops_run_state(settings, "daily")
        followed = _run_due_modes(
            settings,
            ["daily"],
            trigger="vercel_cron",
            business_date=date(2026, 11, 26),
        )

    assert skipped["weekly"]["status"] == "skipped"
    assert run.call_count == 0 or all(
        call.kwargs.get("mode") != "weekly" for call in run.call_args_list
    )
    assert weekly["status"] == "failed"
    assert "45-minute" in weekly["last_error"]
    assert "next pulse" in weekly["last_error"]
    assert daily_after_skip["status"] == "failed"
    assert "queued" in daily_after_skip["last_error"]
    assert followed["daily"]["status"] == "succeeded"
    assert run.call_count == 1

    write_website_ops_run_state(
        settings,
        "daily",
        {
            "status": "running",
            "last_started_at": fresh,
            "run_date": "2026-11-26",
            "last_successful_date": "2026-11-26",
        },
    )
    with patch(
        "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
    ) as fresh_run:
        held = _run_due_modes(
            settings,
            ["daily"],
            trigger="vercel_cron",
            business_date=date(2026, 11, 26),
        )
    assert held["daily"]["status"] == "skipped"
    assert get_website_ops_run_state(settings, "daily")["status"] == "running"
    fresh_run.assert_not_called()


def test_last_pulse_slot_survives_load_and_save(tmp_path) -> None:
    settings = _settings(tmp_path / "ops")
    write_website_ops_run_state(
        settings,
        "daily",
        {
            "status": "succeeded",
            "run_date": "2026-11-26",
            "last_pulse_slot": "2026-11-26:09",
        },
    )
    loaded = load_website_ops_run_state(settings)
    assert loaded["runs"]["daily"]["last_pulse_slot"] == "2026-11-26:09"
    assert get_website_ops_run_state(settings, "daily")["last_pulse_slot"] == "2026-11-26:09"
    write_website_ops_run_state(settings, "daily", {"last_error": ""})
    assert get_website_ops_run_state(settings, "daily")["last_pulse_slot"] == "2026-11-26:09"


def test_finalize_is_not_entered_when_the_deadline_is_close(tmp_path) -> None:
    root = tmp_path / "ops"
    settings = _settings(root)
    state = root / "state"
    state.mkdir(parents=True)
    cursor = {
        "run_date": "2026-11-26",
        "next_stage": "finalize",
        "executed_actions": [],
        "has_baseline": False,
        "monitored_urls": ["https://example.com/"],
        "observations": [],
        "report": _fake_report(),
    }
    (state / "website_ops_daily_pipeline.json").write_text(
        json.dumps(cursor),
        encoding="utf-8",
    )
    with (
        patch(
            "sales_support_agent.services.website_ops.website_ops.write_daily_report_artifacts"
        ) as write_artifacts,
        patch(
            "sales_support_agent.services.website_ops.load_indexing_inventory"
        ) as inventory,
        patch(
            "sales_support_agent.services.website_ops.send_website_ops_report_email"
        ) as email,
    ):
        result = run_website_ops(
            settings,
            mode="daily",
            deadline=100,
            clock=lambda: 70,
            business_date=date(2026, 11, 26),
        )

    assert result.report["run_outcome"]["resume_required"] is True
    assert result.report["run_outcome"]["last_stage"] == "finalize"
    write_artifacts.assert_not_called()
    inventory.assert_not_called()
    email.assert_not_called()
    saved = json.loads((state / "website_ops_daily_pipeline.json").read_text(encoding="utf-8"))
    assert saved["next_stage"] == "finalize"


def test_stage_inner_timeout_aborts_well_under_the_vercel_wall(tmp_path, monkeypatch) -> None:
    assert WEBSITE_OPS_STEP_BUDGET_SECONDS < 800
    assert WEBSITE_OPS_STEP_BUDGET_MAX_SECONDS <= 600
    assert WEBSITE_OPS_STEP_BUDGET_MAX_SECONDS < 800
    assert WEBSITE_OPS_STAGE_INNER_TIMEOUT_SECONDS < 800
    assert WEBSITE_OPS_SNAPSHOT_HEADROOM_SECONDS < WEBSITE_OPS_STEP_BUDGET_SECONDS
    assert WEBSITE_OPS_FINALIZE_MIN_SECONDS < WEBSITE_OPS_STAGE_INNER_TIMEOUT_SECONDS
    assert WEBSITE_OPS_STEP_BUDGET_MAX_SECONDS + WEBSITE_OPS_SNAPSHOT_HEADROOM_SECONDS < 800

    monkeypatch.setattr(
        "sales_support_agent.services.website_ops.WEBSITE_OPS_STAGE_INNER_TIMEOUT_SECONDS",
        0.2,
    )
    root = tmp_path / "ops"
    settings = _settings(root)
    release = Event()

    def slow_pipeline(*args, **kwargs):
        release.wait(30)
        return {"report": _fake_report(), "observations": [], "artifacts": {}}

    started = time.monotonic()
    try:
        with (
            patch(
                "sales_support_agent.services.website_ops.discover_website_ops_urls",
                return_value=("https://example.com/",),
            ),
            patch(
                "sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline",
                side_effect=slow_pipeline,
            ),
        ):
            result = run_website_ops(
                settings,
                mode="daily",
                deadline=started + 500,
                clock=time.monotonic,
                business_date=date(2026, 11, 26),
            )
        elapsed = time.monotonic() - started
    finally:
        release.set()

    assert elapsed < 5
    assert elapsed < 800
    assert result.report["run_outcome"]["resume_required"] is True
    assert result.report["run_outcome"]["last_stage"] == "report"


def test_timed_out_report_worker_does_not_write_artifacts(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "sales_support_agent.services.website_ops.WEBSITE_OPS_STAGE_INNER_TIMEOUT_SECONDS",
        0.2,
    )
    root = tmp_path / "ops"
    settings = _settings(root)
    release = Event()
    seen: dict[str, object] = {}

    def slow_pipeline(*args, **kwargs):
        seen["persist"] = kwargs.get("persist", True)
        release.wait(30)
        if kwargs.get("persist", True):
            output = root / "reports" / "daily"
            output.mkdir(parents=True, exist_ok=True)
            (output / "late.json").write_text("late", encoding="utf-8")
        return {"report": _fake_report(), "observations": [], "artifacts": {}}

    started = time.monotonic()
    try:
        with (
            patch(
                "sales_support_agent.services.website_ops.discover_website_ops_urls",
                return_value=("https://example.com/",),
            ),
            patch(
                "sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline",
                side_effect=slow_pipeline,
            ),
        ):
            result = run_website_ops(
                settings,
                mode="daily",
                deadline=started + 500,
                clock=time.monotonic,
                business_date=date(2026, 11, 26),
            )
    finally:
        release.set()
    time.sleep(0.3)

    assert seen["persist"] is False
    assert result.report["run_outcome"]["resume_required"] is True
    late = root / "reports" / "daily" / "late.json"
    assert not late.exists()
    report_dir = root / "reports" / "daily"
    assert not report_dir.exists() or not any(report_dir.glob("*.json"))


def test_http_trigger_labels_vercel_get_and_render_post(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "sales_support_agent.api.website_ops_jobs_router.WEBSITE_OPS_PULSE_HOURS",
        tuple(range(24)),
    )
    root = tmp_path / "ops"
    app = _leased_app(root, engine=None)
    triggers: list[str] = []

    def capture(*args, **kwargs):
        triggers.append(kwargs["trigger"])
        return {"daily": {"status": "succeeded"}}

    with (
        patch(
            "sales_support_agent.models.database.get_engine",
            side_effect=RuntimeError("isolated"),
        ),
        patch(
            "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
            side_effect=capture,
        ),
    ):
        asyncio.run(run_scheduled_website_ops(_run_request(app, "GET")))
        asyncio.run(
            run_scheduled_website_ops(
                _run_request(app, "POST", b'{"mode":"scheduled"}'),
            )
        )
        asyncio.run(
            run_scheduled_website_ops(
                _run_request(app, "POST", b'{"mode":"scheduled","force":true}'),
            )
        )

    assert triggers == ["vercel_cron", "render_cron", "internal_force"]
