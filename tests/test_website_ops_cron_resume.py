"""Cron persistence, chunking, and in-progress outcome coverage for Website Ops."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from sales_support_agent.api.website_ops_jobs_router import (
    WEBSITE_OPS_STEP_BUDGET_MAX_SECONDS,
    _run_due_modes,
    _website_ops_step_budget_seconds,
    router as website_ops_jobs_router,
)
from sales_support_agent.services import website_ops_vendor as website_ops
from sales_support_agent.services.website_ops import (
    get_website_ops_run_state,
    load_website_ops_run_state,
    run_website_ops,
    save_feedback_record,
)
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
            deadline=50,
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

    def execute(settings, config, record, raise_on_error=False):
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
            deadline=50,
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
