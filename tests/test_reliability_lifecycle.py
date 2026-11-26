"""Regression coverage for Render-safe Agent startup and scheduling."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import text

from sales_support_agent.models import database
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.services.job_lease import (
    claim_scheduled_job,
    ensure_job_lease_schema,
    expire_stale_scheduled_jobs,
    finish_scheduled_job,
)
from sales_support_agent.services.website_ops_storage import (
    ensure_website_ops_storage_schema,
    restore_website_ops_root,
    snapshot_website_ops_root,
    website_ops_cache_transaction,
    website_ops_storage_status,
)


@pytest.fixture(autouse=True)
def preserve_global_database_engine():
    """Reliability tests must not retarget the already-imported application."""

    original = database.engine
    yield
    replacement = database.engine
    if replacement is not None and replacement is not original:
        replacement.dispose()
    database.engine = original


def test_render_blueprint_uses_truthful_readiness_and_predeploy() -> None:
    blueprint = Path("render.yaml").read_text(encoding="utf-8")
    assert "plan: standard" in blueprint
    assert "healthCheckPath: /health/ready" in blueprint
    assert "preDeployCommand: python scripts/predeploy_agent.py" in blueprint
    assert "autoDeployTrigger: commit" in blueprint
    assert "AGENT_PREPARE_DATABASE_ON_STARTUP" in blueprint
    assert "WEBSITE_OPS_EMBEDDED_SCHEDULER" in blueprint
    assert "OUTBOUND_EMBEDDED_SCHEDULER" in blueprint


def test_render_crons_strip_accidental_whitespace_from_internal_key() -> None:
    blueprint = Path("render.yaml").read_text(encoding="utf-8")
    raw_header = 'X-Internal-Api-Key: $SALES_AGENT_INTERNAL_API_KEY'
    normalized_header = (
        'X-Internal-Api-Key: $(printf %s "$SALES_AGENT_INTERNAL_API_KEY")'
    )

    assert raw_header not in blueprint
    assert blueprint.count(normalized_header) == 12


def test_root_production_app_exposes_reliability_probes() -> None:
    import main as production_main

    paths = {
        route.path
        for route in production_main.app.routes
        if hasattr(route, "path")
    }
    assert {
        "/health",
        "/health/live",
        "/health/ready",
        "/health/storage",
    } <= paths


def test_production_mode_skips_schema_and_backfill_during_app_construction(
    tmp_path: Path,
) -> None:
    from sales_support_agent import main as agent_main

    env = {
        "SALES_AGENT_DB_URL": f"sqlite:///{tmp_path / 'startup.db'}",
        "AGENT_PREPARE_DATABASE_ON_STARTUP": "false",
    }
    with (
        patch.dict("os.environ", env, clear=False),
        patch.object(agent_main, "init_database") as init,
        patch.object(agent_main, "backfill_building_inquiry_assignments") as backfill,
    ):
        app = agent_main.create_app()
    init.assert_not_called()
    backfill.assert_not_called()
    app.state.session_factory.kw["bind"].dispose()


def test_liveness_and_readiness_have_distinct_failure_contracts(
    tmp_path: Path,
) -> None:
    from sales_support_agent import main as agent_main

    env = {
        "SALES_AGENT_DB_URL": f"sqlite:///{tmp_path / 'health.db'}",
        "AGENT_PREPARE_DATABASE_ON_STARTUP": "true",
        "WEBSITE_OPS_DATABASE_MIRROR": "false",
    }
    with patch.dict("os.environ", env, clear=False):
        app = agent_main.create_app()
    with TestClient(app) as client:
        live = client.get("/health/live")
        ready = client.get("/health/ready")
        assert live.status_code == 200
        assert live.json()["status"] == "live"
        assert ready.status_code == 200
        assert ready.json()["status"] == "ready"
        assert 'desc="0 queries"' in live.headers["server-timing"]
        assert 'desc="1 queries"' in ready.headers["server-timing"]

        real_factory = app.state.session_factory

        class BrokenFactory:
            def __call__(self):
                raise RuntimeError("database unavailable")

        app.state.session_factory = BrokenFactory()
        failed = client.get("/health/ready")
        app.state.session_factory = real_factory
        assert failed.status_code == 503
        assert failed.json()["reason"] == "database_unavailable"


def test_job_lease_allows_one_owner_and_recovers_failure(tmp_path: Path) -> None:
    factory = create_session_factory(f"sqlite:///{tmp_path / 'lease.db'}")
    init_database(factory)
    engine = factory.kw["bind"]
    ensure_job_lease_schema(engine)
    first = claim_scheduled_job(engine, job_key="daily", run_key="2026-07-27")
    assert first is not None
    assert claim_scheduled_job(
        engine,
        job_key="daily",
        run_key="2026-07-27",
    ) is None
    finish_scheduled_job(engine, first, status="failed", details={"reason": "test"})
    retry = claim_scheduled_job(engine, job_key="daily", run_key="2026-07-27")
    assert retry is not None
    finish_scheduled_job(engine, retry, status="succeeded")
    assert claim_scheduled_job(
        engine, job_key="daily", run_key="2026-07-27",
    ) is None
    recovered = claim_scheduled_job(
        engine, job_key="daily", run_key="2026-07-27", recover_succeeded=True,
    )
    assert recovered is not None
    engine.dispose()


def test_website_ops_disk_mirror_round_trip(tmp_path: Path) -> None:
    factory = create_session_factory(f"sqlite:///{tmp_path / 'mirror.db'}")
    engine = factory.kw["bind"]
    ensure_website_ops_storage_schema(engine)
    source = tmp_path / "source"
    source.mkdir()
    (source / "state").mkdir()
    (source / "state" / "run.json").write_text('{"status":"ready"}')
    stats = snapshot_website_ops_root(engine, source)
    assert stats["files"] == 1
    assert website_ops_storage_status(engine)["files"] == 1
    target = tmp_path / "target"
    restored = restore_website_ops_root(engine, target)
    assert restored["files"] == 1
    assert (target / "state" / "run.json").read_text() == '{"status":"ready"}'
    engine.dispose()


def test_runtime_cache_transaction_never_rehydrates_stale_database_state(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    root.mkdir()
    engine = object()
    with (
        patch(
            "sales_support_agent.services.website_ops_storage.restore_website_ops_root"
        ) as restore,
        patch(
            "sales_support_agent.services.website_ops_storage.snapshot_website_ops_root"
        ) as snapshot,
    ):
        with website_ops_cache_transaction(engine, root):
            (root / "run.json").write_text('{"status":"running"}')

    restore.assert_not_called()
    snapshot.assert_called_once_with(engine, root)


def _insert_lease(
    engine,
    *,
    job_key: str,
    run_key: str,
    status: str,
    lease_expires_at: str,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO scheduled_job_runs (
                    job_key, run_key, status, owner_token, started_at,
                    lease_expires_at, details_json
                ) VALUES (
                    :job_key, :run_key, :status, 'owner', :started_at,
                    :lease_expires_at, '{}'
                )
                """
            ),
            {
                "job_key": job_key,
                "run_key": run_key,
                "status": status,
                "started_at": "2026-11-26T12:00:00+00:00",
                "lease_expires_at": lease_expires_at,
            },
        )


def _lease_status(engine, run_key: str) -> str:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT status FROM scheduled_job_runs WHERE run_key = :run_key
                """
            ),
            {"run_key": run_key},
        ).one()
    return str(row[0])


def test_expire_stale_scheduled_jobs_fails_only_expired_running_rows(
    tmp_path: Path,
) -> None:
    factory = create_session_factory(f"sqlite:///{tmp_path / 'lease.db'}")
    engine = factory.kw["bind"]
    ensure_job_lease_schema(engine)
    now = datetime(2026, 11, 26, 18, 0, tzinfo=timezone.utc)
    past = (now - timedelta(minutes=5)).isoformat()
    future = (now + timedelta(minutes=30)).isoformat()
    _insert_lease(
        engine,
        job_key="website_ops",
        run_key="stale-running",
        status="running",
        lease_expires_at=past,
    )
    _insert_lease(
        engine,
        job_key="website_ops",
        run_key="live-running",
        status="running",
        lease_expires_at=future,
    )
    _insert_lease(
        engine,
        job_key="website_ops",
        run_key="boundary-running",
        status="running",
        lease_expires_at=now.isoformat(),
    )
    _insert_lease(
        engine,
        job_key="website_ops",
        run_key="already-succeeded",
        status="succeeded",
        lease_expires_at=past,
    )
    _insert_lease(
        engine,
        job_key="outbound_morning",
        run_key="other-job",
        status="running",
        lease_expires_at=past,
    )

    expired = expire_stale_scheduled_jobs(
        engine,
        now=now,
        job_key="website_ops",
    )

    assert expired == 1
    assert _lease_status(engine, "stale-running") == "failed"
    assert _lease_status(engine, "live-running") == "running"
    assert _lease_status(engine, "boundary-running") == "running"
    assert _lease_status(engine, "already-succeeded") == "succeeded"
    assert _lease_status(engine, "other-job") == "running"
    with engine.connect() as connection:
        details = connection.execute(
            text(
                """
                SELECT completed_at, details_json
                  FROM scheduled_job_runs
                 WHERE run_key = 'stale-running'
                """
            )
        ).one()
    assert details[0]
    assert "Stale running lease expired" in str(details[1])
    engine.dispose()


def test_snapshot_uploads_only_changed_files(tmp_path: Path) -> None:
    factory = create_session_factory(f"sqlite:///{tmp_path / 'mirror.db'}")
    engine = factory.kw["bind"]
    ensure_website_ops_storage_schema(engine)
    source = tmp_path / "source"
    (source / "state").mkdir(parents=True)
    target_file = source / "state" / "run.json"
    target_file.write_text('{"status":"ready"}', encoding="utf-8")
    first = snapshot_website_ops_root(engine, source)
    assert first["files"] == 1
    second = snapshot_website_ops_root(engine, source)
    assert second["files"] == 0
    assert second["unchanged"] == 1
    target_file.write_text('{"status":"changed"}', encoding="utf-8")
    third = snapshot_website_ops_root(engine, source)
    assert third["files"] == 1
    restored = restore_website_ops_root(engine, tmp_path / "target")
    assert restored["files"] == 1
    assert (tmp_path / "target" / "state" / "run.json").read_text(encoding="utf-8") == (
        '{"status":"changed"}'
    )
    engine.dispose()


def test_cold_start_restores_the_mirror_only_for_website_ops_routes(
    tmp_path: Path,
) -> None:
    from sales_support_agent.services import website_ops_storage as storage

    storage._RESTORED_CACHE_KEYS.clear()
    env = {
        "SALES_AGENT_DB_URL": f"sqlite:///{tmp_path / 'startup.db'}",
        "AGENT_PREPARE_DATABASE_ON_STARTUP": "false",
        "WEBSITE_OPS_DATABASE_MIRROR": "true",
    }
    from sales_support_agent import main as agent_main

    with (
        patch.dict("os.environ", env, clear=False),
        patch(
            "sales_support_agent.services.website_ops_storage.restore_website_ops_root",
            return_value={"files": 0, "bytes": 0, "skipped": 0},
        ) as restore,
        patch(
            "sales_support_agent.services.website_ops_storage.synchronize_website_ops_cache"
        ) as synchronize,
    ):
        app = agent_main.create_app()
        with TestClient(app) as client:
            live = client.get("/health/live")
            assert live.status_code == 200
            assert restore.call_count == 0
            assert synchronize.call_count == 0
            client.get("/api/jobs/website-ops/health")
            assert restore.call_count == 1
            client.get("/api/jobs/website-ops/health")
            assert restore.call_count == 1
    app.state.session_factory.kw["bind"].dispose()


def test_website_ops_render_cron_curl_stops_before_the_vercel_wall() -> None:
    import json
    import re

    blueprint = Path("render.yaml").read_text(encoding="utf-8")
    section = blueprint.split("name: website-ops-scheduler", 1)[1].split(
        "\n  - type:", 1
    )[0]
    match = re.search(r"--max-time\s+(\d+)", section)
    assert match is not None
    assert int(match.group(1)) < 800
    assert "name: website-ops-scheduler" in blueprint
    vercel = json.loads(Path("vercel.json").read_text(encoding="utf-8"))
    assert any(item["path"] == "/api/jobs/website-ops/run" for item in vercel["crons"])
