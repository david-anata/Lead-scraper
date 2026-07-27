"""Regression coverage for Render-safe Agent startup and scheduling."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
import pytest

from sales_support_agent.models import database
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.services.job_lease import (
    claim_scheduled_job,
    ensure_job_lease_schema,
    finish_scheduled_job,
)
from sales_support_agent.services.website_ops_storage import (
    ensure_website_ops_storage_schema,
    restore_website_ops_root,
    snapshot_website_ops_root,
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
    assert "healthCheckPath: /health/ready" in blueprint
    assert "preDeployCommand: python scripts/predeploy_agent.py" in blueprint
    assert "autoDeployTrigger: checksPass" in blueprint
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
    assert blueprint.count(normalized_header) == 8


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
