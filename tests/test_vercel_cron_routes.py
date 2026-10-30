import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from sales_support_agent.api.content_router import router as content_router
from sales_support_agent.api.website_ops_jobs_router import router as website_ops_router


def _client() -> TestClient:
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal-test-key")
    app.state.session_factory = object()
    app.include_router(content_router)
    app.include_router(website_ops_router)
    return TestClient(app)


def test_vercel_content_cron_uses_get_and_bearer_auth() -> None:
    with mock.patch.dict("os.environ", {"CRON_SECRET": "cron-test-key"}), mock.patch(
        "sales_support_agent.api.content_router.run_content_cycle",
        return_value={"status": "ok"},
    ) as run:
        response = _client().get(
            "/api/jobs/content/run",
            headers={"Authorization": "Bearer cron-test-key"},
        )

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert run.call_args.kwargs == {"mode": "scheduled", "force": False}


def test_vercel_website_ops_cron_accepts_get() -> None:
    response = _client().get("/api/jobs/website-ops/run")
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid internal API key."


def _cron_config() -> dict:
    return json.loads(
        (Path(__file__).parents[1] / "vercel.json").read_text(encoding="utf-8")
    )


def test_vercel_crons_target_registered_job_routes() -> None:
    config = _cron_config()
    paths = {item["path"] for item in config["crons"]}
    assert "/api/jobs/content/run" in paths
    assert "/api/jobs/website-ops/run" in paths
    assert "/api/vercel-cron/content" not in paths
    assert "/api/vercel-cron/website-ops" not in paths


def test_outbound_enrich_crons_are_single_hour_and_match_the_handler() -> None:
    """One cron per path, same shape as Abby. /run is the temporary recovery."""

    config = _cron_config()
    paths = [item["path"] for item in config["crons"]]
    assert len(paths) == len(set(paths))
    by_path = {item["path"]: item for item in config["crons"]}
    assert by_path["/api/jobs/outbound-enrich/at-1415"]["schedule"] == "15 14 * * 1-5"
    assert by_path["/api/jobs/outbound-enrich/at-1515"]["schedule"] == "15 15 * * 1-5"
    assert by_path["/api/jobs/outbound-enrich/at-1615"]["schedule"] == "15 16 * * 1-5"
    assert by_path["/api/jobs/outbound-enrich/at-1715"]["schedule"] == "15 17 * * 1-5"
    recovery = by_path["/api/jobs/outbound-enrich/run"]
    assert recovery["schedule"] == "30 20 * * 1-5"
    for item in config["crons"]:
        assert set(item) == {"path", "schedule"}
    assert by_path["/api/jobs/outbound-enrich/missed-run"]["schedule"] == "30 15 * * 1-5"
    assert by_path["/api/jobs/abby/run"]["schedule"] == "*/5 * * * *"
    assert by_path["/api/jobs/instantly/analytics-sync"]["schedule"] == "0 12 * * *"
    assert by_path["/api/jobs/outbound-campaign-score/at-1300"]["schedule"] == "0 13 * * 1"
    assert by_path["/api/jobs/outbound-campaign-score/at-1400"]["schedule"] == "0 14 * * 1"
    assert by_path["/api/jobs/abby/followups"]["schedule"] == "*/15 * * * *"
    assert by_path["/api/jobs/sales-call-prep/run"]["schedule"] == "*/15 * * * *"
    assert by_path["/api/jobs/instantly/register-webhook"]["schedule"] == "7 * * * *"
    # Hour lists on one path were listed by Vercel and never invoked.
    # The project cap is 100 crons on every plan (since 2026-01-20).
    for path, item in by_path.items():
        if path.startswith("/api/jobs/outbound-enrich/"):
            assert "," not in item["schedule"]
    assert len(config["crons"]) == 25
    assert len(config["crons"]) <= 100


def test_known_vercel_cron_paths_have_no_handler() -> None:
    """These cron paths 404. The real jobs live on other POST routes."""

    from fastapi.routing import iter_route_contexts

    from sales_support_agent.main import app

    registered = {ctx.path for ctx in iter_route_contexts(app.routes)}
    for path in (
        "/api/vercel-cron/durable-tasks",
        "/api/vercel-cron/gmail-sync",
        "/api/vercel-cron/building-operations",
    ):
        assert path not in registered
    assert "/api/jobs/outbound-enrich/run" in registered
    assert "/api/jobs/outbound-enrich/at-1415" in registered
    assert "/api/jobs/outbound-enrich/missed-run" in registered
    assert "/api/jobs/instantly/analytics-sync" in registered
    assert "/api/jobs/gmail-sync/run" in registered
    assert "/api/jobs/outbound-campaign-score/run" in registered
    assert "/api/jobs/outbound-campaign-score/at-1300" in registered
    assert "/api/jobs/outbound-campaign-score/at-1400" in registered
    assert "/api/jobs/sales-call-prep/run" in registered
