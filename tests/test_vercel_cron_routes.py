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


def test_vercel_crons_target_registered_job_routes() -> None:
    config = json.loads(
        (Path(__file__).parents[1] / "vercel.json").read_text(encoding="utf-8")
    )
    paths = {item["path"] for item in config["crons"]}
    assert "/api/jobs/content/run" in paths
    assert "/api/jobs/website-ops/run" in paths
    assert "/api/vercel-cron/content" not in paths
    assert "/api/vercel-cron/website-ops" not in paths
