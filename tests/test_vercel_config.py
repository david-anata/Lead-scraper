import json
from pathlib import Path


def test_vercel_services_config_builds_the_fastapi_entrypoint() -> None:
    config = json.loads((Path(__file__).parents[1] / "vercel.json").read_text(encoding="utf-8"))

    assert "services" not in config
    service = config["experimentalServices"]["sales-support-agent"]
    assert service["entrypoint"] == "api/index.py"
    assert service["routePrefix"] == "/"
    assert service["framework"] == "fastapi"


def test_vercel_cron_routes_are_preserved() -> None:
    config = json.loads((Path(__file__).parents[1] / "vercel.json").read_text(encoding="utf-8"))
    paths = {entry["path"] for entry in config["crons"]}

    assert "/api/vercel-cron/content" in paths
    assert "/api/vercel-cron/website-ops" in paths
    assert "/api/vercel-cron/synthetic-health" in paths
